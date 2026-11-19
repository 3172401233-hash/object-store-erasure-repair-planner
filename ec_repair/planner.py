"""纠删码对象修复规划器。

规划器只做"纸面"调度：依据传入的分片状态模拟每轮重建，计算带宽占用
和源节点读取负载，不会真正读取磁盘或访问网络。

建模约定（本地模拟）：

* 单分片大小 = ``ceil(对象大小 / k)``，对象大小按 k 个数据分片均摊。
* 重建一个缺失分片需要读取 k 个有效源分片并写出 1 个新分片，
  因此单任务带宽为 ``(k + 1) * 单分片大小``。
* 只有初始状态下可用且校验通过的分片能作为修复源；校验失败
  （``CORRUPT`` 或摘要不匹配）的分片一律排除，且本身也需要重建。
* 同一对象的分片不共置节点；每个源节点每轮最多承担
  ``max_node_streams`` 个分片读取，防止批量故障时健康节点被打满。
"""

from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .models import (
    ECObject,
    ObjectStatus,
    RepairPlan,
    RepairRound,
    RepairTask,
    Shard,
    SkipReason,
    SkipRecord,
)


class RepairPlanner:
    """按优先级和容量约束生成分片修复计划。"""

    def __init__(
        self,
        max_bandwidth_bytes: int,
        max_node_streams: int,
        risk_weight: float = 0.7,
        heat_weight: float = 0.3,
    ) -> None:
        if max_bandwidth_bytes <= 0:
            raise ValueError("每轮可用修复带宽必须为正数")
        if max_node_streams <= 0:
            raise ValueError("每个源节点每轮读取流上限必须为正数")
        if abs((risk_weight + heat_weight) - 1.0) > 1e-9:
            raise ValueError("risk_weight 与 heat_weight 之和必须为 1")
        self.max_bandwidth_bytes = max_bandwidth_bytes
        self.max_node_streams = max_node_streams
        self.risk_weight = risk_weight
        self.heat_weight = heat_weight

    def plan(self, objects: Sequence[ECObject]) -> RepairPlan:
        """对一批对象生成完整修复规划。"""
        ids = [obj.object_id for obj in objects]
        if len(ids) != len(set(ids)):
            raise ValueError("对象 ID 必须唯一，发现重复: %s" % ids)
        for obj in objects:
            obj.validate()

        statuses = {obj.object_id: self._analyze(obj, objects) for obj in objects}

        ordered_objects = sorted(
            objects,
            key=lambda obj: (
                -statuses[obj.object_id].priority,
                -obj.heat,
                -obj.size_bytes,
                obj.object_id,
            ),
        )

        # 每个对象待修复分片：(分片编号 -> 重建目标节点)，目标节点沿用原位置。
        pending = {
            obj.object_id: {
                shard.index: shard.node_id
                for shard in obj.missing_shards()
            }
            for obj in objects
            if statuses[obj.object_id].recoverable
        }
        sources = {
            obj.object_id: obj.valid_shards()
            for obj in objects
        }

        plan = RepairPlan(object_status=statuses)
        plan.unrecoverable = [
            statuses[obj.object_id]
            for obj in ordered_objects
            if not statuses[obj.object_id].recoverable
        ]

        total_node_load = defaultdict(int)  # type: Dict[str, int]
        skip_reasons = {}  # type: Dict[Tuple[str, int], SkipReason]
        round_index = 0

        # 不可恢复对象无法安排任何修复，显式登记跳过原因。
        for obj in ordered_objects:
            if not statuses[obj.object_id].recoverable:
                for shard in obj.missing_shards():
                    key = (obj.object_id, shard.index)
                    skip_reasons[key] = SkipReason.INSUFFICIENT_SOURCES
                    plan.final_skipped.append(
                        SkipRecord(
                            obj.object_id,
                            shard.index,
                            SkipReason.INSUFFICIENT_SOURCES,
                        )
                    )

        while any(pending.values()):
            round_index += 1
            round_obj = RepairRound(round_index=round_index)
            node_load = defaultdict(int)  # type: Dict[str, int]
            remaining_bandwidth = self.max_bandwidth_bytes

            for obj in ordered_objects:
                missing_indexes = sorted(pending.get(obj.object_id, {}).keys())
                if not missing_indexes:
                    continue

                status = statuses[obj.object_id]
                source_shards = sources[obj.object_id]

                for shard_index in missing_indexes:
                    selected = self._select_sources(obj.k, source_shards, node_load)
                    if selected is None:
                        round_obj.skipped.append(
                            SkipRecord(
                                obj.object_id,
                                shard_index,
                                SkipReason.NODE_OVERLOADED,
                            )
                        )
                        # 该对象本轮受节点流上限阻塞，剩余分片留给下一轮，
                        # 但继续尝试优先级更低的其他对象。
                        for deferred in missing_indexes:
                            if deferred != shard_index:
                                round_obj.skipped.append(
                                    SkipRecord(
                                        obj.object_id,
                                        deferred,
                                        SkipReason.NODE_OVERLOADED,
                                    )
                                )
                        break

                    cost = (obj.k + 1) * obj.shard_size
                    if cost > remaining_bandwidth:
                        round_obj.skipped.append(
                            SkipRecord(
                                obj.object_id,
                                shard_index,
                                SkipReason.BANDWIDTH_EXHAUSTED,
                            )
                        )
                        # 本轮带宽不足：当前对象剩余分片与后续对象一并推迟，
                        # 由下一轮重新调度，避免在同一轮里反复空转。
                        for deferred in missing_indexes:
                            if deferred != shard_index:
                                round_obj.skipped.append(
                                    SkipRecord(
                                        obj.object_id,
                                        deferred,
                                        SkipReason.BANDWIDTH_EXHAUSTED,
                                    )
                                )
                        break

                    target_node = pending[obj.object_id].pop(shard_index)
                    source_nodes = tuple(shard.node_id for shard in selected)
                    for source in selected:
                        node_load[source.node_id] += 1
                        total_node_load[source.node_id] += 1
                    remaining_bandwidth -= cost

                    task = RepairTask(
                        object_id=obj.object_id,
                        shard_index=shard_index,
                        target_node_id=target_node,
                        source_node_ids=source_nodes,
                        bandwidth_bytes=cost,
                        priority=status.priority,
                    )
                    round_obj.tasks.append(task)
                    plan.repair_order.append((obj.object_id, shard_index))

            round_obj.bandwidth_bytes = self.max_bandwidth_bytes - remaining_bandwidth
            round_obj.node_load = dict(node_load)
            for record in round_obj.skipped:
                skip_reasons[(record.object_id, record.shard_index)] = record.reason

            if not round_obj.tasks:
                # 没有任何进展：剩余分片受节点流上限约束持续无法调度。
                already_skipped = {
                    (record.object_id, record.shard_index)
                    for record in plan.final_skipped
                }
                for record in round_obj.skipped:
                    key = (record.object_id, record.shard_index)
                    if key not in already_skipped:
                        already_skipped.add(key)
                        plan.final_skipped.append(record)
                break

            plan.rounds.append(round_obj)

        # 调度结束后仍残留的待修复分片统一记录跳过原因。
        already_skipped = {
            (record.object_id, record.shard_index)
            for record in plan.final_skipped
        }
        for obj in ordered_objects:
            for shard_index in sorted(pending.get(obj.object_id, {}).keys()):
                key = (obj.object_id, shard_index)
                if key not in already_skipped:
                    already_skipped.add(key)
                    plan.final_skipped.append(
                        SkipRecord(
                            obj.object_id,
                            shard_index,
                            skip_reasons.get(key, SkipReason.BANDWIDTH_EXHAUSTED),
                        )
                    )

        plan.total_bandwidth_bytes = sum(
            round_obj.bandwidth_bytes for round_obj in plan.rounds
        )
        plan.final_node_load = dict(sorted(total_node_load.items()))
        return plan

    def _select_sources(
        self,
        k: int,
        source_shards: Sequence[Shard],
        node_load: Dict[str, int],
    ) -> Optional[List[Shard]]:
        """选出 k 个有效源分片，并保证不突破节点每轮读取流上限。

        优先选择当前轮负载最低的节点，把读取压力均匀分散到健康节点。
        分片不共置，因此每个节点至多贡献一个分片。
        """
        candidates = sorted(
            source_shards,
            key=lambda shard: (node_load.get(shard.node_id, 0), shard.index),
        )
        selected = []  # type: List[Shard]
        for shard in candidates:
            if node_load.get(shard.node_id, 0) >= self.max_node_streams:
                continue
            selected.append(shard)
            if len(selected) == k:
                return selected
        return None

    def _analyze(
        self,
        obj: ECObject,
        all_objects: Iterable[ECObject],
    ) -> ObjectStatus:
        """计算对象初始可读性、缺失数量与修复优先级。"""
        valid = obj.valid_shards()
        missing = obj.missing_shards()
        corrupt_count = sum(
            1 for shard in missing if not shard.checksum_valid
        )
        valid_count = len(valid)
        missing_count = len(missing)
        recoverable = valid_count >= obj.k

        # 风险分：已消耗的冗余比例，越接近容错阈值 m 越危险；无校验分片时
        # 任何缺失都意味着不可恢复，风险直接取 1。
        if obj.m == 0:
            risk_score = 1.0 if missing_count else 0.0
        else:
            risk_score = min(1.0, missing_count / obj.m)

        max_heat = max((item.heat for item in all_objects), default=0.0)
        heat_score = obj.heat / max_heat if max_heat > 0 else 0.0
        priority = (
            self.risk_weight * risk_score + self.heat_weight * heat_score
        )

        reason = None
        if not recoverable:
            reason = (
                "有效分片 %d 个，少于恢复所需的 k=%d（缺失 %d 个，"
                "其中校验失败 %d 个，校验失败分片禁止作为修复源）"
                % (valid_count, obj.k, missing_count, corrupt_count)
            )

        return ObjectStatus(
            object_id=obj.object_id,
            readable=recoverable,
            recoverable=recoverable,
            missing_count=missing_count,
            valid_count=valid_count,
            corrupt_count=corrupt_count,
            risk_score=risk_score,
            heat_score=heat_score,
            priority=priority,
            unrecoverable_reason=reason,
        )
