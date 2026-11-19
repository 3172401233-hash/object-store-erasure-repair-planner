"""规划器使用的数据模型。

所有分片位置（``node_id``）都只是模拟的节点标识，规划器不会发起任何
真实的磁盘或网络访问。
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


class ShardState(Enum):
    """分片的可用状态。"""

    AVAILABLE = "available"          # 可用且校验通过，可作为修复源
    MISSING = "missing"              # 所在磁盘故障，分片丢失
    CORRUPT = "corrupt"              # 可读但校验摘要不匹配，禁止作为修复源


class SkipReason(Enum):
    """修复任务无法被调度或对象不可恢复的原因。"""

    INSUFFICIENT_SOURCES = "insufficient_sources"
    BANDWIDTH_EXHAUSTED = "bandwidth_exhausted"
    NODE_OVERLOADED = "node_overloaded"


@dataclass(frozen=True)
class Shard:
    """单个纠删码分片。

    :param index: 分片编号，范围为 ``0 .. k+m-1``。
    :param node_id: 分片所在存储节点（模拟标识）。
    :param state: 分片可用状态。
    :param checksum: 分片校验摘要；与 ``expected_checksum`` 不一致即为损坏。
    :param expected_checksum: 期望的校验摘要。
    """

    index: int
    node_id: str
    state: ShardState
    checksum: Optional[str] = None
    expected_checksum: Optional[str] = None

    @property
    def checksum_valid(self) -> bool:
        """校验是否通过。没有登记摘要时按通过处理。"""
        if self.state == ShardState.CORRUPT:
            return False
        if self.checksum is None or self.expected_checksum is None:
            return True
        return self.checksum == self.expected_checksum

    @property
    def is_valid_source(self) -> bool:
        """是否可作为重建读取的源分片：可用且校验通过。"""
        return self.state == ShardState.AVAILABLE and self.checksum_valid


@dataclass
class ECObject:
    """一个纠删码对象。"""

    object_id: str
    k: int
    m: int
    size_bytes: int
    shards: List[Shard]
    checksum: Optional[str] = None
    heat: float = 0.0

    @property
    def shard_size(self) -> int:
        """单个分片的模拟大小，对象大小按 ``k`` 个数据分片均摊。"""
        if self.k <= 0:
            raise ValueError("k 必须为正整数")
        return (self.size_bytes + self.k - 1) // self.k

    def valid_shards(self) -> List[Shard]:
        """返回所有可作为修复源的有效分片。"""
        return [shard for shard in self.shards if shard.is_valid_source]

    def missing_shards(self) -> List[Shard]:
        """返回所有需要修复的分片（丢失或损坏）。"""
        return [
            shard
            for shard in self.shards
            if shard.state != ShardState.AVAILABLE or not shard.checksum_valid
        ]

    def validate(self) -> None:
        """校验对象配置是否自洽。"""
        if self.k <= 0 or self.m < 0:
            raise ValueError("k 必须为正整数且 m 不能为负: %s" % self.object_id)
        if len(self.shards) != self.k + self.m:
            raise ValueError(
                "对象 %s 的分片数 %d 与 k+m=%d 不一致"
                % (self.object_id, len(self.shards), self.k + self.m)
            )
        indexes = sorted(shard.index for shard in self.shards)
        if indexes != list(range(self.k + self.m)):
            raise ValueError("对象 %s 的分片编号必须覆盖 0..k+m-1" % self.object_id)
        if len({shard.node_id for shard in self.shards}) != len(self.shards):
            raise ValueError("对象 %s 的分片不能共置在同一节点上" % self.object_id)
        if self.size_bytes < 0:
            raise ValueError("对象 %s 的大小不能为负" % self.object_id)
        if self.heat < 0:
            raise ValueError("对象 %s 的访问热度不能为负" % self.object_id)


@dataclass
class ObjectStatus:
    """单个对象的可读性与风险分析结果。"""

    object_id: str
    readable: bool
    recoverable: bool
    missing_count: int
    valid_count: int
    corrupt_count: int
    risk_score: float
    heat_score: float
    priority: float
    unrecoverable_reason: Optional[str] = None


@dataclass(frozen=True)
class RepairTask:
    """一次单分片重建任务。"""

    object_id: str
    shard_index: int
    target_node_id: str
    source_node_ids: Tuple[str, ...]
    bandwidth_bytes: int
    priority: float


@dataclass(frozen=True)
class SkipRecord:
    """某轮中被跳过的分片及其原因。"""

    object_id: str
    shard_index: int
    reason: SkipReason


@dataclass
class RepairRound:
    """一轮并发修复的调度结果。"""

    round_index: int
    tasks: List[RepairTask] = field(default_factory=list)
    bandwidth_bytes: int = 0
    node_load: Dict[str, int] = field(default_factory=dict)
    skipped: List[SkipRecord] = field(default_factory=list)


@dataclass
class RepairPlan:
    """完整修复规划输出。"""

    rounds: List[RepairRound] = field(default_factory=list)
    object_status: Dict[str, ObjectStatus] = field(default_factory=dict)
    unrecoverable: List[ObjectStatus] = field(default_factory=list)
    repair_order: List[Tuple[str, int]] = field(default_factory=list)
    total_bandwidth_bytes: int = 0
    final_node_load: Dict[str, int] = field(default_factory=dict)
    final_skipped: List[SkipRecord] = field(default_factory=list)

    @property
    def object_order(self) -> List[str]:
        """按首次进入修复的顺序排列的对象 ID。"""
        ordered = []  # type: List[str]
        for object_id, _ in self.repair_order:
            if object_id not in ordered:
                ordered.append(object_id)
        return ordered
