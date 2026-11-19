"""本地模拟运行示例：打印修复顺序、每轮带宽和源节点负载。

运行方式：python demo.py
"""

from ec_repair import (
    ECObject,
    RepairPlanner,
    Shard,
    ShardState,
    SkipReason,
)


def make_object(object_id, k, m, size, missing, corrupt=(), heat=0.0):
    shards = []
    for index in range(k + m):
        node_id = "%s-node-%d" % (object_id, index)
        if index in missing:
            state, checksum = ShardState.MISSING, None
        elif index in corrupt:
            state, checksum = ShardState.CORRUPT, "bad"
        else:
            state, checksum = ShardState.AVAILABLE, "good-%d" % index
        shards.append(
            Shard(index, node_id, state, checksum, "good-%d" % index)
        )
    return ECObject(object_id, k, m, size, shards,
                    checksum="sum-%s" % object_id, heat=heat)


def main():
    objects = [
        # 热点对象：丢 1 个分片，访问热度高。
        make_object("hot", 3, 2, 300, missing={4}, heat=90.0),
        # 高风险对象：冗余全部耗尽，恰好还能恢复。
        make_object("risky", 3, 2, 300, missing={3, 4}, heat=5.0),
        # 含校验失败分片：损坏分片不能作为修复源。
        make_object("crc", 3, 2, 300, missing=set(), corrupt={2}, heat=2.0),
        # 超过容错阈值：有效分片少于 k，不可恢复。
        make_object("dead", 3, 2, 300, missing={2, 3, 4}, heat=50.0),
    ]

    planner = RepairPlanner(max_bandwidth_bytes=600, max_node_streams=2)
    plan = planner.plan(objects)

    print("== 对象状态 ==")
    for status in plan.object_status.values():
        print(
            "%s: readable=%s missing=%d valid=%d priority=%.3f"
            % (
                status.object_id,
                status.readable,
                status.missing_count,
                status.valid_count,
                status.priority,
            )
        )

    print("\n== 修复顺序 ==")
    for object_id, shard_index in plan.repair_order:
        print("对象 %s 修复分片 %d" % (object_id, shard_index))

    print("\n== 每轮调度 ==")
    for round_obj in plan.rounds:
        print(
            "第 %d 轮: 任务 %d 个, 带宽 %d 字节, 节点负载 %s"
            % (
                round_obj.round_index,
                len(round_obj.tasks),
                round_obj.bandwidth_bytes,
                round_obj.node_load,
            )
        )

    print("\n== 仍不可恢复对象 ==")
    for status in plan.unrecoverable:
        print("%s: %s" % (status.object_id, status.unrecoverable_reason))

    print("\n== 跳过记录 ==")
    for record in plan.final_skipped:
        print(
            "对象 %s 分片 %d: %s"
            % (record.object_id, record.shard_index, record.reason.value)
        )


if __name__ == "__main__":
    main()
