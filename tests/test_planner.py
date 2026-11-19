import unittest

from ec_repair import (
    ECObject,
    RepairPlanner,
    Shard,
    ShardState,
    SkipReason,
)


def build_shards(k, m, missing=(), corrupt=(), prefix="node"):
    """构造一组模拟分片：missing/corrupt 中的编号分别置为丢失/校验失败。"""
    shards = []
    for index in range(k + m):
        node_id = "%s-%d" % (prefix, index)
        if index in missing:
            shards.append(
                Shard(index, node_id, ShardState.MISSING, None, "good-%d" % index)
            )
        elif index in corrupt:
            shards.append(
                Shard(index, node_id, ShardState.CORRUPT, "bad-%d" % index,
                      "good-%d" % index)
            )
        else:
            shards.append(
                Shard(index, node_id, ShardState.AVAILABLE,
                      "good-%d" % index, "good-%d" % index)
            )
    return shards


def make_object(object_id, k, m, size, missing=(), corrupt=(), heat=0.0):
    return ECObject(
        object_id=object_id,
        k=k,
        m=m,
        size_bytes=size,
        shards=build_shards(k, m, missing, corrupt, prefix=object_id),
        checksum="checksum-%s" % object_id,
        heat=heat,
    )


class TestExactlyRecoverable(unittest.TestCase):
    def test_k_valid_shards_repairs_all_missing(self):
        obj = make_object("obj-a", k=3, m=2, size=300, missing=(3, 4))
        planner = RepairPlanner(max_bandwidth_bytes=1000, max_node_streams=10)

        plan = planner.plan([obj])
        status = plan.object_status["obj-a"]

        self.assertTrue(status.readable)
        self.assertTrue(status.recoverable)
        self.assertEqual(status.missing_count, 2)
        self.assertEqual(status.valid_count, 3)
        self.assertEqual(len(plan.rounds), 1)
        self.assertEqual(plan.repair_order, [("obj-a", 3), ("obj-a", 4)])
        # 单分片 100 字节，单任务读取 3 个源 + 写 1 个分片 = 400 字节。
        self.assertEqual(plan.rounds[0].bandwidth_bytes, 800)
        self.assertEqual(plan.total_bandwidth_bytes, 800)
        # 每个健康源节点在两个任务中各被读取一次。
        self.assertEqual(
            plan.final_node_load,
            {"obj-a-0": 2, "obj-a-1": 2, "obj-a-2": 2},
        )
        self.assertEqual(plan.final_skipped, [])


class TestBeyondTolerance(unittest.TestCase):
    def test_more_than_m_loss_is_unrecoverable(self):
        # k=3,m=2 最多容忍 2 个分片失效，丢失 3 个即超过容错阈值。
        obj = make_object("obj-dead", k=3, m=2, size=300, missing=(2, 3, 4))
        planner = RepairPlanner(max_bandwidth_bytes=1000, max_node_streams=10)

        plan = planner.plan([obj])
        status = plan.object_status["obj-dead"]

        self.assertFalse(status.readable)
        self.assertFalse(status.recoverable)
        self.assertEqual(status.missing_count, 3)
        self.assertEqual(status.valid_count, 2)
        self.assertEqual(len(plan.unrecoverable), 1)
        self.assertIn("k=3", status.unrecoverable_reason)
        self.assertEqual(plan.rounds, [])
        self.assertEqual(plan.repair_order, [])
        reasons = {(record.object_id, record.shard_index): record.reason
                   for record in plan.final_skipped}
        self.assertEqual(
            reasons[("obj-dead", 2)], SkipReason.INSUFFICIENT_SOURCES
        )
        self.assertEqual(
            reasons[("obj-dead", 3)], SkipReason.INSUFFICIENT_SOURCES
        )


class TestHotObject(unittest.TestCase):
    def test_hot_object_first_when_risk_equal(self):
        cold = make_object("obj-cold", k=3, m=2, size=300, missing=(4,), heat=1.0)
        hot = make_object("obj-hot", k=3, m=2, size=300, missing=(4,), heat=100.0)
        planner = RepairPlanner(max_bandwidth_bytes=1000, max_node_streams=10)

        plan = planner.plan([cold, hot])

        self.assertEqual(plan.object_order, ["obj-hot", "obj-cold"])
        self.assertGreater(
            plan.object_status["obj-hot"].priority,
            plan.object_status["obj-cold"].priority,
        )

    def test_unrecoverable_risk_outranks_heat(self):
        # 冷对象已耗尽全部冗余（风险 1.0），热对象只丢一个分片（风险 0.5）。
        cold_risky = make_object(
            "obj-cold-risky", k=3, m=2, size=300, missing=(3, 4), heat=1.0
        )
        hot_safe = make_object(
            "obj-hot-safe", k=3, m=2, size=300, missing=(4,), heat=100.0
        )
        planner = RepairPlanner(max_bandwidth_bytes=1000, max_node_streams=10)

        plan = planner.plan([hot_safe, cold_risky])

        self.assertEqual(
            plan.object_order, ["obj-cold-risky", "obj-hot-safe"]
        )


class TestSourceOverload(unittest.TestCase):
    def test_node_stream_limit_defers_shards_to_next_round(self):
        obj = make_object("obj-load", k=3, m=2, size=300, missing=(3, 4))
        # 每个健康节点每轮最多贡献 1 个读取流，第二个重建任务只能等下一轮。
        planner = RepairPlanner(max_bandwidth_bytes=1000, max_node_streams=1)

        plan = planner.plan([obj])

        self.assertEqual(len(plan.rounds), 2)
        first_round = plan.rounds[0]
        self.assertEqual(len(first_round.tasks), 1)
        self.assertEqual(
            first_round.skipped[0].reason, SkipReason.NODE_OVERLOADED
        )
        self.assertEqual(
            first_round.skipped[0].shard_index, 4
        )
        # 每个节点单轮负载都不允许超过流上限。
        for round_obj in plan.rounds:
            self.assertTrue(
                all(load <= 1 for load in round_obj.node_load.values())
            )
        self.assertEqual(
            plan.repair_order, [("obj-load", 3), ("obj-load", 4)]
        )
        self.assertEqual(plan.final_skipped, [])


class TestChecksumFailure(unittest.TestCase):
    def test_corrupt_shard_excluded_from_sources_but_rebuilt(self):
        obj = make_object("obj-crc", k=3, m=2, size=300, corrupt=(3,))
        planner = RepairPlanner(max_bandwidth_bytes=1000, max_node_streams=10)

        plan = planner.plan([obj])
        status = plan.object_status["obj-crc"]

        self.assertTrue(status.recoverable)
        self.assertEqual(status.corrupt_count, 1)
        self.assertEqual(status.missing_count, 1)
        self.assertEqual(plan.repair_order, [("obj-crc", 3)])
        for task in plan.rounds[0].tasks:
            self.assertNotIn("obj-crc-3", task.source_node_ids)
            self.assertEqual(len(task.source_node_ids), 3)

    def test_corrupt_shards_count_toward_failure_threshold(self):
        obj = make_object(
            "obj-crc-dead", k=3, m=2, size=300,
            missing=(3, 4), corrupt=(2,),
        )
        planner = RepairPlanner(max_bandwidth_bytes=1000, max_node_streams=10)

        plan = planner.plan([obj])
        status = plan.object_status["obj-crc-dead"]

        self.assertFalse(status.recoverable)
        self.assertEqual(status.valid_count, 2)
        self.assertEqual(status.corrupt_count, 1)
        self.assertIn("校验失败", status.unrecoverable_reason)
        self.assertEqual(plan.repair_order, [])


class TestBatchRepair(unittest.TestCase):
    def test_bandwidth_bounds_rounds_and_no_duplicate_repair(self):
        objects = [
            make_object("obj-%d" % i, k=2, m=2, size=100, missing=(2, 3))
            for i in range(3)
        ]
        # 单分片 50 字节，单任务成本 (2+1)*50=150；每轮带宽 300 恰好放 2 个任务。
        planner = RepairPlanner(max_bandwidth_bytes=300, max_node_streams=10)

        plan = planner.plan(objects)

        self.assertEqual(len(plan.rounds), 3)
        for round_obj in plan.rounds:
            self.assertEqual(len(round_obj.tasks), 2)
            self.assertLessEqual(
                round_obj.bandwidth_bytes, planner.max_bandwidth_bytes
            )
            self.assertEqual(round_obj.bandwidth_bytes, 300)
        expected_order = [
            ("obj-0", 2), ("obj-0", 3),
            ("obj-1", 2), ("obj-1", 3),
            ("obj-2", 2), ("obj-2", 3),
        ]
        self.assertEqual(plan.repair_order, expected_order)
        # 同一分片不会被重复修复。
        self.assertEqual(
            len(plan.repair_order), len(set(plan.repair_order))
        )
        self.assertEqual(plan.total_bandwidth_bytes, 900)
        self.assertEqual(plan.final_skipped, [])


class TestHealthyAndValidation(unittest.TestCase):
    def test_healthy_object_needs_no_repair(self):
        obj = make_object("obj-ok", k=3, m=2, size=300)
        planner = RepairPlanner(max_bandwidth_bytes=1000, max_node_streams=10)

        plan = planner.plan([obj])
        status = plan.object_status["obj-ok"]

        self.assertTrue(status.readable)
        self.assertEqual(status.missing_count, 0)
        self.assertEqual(status.priority, 0.0)
        self.assertEqual(plan.rounds, [])
        self.assertEqual(plan.repair_order, [])

    def test_invalid_config_raises(self):
        with self.assertRaises(ValueError):
            RepairPlanner(max_bandwidth_bytes=0, max_node_streams=2)
        with self.assertRaises(ValueError):
            RepairPlanner(max_bandwidth_bytes=100, max_node_streams=0)
        with self.assertRaises(ValueError):
            RepairPlanner(
                max_bandwidth_bytes=100, max_node_streams=2,
                risk_weight=0.5, heat_weight=0.4,
            )
        bad_object = make_object("obj-bad", k=3, m=2, size=300, missing=(4,))
        bad_object.shards.pop()
        with self.assertRaises(ValueError):
            RepairPlanner(100, 2).plan([bad_object])

    def test_duplicate_object_id_raises(self):
        first = make_object("dup", k=3, m=2, size=300, missing=(4,))
        second = make_object("dup", k=3, m=2, size=300, missing=(4,))
        with self.assertRaises(ValueError):
            RepairPlanner(1000, 10).plan([first, second])


if __name__ == "__main__":
    unittest.main()
