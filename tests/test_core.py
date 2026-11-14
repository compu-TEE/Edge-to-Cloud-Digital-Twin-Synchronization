"""Unit tests for edge/core.py, cloud/core.py and the shared conflict rule.

Run:  python -m unittest discover -s tests -v
"""
import itertools
import os
import random
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "edge"))
import conflict  # noqa: E402
import core as edge  # noqa: E402

# cloud/core.py has the same module name: load it under another name.
import importlib.util  # noqa: E402
spec = importlib.util.spec_from_file_location(
    "cloud_core", os.path.join(ROOT, "cloud", "core.py"))
cloud = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cloud)

T0 = datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc)


def ev(i, twin="t1", prop="p", value=None, secs=0, eid=None):
    return {"event_id": eid or f"e{i:06d}", "twin_id": twin, "property": prop,
            "value": i if value is None else value,
            "event_time": (T0 + timedelta(seconds=secs)).isoformat()}


class Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.edge = edge.connect(os.path.join(self.dir.name, "edge.db"))
        self.cloud = cloud.connect(os.path.join(self.dir.name, "cloud.db"))

    def tearDown(self):
        self.edge.close()
        self.cloud.close()
        self.dir.cleanup()


class TestConflictRule(unittest.TestCase):
    def test_copies_identical(self):
        with open(os.path.join(ROOT, "edge", "conflict.py")) as fa, \
                open(os.path.join(ROOT, "cloud", "conflict.py")) as fb:
            self.assertEqual(fa.read(), fb.read())

    def test_order_and_tiebreak(self):
        self.assertTrue(conflict.wins("2026-01-01T00:00:02Z", "a",
                                      "2026-01-01T00:00:01Z", "z"))
        self.assertFalse(conflict.wins("2026-01-01T00:00:01Z", "z",
                                       "2026-01-01T00:00:02Z", "a"))
        # equal instants in different zone notations -> id decides
        self.assertTrue(conflict.wins("2026-01-01T01:00:00+01:00", "b",
                                      "2026-01-01T00:00:00Z", "a"))
        self.assertFalse(conflict.wins("2026-01-01T00:00:00Z", "a",
                                       "2026-01-01T00:00:00Z", "a"))

    def test_permutation_invariance(self):
        """Any delivery order of the same events yields the same winner."""
        events = [ev(i, secs=s, eid=f"x{i}") for i, s in
                  enumerate([5, 3, 9, 9, 1, 7])]
        finals = set()
        for perm in itertools.permutations(events):
            cur = (None, None)
            for e in perm:
                if conflict.wins(e["event_time"], e["event_id"], *cur):
                    cur = (e["event_time"], e["event_id"])
            finals.add(cur)
        self.assertEqual(len(finals), 1)


class TestEdgeCore(Base):
    def test_stale_event_does_not_replace_value(self):
        edge.apply_event(self.edge, ev(1, value=88.0, secs=60), 100)
        r = edge.apply_event(self.edge, ev(2, value=22.0, secs=0), 100)
        self.assertEqual(r["outcome"], "stale")
        self.assertEqual(edge.twin_state(self.edge)["t1"]["p"], 88.0)
        # stale event is still recorded and queued (history is complete)
        self.assertEqual(edge.metrics(self.edge)["events_total"], 2)

    def test_duplicate_ignored(self):
        e = ev(1)
        edge.apply_event(self.edge, e, 100)
        self.assertEqual(edge.apply_event(self.edge, e, 100)["outcome"], "duplicate")
        self.assertEqual(edge.metrics(self.edge)["events_total"], 1)

    def test_bounded_outbox_evicts_oldest_but_protects_latest(self):
        cap, props = 10, [f"p{i}" for i in range(4)]
        n = 200
        for i in range(n):
            edge.apply_event(self.edge, ev(i, prop=props[i % 4], secs=i), cap)
            self.assertLessEqual(edge.unacked_count(self.edge), cap)
        m = edge.metrics(self.edge)
        self.assertEqual(m["dropped_events"], 0)
        self.assertEqual(m["evicted_events"], n - cap)
        # current state is the newest value of every property
        st = edge.twin_state(self.edge)["t1"]
        for k, p in enumerate(props):
            last = max(i for i in range(n) if i % 4 == k)
            self.assertEqual(st[p], last)
        # the newest events survive in the queue (oldest were evicted)
        queued = {r[0] for r in self.edge.execute(
            "SELECT event_id FROM outbox WHERE status='pending'")}
        self.assertEqual(queued, {f"e{i:06d}" for i in range(n - cap, n)})

    def test_reject_is_atomic_when_nothing_evictable(self):
        cap = 2
        edge.apply_event(self.edge, ev(0, prop="a", secs=0), cap)
        edge.apply_event(self.edge, ev(1, prop="b", secs=1), cap)
        before = edge.twin_state(self.edge)
        r = edge.apply_event(self.edge, ev(2, prop="c", secs=2), cap)
        self.assertEqual(r["outcome"], "dropped")
        self.assertEqual(edge.twin_state(self.edge), before)  # no divergence
        self.assertEqual(edge.metrics(self.edge)["dropped_events"], 1)
        self.assertIsNone(self.edge.execute(
            "SELECT 1 FROM property_versions WHERE property='c'").fetchone())

    def test_acked_rows_free_capacity_and_prune_bounds_disk(self):
        for i in range(50):
            edge.apply_event(self.edge, ev(i, prop=f"p{i%3}", secs=i), 20)
        ids = [r[0] for r in self.edge.execute(
            "SELECT event_id FROM outbox WHERE status='pending'")]
        edge.mark_acked(self.edge, ids)
        self.assertEqual(edge.unacked_count(self.edge), 0)
        edge.prune(self.edge, keep=5)
        left = self.edge.execute(
            "SELECT COUNT(*) FROM outbox WHERE status IN ('acked','evicted')"
        ).fetchone()[0]
        self.assertEqual(left, 5)

    def test_cloud_origin_not_queued(self):
        e = ev(1, eid="cloud-1")
        r = edge.apply_event(self.edge, e, 1, origin="cloud")
        self.assertEqual(r["outcome"], "applied")
        self.assertEqual(edge.unacked_count(self.edge), 0)


class TestBidirectionalConflict(Base):
    def sync(self):
        """Deliver everything both ways (what Kafka does after reconnect)."""
        rows = self.edge.execute(
            "SELECT event_id, payload_json FROM outbox WHERE status='pending'"
        ).fetchall()
        import json
        cloud.process_batch(self.cloud, [json.loads(p) for _, p in rows])
        edge.mark_acked(self.edge, [r[0] for r in rows])
        for eid, payload in cloud.downlink_due(self.cloud):
            edge.apply_event(self.edge, json.loads(payload), 10, origin="cloud")
        cloud.downlink_ack(self.cloud, [e for e, _ in cloud.downlink_due(self.cloud)])

    def run_case(self, edge_secs, cloud_secs):
        edge.apply_event(self.edge, ev(1, prop="setpoint", value="EDGE",
                                       secs=edge_secs), 10)
        cloud.local_write(self.cloud, "t1", "setpoint", "CLOUD",
                          (T0 + timedelta(seconds=cloud_secs)).isoformat())
        self.sync()
        return (edge.twin_state(self.edge)["t1"]["setpoint"],
                cloud.twin_state(self.cloud)["t1"]["setpoint"])

    def test_cloud_newer_wins_on_both(self):
        self.assertEqual(self.run_case(10, 20), ("CLOUD", "CLOUD"))

    def test_edge_newer_wins_on_both(self):
        self.assertEqual(self.run_case(20, 10), ("EDGE", "EDGE"))

    def test_equal_timestamp_converges(self):
        e, c = self.run_case(15, 15)
        self.assertEqual(e, c)  # deterministic tie-break, same on both tiers

    def test_event_histories_match_after_sync(self):
        self.run_case(10, 20)
        self.assertEqual(set(edge.event_ids(self.edge)),
                         set(cloud.event_ids(self.cloud)))

    def test_random_interleavings_always_converge(self):
        rng = random.Random(7)
        import json
        for trial in range(30):
            self.tearDown(); self.setUp()
            for k in range(rng.randint(3, 12)):
                s = rng.randint(0, 30)
                if rng.random() < 0.5:
                    edge.apply_event(self.edge, ev(k, prop="sp", secs=s,
                                                   eid=f"E{trial}-{k}"), 50)
                else:
                    cloud.local_write(self.cloud, "t1", "sp", k,
                                      (T0 + timedelta(seconds=s)).isoformat())
            self.sync()
            self.assertEqual(edge.twin_state(self.edge),
                             cloud.twin_state(self.cloud), f"trial {trial}")


class TestCloudCore(Base):
    def test_batch_is_idempotent(self):
        batch = [ev(i, prop=f"p{i%3}", secs=i) for i in range(30)]
        cloud.process_batch(self.cloud, batch)
        res = cloud.process_batch(self.cloud, batch)
        self.assertEqual(set(res), {"duplicate"})
        self.assertEqual(len(cloud.event_ids(self.cloud)), 30)

    def test_bad_event_rolls_back_whole_batch(self):
        good = ev(1)
        bad = {"event_id": "bad"}
        with self.assertRaises(ValueError):
            cloud.process_batch(self.cloud, [good, bad])
        self.assertEqual(cloud.event_ids(self.cloud), [])

    def test_local_write_queues_downlink(self):
        cloud.local_write(self.cloud, "t1", "sp", 5)
        self.assertEqual(cloud.downlink_unacked(self.cloud), 1)
        self.assertEqual(cloud.twin_state(self.cloud)["t1"]["sp"], 5)


if __name__ == "__main__":
    unittest.main()
