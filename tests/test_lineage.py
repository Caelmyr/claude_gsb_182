"""Tests for data lineage (数据血缘): provenance graph + per-result tracing.

Covers the hard properties the feature exists for:

* a result fans out to *every* input shard that fed it (many-to-one merge);
* the fan-in exactly matches the shards that actually emitted the traced key
  (ground truth is recomputed straight from the shuffle store);
* failed/retried attempts and attempts lost to a dead worker remain in the
  chain, while edges always point at the *winning* worker;
* reduce-side result keys are discoverable by key-only lookup;
* truncated key fingerprints degrade to a marked "may contain" segment rather
  than silently dropping provenance.
"""

import collections
import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.models import WorkerRecord
from backend.common.storage import Storage
from backend.master.job_manager import JobManager
from backend.master.lineage import LineageTracker
from backend.master.shuffle import ShuffleCoordinator
from backend.tasks.registry import get_reducer
from backend.worker.executor import _run_map
from backend.worker.shuffle_store import ShuffleStore


class _Registry:
    """Minimal worker registry stub for name resolution."""

    def __init__(self):
        self.workers = {}

    def add(self, wid, name):
        self.workers[wid] = WorkerRecord(worker_id=wid, name=name, host="h", port=1)

    def get(self, wid):
        return self.workers.get(wid)


class LineageTestBase(unittest.TestCase):
    NUM_MAP = 3
    NUM_REDUCE = 2
    ROWS = 50

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.jm = JobManager(self.storage, ClusterConfig(), LogBus(self.storage))
        self.tracker = self.jm.lineage
        self.registry = _Registry()
        for i in range(5):
            wid = f"w{chr(65 + i)}"
            self.registry.add(wid, f"node-{chr(65 + i)}")
        self.job = self.jm.submit({
            "name": "t", "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": self.NUM_MAP, "num_reduce_tasks": self.NUM_REDUCE,
            "input_rows": self.ROWS, "params": {},
        })
        self.store = ShuffleStore(self.tmp)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers ---------------------------------------------------------
    def _run_all_maps(self, map_workers=None, fail_task="", fail_worker="",
                      retry_worker=""):
        """Run every map task; optionally fail one task's first attempt."""
        map_workers = map_workers or ["wA", "wB", "wC"]
        results = {}
        for mt, wid0 in zip(self.jm.tasks_for(self.job.job_id, C.TASK_MAP), map_workers):
            wid = wid0
            self.tracker.record_dispatch(self.job, mt, wid)
            self.tracker.record_running(self.job.job_id, mt.task_id, wid)
            if mt.task_id == fail_task:
                self.tracker.record_failure(self.job, mt, fail_worker or wid, "simulated boom")
                self.jm.update_task(self.job.job_id, mt.task_id, attempts=mt.attempts + 1)
                wid = retry_worker
                self.tracker.record_dispatch(self.job, mt, wid)
                self.tracker.record_running(self.job.job_id, mt.task_id, wid)
            data = self.jm.planner.load_input_shard(self.job.job_id, mt.input_shard)
            spec = {
                "task_id": mt.task_id, "job_id": self.job.job_id, "kind": "map",
                "mapper": "wordcount_mapper", "reducer": "count_reducer",
                "params": {}, "partition_count": self.NUM_REDUCE,
                "records": data, "spill_records": 200, "tmp_dir": self.tmp,
            }
            res = _run_map(spec, self.tmp, lambda *a: None)
            self.tracker.record_map_success(self.job, mt, wid, res)
            self.jm.update_task(self.job.job_id, mt.task_id, worker_id=wid)
            results[mt.task_id] = res
        return results

    def _build_shuffle(self):
        ShuffleCoordinator(
            self.storage, self.jm, self.registry, LogBus(self.storage),
        ).build(self.job)
        self.tracker.record_shuffle(self.job)

    def _run_all_reduces(self, reduce_worker="wE"):
        maps = self.jm.tasks_for(self.job.job_id, C.TASK_MAP)
        reducer = get_reducer("count_reducer")
        out = {}
        for rt in self.jm.tasks_for(self.job.job_id, C.TASK_REDUCE):
            self.tracker.record_dispatch(self.job, rt, reduce_worker)
            self.tracker.record_running(self.job.job_id, rt.task_id, reduce_worker)
            grouped = collections.defaultdict(list)
            for mt in maps:
                for k, v in self.store.read_partition(self.job.job_id, mt.task_id, rt.partition):
                    grouped[k].append(v)
            results = [reducer(k, vs, {}) for k, vs in grouped.items()]
            self.tracker.record_reduce_success(
                self.job, rt, reduce_worker, {"results": results},
            )
            out[rt.partition] = {k: r["count"] for r in results for k in [r["key"]]}
        return out

    def _full_pipeline(self, **map_kw):
        self._run_all_maps(**map_kw)
        self._build_shuffle()
        return self._run_all_reduces()


class TestLineageGraphLifecycle(LineageTestBase):
    def test_graph_seeded_with_shards_and_tasks_at_submit(self):
        doc = self.tracker.get_graph(self.job.job_id)
        self.assertEqual(len(doc["input_shards"]), self.NUM_MAP)
        self.assertEqual(
            sorted(s["shard_id"] for s in doc["input_shards"]),
            [f"in-{i:04d}" for i in range(self.NUM_MAP)],
        )
        # shard -> map task allocation matches the planned tasks
        self.assertEqual(doc["input_shards"][1]["map_task_id"], "m-0001")
        self.assertEqual(len(doc["tasks"]), self.NUM_MAP + self.NUM_REDUCE)
        self.assertTrue(all(t["attempts"] == [] for t in doc["tasks"].values()))

    def test_dispatch_running_success_records_attempt(self):
        mt = self.jm.tasks_for(self.job.job_id, C.TASK_MAP)[0]
        self.tracker.record_dispatch(self.job, mt, "wA")
        self.tracker.record_running(self.job.job_id, mt.task_id, "wA")
        node = self.tracker.get_graph(self.job.job_id)["tasks"][mt.task_id]
        self.assertEqual(node["attempts"][0]["status"], C.ATTEMPT_RUNNING)
        self.tracker.record_map_success(self.job, mt, "wA", {"partition_lineage": {},
                                                             "records_processed": 1})
        node = self.tracker.get_graph(self.job.job_id)["tasks"][mt.task_id]
        self.assertEqual(node["winning_worker_id"], "wA")
        self.assertEqual(node["attempts"][0]["status"], C.ATTEMPT_SUCCEEDED)

    def test_failed_then_retried_attempts_are_both_retained(self):
        self._full_pipeline(fail_task="m-0001", fail_worker="wB", retry_worker="wD")
        node = self.tracker.get_graph(self.job.job_id)["tasks"]["m-0001"]
        statuses = [(a["attempt_no"], a["worker_id"], a["status"]) for a in node["attempts"]]
        self.assertEqual(statuses[0], (1, "wB", C.ATTEMPT_FAILED))
        self.assertEqual(statuses[1], (2, "wD", C.ATTEMPT_SUCCEEDED))
        # the edge uses the winner (retry) worker, not the failed one
        self.assertEqual(node["winning_worker_id"], "wD")

    def test_worker_loss_marks_inflight_attempt_lost(self):
        mt = self.jm.tasks_for(self.job.job_id, C.TASK_MAP)[0]
        self.tracker.record_dispatch(self.job, mt, "wA")
        self.tracker.record_running(self.job.job_id, mt.task_id, "wA")
        self.tracker.record_worker_lost(self.job.job_id, mt.task_id, "wA",
                                        reason="node-A died")
        # retried elsewhere
        self.tracker.record_dispatch(self.job, mt, "wB")
        self.tracker.record_running(self.job.job_id, mt.task_id, "wB")
        self.tracker.record_map_success(self.job, mt, "wB", {"partition_lineage": {}})
        node = self.tracker.get_graph(self.job.job_id)["tasks"][mt.task_id]
        self.assertEqual(node["attempts"][0]["status"], C.ATTEMPT_LOST)
        self.assertEqual(node["winning_worker_id"], "wB")


class TestLineageTrace(LineageTestBase):
    def test_overview_lists_result_partitions_and_keys(self):
        self._full_pipeline()
        ov = self.tracker.result_overview(self.job.job_id, registry=self.registry)
        self.assertTrue(ov["available"])
        self.assertEqual(len(ov["partitions"]), self.NUM_REDUCE)
        names = [p["partition_name"] for p in ov["partitions"]]
        self.assertEqual(names, ["part-0000", "part-0001"])
        self.assertTrue(all(p["keys"] for p in ov["partitions"]))
        # reduce worker name resolved via registry
        self.assertEqual(ov["partitions"][0]["worker_name"], "node-E")

    def test_trace_fan_in_matches_ground_truth_for_every_key(self):
        self._full_pipeline()
        maps = self.jm.tasks_for(self.job.job_id, C.TASK_MAP)
        ov = self.tracker.result_overview(self.job.job_id, registry=self.registry)
        for part in ov["partitions"]:
            p = part["partition"]
            for key in part["keys"]:
                trace = self.tracker.trace(self.job.job_id, partition=p, key=key,
                                           registry=self.registry)
                self.assertTrue(trace["found"], msg=key)
                traced_shards = {i["shard_id"] for i in trace["inputs"]}
                # ground truth straight from the shuffle partition files
                truth = set()
                for mt in maps:
                    keys_here = {k for k, _ in
                                 self.store.read_partition(self.job.job_id, mt.task_id, p)}
                    if key in keys_here:
                        truth.add(mt.input_shard)
                self.assertEqual(traced_shards, truth, msg=f"{key} part {p}")
                # every contributing shuffle segment claims the key definitively
                self.assertTrue(all(s["had_key"] is True and s["definitive"]
                                    for s in trace["shuffle"]["segments"]))

    def test_trace_chain_order_inputs_maps_shuffle_reduce_output(self):
        self._full_pipeline()
        ov = self.tracker.result_overview(self.job.job_id, registry=self.registry)
        part = next(p for p in ov["partitions"] if p["keys"])
        key = part["keys"][0]
        tr = self.tracker.trace(self.job.job_id, partition=part["partition"], key=key,
                                registry=self.registry)
        self.assertTrue({s["shard_id"] for s in tr["inputs"]})
        self.assertEqual(len(tr["map_attempts"]), tr["summary"]["map_task_count"])
        self.assertEqual(tr["shuffle"]["reduce_task_id"], tr["reduce"]["task_id"])
        self.assertEqual(tr["output"]["partition_name"], part["partition_name"])
        self.assertEqual(tr["output"]["reduce_task_id"],
                         f"r-{part['partition']:04d}")
        self.assertEqual(tr["output"]["worker_id"], "wE")

    def test_key_only_lookup_resolves_partition(self):
        self._full_pipeline()
        ov = self.tracker.result_overview(self.job.job_id, registry=self.registry)
        key = ov["partitions"][1]["keys"][0]
        tr = self.tracker.trace(self.job.job_id, key=key, registry=self.registry)
        self.assertTrue(tr["found"])
        self.assertEqual(tr["output"]["partition_name"], "part-0001")

    def test_unknown_key_is_reported_not_fabricated(self):
        self._full_pipeline()
        part = self.tracker.result_overview(self.job.job_id,
                                            registry=self.registry)["partitions"][0]
        tr = self.tracker.trace(self.job.job_id, partition=part["partition"],
                                key="no_such_key_zzz", registry=self.registry)
        self.assertFalse(tr["found"])
        self.assertIn("not present", tr["reason"])

    def test_retry_chain_visible_in_trace_timeline(self):
        self._full_pipeline(fail_task="m-0000", fail_worker="wA", retry_worker="wC")
        ov = self.tracker.result_overview(self.job.job_id, registry=self.registry)
        # Choose a key the *retried* task contributed: its winning attempt ran
        # on wC, so the key must be present in that task's output fingerprint.
        graph = self.tracker.get_graph(self.job.job_id)
        fps = graph["tasks"]["m-0000"]["output_partitions"]
        pname = next(name for name, fp in fps.items() if fp.get("keys"))
        key = next(iter(fps[pname]["keys"]))
        part = next(p for p in ov["partitions"] if p["partition_name"] == pname)
        self.assertIn(key, part["keys"])

        tr = self.tracker.trace(self.job.job_id, partition=pname, key=key,
                                registry=self.registry)
        failed = [e for e in tr["events"]
                  if e["status"] in (C.ATTEMPT_FAILED, C.ATTEMPT_LOST)]
        self.assertTrue(failed)
        self.assertEqual(failed[0]["worker_id"], "wA")
        # and the failed worker never appears as a shuffle source
        self.assertNotIn("wA", {s["worker_id"] for s in tr["shuffle"]["segments"]})
        self.assertIn("wC", {s["worker_id"] for s in tr["shuffle"]["segments"]})
        self.assertGreaterEqual(tr["summary"]["failed_attempts"], 1)

    def test_trace_before_results_ready(self):
        self._run_all_maps()
        tr = self.tracker.trace(self.job.job_id, partition=0, key="x",
                                registry=self.registry)
        self.assertTrue(tr.get("ready") is False)
        self.assertEqual(len(tr["inputs"]), self.NUM_MAP)

    def test_truncated_fingerprint_marks_segment_indefinite(self):
        self._run_all_maps()
        self._build_shuffle()
        # Choose a partition that actually received keys from m-0000, then
        # tamper that fingerprint to look capped/truncated.
        doc = self.tracker.get_graph(self.job.job_id)
        pname = next(
            name for name, fp in doc["tasks"]["m-0000"]["output_partitions"].items()
            if fp.get("keys")
        )

        def truncate(d):
            d["tasks"]["m-0000"]["output_partitions"][pname] = {
                "keys": [], "truncated": True,
            }
            return d
        self.storage.update(
            truncate, "jobs", self.job.job_id, "lineage", "lineage.json", default={},
        )
        self._run_all_reduces()
        ov = self.tracker.result_overview(self.job.job_id, registry=self.registry)
        part = next(p for p in ov["partitions"] if p["partition_name"] == pname)
        key = part["keys"][0]
        tr = self.tracker.trace(self.job.job_id, partition=pname, key=key,
                                registry=self.registry)
        seg = next(s for s in tr["shuffle"]["segments"] if s["map_task_id"] == "m-0000")
        self.assertFalse(seg["definitive"])
        self.assertTrue(seg["fingerprint_truncated"])


if __name__ == "__main__":
    unittest.main()
