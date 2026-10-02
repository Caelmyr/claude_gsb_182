"""Tests for data lineage: provenance capture, retry-trail and trace chain."""

import shutil
import tempfile
import unittest

from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.lineage import LineageStore
from backend.master.registry import WorkerRegistry
from backend.common.models import WorkerRecord, new_worker


def _worker(wid: str, name: str = "", port: int = 9000) -> WorkerRecord:
    return new_worker(wid, name or wid, "127.0.0.1", port, 4, 1024)


class LineageTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(max_attempts=3)
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        self.registry._workers = {
            "w1": _worker("w1", "node-alpha", 9001),
            "w2": _worker("w2", "node-beta", 9002),
            "w3": _worker("w3", "node-gamma", 9003),
        }
        self.lineage = LineageStore(self.storage, self.jm, self.registry)
        self.ft = FaultTolerance(self.storage, self.jm, self.config,
                                 self.logbus, lineage=self.lineage)
        self.job = self.jm.submit({
            "name": "lineage-job",
            "mapper": "wordcount_mapper",
            "reducer": "count_reducer",
            "num_map_tasks": 3, "num_reduce_tasks": 2,
            "input_rows": 300, "params": {},
        })
        self.job_id = self.job.job_id

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers: simulate the worker->master completion flow -------------
    def _map_success(self, map_task, worker_id, seq, attempt, key="alpha", partition=0,
                     count=3, processed=100, speculative=False):
        payload = {
            "worker_id": worker_id,
            "status": "SUCCEEDED",
            "exec_seq": seq,
            "attempt": attempt - 1,
            "speculative": speculative,
            "records_processed": processed,
            "records_emitted": count,
            "partition_sizes": {f"part-{partition:04d}.jsonl": 42},
            "partition_keys": {str(partition): {key: count}},
            "results": [],
        }
        self._finish(map_task, payload)

    def _reduce_success(self, reduce_task, worker_id, seq, attempt, sources,
                        keys):
        payload = {
            "worker_id": worker_id,
            "status": "SUCCEEDED",
            "exec_seq": seq,
            "attempt": attempt - 1,
            "records_processed": sum(sources.values()),
            "records_emitted": len(keys),
            "source_counts": dict(sources),
            "results": [{"key": k, "count": c} for k, c in keys.items()],
        }
        # Attach fetch plan to the reduce task stats like shuffle.build does.
        plan = [
            {"worker_id": "w1" if i == 0 else ("w2" if i == 1 else "w3"),
             "worker_url": "http://127.0.0.1:9000",
             "map_task_id": mid}
            for i, mid in enumerate(sources)
        ]
        self.jm.update_task(self.job_id, reduce_task.task_id,
                            stats={**(reduce_task.stats or {}), "fetch_plan": plan})
        self._finish(reduce_task, payload)

    def _finish(self, task, payload):
        payload = {"job_id": self.job_id, "task_id": task.task_id, **payload}
        # Re-read the task the way the scheduler does, then emulate on_task_complete
        t = self.jm.get_task(self.job_id, task.task_id)
        seq = int(payload["exec_seq"])
        attempt_no = int(payload["attempt"]) + 1
        if payload["status"] == "SUCCEEDED":
            self.jm.update_task(self.job_id, t.task_id, status="SUCCEEDED",
                                worker_id=payload["worker_id"],
                                records_processed=payload.get("records_processed", 0),
                                records_emitted=payload.get("records_emitted", 0))
            t = self.jm.get_task(self.job_id, t.task_id)
            self.lineage.record_exec(
                self.job_id, t.task_id, seq, attempt_no,
                worker_id=payload["worker_id"], kind=t.kind,
                speculative=bool(payload.get("speculative", False)),
                status="SUCCEEDED",
                records_processed=payload.get("records_processed", 0),
                records_emitted=payload.get("records_emitted", 0),
                input_shard=t.input_shard,
                partition=t.partition if t.kind == "reduce" else -1,
            )
            if t.kind == "map":
                self.lineage.record_map_output(self.job_id, t, payload, seq, attempt_no)
            else:
                fetch_plan = (t.stats or {}).get("fetch_plan", [])
                self.lineage.record_reduce_input(self.job_id, t, payload,
                                                 fetch_plan, seq, attempt_no)

    def _dispatch(self, task, worker_id, speculative=False):
        seq = self.lineage.next_seq(task.task_id)
        attempt_no = task.attempts + 1
        self.jm.update_task(self.job_id, task.task_id, status="ASSIGNED",
                            worker_id=None if speculative else worker_id)
        self.lineage.record_exec(
            self.job_id, task.task_id, seq, attempt_no,
            worker_id=worker_id, kind=task.kind, speculative=speculative,
            status="ASSIGNED", input_shard=task.input_shard,
            partition=task.partition if task.kind == "reduce" else -1,
        )
        return seq, attempt_no


class TestLineageCapture(LineageTestBase):
    def test_trace_finds_contributing_shards_and_partition(self):
        maps = self.jm.tasks_for(self.job_id, "map")
        reduces = self.jm.tasks_for(self.job_id, "reduce")

        # m-0000 and m-0002 contribute "alpha" to partition 0; m-0001 does not.
        self._dispatch(maps[0], "w1")
        self._map_success(maps[0], "w1", seq=1, attempt=1, partition=0, count=2)
        self._dispatch(maps[1], "w2")
        self._map_success(maps[1], "w2", seq=1, attempt=1, key="beta",
                          partition=1, count=5)
        self._dispatch(maps[2], "w3")
        self._map_success(maps[2], "w3", seq=1, attempt=1, partition=0, count=4)

        rt = reduces[0]
        self._dispatch(rt, "w2")
        self._reduce_success(rt, "w2", seq=1, attempt=1,
                              sources={"m-0000": 2, "m-0001": 0, "m-0002": 4},
                              keys={"alpha": 6})

        trace = self.lineage.trace_result(self.job_id, key="alpha")
        self.assertTrue(trace["found"])
        self.assertEqual(trace["partition"], 0)
        self.assertEqual(trace["reduce_task_id"], "r-0000")

        by_stage = {s["stage"]: s for s in trace["stages"]}
        # Input layer: exactly the two shards that emitted alpha.
        shards = by_stage["input"]["shards"]
        self.assertEqual([s["shard_id"] for s in shards], ["in-0000", "in-0002"])
        self.assertEqual({s["contributions"] for s in shards}, {2, 4})
        # Shuffle layer: all three sources exist, only two contributed.
        sources = by_stage["shuffle"]["sources"]
        self.assertEqual(len(sources), 3)
        self.assertEqual(sum(1 for s in sources if s["contributed"]), 2)
        # Output layer points at the final partition.
        self.assertEqual(by_stage["output"]["partition_name"], "part-0000")

    def test_retry_chain_is_preserved_across_nodes(self):
        maps = self.jm.tasks_for(self.job_id, "map")
        m0 = maps[0]

        # Attempt 1 on w1 fails -> retry; attempt 2 on w3 wins.
        seq1, at1 = self._dispatch(m0, "w1")
        self.lineage.mark_exec_failed(self.job_id, m0.task_id, seq1, at1, "boom")
        self.ft.handle_task_failure(
            self.jm.get_job(self.job_id), m0, "boom", worker_id="w1",
        )
        m0 = self.jm.get_task(self.job_id, m0.task_id)
        self.assertEqual(m0.attempts, 1)

        seq2, at2 = self._dispatch(m0, "w3")
        self._map_success(m0, "w3", seq=seq2, attempt=at2, count=1)

        view = self.lineage._attempt_view(self.job_id, m0.task_id, [])
        statuses = [(a["worker_name"], a["status"], a["attempt"]) for a in view["attempts"]]
        self.assertEqual(statuses[0], ("node-alpha", "failed", 1))
        self.assertEqual(statuses[1], ("node-gamma", "SUCCEEDED", 2))
        self.assertEqual(view["winning_seq"], seq2)

    def test_worker_death_marks_open_attempt_and_continues_chain(self):
        maps = self.jm.tasks_for(self.job_id, "map")
        m0 = maps[0]
        seq, at = self._dispatch(m0, "w1")
        self.jm.update_task(self.job_id, m0.task_id, status="RUNNING", worker_id="w1")

        self.ft.handle_worker_death(self.registry.get("w1"))
        execs = self.lineage.executions(self.job_id, m0.task_id)
        self.assertEqual(execs[0]["status"], "worker_lost")

        # Reassignment then succeeds on another node.
        m0 = self.jm.get_task(self.job_id, m0.task_id)
        self.assertEqual(m0.status, "RETRYING")
        seq2, at2 = self._dispatch(m0, "w2")
        self._map_success(m0, "w2", seq=seq2, attempt=at2, count=1)
        statuses = [e["status"] for e in self.lineage.executions(self.job_id, m0.task_id)]
        self.assertEqual(statuses, ["worker_lost", "SUCCEEDED"])

    def test_trace_missing_key_warns_instead_of_breaking(self):
        maps = self.jm.tasks_for(self.job_id, "map")
        reduces = self.jm.tasks_for(self.job_id, "reduce")
        for i, m in enumerate(maps):
            self._dispatch(m, f"w{i+1}")
            self._map_success(m, f"w{i+1}", seq=1, attempt=1,
                              key="gamma", partition=0, count=1)
        rt = reduces[0]
        self._dispatch(rt, "w1")
        self._reduce_success(rt, "w1", seq=1, attempt=1,
                              sources={"m-0000": 1, "m-0001": 1, "m-0002": 1},
                              keys={"gamma": 3})
        trace = self.lineage.trace_result(self.job_id, key="alpha")
        self.assertTrue(trace["found"])  # partition found, but key absent
        self.assertTrue(any("key-not-in-mappers" in w for w in trace["warnings"]))

    def test_summary_counts_attempts(self):
        maps = self.jm.tasks_for(self.job_id, "map")
        m0 = maps[0]
        seq1, at1 = self._dispatch(m0, "w1")
        self.lineage.mark_exec_failed(self.job_id, m0.task_id, seq1, at1, "x")
        self.ft.handle_task_failure(self.jm.get_job(self.job_id), m0, "x", "w1")
        m0 = self.jm.get_task(self.job_id, m0.task_id)
        seq2, at2 = self._dispatch(m0, "w2")
        self._map_success(m0, "w2", seq=seq2, attempt=at2, count=1)

        summary = self.lineage.summary(self.job_id)
        self.assertEqual(summary["map_outputs"], 1)
        self.assertEqual(summary["failed_attempts"], 1)
        self.assertEqual(summary["total_attempts"], 2)
        self.assertIn("m-0000", summary["retried_tasks"])

    def test_lineage_persists_across_store_reopen(self):
        maps = self.jm.tasks_for(self.job_id, "map")
        self._dispatch(maps[0], "w1")
        self._map_success(maps[0], "w1", seq=1, attempt=1, count=2)
        reopened = LineageStore(self.storage, self.jm, self.registry)
        docs = reopened.map_outputs(self.job_id)
        self.assertEqual(len(docs), 1)
        self.assertEqual(docs[0]["input_shard"], "in-0000")
        self.assertEqual(docs[0]["partition_keys"]["0"]["alpha"], 2)

    def test_trace_by_partition_ignores_key(self):
        maps = self.jm.tasks_for(self.job_id, "map")
        reduces = self.jm.tasks_for(self.job_id, "reduce")
        for i, m in enumerate(maps):
            self._dispatch(m, f"w{i+1}")
            self._map_success(m, f"w{i+1}", seq=1, attempt=1,
                              key="alpha", partition=1, count=1)
        rt = next(t for t in reduces if t.partition == 1)
        self._dispatch(rt, "w1")
        self._reduce_success(rt, "w1", seq=1, attempt=1,
                              sources={"m-0000": 1, "m-0001": 1, "m-0002": 1},
                              keys={"alpha": 3})
        trace = self.lineage.trace_result(self.job_id, partition=1)
        self.assertTrue(trace["found"])
        self.assertEqual(trace["partition"], 1)
        self.assertEqual(len(trace["stages"][2]["sources"]), 3)

    def test_speculative_winner_on_different_node_points_fetch_at_winner(self):
        maps = self.jm.tasks_for(self.job_id, "map")
        m0 = maps[0]
        # Original attempt runs on w1; a speculative copy wins on w2.
        seq1, at1 = self._dispatch(m0, "w1")
        m0 = self.jm.get_task(self.job_id, m0.task_id)
        seq2, at2 = self._dispatch(m0, "w2", speculative=True)
        self._map_success(m0, "w2", seq=seq2, attempt=at2, count=1, speculative=True)
        doc = self.lineage.map_outputs(self.job_id)[0]
        self.assertEqual(doc["worker_id"], "w2")
        self.assertEqual(doc["worker_name"], "node-beta")
        view = self.lineage._attempt_view(self.job_id, m0.task_id, [])
        by_seq = {a["seq"]: a for a in view["attempts"]}
        self.assertEqual(by_seq[seq2]["status"], "SUCCEEDED")
        self.assertTrue(by_seq[seq1]["speculative"] is False)
        self.assertTrue(by_seq[seq2]["speculative"])

    def test_trace_unavailable_before_reduce(self):
        trace = self.lineage.trace_result(self.job_id, key="alpha")
        self.assertFalse(trace["found"])
        self.assertTrue(trace["warnings"])


if __name__ == "__main__":
    unittest.main()
