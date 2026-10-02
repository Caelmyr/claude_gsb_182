"""Data lineage (数据血缘): per-result provenance tracking.

Given an output record (a result key in a result partition) this module can
reconstruct the *complete* chain that produced it:

    input shard(s) in-XXXX
        -> map task m-XXXX @ worker (one or more attempts)
            -> shuffle partition part-YYYY transferred over HTTP
                -> reduce task r-YYYY @ worker (merged from every mapper)
                    -> result partition part-YYYY

Three properties make the chain reliable in a real, failure-prone job:

* **many-to-one merge is first class.**  A reduce result is produced from
  *every* map task that emitted its key, so a trace fans out to multiple input
  shards instead of picking one arbitrarily.  The map side records, per output
  partition, the set of keys it emitted (a bounded "lineage fingerprint"); the
  trace uses those fingerprints to name exactly the shards that fed the result.
* **retries never break the chain.**  Every *execution attempt* of a logical
  task is its own lineage node (dispatch -> running -> succeeded / failed /
  lost / cancelled), including attempts on different workers after a node
  death and speculative duplicates.  The edge in the provenance chain always
  points at the *winning* attempt — the one whose output the job actually used
  — while failed/superseded attempts stay visible right next to it.
* **the graph is durable and append-only per transition.**  The whole graph
  for a job lives in one atomically-updated JSON document
  (``jobs/<job>/lineage/lineage.json``); every state transition is a
  read-modify-write under the storage file lock, so concurrent worker reports
  can never lose an attempt.

The tracker is deliberately storage-only: it does not own any scheduling
decision, it just records what the scheduler/fault-tolerance code already did.
"""

from __future__ import annotations

from typing import Any, Optional

from backend.common import constants as C
from backend.common.ids import partition_name
from backend.common.jsonutil import now_ms
from backend.common.storage import Storage, list_files, read_json

# Terminal attempt statuses — an attempt in one of these states no longer
# consumes the task (a later dispatch is a fresh attempt row).
_OPEN_STATES = {C.ATTEMPT_ASSIGNED, C.ATTEMPT_RUNNING}
_TERMINAL_STATES = {
    C.ATTEMPT_SUCCEEDED, C.ATTEMPT_FAILED, C.ATTEMPT_LOST, C.ATTEMPT_CANCELLED,
}

DEFAULT_KEY_CAP = 5000          # max distinct keys fingerprinted per partition


class LineageTracker:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------
    def _path(self, job_id: str) -> list[str]:
        return ["jobs", job_id, "lineage", "lineage.json"]

    def _mutate(self, job_id: str, fn) -> None:
        """Read-modify-write the lineage document under the file lock."""
        def updater(doc: Optional[dict]) -> dict:
            doc = dict(doc or {})
            doc.setdefault("job_id", job_id)
            doc.setdefault("created_ms", now_ms())
            doc.setdefault("input_shards", [])
            doc.setdefault("tasks", {})
            doc.setdefault("shuffle", {})
            doc.setdefault("results", {})
            fn(doc)
            doc["updated_ms"] = now_ms()
            return doc

        self.storage.update(updater, *self._path(job_id), default={})

    def get_graph(self, job_id: str) -> Optional[dict]:
        return self.storage.read(*self._path(job_id), default=None)

    # ------------------------------------------------------------------
    # Job submission: shards + task skeleton
    # ------------------------------------------------------------------
    def init_job(self, job, plan: dict) -> None:
        """Create the lineage graph when a job is accepted.

        ``plan`` is the ShardPlanner result; one input shard node is created
        per planned shard and one task node per map/reduce task, so the graph
        structure mirrors the actual shard/task allocation from the very
        beginning (even before anything is dispatched).
        """
        shard_meta = plan.get("input_shard_meta") or [
            {"shard_id": sid, "index": i, "count": 0}
            for i, sid in enumerate(plan.get("input_shards", []))
        ]

        def build(doc: dict) -> None:
            doc["input_shards"] = [
                {
                    "shard_id": s.get("shard_id"),
                    "index": int(s.get("index", 0)),
                    "records": int(s.get("count", 0)),
                    "map_task_id": f"m-{int(s.get('index', 0)):04d}",
                }
                for s in sorted(shard_meta, key=lambda s: int(s.get("index", 0)))
            ]
            tasks: dict[str, dict] = {}
            for t in plan.get("map_tasks", []):
                tasks[t.task_id] = {
                    "kind": C.TASK_MAP,
                    "index": t.index,
                    "input_shard": t.input_shard,
                    "attempt_seq": 0,
                    "attempts": [],
                    "output_partitions": {},
                    "winning_attempt_no": 0,
                    "winning_worker_id": "",
                }
            for t in plan.get("reduce_tasks", []):
                tasks[t.task_id] = {
                    "kind": C.TASK_REDUCE,
                    "index": t.index,
                    "partition": t.partition,
                    "attempt_seq": 0,
                    "attempts": [],
                    "winning_attempt_no": 0,
                    "winning_worker_id": "",
                }
            doc["tasks"] = tasks

        self._mutate(job.job_id, build)

    # ------------------------------------------------------------------
    # Attempt transitions (called by the scheduler / fault tolerance)
    # ------------------------------------------------------------------
    def _task(self, doc: dict, task_id: str) -> Optional[dict]:
        return (doc.get("tasks") or {}).get(task_id)

    def _find_open_attempt(self, task: dict, worker_id: str = "") -> Optional[int]:
        """Index of the live attempt row, preferably one on ``worker_id``."""
        attempts = task.get("attempts", [])
        if worker_id:
            for i in range(len(attempts) - 1, -1, -1):
                if attempts[i].get("status") in _OPEN_STATES and attempts[i].get("worker_id") == worker_id:
                    return i
        for i in range(len(attempts) - 1, -1, -1):
            if attempts[i].get("status") in _OPEN_STATES:
                return i
        return None

    def _close_others(self, task: dict, winner_no: int, status: str, reason: str) -> None:
        """Every still-open attempt that is not the winner gets superseded."""
        ts = now_ms()
        for a in task.get("attempts", []):
            if a.get("attempt_no") == winner_no:
                continue
            if a.get("status") in _OPEN_STATES:
                a["status"] = status
                a["finished_ms"] = ts
                a["error"] = reason

    def record_dispatch(self, job, task, worker_id: str, speculative: bool = False) -> None:
        """A task (or a speculative duplicate of it) was sent to a worker."""
        def upd(doc: dict) -> None:
            node = self._task(doc, task.task_id)
            if node is None:
                return
            node["attempt_seq"] = int(node.get("attempt_seq", 0)) + 1
            node.setdefault("attempts", []).append({
                "attempt_no": node["attempt_seq"],
                "worker_id": worker_id,
                "speculative": bool(speculative),
                "status": C.ATTEMPT_ASSIGNED,
                "dispatched_ms": now_ms(),
                "started_ms": 0,
                "finished_ms": 0,
                "error": "",
            })

        self._mutate(job.job_id, upd)

    def record_running(self, job_id: str, task_id: str, worker_id: str = "") -> None:
        """A worker acknowledged the task and started processing."""
        def upd(doc: dict) -> None:
            node = self._task(doc, task_id)
            if node is None:
                return
            idx = self._find_open_attempt(node, worker_id)
            if idx is None:
                return
            att = node["attempts"][idx]
            att["status"] = C.ATTEMPT_RUNNING
            if not att.get("started_ms"):
                att["started_ms"] = now_ms()

        self._mutate(job_id, upd)

    def record_failure(self, job, task, worker_id: str = "", error: str = "") -> None:
        """An attempt reported an error (it will be retried or kill the job)."""
        def upd(doc: dict) -> None:
            node = self._task(doc, task.task_id)
            if node is None:
                return
            idx = self._find_open_attempt(node, worker_id)
            if idx is None:
                return
            att = node["attempts"][idx]
            att["status"] = C.ATTEMPT_FAILED
            att["finished_ms"] = now_ms()
            att["error"] = error or att.get("error", "")

        self._mutate(job.job_id, upd)

    def record_worker_lost(self, job_id: str, task_id: str, worker_id: str,
                           reason: str = "worker lost") -> None:
        """All in-flight attempts of a task on a dead worker become 'lost'.

        The task is then reassigned, so the next successful attempt keeps the
        chain connected; the lost attempt is preserved to explain the retry.
        """
        def upd(doc: dict) -> None:
            node = self._task(doc, task_id)
            if node is None:
                return
            ts = now_ms()
            for att in node.get("attempts", []):
                if att.get("worker_id") == worker_id and att.get("status") in _OPEN_STATES:
                    att["status"] = C.ATTEMPT_LOST
                    att["finished_ms"] = ts
                    att["error"] = reason

        self._mutate(job_id, upd)

    def record_map_success(self, job, task, worker_id: str, payload: dict) -> None:
        """Winning map attempt finished; persist its per-partition key fingerprints."""
        fingerprints = self._normalize_fingerprints(payload.get("partition_lineage", {}))

        def upd(doc: dict) -> None:
            node = self._task(doc, task.task_id)
            if node is None:
                return
            idx = self._find_open_attempt(node, worker_id)
            winner_no = self._win(node, idx, worker_id)
            self._close_others(node, winner_no, C.ATTEMPT_CANCELLED,
                               "superseded by winning attempt")
            node["output_partitions"] = fingerprints
            node["records_processed"] = int(payload.get("records_processed", 0))
            node["records_emitted"] = int(payload.get("records_emitted", 0))

        self._mutate(job.job_id, upd)

    def record_reduce_success(self, job, task, worker_id: str, payload: dict) -> None:
        """Winning reduce attempt finished; remember the keys now in its partition."""
        results = payload.get("results", []) or []
        keys = sorted(
            {str(r.get("key")) for r in results if isinstance(r, dict) and r.get("key") is not None}
        )

        def upd(doc: dict) -> None:
            node = self._task(doc, task.task_id)
            if node is None:
                return
            idx = self._find_open_attempt(node, worker_id)
            winner_no = self._win(node, idx, worker_id)
            self._close_others(node, winner_no, C.ATTEMPT_CANCELLED,
                               "superseded by winning attempt")
            pname = partition_name(int(task.partition))
            doc.setdefault("results", {})[pname] = {
                "partition": int(task.partition),
                "partition_name": pname,
                "reduce_task_id": task.task_id,
                "worker_id": worker_id,
                "count": len(results),
                "keys": keys,
                "written_ms": now_ms(),
            }

        self._mutate(job.job_id, upd)

    def _win(self, node: dict, idx: Optional[int], worker_id: str) -> int:
        """Mark attempt ``idx`` succeeded; return its attempt number."""
        if idx is None:
            # Defensive: completion without a recorded dispatch (e.g. master
            # restarted mid-job) — synthesise the attempt so the edge exists.
            node["attempt_seq"] = int(node.get("attempt_seq", 0)) + 1
            node.setdefault("attempts", []).append({
                "attempt_no": node["attempt_seq"],
                "worker_id": worker_id,
                "speculative": False,
                "status": C.ATTEMPT_SUCCEEDED,
                "dispatched_ms": now_ms(),
                "started_ms": 0,
                "finished_ms": now_ms(),
                "error": "",
            })
            idx = len(node["attempts"]) - 1
        att = node["attempts"][idx]
        att["status"] = C.ATTEMPT_SUCCEEDED
        att["finished_ms"] = now_ms()
        att["error"] = ""
        node["winning_attempt_no"] = att["attempt_no"]
        node["winning_worker_id"] = att.get("worker_id", worker_id)
        return att["attempt_no"]

    # ------------------------------------------------------------------
    # Shuffle: snapshot the actual transfer plan (mapper worker -> partition)
    # ------------------------------------------------------------------
    def record_shuffle(self, job) -> None:
        """Copy the built shuffle matrix into the lineage graph.

        This is the merge layer: for every reduce partition it names the map
        task and serving worker of every pulled segment, with byte counts.
        """
        partitions: dict[str, dict] = {}
        root = self.storage.path("jobs", job.job_id, "shuffle")
        for path in list_files(root, suffix=".json"):
            d = read_json(path)
            if not d:
                continue
            pname = d.get("partition_name") or partition_name(int(d.get("partition", 0)))
            partitions[pname] = {
                "partition": int(d.get("partition", 0)),
                "partition_name": pname,
                "reduce_task_id": d.get("reduce_task_id", ""),
                "status": d.get("status", "ready"),
                "sources": [
                    {
                        "map_task_id": s.get("map_task_id", ""),
                        "worker_id": s.get("worker_id", ""),
                        "bytes": int(s.get("bytes", 0)),
                    }
                    for s in d.get("sources", [])
                ],
            }

        def upd(doc: dict) -> None:
            doc["shuffle"] = partitions

        self._mutate(job.job_id, upd)

    # ------------------------------------------------------------------
    # Fingerprint normalisation (worker -> storage format)
    # ------------------------------------------------------------------
    def _normalize_fingerprints(self, raw: dict) -> dict:
        out: dict[str, dict] = {}
        for name, info in (raw or {}).items():
            # Accept both "part-0003" and "part-0003.jsonl" style keys.
            pname = name.split(".", 1)[0]
            info = info if isinstance(info, dict) else {}
            keys = info.get("keys", [])
            out[pname] = {
                "keys": sorted((str(k) for k in keys), key=str),
                "truncated": bool(info.get("truncated", False)),
            }
        return out

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    def result_overview(self, job_id: str, registry=None) -> dict:
        doc = self.get_graph(job_id)
        if doc is None:
            return {"available": False, "partitions": []}
        return {
            "available": True,
            "partitions": [
                self._resolve_result(job_id, doc, pname, registry)
                for pname in sorted(doc.get("results", {}), key=lambda n: doc["results"][n].get("partition", 0))
            ],
        }

    def trace(self, job_id: str, partition: Any = None, key: Optional[str] = None,
              registry=None) -> dict:
        """Build the full provenance chain for one result key/partition.

        ``partition`` accepts a partition index, ``part-XXXX`` / ``r-XXXX``
        names; ``key`` narrows the fan-in to exactly the shards that emitted
        that key (when omitted, every source segment of the partition is
        listed).  Returned columns, in downstream order:

        ``inputs -> map_attempts -> shuffle -> reduce -> output``
        plus a flat chronological ``events`` timeline covering *every* attempt
        (failed, lost, speculative and winning).
        """
        doc = self.get_graph(job_id)
        if doc is None:
            return {"available": False, "job_id": job_id, "reason": "lineage graph not found"}

        results = doc.get("results", {})

        # Resolve which partition the key lives in when only the key is given.
        key = None if key is None or key == "" else str(key)
        pname = self._resolve_partition_name(doc, partition, key)
        if not results:
            # Job still running: surface the structural chain that exists so
            # far (shards, map tasks/attempts) instead of a hard not-found.
            map_cols = []
            if pname is not None:
                for src in (doc.get("shuffle", {}) or {}).get(pname, {}).get("sources", []):
                    map_cols.append(self._task_column(job_id, doc, src["map_task_id"], registry))
            return {
                "available": True, "job_id": job_id, "found": False, "ready": False,
                "partition_name": pname, "key": key,
                "reason": "job has not produced results yet",
                "inputs": self._all_inputs(doc, registry),
                "map_attempts": map_cols,
            }
        if pname is None:
            return self.result_overview(job_id, registry) | {"job_id": job_id, "mode": "overview"}

        result = results.get(pname)
        if result is None:
            return {"available": True, "job_id": job_id, "ready": False,
                    "reason": f"partition {pname} has no result yet"}

        if key is not None and key not in set(result.get("keys", [])):
            return {
                "available": True, "job_id": job_id, "found": False,
                "partition_name": pname, "key": key,
                "reason": f"key {key!r} is not present in {pname}",
                "result_keys": result.get("keys", []),
            }

        tasks = doc.get("tasks", {})
        shuffle_part = (doc.get("shuffle", {}) or {}).get(pname, {})
        reduce_task_id = result.get("reduce_task_id") or shuffle_part.get("reduce_task_id", "")
        reduce_node = tasks.get(reduce_task_id, {})

        # ---- fan-in: which map segments actually fed this result ----------
        sources = shuffle_part.get("sources", [])
        contributing: list[dict] = []
        for src in sources:
            mtask = tasks.get(src["map_task_id"], {})
            out_parts = mtask.get("output_partitions", {}) or {}
            fp = out_parts.get(pname, {})
            fp_keys = set(fp.get("keys", []))
            if key is not None:
                if fp.get("truncated"):
                    # Fingerprint cap hit: the key set is incomplete, so the
                    # segment *may* contain the key. Surface it, marked unproven.
                    had_key, definitive = None, False
                else:
                    had_key, definitive = key in fp_keys, True
                if had_key is False:
                    continue
            else:
                had_key, definitive = (int(src.get("bytes", 0)) > 0), True
            contributing.append({
                "map_task_id": src["map_task_id"],
                "worker_id": src.get("worker_id", ""),
                "worker_name": self._worker_name(registry, src.get("worker_id", "")),
                "bytes": int(src.get("bytes", 0)),
                "input_shard": mtask.get("input_shard", ""),
                "had_key": had_key,
                "definitive": definitive,
                "fingerprint_truncated": bool(fp.get("truncated", False)),
                "fingerprint_key_count": len(fp_keys),
            })

        shard_ids = {c["input_shard"] for c in contributing if c["input_shard"]}
        inputs = [
            self._shard_view(s, registry)
            for s in doc.get("input_shards", []) if s.get("shard_id") in shard_ids
        ]

        map_columns = [
            self._task_column(job_id, doc, c["map_task_id"], registry) for c in contributing
        ]
        reduce_column = self._task_column(job_id, doc, reduce_task_id, registry)

        events = self._events_timeline(map_columns + [reduce_column])

        visited = {e["worker_id"] for e in events if e.get("worker_id")}
        return {
            "available": True,
            "found": True,
            "job_id": job_id,
            "mode": "trace",
            "key": key,
            "inputs": inputs,
            "map_attempts": map_columns,
            "shuffle": self._shuffle_view(pname, shuffle_part, contributing, registry),
            "reduce": reduce_column,
            "output": self._resolve_result(job_id, doc, pname, registry, key=key),
            "events": events,
            "summary": {
                "input_shard_count": len(inputs),
                "map_task_count": len(map_columns),
                "shuffle_segment_count": len(contributing),
                "total_attempts": len(events),
                "failed_attempts": sum(1 for e in events if e["status"] in (C.ATTEMPT_FAILED, C.ATTEMPT_LOST)),
                "speculative_attempts": sum(1 for e in events if e.get("speculative")),
                "nodes_visited": len(visited),
                "workers": sorted(visited),
            },
        }

    # ------------------------------------------------------------------
    # Trace assembly helpers
    # ------------------------------------------------------------------
    def _resolve_partition_name(self, doc: dict, partition: Any, key: Optional[str]) -> Optional[str]:
        results = doc.get("results", {})
        if partition not in (None, ""):
            p = str(partition)
            if p.isdigit():
                return partition_name(int(p))
            if p.startswith("part-"):
                return p.split(".", 1)[0]
            if p.startswith(("m-", "r-")):
                node = (doc.get("tasks") or {}).get(p)
                if node and node.get("kind") == C.TASK_REDUCE:
                    return partition_name(int(node.get("partition", 0)))
        if key is not None:
            for pname, info in results.items():
                if key in set(info.get("keys", [])):
                    return pname
        if partition in (None, "") and key is None and results:
            return sorted(results, key=lambda n: results[n].get("partition", 0))[0]
        return None

    def _worker_name(self, registry, worker_id: str) -> str:
        if not worker_id or registry is None:
            return worker_id or "-"
        w = registry.get(worker_id)
        return w.name if w else worker_id

    def _shard_view(self, shard: dict, registry) -> dict:
        return {
            "shard_id": shard.get("shard_id"),
            "index": shard.get("index", 0),
            "records": shard.get("records", 0),
            "map_task_id": shard.get("map_task_id", ""),
        }

    def _all_inputs(self, doc: dict, registry) -> list[dict]:
        return [self._shard_view(s, registry) for s in doc.get("input_shards", [])]

    def _task_column(self, job_id: str, doc: dict, task_id: str, registry) -> dict:
        node = (doc.get("tasks") or {}).get(task_id, {})
        attempts = []
        for a in node.get("attempts", []):
            attempts.append({
                "attempt_no": a.get("attempt_no", 0),
                "worker_id": a.get("worker_id", ""),
                "worker_name": self._worker_name(registry, a.get("worker_id", "")),
                "status": a.get("status", ""),
                "speculative": bool(a.get("speculative")),
                "dispatched_ms": a.get("dispatched_ms", 0),
                "started_ms": a.get("started_ms", 0),
                "finished_ms": a.get("finished_ms", 0),
                "error": a.get("error", ""),
                "is_winner": a.get("attempt_no") == node.get("winning_attempt_no", 0),
            })
        attempts.sort(key=lambda a: a.get("attempt_no", 0))
        return {
            "task_id": task_id,
            "kind": node.get("kind", ""),
            "index": node.get("index", 0),
            "input_shard": node.get("input_shard", ""),
            "partition": node.get("partition"),
            "winning_attempt_no": node.get("winning_attempt_no", 0),
            "winning_worker_id": node.get("winning_worker_id", ""),
            "winning_worker_name": self._worker_name(registry, node.get("winning_worker_id", "")),
            "attempt_count": len(attempts),
            "attempts": attempts,
        }

    def _shuffle_view(self, pname: str, part_doc: dict, contributing: list[dict],
                      registry) -> dict:
        return {
            "partition": part_doc.get("partition"),
            "partition_name": pname,
            "status": part_doc.get("status", ""),
            "reduce_task_id": part_doc.get("reduce_task_id", ""),
            "num_sources": len(part_doc.get("sources", [])),
            "num_contributing": len(contributing),
            "total_bytes": sum(int(s.get("bytes", 0)) for s in part_doc.get("sources", [])),
            "segments": [
                {
                    "map_task_id": c["map_task_id"],
                    "worker_id": c["worker_id"],
                    "worker_name": c["worker_name"],
                    "bytes": c["bytes"],
                    "had_key": c["had_key"],
                    "definitive": c["definitive"],
                    "fingerprint_truncated": c["fingerprint_truncated"],
                    "fingerprint_key_count": c["fingerprint_key_count"],
                }
                for c in contributing
            ],
        }

    def _resolve_result(self, job_id: str, doc: dict, pname: str, registry,
                        key: Optional[str] = None) -> dict:
        info = doc.get("results", {}).get(pname, {})
        return {
            "partition": info.get("partition"),
            "partition_name": pname,
            "reduce_task_id": info.get("reduce_task_id", ""),
            "worker_id": info.get("worker_id", ""),
            "worker_name": self._worker_name(registry, info.get("worker_id", "")),
            "count": info.get("count", 0),
            "written_ms": info.get("written_ms", 0),
            "key": key,
            "keys": info.get("keys", []),
        }

    def _events_timeline(self, columns: list[dict]) -> list[dict]:
        """Flat chronological list of every attempt of the involved tasks."""
        events: list[dict] = []
        for col in columns:
            for a in col.get("attempts", []):
                events.append({
                    "task_id": col.get("task_id", ""),
                    "kind": col.get("kind", ""),
                    "attempt_no": a["attempt_no"],
                    "worker_id": a["worker_id"],
                    "worker_name": a["worker_name"],
                    "status": a["status"],
                    "speculative": a["speculative"],
                    "is_winner": a["is_winner"],
                    "dispatched_ms": a["dispatched_ms"],
                    "started_ms": a["started_ms"],
                    "finished_ms": a["finished_ms"],
                    "error": a["error"],
                })
        events.sort(key=lambda e: (e.get("dispatched_ms") or 0, e.get("attempt_no") or 0))
        return events
