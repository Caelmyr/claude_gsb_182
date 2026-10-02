"""Data lineage: durable provenance records and result trace reconstruction.

Every result record produced by a reducer is traceable back to the input
shards that fed it.  Tracing a MapReduce result across a live cluster is
tricky because the chain is neither straight nor short:

* one reduce result is produced from **many** map outputs merged together, and
  each map output derives from one input shard — the chain fans in;
* a task may be retried on a different node, may lose a speculative race, or
  may be reassigned after its worker dies — the chain must keep *every* attempt
  rather than overwrite the winning one, or it breaks exactly at failure time.

The lineage store therefore keeps three append-only document families per job
(all atomic JSON writes, reused after a Master restart):

* ``map-output/{map_task_id}.json``   — winning map attempt, the worker it ran
  on, its input shard, and the per-partition ``{key: count}`` contribution map
  that pins an output key to the shards that actually contributed to it;
* ``reduce-input/{reduce_task_id}.json`` — winning reduce attempt, its output
  partition/keys and the merge sources (map task + worker + fetched count)
  pulled during the successful shuffle;
* ``exec/{task_id}__{seq}.json``      — one document per *attempt* (including
  failed attempts, speculative copies and worker-death reassignments), so the
  full node/attempt trail is preserved with the winning attempt flagged.

``trace_result`` joins those documents into the single chain rendered on the
results page: input shards → map attempts/shuffle transfers → reduce attempts
→ final partition.
"""

from __future__ import annotations

import threading
from typing import Any, Optional

from backend.common import constants as C
from backend.common.hashing import partition_for
from backend.common.ids import partition_name, shard_id
from backend.common.jsonutil import now_ms
from backend.common.storage import Storage, list_files, read_json
from backend.master.job_manager import JobManager
from backend.master.registry import WorkerRegistry


class LineageStore:
    def __init__(self, storage: Storage, job_manager: JobManager,
                 registry: WorkerRegistry) -> None:
        self.storage = storage
        self.job_manager = job_manager
        self.registry = registry
        self._lock = threading.Lock()
        # Per-task monotonic execution sequence; survives within the process
        # and is only used to order attempts for display (attempt number is the
        # durable retry counter stored on the Task record).
        self._seq: dict[str, int] = {}

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------
    def _root(self, job_id: str) -> list[str]:
        return ["jobs", job_id, "lineage"]

    def _map_path(self, job_id: str, task_id: str) -> list[str]:
        return [*self._root(job_id), "map-output", f"{task_id}.json"]

    def _reduce_path(self, job_id: str, task_id: str) -> list[str]:
        return [*self._root(job_id), "reduce-input", f"{task_id}.json"]

    def _exec_path(self, job_id: str, task_id: str, seq: int) -> list[str]:
        return [*self._root(job_id), "exec", f"{task_id}__{seq:03d}.json"]

    # ------------------------------------------------------------------
    # Attempt sequence
    # ------------------------------------------------------------------
    def next_seq(self, task_id: str) -> int:
        """Allocate the next execution slot for a task (dispatch = one slot)."""
        with self._lock:
            seq = self._seq.get(task_id, 0) + 1
            self._seq[task_id] = seq
            return seq

    # ------------------------------------------------------------------
    # Execution (attempt) trail
    # ------------------------------------------------------------------
    def record_exec(
        self,
        job_id: str,
        task_id: str,
        seq: int,
        attempt: int,
        worker_id: str,
        kind: str,
        speculative: bool = False,
        status: str = "dispatched",
        error: str = "",
        dispatched_ms: Optional[int] = None,
        started_ms: int = 0,
        finished_ms: int = 0,
        records_processed: int = 0,
        records_emitted: int = 0,
        input_shard: str = "",
        partition: int = -1,
        extra: Optional[dict] = None,
    ) -> dict:
        """Write (or rewrite) the document for one task execution attempt."""
        doc = {
            "job_id": job_id,
            "task_id": task_id,
            "kind": kind,
            "seq": seq,
            "attempt": attempt,
            "worker_id": worker_id or "",
            "worker_name": self._worker_name(worker_id),
            "speculative": bool(speculative),
            "status": status,
            "error": error or "",
            "dispatched_ms": dispatched_ms or now_ms(),
            "started_ms": started_ms,
            "finished_ms": finished_ms,
            "records_processed": int(records_processed or 0),
            "records_emitted": int(records_emitted or 0),
            "input_shard": input_shard,
            "partition": partition,
            "updated_ms": now_ms(),
        }
        if extra:
            doc.update(extra)
        self.storage.write(doc, *self._exec_path(job_id, task_id, seq))
        return doc

    def mark_exec_failed(self, job_id: str, task_id: str, seq: int,
                         attempt: int, error: str) -> None:
        """Stamp the dispatch document for an attempt as failed/reassigned."""
        path = self._exec_path(job_id, task_id, seq)
        doc = self.storage.read(*path, default=None)
        if doc is None:
            # Master restarted mid-flight: reconstruct a minimal document so the
            # retry trail still has a node instead of a broken edge.
            task = self.job_manager.get_task(job_id, task_id)
            doc = self.record_exec(
                job_id, task_id, seq, attempt,
                worker_id=(task.worker_id if task else "") or "",
                kind=(task.kind if task else ""),
                dispatched_ms=0,
                input_shard=(task.input_shard if task else ""),
                partition=(task.partition if task else -1),
            )
        doc["status"] = "failed"
        doc["error"] = error or ""
        doc["finished_ms"] = now_ms()
        doc["updated_ms"] = now_ms()
        self.storage.write(doc, *path)

    # ------------------------------------------------------------------
    # Winning-attempt provenance
    # ------------------------------------------------------------------
    def record_map_output(self, job_id: str, task: Any, payload: dict,
                          seq: int, attempt: int) -> None:
        """Persist the winning map attempt: shard -> worker -> partition keys."""
        partition_keys = payload.get("partition_keys", {}) or {}
        # Normalise JSON object keys ("0", "1", ...) and keep int counts.
        normalised: dict[str, dict[str, int]] = {}
        total_contrib = 0
        for p, keys in partition_keys.items():
            if not isinstance(keys, dict):
                continue
            clean = {str(k): int(v) for k, v in keys.items() if int(v) > 0}
            if clean:
                normalised[str(p)] = clean
                total_contrib += sum(clean.values())
        doc = {
            "job_id": job_id,
            "task_id": task.task_id,
            "task_index": task.index,
            "kind": C.TASK_MAP,
            "input_shard": task.input_shard,
            "shard_index": task.index,
            "worker_id": payload.get("worker_id", ""),
            "worker_name": self._worker_name(payload.get("worker_id", "")),
            "seq": seq,
            "attempt": attempt,
            "records_processed": int(payload.get("records_processed", 0)),
            "records_emitted": int(payload.get("records_emitted", 0)),
            "finished_ms": now_ms(),
            "partition_keys": normalised,
            "total_contributions": total_contrib,
            "partition_sizes": payload.get("partition_sizes", {}) or {},
        }
        self.storage.write(doc, *self._map_path(job_id, task.task_id))

    def record_reduce_input(self, job_id: str, task: Any, payload: dict,
                            fetch_plan: list[dict], seq: int, attempt: int) -> None:
        """Persist the winning reduce attempt: merge sources and output keys."""
        sources: list[dict] = []
        fetched_total = 0
        source_counts = payload.get("source_counts", {}) or {}
        for src in fetch_plan:
            map_task_id = src.get("map_task_id", "")
            count = int(source_counts.get(map_task_id, 0) or 0)
            worker_id = src.get("worker_id", "")
            sources.append({
                "map_task_id": map_task_id,
                "worker_id": worker_id,
                "worker_name": self._worker_name(worker_id),
                "worker_url": src.get("worker_url", ""),
                "partition": task.partition,
                "records_fetched": count,
            })
            fetched_total += count

        output_keys: dict[str, int] = {}
        for rec in payload.get("results", []) or []:
            if isinstance(rec, dict) and "key" in rec:
                output_keys[str(rec["key"])] = int(rec.get("count", 1) or 1)
        doc = {
            "job_id": job_id,
            "task_id": task.task_id,
            "task_index": task.index,
            "kind": C.TASK_REDUCE,
            "partition": task.partition,
            "partition_name": partition_name(task.partition),
            "worker_id": payload.get("worker_id", ""),
            "worker_name": self._worker_name(payload.get("worker_id", "")),
            "seq": seq,
            "attempt": attempt,
            "sources": sources,
            "num_sources": len(sources),
            "records_fetched": fetched_total,
            "records_emitted": int(payload.get("records_emitted", len(output_keys))),
            "output_keys": output_keys,
            "finished_ms": now_ms(),
        }
        self.storage.write(doc, *self._reduce_path(job_id, task.task_id))

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------
    def map_outputs(self, job_id: str) -> list[dict]:
        out: list[dict] = []
        root = self.storage.path(*self._root(job_id), "map-output")
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc:
                out.append(doc)
        out.sort(key=lambda d: d.get("task_index", d.get("task_id", "")))
        return out

    def reduce_inputs(self, job_id: str) -> list[dict]:
        out: list[dict] = []
        root = self.storage.path(*self._root(job_id), "reduce-input")
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc:
                out.append(doc)
        out.sort(key=lambda d: d.get("partition", 0))
        return out

    def executions(self, job_id: str, task_id: str = "") -> list[dict]:
        out: list[dict] = []
        root = self.storage.path(*self._root(job_id), "exec")
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc and (not task_id or doc.get("task_id") == task_id):
                out.append(doc)
        out.sort(key=lambda d: (d.get("task_id", ""), d.get("seq", 0)))
        return out

    def _winning_exec(self, job_id: str, task_id: str) -> Optional[dict]:
        execs = self.executions(job_id, task_id)
        succeeded = [e for e in execs if e.get("status") == C.TASK_SUCCEEDED]
        return succeeded[-1] if succeeded else (execs[-1] if execs else None)

    # ------------------------------------------------------------------
    # Trace reconstruction
    # ------------------------------------------------------------------
    def trace_result(self, job_id: str, key: str = "", partition: Optional[int] = None) -> dict:
        """Build the full lineage chain for one result key (or one partition).

        Chain (left -> right): input shards -> winning map attempts (with the
        shuffle transfer edge) -> winning reduce attempt -> result partition.
        Every failed / speculative attempt is attached to its task under
        ``attempts`` so retries and node changes never break the trail.
        """
        job = self.job_manager.get_job(job_id)
        reduce_docs = self.reduce_inputs(job_id)

        target: Optional[dict] = None
        if partition is not None:
            target = next((d for d in reduce_docs if d.get("partition") == int(partition)), None)
        if target is None and key:
            target = next((d for d in reduce_docs if str(key) in (d.get("output_keys") or {})), None)
        if target is None and key and reduce_docs:
            # The key is missing from every reduce output: it *should* live in
            # its deterministic hash partition. Open that partition's lineage so
            # the chain shows where the key was lost instead of 404-ing.
            num_r = len(reduce_docs)
            p_expected = partition_for(key, num_r)
            target = next((d for d in reduce_docs if d.get("partition") == p_expected), reduce_docs[0])
        if target is None and not key and partition is None and reduce_docs:
            target = reduce_docs[0]

        warnings: list[str] = []
        if target is None:
            return {
                "job_id": job_id,
                "found": False,
                "key": key,
                "partition": partition,
                "warnings": ["lineage-unavailable: 作业尚未完成或没有匹配的结果 (job not finished / no match)"],
                "stages": [],
            }

        p = int(target["partition"])
        map_docs = {d["task_id"]: d for d in self.map_outputs(job_id)}
        faults_by_task = self._fault_index(job_id)

        # ---- merge (shuffle) layer: only sources that fed this partition ----
        merge_sources: list[dict] = []
        contributing_shards: dict[str, dict] = {}
        key_present_anywhere = False
        for src in target.get("sources", []):
            md = map_docs.get(src["map_task_id"])
            key_count = 0
            contributed = False
            if md is not None:
                keys = (md.get("partition_keys") or {}).get(str(p), {}) or {}
                key_count = int(keys.get(str(key), 0)) if key else int(src.get("records_fetched", 0))
                key_present_anywhere = key_present_anywhere or (str(key) in keys)
                contributed = key_count > 0 if key else int(src.get("records_fetched", 0)) > 0
                shard_id_ = md.get("input_shard") or shard_id("in", md.get("task_index", 0))
                if contributed:
                    contributing_shards[shard_id_] = {
                        "shard_id": shard_id_,
                        "index": md.get("task_index", 0),
                        "map_task_id": md["task_id"],
                        "map_worker_id": md.get("worker_id", ""),
                        "map_worker_name": md.get("worker_name", ""),
                        "contributions": key_count,
                        "records_processed": md.get("records_processed", 0),
                        "attempt": md.get("attempt", 1),
                        "seq": md.get("seq", 0),
                    }
            merge_sources.append({
                "map_task_id": src["map_task_id"],
                "map_worker_id": src.get("worker_id", ""),
                "map_worker_name": src.get("worker_name", ""),
                "records_fetched": int(src.get("records_fetched", 0)),
                "key_contributions": key_count,
                "contributed": bool(contributed),
                "available": md is not None,
            })

        # ---- input shards in deterministic shard order ----
        shard_nodes = [contributing_shards[k] for k in sorted(contributing_shards)]
        if key and not key_present_anywhere:
            warnings.append(
                f"key-not-in-mappers: 所有 Map 输出的分区 {partition_name(p)} 中都没有键 {key!r}；"
                "若结果异常，问题出在 Map/输入侧 (the key never reached the shuffle)"
            )
        elif key and str(key) not in (target.get("output_keys") or {}):
            warnings.append(
                f"key-not-in-output: 键 {key!r} 进入了合并输入但未出现在 Reduce 输出中，"
                "请检查 Reduce/合并环节 (entered the merged input but missing from reduce output)"
            )
        if key and key_present_anywhere and not shard_nodes:
            warnings.append(
                "key-dropped-in-shuffle: 键存在于 Map 输出但未被成功的 Reduce 拉取/合并，"
                "请检查 Shuffle 拉取与合并环节 (present in map output, missing from the merged reduce input)"
            )

        # ---- attempt trails (retries / reassignments / speculation) ----
        map_attempts = {
            tid: self._attempt_view(job_id, tid, faults_by_task.get(tid, []))
            for tid in {s["map_task_id"] for s in target.get("sources", [])}
        }
        reduce_attempts = self._attempt_view(job_id, target["task_id"],
                                             faults_by_task.get(target["task_id"], []))

        partition_doc = self._result_partition_doc(job_id, p)
        stages = [
            {
                "stage": C.STAGE_INPUT,
                "label": C.stage_label(C.STAGE_INPUT),
                "shards": shard_nodes,
            },
            {
                "stage": C.STAGE_MAP,
                "label": C.stage_label(C.STAGE_MAP),
                "attempts_by_task": map_attempts,
            },
            {
                "stage": C.STAGE_SHUFFLE,
                "label": C.stage_label(C.STAGE_SHUFFLE),
                "partition": p,
                "partition_name": partition_name(p),
                "sources": merge_sources,
                "num_sources": len(merge_sources),
                "num_contributing": len(shard_nodes),
                "records_fetched": int(target.get("records_fetched", 0)),
            },
            {
                "stage": C.STAGE_REDUCE,
                "label": C.stage_label(C.STAGE_REDUCE),
                "task_id": target["task_id"],
                "attempts": reduce_attempts,
                "output_keys": target.get("output_keys", {}),
                "key_count": int((target.get("output_keys") or {}).get(str(key), 0)) if key else None,
            },
            {
                "stage": "output",
                "label": "最终分区 Output",
                "partition": p,
                "partition_name": partition_name(p),
                "record": partition_doc,
            },
        ]
        return {
            "job_id": job_id,
            "found": True,
            "key": key,
            "partition": p,
            "partition_name": partition_name(p),
            "reduce_task_id": target["task_id"],
            "reduce_worker_id": target.get("worker_id", ""),
            "reduce_worker_name": target.get("worker_name", ""),
            "warnings": warnings,
            "stages": stages,
        }

    def summary(self, job_id: str) -> dict:
        """Job-wide lineage overview backing the results-page stats."""
        maps = self.map_outputs(job_id)
        reduces = self.reduce_inputs(job_id)
        execs = self.executions(job_id)
        total_attempts = len(execs)
        failed_attempts = sum(1 for e in execs if e.get("status") == "failed")
        speculative = sum(1 for e in execs if e.get("speculative"))
        return {
            "map_outputs": len(maps),
            "reduce_outputs": len(reduces),
            "total_attempts": total_attempts,
            "failed_attempts": failed_attempts,
            "speculative_attempts": speculative,
            "retried_tasks": sorted({e["task_id"] for e in execs if e.get("status") == "failed"}),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _attempt_view(self, job_id: str, task_id: str, faults: list[dict]) -> dict:
        execs = self.executions(job_id, task_id)
        task = self.job_manager.get_task(job_id, task_id)
        attempts = []
        for e in execs:
            attempts.append({
                "seq": e.get("seq", 0),
                "attempt": e.get("attempt", 1),
                "worker_id": e.get("worker_id", ""),
                "worker_name": e.get("worker_name", ""),
                "speculative": bool(e.get("speculative")),
                "status": e.get("status", ""),
                "error": e.get("error", ""),
                "dispatched_ms": e.get("dispatched_ms", 0),
                "finished_ms": e.get("finished_ms", 0),
                "records_processed": e.get("records_processed", 0),
                "records_emitted": e.get("records_emitted", 0),
            })
        winning_seq = None
        win = self._winning_exec(job_id, task_id)
        if win:
            winning_seq = win.get("seq")
        return {
            "task_id": task_id,
            "kind": task.kind if task else "",
            "input_shard": task.input_shard if task and task.kind == C.TASK_MAP else "",
            "partition": task.partition if task and task.kind == C.TASK_REDUCE else None,
            "winning_seq": winning_seq,
            "attempts": attempts,
            "faults": faults,
        }

    def _fault_index(self, job_id: str) -> dict[str, list[dict]]:
        index: dict[str, list[dict]] = {}
        root = self.storage.path("jobs", job_id, "faults")
        for path in list_files(root, suffix=".json"):
            doc = read_json(path)
            if doc and doc.get("task_id"):
                index.setdefault(doc["task_id"], []).append({
                    "kind": doc.get("kind", ""),
                    "message": doc.get("message", ""),
                    "worker_id": doc.get("worker_id", ""),
                    "worker_name": self._worker_name(doc.get("worker_id", "")),
                    "attempt": doc.get("attempt", 0),
                    "created_ms": doc.get("created_ms", 0),
                })
        for events in index.values():
            events.sort(key=lambda d: d.get("created_ms", 0))
        return index

    def _result_partition_doc(self, job_id: str, partition: int) -> Optional[dict]:
        pname = partition_name(partition)
        doc = self.storage.read(
            "jobs", job_id, "results", C.STAGE_REDUCE, f"{pname}.json", default=None,
        )
        if not doc:
            return None
        return {
            "partition_name": doc.get("partition_name", pname),
            "task_id": doc.get("task_id", ""),
            "count": doc.get("count", 0),
            "written_ms": doc.get("written_ms", 0),
        }

    def _worker_name(self, worker_id: str) -> str:
        if not worker_id:
            return ""
        worker = self.registry.get(worker_id)
        return worker.name if worker else worker_id
