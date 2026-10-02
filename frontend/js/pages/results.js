/* 结果导出 + 数据血缘 Results & Lineage */
Components.init('results');
const C = Components;

let currentJob = '';
let partitions = [];

async function render() {
  if (!currentJob) return;
  let d;
  try { d = await API.get('/api/jobs/' + currentJob + '/results?limit=100'); } catch (e) { return; }

  document.getElementById('dl-json').href = '/api/jobs/' + currentJob + '/results/download?format=json';
  document.getElementById('dl-csv').href = '/api/jobs/' + currentJob + '/results/download?format=csv';

  const lin = d.lineage || {};
  document.getElementById('stats').innerHTML = [
    { label: '结果记录 Total records', value: C.fmtNum(d.total) },
    { label: '分区数 Partitions', value: d.partitions.length },
    { label: '任务执行总次数 Attempts', value: C.fmtNum(lin.total_attempts) },
    { label: '失败/重试 Failed attempts', value: C.fmtNum(lin.failed_attempts) },
    { label: '推测副本 Speculative', value: C.fmtNum(lin.speculative_attempts) },
    { label: '作业状态 Status', value: d.status },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value">${C.esc(s.value)}</div></div>`).join('');

  document.getElementById('partitions').innerHTML = d.partitions.length
    ? C.table([
        { key: 'partition_name', label: '分区 Partition', render: r => `<span class="mono">${C.esc(r.partition_name)}</span>` },
        { key: 'count', label: '记录数 Count', render: r => C.fmtNum(r.count), num: true },
        { key: 'task_id', label: 'Reduce 任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
      ], d.partitions)
    : C.empty('暂无结果 No results — 作业可能尚未完成');

  partitions = d.partitions || [];
  const psel = document.getElementById('lineage-partition');
  psel.innerHTML = '<option value="">按分区 By partition…</option>' +
    partitions.map(p => `<option value="${p.partition}">${C.esc(p.partition_name)} (${C.fmtNum(p.count)})</option>`).join('');

  const records = d.records || [];
  document.getElementById('preview').innerHTML = records.length
    ? C.table([
        { key: 'key', label: 'Key (点击行追溯 click row →)', render: r => `<b>${C.esc(r.key)}</b>` },
        { key: 'value', label: 'Value', render: r => C.valueCell(Object.fromEntries(
            Object.entries(r).filter(([k]) => !k.startsWith('_')))) },
        { key: '_partition_name', label: '分区 Partition', render: r => `<span class="mono">${C.esc(r._partition_name || '')}</span>` },
        { key: '_trace', label: '', render: () => `<span class="muted small">追溯 ›</span>` },
      ], records, { onClick: true })
    : C.empty('暂无结果 No results');

  // Click a result row -> trace that key.
  const previewEl = document.getElementById('preview');
  previewEl.querySelectorAll('tbody tr[data-row]').forEach(tr => {
    tr.classList.add('clickable');
    tr.addEventListener('click', () => {
      const i = Number(tr.getAttribute('data-row'));
      const rec = records[i];
      if (!rec) return;
      document.getElementById('lineage-key').value = rec.key;
      document.getElementById('lineage-partition').value = rec._partition != null ? String(rec._partition) : '';
      traceLineage({ key: rec.key });
    });
  });
}

// ---------------------------------------------------------------------------
// Lineage trace view
// ---------------------------------------------------------------------------
function badgeForStatus(status) {
  const map = {
    SUCCEEDED: 'good', failed: 'bad', worker_lost: 'bad',
    speculative_lost: 'warn', ASSIGNED: 'aqua', RUNNING: 'run', RETRYING: 'warn',
  };
  const labels = {
    SUCCEEDED: '成功 won', failed: '失败 failed', worker_lost: '节点失联 worker lost',
    speculative_lost: '推测落败 lost race', ASSIGNED: '已分配 assigned', RUNNING: '运行中 running',
  };
  return `<span class="badge ${map[status] || 'muted'}">${C.esc(labels[status] || status || '-')}</span>`;
}

function nodeHtml({ title, lines, muted }) {
  return `<div class="ln-node${muted ? ' muted' : ''}">
    <div class="ln-node-title mono">${C.esc(title)}</div>
    <div class="ln-meta">${lines.filter(Boolean).map(l => `<div>${l}</div>`).join('')}</div>
  </div>`;
}

function attemptsBlock(view) {
  const atts = view.attempts || [];
  if (!atts.length) return '';
  return `<div class="ln-attempts">` + atts.map(a => {
    const winner = a.seq === view.winning_seq;
    const cls = winner ? 'winner' : (a.status === 'SUCCEEDED' ? '' : a.status);
    return `<div class="ln-attempt ${cls}">
      <div><b>#${a.attempt}</b> · ${C.esc(a.worker_name || a.worker_id || '-')} ${a.speculative ? '·推测 spec' : ''}</div>
      <div>${badgeForStatus(a.status)}</div>
      ${a.error ? `<div class="muted small">${C.esc(a.error)}</div>` : ''}
    </div>`;
  }).join('') + `</div>`;
}

function faultsBlock(faults) {
  if (!faults || !faults.length) return '';
  return faults.map(f =>
    `<div class="small muted">↳ ${C.esc(f.kind)}: ${C.esc(f.message)} @${C.esc(f.worker_name || f.worker_id || '')}</div>`
  ).join('');
}

function renderTrace(t) {
  if (!t.found) {
    return C.empty(t.warnings && t.warnings.length
      ? t.warnings.join('；')
      : '血缘暂不可用 Lineage unavailable — 作业完成后可追溯');
  }
  const byStage = {};
  t.stages.forEach(s => { byStage[s.stage] = s; });

  const header = `<div class="ln-head">
      <span class="ln-title">结果 Key</span>
      <span class="mono" style="font-size:15px"><b>${C.esc(t.key || '(partition)')}</b></span>
      <span class="badge muted">${C.esc(t.partition_name)}</span>
      <span class="small muted">Reduce: <span class="mono">${C.esc(t.reduce_task_id)}</span> @ ${C.esc(t.reduce_worker_name || t.reduce_worker_id)}</span>
    </div>`;

  const warnings = (t.warnings || []).map(w => `<div class="ln-warn">${C.esc(w)}</div>`).join('');

  // Input shards
  const inShards = (byStage.input.shards || []);
  const inputNodes = inShards.length
    ? inShards.map(s => nodeHtml({
        title: s.shard_id,
        lines: [
          `Map: <b>${C.esc(s.map_task_id)}</b> @ ${C.esc(s.map_worker_name || s.map_worker_id)}`,
          `该键贡献 <b>${C.fmtNum(s.contributions)}</b> 条`,
          `分片记录 <b>${C.fmtNum(s.records_processed)}</b> 条`,
          s.attempt > 1 ? `经 ${s.attempt} 次尝试成功` : '',
        ],
      }))
    : [nodeHtml({ title: '无贡献分片', lines: ['该键没有分片贡献 (检查异常诊断)'], muted: true })];

  // Map attempts (retries across nodes)
  const mapViews = byStage.map.attempts_by_task || {};
  const mapAttemptNodes = Object.keys(mapViews).sort().map(tid => {
    const v = mapViews[tid];
    return `<div class="ln-node">
      <div class="ln-node-title mono">${C.esc(tid)}</div>
      <div class="ln-meta">输入 ${C.esc(v.input_shard || '-')} · ${v.attempts.length} 次执行</div>
      ${attemptsBlock(v)}${faultsBlock(v.faults)}
    </div>`;
  }).join('');

  // Shuffle merge
  const sh = byStage.shuffle;
  const shuffleNodes = (sh.sources || []).map(s => nodeHtml({
    title: s.map_task_id,
    muted: !s.contributed,
    lines: [
      `@ ${C.esc(s.map_worker_name || s.map_worker_id || '-')}`,
      s.contributed
        ? `该键拉取 <b>${C.fmtNum(s.key_contributions)}</b> 条`
        : '该键 <b>0</b> 条',
      `分区共拉取 ${C.fmtNum(s.records_fetched)} 条`,
    ],
  })).join('');

  // Reduce attempts
  const rv = byStage.reduce.attempts || {};
  const reduceNode = `<div class="ln-node" style="border-left-color:var(--series-3)">
    <div class="ln-node-title mono">${C.esc(byStage.reduce.task_id)}</div>
    <div class="ln-meta">
      <div>合并 ${C.fmtNum(sh.num_contributing)}/${C.fmtNum(sh.num_sources)} 个 Map 来源，
        共 ${C.fmtNum(sh.records_fetched)} 条 → <b>${C.fmtNum(byStage.reduce.key_count)}</b> 条结果</div>
    </div>
    ${attemptsBlock(rv)}${faultsBlock(rv.faults)}
  </div>`;

  // Output
  const out = byStage.output.record || {};
  const outputNode = nodeHtml({
    title: out.partition_name || t.partition_name,
    lines: [
      `落盘任务 <b>${C.esc(out.task_id || t.reduce_task_id)}</b>`,
      `分区记录数 ${C.fmtNum(out.count)}`,
      out.written_ms ? `写入时间 ${C.fmtTime(out.written_ms)}` : '',
    ],
  });

  const stageCard = (title, body) =>
    `<div class="ln-stage"><div class="ln-head"><span class="ln-title">${title}</span></div>
       <div class="ln-nodes">${body}</div></div>`;
  const arrow = `<div class="ln-arrow">▼</div>`;

  return `<div class="lineage-chain">
    ${header}${warnings}
    ${stageCard('① 输入分片 Input shards (实际贡献该键的分片)', inputNodes.join(''))}
    ${arrow}
    ${stageCard('② Map 处理任务 (含失败重试 / 节点切换 / 推测执行)', mapAttemptNodes)}
    ${arrow}
    ${stageCard('③ Shuffle 拉取与合并 merge (分区 ' + C.esc(t.partition_name) + ')', shuffleNodes)}
    ${arrow}
    ${stageCard('④ Reduce 任务 (合并所有来源 → 结果)', reduceNode)}
    ${arrow}
    ${stageCard('⑤ 最终分区 Output partition', outputNode)}
  </div>`;
}

async function traceLineage({ key = null, partition = null } = {}) {
  if (!currentJob) return;
  const keyVal = key != null ? key : document.getElementById('lineage-key').value.trim();
  const pVal = partition != null ? partition : document.getElementById('lineage-partition').value;
  const params = new URLSearchParams();
  if (keyVal) params.set('key', keyVal);
  if (pVal !== '' && pVal != null) params.set('partition', pVal);
  const host = document.getElementById('lineage');
  host.innerHTML = C.empty('追溯中 Tracing…');
  try {
    const t = await API.get(`/api/jobs/${currentJob}/lineage?${params.toString()}`);
    host.innerHTML = renderTrace(t);
  } catch (e) {
    host.innerHTML = C.empty('追溯失败: ' + e.message);
  }
}

document.getElementById('lineage-go').addEventListener('click', () => traceLineage());
document.getElementById('lineage-key').addEventListener('keydown', e => {
  if (e.key === 'Enter') traceLineage();
});
document.getElementById('lineage-partition').addEventListener('change', e => {
  if (e.target.value !== '') traceLineage({ partition: Number(e.target.value) });
});

C.jobPicker('job-picker', (id) => {
  currentJob = id;
  document.getElementById('lineage').innerHTML = '';
  render();
});
C.poll(render, 3000).start();
