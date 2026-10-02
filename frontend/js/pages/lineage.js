/* 数据血缘 Data Lineage — trace one result back through shards, map attempts,
   shuffle merge, reduce attempts and the final partition. */
Components.init('lineage');
const C = Components;

let currentJob = '';
let overview = null;
let currentPartition = '';
let preselect = (() => {
  const q = new URLSearchParams(location.search);
  return { job: q.get('job') || '', partition: q.get('partition') || '', key: q.get('key') || '' };
})();

const ATTEMPT_LABEL = {
  assigned: '已分配', running: '运行中', succeeded: '成功',
  failed: '失败', lost: '失联丢失', cancelled: '已取消(推测落败)',
};
const ATTEMPT_CLS = {
  assigned: 'muted', running: 'run', succeeded: 'good',
  failed: 'bad', lost: 'bad', cancelled: 'muted',
};

// --------------------------------------------------------------------------
function el(id) { return document.getElementById(id); }

function workerCell(name, id) {
  if (!id) return '<span class="muted">-</span>';
  return `<b>${C.esc(name || id)}</b><div class="mono small muted">${C.esc(id)}</div>`;
}

function attemptChips(col) {
  const chips = (col.attempts || []).map(a => {
    const cls = ATTEMPT_CLS[a.status] || 'muted';
    const win = a.is_winner ? ' <span class="badge good">采用 winner</span>' : '';
    const spec = a.speculative ? ' <span class="badge warn">推测</span>' : '';
    const err = a.error ? ` title="${C.esc(a.error)}"` : '';
    return `<div class="att-chip ${cls}"${err}>
        <span class="mono">#${a.attempt_no}</span>
        <span class="badge ${cls}">${C.esc(ATTEMPT_LABEL[a.status] || a.status)}</span>
        <span>${C.esc(a.worker_name || a.worker_id || '-')}</span>${spec}${win}
      </div>`;
  }).join('');
  return `<div class="att-list">${chips || C.empty('无执行尝试')}</div>`;
}

function nodeCard(title, subtitle, body, extraCls) {
  return `<div class="ln-node ${extraCls || ''}">
      <div class="ln-node-title">${title}</div>
      <div class="ln-node-sub muted small">${subtitle}</div>
      <div class="ln-node-body">${body}</div>
    </div>`;
}

const ARROW = '<div class="ln-arrow">→</div>';

// --------------------------------------------------------------------------
function renderChain(d) {
  const host = el('chain');
  if (!d.found) { host.innerHTML = C.empty('选择分区与 Key 后展示血缘链路'); return; }

  const inputs = (d.inputs || []).map(s => nodeCard(
    `<span class="mono">${C.esc(s.shard_id)}</span>`,
    `输入分片 · ${C.fmtNum(s.records)} 行`,
    `→ <span class="mono">${C.esc(s.map_task_id)}</span>`,
    'ln-input',
  )).join('');

  const mapNodes = (d.map_attempts || []).map(m => nodeCard(
    `<span class="mono">${C.esc(m.task_id)}</span>`,
    `Map 任务 · ${m.attempt_count} 次尝试`,
    `<div class="small">采用节点：${workerCell(m.winning_worker_name, m.winning_worker_id)}</div>${attemptChips(m)}`,
    'ln-map',
  )).join('');

  const sh = d.shuffle || {};
  const shuffleNode = nodeCard(
    `<span class="mono">${C.esc(sh.partition_name || '-')}</span>`,
    `Shuffle 拉取合并 · ${sh.num_contributing || 0}/${sh.num_sources || 0} 个源含此 Key`,
    `<div class="small">传输量：<b>${C.fmtBytes(sh.total_bytes)}</b></div>
     <div class="small muted">外部排序合并 external merge-sort</div>`,
    'ln-shuffle',
  );

  const r = d.reduce || {};
  const reduceNode = nodeCard(
    `<span class="mono">${C.esc(r.task_id || '-')}</span>`,
    `Reduce 任务 · ${r.attempt_count || 0} 次尝试`,
    `<div class="small">采用节点：${workerCell(r.winning_worker_name, r.winning_worker_id)}</div>${attemptChips(r)}`,
    'ln-reduce',
  );

  const o = d.output || {};
  const outNode = nodeCard(
    `<span class="mono">${C.esc(o.partition_name || '-')}</span>`,
    `最终结果分区 · ${C.fmtNum(o.count)} 条结果`,
    `<div class="small">写入节点：${workerCell(o.worker_name, o.worker_id)}</div>
     ${o.key ? `<div class="small">Key：<b class="mono">${C.esc(o.key)}</b></div>` : ''}
     <div class="small muted">${C.fmtTime(o.written_ms)}</div>`,
    'ln-output',
  );

  host.innerHTML =
    `<div class="ln-col"><div class="ln-col-head">输入分片 Input</div><div class="ln-stack">${inputs || C.empty('-')}</div></div>` +
    ARROW +
    `<div class="ln-col"><div class="ln-col-head">Map 任务（含重试）</div><div class="ln-stack">${mapNodes || C.empty('-')}</div></div>` +
    ARROW +
    `<div class="ln-col"><div class="ln-col-head">Shuffle 合并</div><div class="ln-stack">${shuffleNode}</div></div>` +
    ARROW +
    `<div class="ln-col"><div class="ln-col-head">Reduce 任务</div><div class="ln-stack">${reduceNode}</div></div>` +
    ARROW +
    `<div class="ln-col"><div class="ln-col-head">最终分区 Output</div><div class="ln-stack">${outNode}</div></div>`;
}

// --------------------------------------------------------------------------
function renderSegments(d) {
  const segs = (d.shuffle && d.shuffle.segments) || [];
  el('segments').innerHTML = segs.length ? C.table([
    { key: 'map_task_id', label: 'Map 任务', render: x => `<span class="mono">${C.esc(x.map_task_id)}</span>` },
    { key: 'worker_name', label: '拉取节点 Node', render: x => C.esc(x.worker_name || x.worker_id) },
    { key: 'bytes', label: '传输量', render: x => C.fmtBytes(x.bytes), num: true },
    { key: 'had_key', label: '贡献此 Key', render: x =>
        x.had_key === true ? '<span class="badge good">是 yes</span>'
        : x.had_key === false ? '<span class="badge muted">否 no</span>'
        : '<span class="badge warn">可能 maybe *</span>' },
  ], segs) : C.empty('暂无 No merge data');
}

function renderTimeline(d) {
  const evs = (d.events || []).map(e => ({ ...e, _at: e.finished_ms || e.started_ms || e.dispatched_ms }));
  el('timeline').innerHTML = evs.length ? C.table([
    { key: 'dispatched_ms', label: '时间', render: x => C.fmtTime(x.dispatched_ms) },
    { key: 'task_id', label: '任务', render: x => `<span class="mono">${C.esc(x.task_id)}</span>` },
    { key: 'attempt_no', label: '#', num: true, render: x => x.attempt_no },
    { key: 'worker_name', label: '节点 Node', render: x => C.esc(x.worker_name || x.worker_id) },
    { key: 'status', label: '结果', render: x => {
        const cls = ATTEMPT_CLS[x.status] || 'muted';
        const spec = x.speculative ? ' <span class="badge warn">推测</span>' : '';
        const win = x.is_winner ? ' <span class="badge good">采用</span>' : '';
        return `<span class="badge ${cls}">${C.esc(ATTEMPT_LABEL[x.status] || x.status)}</span>${spec}${win}`;
    } },
    { key: 'error', label: '原因 Error', render: x => x.error ? `<span class="small bad-text">${C.esc(x.error)}</span>` : '<span class="muted">-</span>' },
  ], evs) : C.empty('暂无执行尝试 No attempts');
}

// --------------------------------------------------------------------------
function renderSummary(d) {
  const s = d.summary || {};
  el('stats').innerHTML = [
    { label: '输入分片 Input shards', value: s.input_shard_count ?? '-' },
    { label: 'Map 来源 Map sources', value: s.map_task_count ?? '-' },
    { label: '合并段 Merge segments', value: s.shuffle_segment_count ?? '-' },
    { label: '总尝试 Attempts', value: s.total_attempts ?? '-' },
    { label: '失败/丢失 Failed+Lost', value: s.failed_attempts ?? 0, cls: s.failed_attempts ? 'bad' : 'good' },
    { label: '推测执行 Speculative', value: s.speculative_attempts ?? 0, cls: s.speculative_attempts ? '' : 'good' },
    { label: '经历节点 Nodes', value: s.nodes_visited ?? '-' },
  ].map(x => `<div class="stat"><div class="label">${x.label}</div><div class="value ${x.cls || ''}">${C.fmtNum(x.value)}</div></div>`).join('');
}

// --------------------------------------------------------------------------
function renderPartitionKeys() {
  const parts = (overview && overview.partitions) || [];
  el('partition-keys').innerHTML = parts.length ? C.table([
    { key: 'partition_name', label: '分区 Partition', render: x =>
        `<a href="#" class="ln-pick" data-p="${C.esc(x.partition_name)}"><span class="mono">${C.esc(x.partition_name)}</span></a>` },
    { key: 'reduce_task_id', label: 'Reduce 任务', render: x => `<span class="mono">${C.esc(x.reduce_task_id)}</span>` },
    { key: 'worker_name', label: '写入节点 Node', render: x => C.esc(x.worker_name || x.worker_id) },
    { key: 'count', label: 'Key 数', render: x => C.fmtNum(x.count), num: true },
    { key: 'keys', label: 'Key（点击追踪）', render: x =>
        (x.keys || []).slice(0, 40).map(k =>
          `<a href="#" class="ln-key" data-p="${C.esc(x.partition_name)}" data-k="${C.esc(k)}"><code>${C.esc(k)}</code></a>`
        ).join(' ') + ((x.keys || []).length > 40 ? ` <span class="muted small">+${x.keys.length - 40}…</span>` : '') },
  ], parts) : C.empty('作业尚未产出结果 No results yet — 作业可能还在运行');

  el('partition-keys').querySelectorAll('.ln-key').forEach(a =>
    a.addEventListener('click', (ev) => {
      ev.preventDefault();
      el('partition-select').value = a.dataset.p;
      el('key-input').value = a.dataset.k;
      doTrace();
    }));
  el('partition-keys').querySelectorAll('.ln-pick').forEach(a =>
    a.addEventListener('click', (ev) => {
      ev.preventDefault();
      el('partition-select').value = a.dataset.p;
      el('key-input').value = '';
      doTrace();
    }));
}

// --------------------------------------------------------------------------
function populateSelectors() {
  const parts = (overview && overview.partitions) || [];
  el('partition-select').innerHTML =
    '<option value="">选择分区…</option>' +
    parts.map(p => `<option value="${C.esc(p.partition_name)}">${C.esc(p.partition_name)} · ${C.fmtNum(p.count)} keys · ${C.esc(p.worker_name || p.worker_id || '')}</option>`).join('');
  el('key-list').innerHTML = parts.map(p =>
    (p.keys || []).map(k => `<option value="${C.esc(k)}"></option>`).join('')).join('');
  if (parts.length && !currentPartition) { el('partition-select').value = parts[0].partition_name; }
  else if (currentPartition) { el('partition-select').value = currentPartition; }
}

// --------------------------------------------------------------------------
async function loadOverview() {
  try {
    overview = await API.get('/api/jobs/' + currentJob + '/lineage');
  } catch (e) { overview = null; return; }
  populateSelectors();
  renderPartitionKeys();
}

async function doTrace() {
  const partition = el('partition-select').value;
  const key = el('key-input').value.trim();
  currentPartition = partition;
  const qs = new URLSearchParams({ trace: '1' });
  if (partition) qs.set('partition', partition);
  if (key) qs.set('key', key);

  let d;
  try { d = await API.get('/api/jobs/' + currentJob + '/lineage?' + qs.toString()); }
  catch (e) { return; }

  const nf = el('notfound');
  if (!d.available) {
    nf.style.display = 'block';
    nf.textContent = d.reason || '暂无血缘数据';
    el('chain').innerHTML = ''; el('segments').innerHTML = '';
    el('timeline').innerHTML = ''; el('stats').innerHTML = '';
    return;
  }
  if (d.found === false) {
    nf.style.display = 'block';
    nf.innerHTML = `<b>${C.esc(d.reason || 'Key 未找到')}</b>` +
      (d.result_keys && d.result_keys.length
        ? `<div class="muted small mt">该分区可用 keys：${d.result_keys.slice(0, 50).map(k => `<code>${C.esc(k)}</code>`).join(' ')}</div>`
        : '');
    return;
  }
  nf.style.display = 'none';
  if (d.found === true) {
    renderSummary(d);
    renderChain(d);
    renderSegments(d);
    renderTimeline(d);
    if (d.output && d.output.partition_name) currentPartition = d.output.partition_name;
  }
}

// --------------------------------------------------------------------------
el('trace-btn').addEventListener('click', doTrace);
el('key-input').addEventListener('keydown', (e) => { if (e.key === 'Enter') doTrace(); });
el('partition-select').addEventListener('change', () => { currentPartition = el('partition-select').value; });

C.jobPicker('job-picker', async (id) => {
  currentJob = id;
  await loadOverview();
  if (preselect.partition) {
    el('partition-select').value = preselect.partition;
    currentPartition = preselect.partition;
  }
  if (preselect.key) el('key-input').value = preselect.key;
  const hadPreselect = !!(preselect.partition || preselect.key);
  preselect = { job: '', partition: '', key: '' };
  if (hadPreselect || !currentPartition) await doTrace();
}, { value: preselect.job });
C.poll(async () => {
  if (!currentJob) return;
  const prevParts = JSON.stringify((overview && overview.partitions || []).map(p => [p.partition_name, p.count]));
  await loadOverview();
  const newParts = JSON.stringify((overview && overview.partitions || []).map(p => [p.partition_name, p.count]));
  if (prevParts !== newParts) await doTrace();
}, 4000).start();
