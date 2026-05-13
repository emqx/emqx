import { api } from './api.js';
import { esc, toast, setMsg } from './ui_helpers.js';
import { loadedPipelines, editingPipelineId, pipelineSteps } from './state.js';
import { setSchemaEditorValue } from './schema_editor.js';
import { renderSteps, collectSteps } from './pipeline_steps.js';

export async function loadPipelines() {
  const list = await api('GET', '/pipelines');
  loadedPipelines.length = 0;
  loadedPipelines.push(...list);
  document.getElementById('cnt-pipelines').textContent = list.length;
  const tbody = document.getElementById('pipelines-body');
  if (!list.length) {
    tbody.innerHTML = '<tr class="empty"><td colspan="5">No pipelines registered</td></tr>';
    return;
  }
  tbody.innerHTML = list.map(p => {
    const trigger  = p.trigger?.topic ?? '—';
    const steps    = (p.steps ?? []).map(s =>
      `<span class="tag ${stepClass(s.type)}">${esc(s.id ?? s.type)}</span>`
    ).join(' ');
    const isActive = p.active === true;
    const statusBadge = isActive
      ? '<span class="tag active">● active</span>'
      : '<span class="tag draft">○ draft</span>';
    const toggleLabel = isActive ? 'deactivate' : 'activate';
    return `<tr>
      <td><code>${esc(p.pipeline_id)}</code></td>
      <td style="color:var(--muted)">${esc(trigger)}</td>
      <td>${steps}</td>
      <td>${statusBadge}</td>
      <td><div style="display:flex;gap:4px">
        <button class="btn sm" onclick="togglePipelineActive('${esc(p.pipeline_id)}')">${toggleLabel}</button>
        <button class="btn sm" onclick="editPipeline('${esc(p.pipeline_id)}')">edit</button>
        <button class="btn sm danger" onclick="deletePipeline('${esc(p.pipeline_id)}')">delete</button>
      </div></td>
    </tr>`;
  }).join('');
}

function stepClass(t) {
  if (t === 'llm_loop')      return 'llm';
  if (t === 'call_skill')    return 'publish';
  if (t === 'break')         return 'kv';
  return '';
}

export async function togglePipelineActive(id) {
  const pipeline = loadedPipelines.find(p => p.pipeline_id === id);
  if (!pipeline) return toast('Pipeline not found — try refreshing', 'err');
  const updated = { ...pipeline, active: !pipeline.active };
  try {
    await api('PUT', `/pipelines/${encodeURIComponent(id)}`, updated);
    toast(`Pipeline "${id}" ${updated.active ? 'activated' : 'deactivated'}`, 'ok');
    await loadPipelines();
  } catch(e) {
    toast(e.message, 'err');
  }
}

export async function deletePipeline(id) {
  if (!confirm(`Delete pipeline "${id}"?`)) return;
  try {
    await api('DELETE', `/pipelines/${encodeURIComponent(id)}`);
    toast('Deleted ' + id, 'ok');
    await loadPipelines();
  } catch(e) {
    toast(e.message, 'err');
  }
}

export async function savePipeline() {
  const id    = document.getElementById('pipe-id').value.trim();
  const topic = document.getElementById('pipe-topic').value.trim();
  if (!id)    return setMsg('pipe-msg', 'Pipeline ID is required', true);
  if (!topic) return setMsg('pipe-msg', 'Trigger topic is required', true);
  const steps  = collectSteps();
  const active = document.getElementById('pipe-active').checked;
  const body   = { pipeline_id: id, trigger: { topic }, steps, active };
  try {
    if (editingPipelineId.value) {
      await api('PUT', `/pipelines/${encodeURIComponent(editingPipelineId.value)}`, body);
      toast('Pipeline "' + id + '" updated', 'ok');
    } else {
      await api('POST', '/pipelines', body);
      toast('Pipeline "' + id + '" created', 'ok');
    }
    setMsg('pipe-msg', 'Saved ✓');
    resetPipelineEditor();
    await loadPipelines();
  } catch(e) {
    setMsg('pipe-msg', e.message, true);
  }
}

export function editPipeline(id) {
  const pipeline = loadedPipelines.find(p => p.pipeline_id === id);
  if (!pipeline) return toast('Pipeline not found in cache — try refreshing', 'err');

  editingPipelineId.value = id;

  document.getElementById('pipe-id').value        = pipeline.pipeline_id;
  document.getElementById('pipe-id').readOnly     = true;
  document.getElementById('pipe-id').style.opacity = '0.6';
  document.getElementById('pipe-topic').value     = pipeline.trigger?.topic ?? '';
  document.getElementById('pipe-active').checked  = pipeline.active === true;
  document.getElementById('pipe-card-title').textContent = 'Edit Pipeline: ' + id;
  document.getElementById('pipe-save-btn').textContent   = 'Update Pipeline';
  document.getElementById('pipe-cancel-btn').style.display = '';

  pipelineSteps.length = 0;
  pipelineSteps.push(...(pipeline.steps ?? []).map((s, i) => deserializeStep(s, i)));
  renderSteps();

  document.getElementById('pipe-card-title').closest('.card').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function deserializeStep(s, i) {
  const id   = s.id ?? ('step_' + (i + 1));
  const type = s.type ?? 'call_skill';
  if (type === 'call_skill') {
    const serVal = v => (v !== null && typeof v === 'object') ? JSON.stringify(v) : v;
    const args = s.args ? Object.entries(s.args).map(([k, v]) => [k, serVal(v)]) : [['', '']];
    return { id, type, skill: s.skill ?? '', args, result_path: s.result_path ?? '' };
  }
  if (type === 'llm_loop') {
    const input = s.input ? Object.entries(s.input) : [['event', '$.event']];
    const setResultSchema = s.set_result_schema || null;
    return { id, type, provider_name: s.provider_name ?? '', model: s.model ?? '', stop_on_finish: s.stop_on_finish ?? true, tools: s.tools ?? [], input, instructions: s.instructions ?? '', set_result_schema: setResultSchema, result_path: s.result_path ?? '' };
  }
  if (type === 'break') {
    return { id, type, path: s.path ?? '', not: s.not === true };
  }
  return { id, type };
}

export function resetPipelineEditor() {
  editingPipelineId.value = null;

  document.getElementById('pipe-id').value          = '';
  document.getElementById('pipe-id').readOnly        = false;
  document.getElementById('pipe-id').style.opacity   = '';
  document.getElementById('pipe-topic').value        = '';
  document.getElementById('pipe-active').checked     = true;
  document.getElementById('pipe-card-title').textContent    = 'New Pipeline';
  document.getElementById('pipe-save-btn').textContent      = 'Create Pipeline';
  document.getElementById('pipe-cancel-btn').style.display  = 'none';

  pipelineSteps.length = 0;
  renderSteps();
}
