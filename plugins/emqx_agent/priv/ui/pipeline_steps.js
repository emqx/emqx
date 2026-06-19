import { loadedTools, loadedProfiles, pipelineSteps } from './state.js';
import { esc, stepClass } from './ui_helpers.js';
import { createSchemaEditor, getSchemaEditorValue } from './schema_editor.js';

export function defaultStep(type) {
  const n = pipelineSteps.length + 1;
  if (type === 'call_tool')     return { id: 'step_' + n, type, tool: '', args: [['', '']], result_path: '' };
  if (type === 'llm_loop')       return { id: 'step_' + n, type, provider_name: '', model: 'gpt-5.4-mini', key_expression: 'message.topic', max_tokens: 2048, max_total_tokens: 50000, persistent: false, tools: [], input: [['event', '$.event']], set_result_schema: { type: 'object', properties: { status: { type: 'string' } }, required: ['status'], additionalProperties: false }, result_path: '' };
  if (type === 'break')          return { id: 'step_' + n, type, path: '', not: false };
  return { id: 'step_' + n, type };
}

export function addStep() {
  pipelineSteps.push(defaultStep('call_tool'));
  renderSteps();
}

export function removeStep(idx) {
  syncAll();
  pipelineSteps.splice(idx, 1);
  renderSteps();
}

export function moveStep(idx, dir) {
  syncAll();
  const t = idx + dir;
  if (t < 0 || t >= pipelineSteps.length) return;
  [pipelineSteps[idx], pipelineSteps[t]] = [pipelineSteps[t], pipelineSteps[idx]];
  renderSteps();
}

export function stepTypeChanged(idx) {
  syncAll();
  const card  = document.querySelector(`[data-step-idx="${idx}"]`);
  const newType = card.querySelector('.step-type-sel').value;
  const oldId   = pipelineSteps[idx].id;
  pipelineSteps[idx] = defaultStep(newType);
  pipelineSteps[idx].id = oldId;
  renderStepBody(idx);
}

export function renderSteps() {
  const container = document.getElementById('pipe-steps-list');
  container.innerHTML = '';
  pipelineSteps.forEach((step, idx) => {
    const card = document.createElement('div');
    card.className = 'step-card';
    card.dataset.stepIdx = idx;
    card.innerHTML = stepCardHTML(step, idx);
    container.appendChild(card);
    if (step.type === 'llm_loop') {
      createSchemaEditor('se-set-result-' + idx, step.set_result_schema);
    }
  });
}

export function renderStepBody(idx) {
  const body = document.getElementById('step-body-' + idx);
  body.innerHTML = stepFieldsHTML(pipelineSteps[idx], idx);
  if (pipelineSteps[idx].type === 'llm_loop') {
    createSchemaEditor('se-set-result-' + idx, pipelineSteps[idx].set_result_schema);
  }
}

function stepCardHTML(step, idx) {
  const last = pipelineSteps.length - 1;
  const types = ['call_tool','llm_loop','break'];
  const typeOpts = types.map(t =>
    `<option value="${t}"${t===step.type?' selected':''}>${t}</option>`
  ).join('');
  return `
    <div class="step-header">
      <span class="step-num">Step ${idx+1}</span>
      <select class="step-type-sel" onchange="stepTypeChanged(${idx})">${typeOpts}</select>
      <div style="flex:1"></div>
      <button class="btn sm" onclick="moveStep(${idx},-1)"${idx===0?' disabled':''} title="Move up">↑</button>
      <button class="btn sm" onclick="moveStep(${idx},1)"${idx===last?' disabled':''} title="Move down">↓</button>
      <button class="btn sm danger" onclick="removeStep(${idx})" title="Remove">×</button>
    </div>
    <div class="step-body" id="step-body-${idx}">${stepFieldsHTML(step, idx)}</div>`;
}

function kvRow(k, v) {
  return `<div class="kv-row">
    <input type="text" class="kv-key" value="${esc(k)}" placeholder="key">
    <input type="text" class="kv-val" value="${esc(v)}" placeholder="$.path or value">
    <button class="btn sm danger" onclick="this.closest('.kv-row').remove()" title="Remove">×</button>
  </div>`;
}

export function addKV(idx, cls) {
  const list = document.querySelector(`[data-step-idx="${idx}"] .${cls}`);
  list.insertAdjacentHTML('beforeend', kvRow('', ''));
}

function stepFieldsHTML(step, idx) {
  const idF = `<div class="field"><label>Step ID</label>
    <input type="text" class="sf-id" value="${esc(step.id)}" placeholder="step_1"></div>`;

  if (step.type === 'call_tool') {
    const toolOpts = loadedTools.map(s => {
      const ref = s.type + '@' + s.id;
      return `<option value="${esc(ref)}"${ref===step.tool?' selected':''}>${esc(s.id)} (${esc(s.type)})</option>`;
    }).join('');
    return idF + `
      <div class="field"><label>Tool</label>
        <select class="sf-tool">
          <option value="">— select tool —</option>${toolOpts}
        </select>
      </div>
      <div class="field"><label>Args <small style="color:var(--muted)">key → $.path or literal</small></label>
        <div class="kv-list sf-args">${(step.args||[]).map(([k,v])=>kvRow(k,v)).join('')}</div>
        <button class="btn sm" style="margin-top:4px" onclick="addKV(${idx},'sf-args')">+ arg</button>
      </div>
      <div class="field"><label>Result path <small style="color:var(--muted)">e.g. $.history</small></label>
        <input type="text" class="sf-result" value="${esc(step.result_path)}" placeholder="$.result">
      </div>`;
  }

  if (step.type === 'llm_loop') {
    const providerOpts = loadedProfiles.map(p =>
      `<option value="${esc(p.name)}"${p.name===step.provider_name?' selected':''}>${esc(p.name)}</option>`
    ).join('');
    const toolsHTML = loadedTools.length
      ? loadedTools.map(s => {
          const ref = s.type + '@' + s.id;
          const on = (step.tools||[]).includes(ref);
          return `<label>
            <input type="checkbox" value="${esc(ref)}"${on?' checked':''}>
            ${esc(s.id)} <small>${esc(s.type)}</small></label>`;
        }).join('')
      : '<span style="color:var(--muted);font-size:11px">No tools registered yet</span>';
    return idF + `
      <div class="row">
        <div class="field"><label>AI provider</label>
          <select class="sf-provider">
            <option value="">— select provider —</option>${providerOpts}
          </select>
        </div>
        <div class="field"><label>Model</label>
          <input type="text" class="sf-model" value="${esc(step.model||'gpt-5.4-mini')}" placeholder="e.g. gpt-5.4-mini">
        </div>
        <div class="field"><label>Key expression</label>
          <input type="text" class="sf-key-expression" value="${esc(step.key_expression||'message.topic')}" placeholder="message.topic">
        </div>
        <div class="field"><label>Max tokens</label>
          <input type="number" class="sf-max-tokens" value="${esc(step.max_tokens||2048)}" min="1">
        </div>
        <div class="field"><label>Max total tokens</label>
          <input type="number" class="sf-max-total-tokens" value="${esc(step.max_total_tokens||50000)}" min="1">
        </div>
      </div>
      <div style="margin-bottom:10px;display:inline-flex;align-items:center;gap:6px">
        <input type="checkbox" class="sf-persistent" style="width:auto;min-width:0;flex:0 0 auto"${step.persistent?' checked':''}>
        <span style="font-size:11px">Persistent mode <small style="color:var(--muted)">(reuse session for the same step key; default: false)</small></span>
      </div>
      <div class="field"><label>Tools (select tools available to the LLM)</label>
        <div class="tools-grid sf-tools">${toolsHTML}</div>
      </div>
      <div class="field"><label>Input <small style="color:var(--muted)">key → $.path (e.g. $.event, $.history)</small></label>
        <div class="kv-list sf-input">${(step.input||[]).map(([k,v])=>kvRow(k,v)).join('')}</div>
        <button class="btn sm" style="margin-top:4px" onclick="addKV(${idx},'sf-input')">+ input</button>
      </div>
      <div class="field"><label>Instructions <small style="color:var(--muted)">System prompt for the LLM (required)</small></label>
        <textarea class="sf-instructions" rows="5" style="font-family:monospace;font-size:11px" placeholder="You are a helpful assistant.">${esc(step.instructions||'')}</textarea>
      </div>
      <div class="field"><label>set_result schema <small style="color:var(--muted)">JSON Schema for structured output (required)</small></label>
        <div id="se-set-result-${idx}" class="sf-set-result-schema-editor"></div>
      </div>
      <div class="field"><label>Result path</label>
        <input type="text" class="sf-result" value="${esc(step.result_path)}" placeholder="$.analysis">
      </div>`;
  }

  if (step.type === 'break') {
    return idF + `
      <div class="field"><label>Path <small style="color:var(--muted)">JSON path in context</small></label>
        <input type="text" class="sf-path" value="${esc(step.path||'')}" placeholder="$.triage.should_break">
      </div>
      <div class="field"><label><input type="checkbox" class="sf-not"${step.not ? ' checked' : ''}> Not</label>
      </div>`;
  }

  return idF;
}

export function syncStep(idx) {
  const card = document.querySelector(`[data-step-idx="${idx}"]`);
  if (!card) return;
  const step = pipelineSteps[idx];
  step.id   = card.querySelector('.sf-id')?.value?.trim() ?? step.id;
  step.type = card.querySelector('.step-type-sel')?.value ?? step.type;
  if (step.type === 'call_tool') {
    step.tool       = card.querySelector('.sf-tool')?.value ?? '';
    step.result_path = card.querySelector('.sf-result')?.value?.trim() ?? '';
    step.args = [...card.querySelectorAll('.sf-args .kv-row')].map(r => [
      r.querySelector('.kv-key').value.trim(),
      r.querySelector('.kv-val').value.trim()
    ]);
  } else if (step.type === 'llm_loop') {
    step.provider_name = card.querySelector('.sf-provider')?.value ?? '';
    step.model = card.querySelector('.sf-model')?.value?.trim() ?? '';
    step.key_expression = card.querySelector('.sf-key-expression')?.value?.trim() || 'message.topic';
    step.max_tokens = Number(card.querySelector('.sf-max-tokens')?.value || 2048);
    step.max_total_tokens = Number(card.querySelector('.sf-max-total-tokens')?.value || 50000);
    step.persistent = card.querySelector('.sf-persistent')?.checked ?? false;
    step.tools = [...card.querySelectorAll('.sf-tools input:checked')].map(c => c.value);
    step.instructions = card.querySelector('.sf-instructions')?.value?.trim() ?? '';
    const seEl = card.querySelector('.sf-set-result-schema-editor');
    step.set_result_schema = seEl ? getSchemaEditorValue(seEl) : null;
    step.result_path = card.querySelector('.sf-result')?.value?.trim() ?? '';
    step.input = [...card.querySelectorAll('.sf-input .kv-row')].map(r => [
      r.querySelector('.kv-key').value.trim(),
      r.querySelector('.kv-val').value.trim()
    ]);
  } else if (step.type === 'break') {
    step.path = card.querySelector('.sf-path')?.value?.trim() ?? '';
    step.not = card.querySelector('.sf-not')?.checked === true;
  }
}

export function syncAll() {
  pipelineSteps.forEach((_, idx) => syncStep(idx));
}

export function collectSteps() {
  syncAll();
  return pipelineSteps.map(s => {
    const out = { id: s.id, type: s.type };
    if (s.type === 'call_tool') {
      if (s.tool) out.tool = s.tool;
      const args = Object.fromEntries((s.args||[]).filter(([k]) => k));
      if (Object.keys(args).length) out.args = args;
      if (s.result_path) out.result_path = s.result_path;
    } else if (s.type === 'llm_loop') {
      if (s.provider_name) out.provider_name = s.provider_name;
      if (s.model) out.model = s.model;
      out.key_expression = s.key_expression || 'message.topic';
      if (s.max_tokens) out.max_tokens = s.max_tokens;
      out.max_total_tokens = s.max_total_tokens || 50000;
      out.persistent = s.persistent ?? false;
      if (s.instructions) out.instructions = s.instructions;
      if (s.tools?.length)   out.tools = s.tools;
      const input = Object.fromEntries((s.input||[]).filter(([k]) => k));
      if (Object.keys(input).length) out.input = input;
      if (s.set_result_schema && typeof s.set_result_schema === 'object') {
        out.set_result_schema = s.set_result_schema;
      }
      if (s.result_path) out.result_path = s.result_path;
    } else if (s.type === 'break') {
      if (s.path) out.path = s.path;
      if (s.not === true) out.not = true;
    }
    return out;
  });
}
