import { api } from './api.js';
import { getSchemaEditorValue, setSchemaEditorValue } from './schema_editor.js';
import { esc, setMsg, toast } from './ui_helpers.js';
import { loadedTools, editingToolKey } from './state.js';
import { renderConnectionOptions } from './connections.js';

export async function loadTools() {
  const list = await api('GET', '/tools');
  loadedTools.length = 0;
  loadedTools.push(...list);
  document.getElementById('cnt-tools').textContent = list.length;
  const tbody = document.getElementById('tools-body');
  if (!list.length) {
    tbody.innerHTML = '<tr class="empty"><td colspan="4">No tools registered</td></tr>';
    return;
  }
  tbody.innerHTML = list.map(s => `
    <tr>
      <td><code>${esc(s.id)}</code></td>
      <td><span class="tag ${typeClass(s.type)}">${esc(s.type)}</span></td>
      <td style="color:var(--muted)">${esc(s.desc ?? '')}</td>
      <td><div style="display:flex;gap:6px">
        <button class="btn sm" onclick="editTool('${esc(s.type)}','${esc(s.id)}')">edit</button>
        <button class="btn sm danger" onclick="deleteTool('${esc(s.type)}','${esc(s.id)}')">delete</button>
      </div></td>
    </tr>`).join('');
}

export function initToolEditors() {
  setImagePathEditorValue('tool-request-images', []);
  setImagePathEditorValue('tool-http-images', []);
}

function typeClass(t) {
  if (t === 'message__publish')  return 'publish';
  if (t === 'message__request')  return 'request';
  if (t === 'http')              return 'http';
  if (t?.startsWith('postgresql')) return 'ch';
  if (t?.startsWith('stream_')) return 'request';
  if (t?.startsWith('kv_')) return 'kv';
  return '';
}

function isStreamTool(type) {
  return type?.startsWith('stream_') || type?.startsWith('kv_');
}

function hasFormat(type) {
  return ['stream_write', 'stream_read', 'kv_write', 'kv_read', 'kv_read_all'].includes(type);
}

export function collectToolBody() {
  const type = document.getElementById('tool-type').value;
  const id   = document.getElementById('tool-id').value.trim();
  const desc = document.getElementById('tool-desc').value.trim();
  let body = { type, id, desc };
  if (type === 'message__publish') {
    body.topic_prefix = document.getElementById('tool-prefix').value.trim();
    const pubSchema = getSchemaEditorValue('se-tool-publish-input');
    body.payload_schema = schemaString(pubSchema);
  } else if (type === 'message__request') {
    body.topic_prefix = document.getElementById('tool-request-prefix').value.trim();
    const reqSchema = getSchemaEditorValue('se-tool-request-payload-schema');
    body.request_payload_schema = schemaString(reqSchema);
    body.payload_type = document.getElementById('tool-request-payload-type').value;
    body.autodiscover_images = document.getElementById('tool-request-autodiscover-images').checked;
    body.images = imagePaths('tool-request-images');
  } else if (type === 'http') {
    body.method        = document.getElementById('tool-method').value;
    body.url           = document.getElementById('tool-url').value.trim();
    body.input_schema  = schemaString(getSchemaEditorValue('se-tool-input-schema'));
    body.payload_type  = document.getElementById('tool-http-payload-type').value;
    body.autodiscover_images = document.getElementById('tool-http-autodiscover-images').checked;
    body.images = imagePaths('tool-http-images');
  } else if (type === 'postgresql__query') {
    body.resource = document.getElementById('tool-resource').value;
    body.query = document.getElementById('tool-query').value.trim();
  } else if (isStreamTool(type)) {
    body.stream = document.getElementById('tool-stream').value.trim();
    if (hasFormat(type)) body.format = document.getElementById('tool-format').value;
  }
  return body;
}

export async function saveTool() {
  const body = collectToolBody();
  if (!body.id) return setMsg('tool-msg', 'ID is required', true);
  try {
    if (editingToolKey.type) {
      await api('PUT', `/tools/${encodeURIComponent(editingToolKey.type)}/${encodeURIComponent(editingToolKey.id)}`, body);
      setMsg('tool-msg', 'Updated ✓');
      toast('Tool "' + body.id + '" updated', 'ok');
      resetToolEditor();
    } else {
      await api('POST', '/tools', body);
      setMsg('tool-msg', 'Created ✓');
      toast('Tool "' + body.id + '" created', 'ok');
    }
    await loadTools();
  } catch(e) {
    setMsg('tool-msg', e.message, true);
  }
}

export function editTool(type, id) {
  const tool = loadedTools.find(s => s.type === type && s.id === id);
  if (!tool) return;

  editingToolKey.type = type;
  editingToolKey.id = id;

  document.getElementById('tool-card-title').textContent = 'Edit Tool — ' + id;
  document.getElementById('tool-cancel-btn').style.display = '';
  document.getElementById('tool-submit-btn').textContent = 'Update';

  document.getElementById('tool-type').value = tool.type;
  document.getElementById('tool-type').disabled = true;
  document.getElementById('tool-id').value = tool.id;
  document.getElementById('tool-id').readOnly = true;
  document.getElementById('tool-desc').value = tool.desc ?? '';
  updateToolForm();

  if (type === 'http') {
    document.getElementById('tool-method').value = tool.method ?? 'post';
    document.getElementById('tool-url').value = tool.url ?? '';
    document.getElementById('tool-http-payload-type').value = tool.payload_type ?? 'json';
    document.getElementById('tool-http-autodiscover-images').checked = tool.autodiscover_images ?? true;
    setImagePathEditorValue('tool-http-images', tool.images ?? []);
    setSchemaEditorValue('se-tool-input-schema', parseSchema(tool.input_schema));
  } else if (type === 'message__publish') {
    document.getElementById('tool-prefix').value = tool.topic_prefix ?? '';
    const legacyPayloadSchema = tool.input_schema?.properties?.payload;
    const payloadSchema = tool.payload_schema ?? legacyPayloadSchema;
    setSchemaEditorValue('se-tool-publish-input', parseSchema(payloadSchema) || defaultPublishInputSchema());
  } else if (type === 'message__request') {
    document.getElementById('tool-request-prefix').value = tool.topic_prefix ?? '';
    document.getElementById('tool-request-payload-type').value = tool.payload_type ?? 'json';
    document.getElementById('tool-request-autodiscover-images').checked = tool.autodiscover_images ?? true;
    setImagePathEditorValue('tool-request-images', tool.images ?? []);
    setSchemaEditorValue('se-tool-request-payload-schema', parseSchema(tool.request_payload_schema));
  } else if (type === 'postgresql__query') {
    document.getElementById('tool-resource').value = tool.resource ?? '';
    document.getElementById('tool-query').value = tool.query ?? '';
  } else if (isStreamTool(type)) {
    document.getElementById('tool-stream').value = tool.stream ?? '';
    if (hasFormat(type)) document.getElementById('tool-format').value = tool.format ?? 'json';
  }

  document.querySelector('#tab-tools .card').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

export function defaultPublishInputSchema() {
  return {
    type: 'object',
    properties: {
      message: { type: 'string' }
    },
    required: ['message'],
    additionalProperties: false
  };
}

function parseSchema(value) {
  if (!value) return null;
  if (typeof value === 'object') return value;
  try {
    const parsed = JSON.parse(value);
    return parsed && typeof parsed === 'object' ? parsed : null;
  } catch(_e) {
    return null;
  }
}

function schemaString(schema) {
  return JSON.stringify(schema);
}

function imagePaths(id) {
  return [...document.getElementById(id).querySelectorAll('.image-path-input')]
    .map(input => input.value.trim())
    .filter(Boolean);
}

function setImagePathEditorValue(id, paths) {
  const container = document.getElementById(id);
  container.innerHTML = '';
  container.classList.add('schema-editor');

  const root = document.createElement('div');
  root.className = 'se-root';

  const rows = document.createElement('div');
  rows.className = 'se-children';
  root.appendChild(rows);

  (paths || []).forEach(path => rows.appendChild(buildImagePathRow(path)));

  const addBtn = document.createElement('button');
  addBtn.className = 'btn sm';
  addBtn.type = 'button';
  addBtn.textContent = '+ Add path';
  addBtn.onclick = () => rows.appendChild(buildImagePathRow(''));
  root.appendChild(addBtn);

  container.appendChild(root);
}

function buildImagePathRow(path) {
  const row = document.createElement('div');
  row.className = 'se-node';
  row.innerHTML = `
    <div class="se-row">
      <input type="text" class="image-path-input" placeholder=".image_url" value="${esc(path)}">
      <button class="btn sm danger" type="button">×</button>
    </div>
  `;
  row.querySelector('button').onclick = () => row.remove();
  return row;
}

export function resetToolEditor() {
  editingToolKey.type = null;
  editingToolKey.id = null;
  document.getElementById('tool-card-title').textContent = 'New Tool';
  document.getElementById('tool-cancel-btn').style.display = 'none';
  document.getElementById('tool-submit-btn').textContent = 'Create';
  document.getElementById('tool-type').disabled = false;
  document.getElementById('tool-id').readOnly = false;
  document.getElementById('tool-type').value = 'message__publish';
  document.getElementById('tool-id').value = '';
  document.getElementById('tool-desc').value = '';
  document.getElementById('tool-prefix').value = '';
  setSchemaEditorValue('se-tool-publish-input', defaultPublishInputSchema());
  document.getElementById('tool-request-prefix').value = '';
  document.getElementById('tool-request-payload-type').value = 'json';
  document.getElementById('tool-request-autodiscover-images').checked = true;
  setImagePathEditorValue('tool-request-images', []);
  setSchemaEditorValue('se-tool-request-payload-schema', null);
  document.getElementById('tool-method').value = 'post';
  document.getElementById('tool-url').value = '';
  document.getElementById('tool-http-payload-type').value = 'json';
  document.getElementById('tool-http-autodiscover-images').checked = true;
  setImagePathEditorValue('tool-http-images', []);
  setSchemaEditorValue('se-tool-input-schema', null);
  renderConnectionOptions();
  document.getElementById('tool-resource').value = '';
  document.getElementById('tool-query').value = '';
  document.getElementById('tool-stream').value = '';
  document.getElementById('tool-format').value = 'json';
  updateToolForm();
  setMsg('tool-msg', '');
}

export async function deleteTool(type, id) {
  if (!confirm(`Delete tool "${id}"?`)) return;
  try {
    await api('DELETE', `/tools/${encodeURIComponent(type)}/${encodeURIComponent(id)}`);
    toast('Deleted ' + id, 'ok');
    await loadTools();
  } catch(e) {
    toast(e.message, 'err');
  }
}

export function updateToolForm() {
  const type = document.getElementById('tool-type').value;
  document.getElementById('f-publish').style.display  = type === 'message__publish'  ? '' : 'none';
  document.getElementById('f-request').style.display  = type === 'message__request'  ? '' : 'none';
  document.getElementById('f-http').style.display     = type === 'http'              ? '' : 'none';
  document.getElementById('f-ch').style.display       = type === 'postgresql__query'  ? '' : 'none';
  document.getElementById('f-stream').style.display   = isStreamTool(type) ? '' : 'none';
  document.getElementById('f-format').style.display   = hasFormat(type) ? '' : 'none';
  if (type === 'postgresql__query') renderConnectionOptions();
}
