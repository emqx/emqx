import { api } from './api.js';
import { getSchemaEditorValue, setSchemaEditorValue } from './schema_editor.js';
import { esc, setMsg, toast } from './ui_helpers.js';
import { loadedSkills, editingSkillKey } from './state.js';

export async function loadSkills() {
  const list = await api('GET', '/skills');
  loadedSkills.length = 0;
  loadedSkills.push(...list);
  document.getElementById('cnt-skills').textContent = list.length;
  const tbody = document.getElementById('skills-body');
  if (!list.length) {
    tbody.innerHTML = '<tr class="empty"><td colspan="4">No skills registered</td></tr>';
    return;
  }
  tbody.innerHTML = list.map(s => `
    <tr>
      <td><code>${esc(s.skill_id)}</code></td>
      <td><span class="tag ${typeClass(s.type)}">${esc(s.type)}</span></td>
      <td style="color:var(--muted)">${esc(s.description ?? '')}</td>
      <td><div style="display:flex;gap:6px">
        <button class="btn sm" onclick="editSkill('${esc(s.type)}','${esc(s.skill_id)}')">edit</button>
        <button class="btn sm danger" onclick="deleteSkill('${esc(s.type)}','${esc(s.skill_id)}')">delete</button>
      </div></td>
    </tr>`).join('');
}

function typeClass(t) {
  if (t === 'message.publish')  return 'publish';
  if (t === 'message.request')  return 'request';
  if (t === 'http')              return 'http';
  if (t?.startsWith('postgresql')) return 'ch';
  return '';
}

export function collectSkillBody() {
  const type = document.getElementById('skill-type').value;
  const id   = document.getElementById('skill-id').value.trim();
  const desc = document.getElementById('skill-desc').value.trim();
  let body = { type, id, desc };
  if (type === 'message.publish') {
    body.topic_prefix = document.getElementById('skill-prefix').value.trim();
    const pubSchema = getSchemaEditorValue('se-skill-publish-input');
    if (Object.keys(pubSchema.properties).length > 0) {
      body.payload_schema = pubSchema;
    }
  } else if (type === 'message.request') {
    body.topic_prefix = document.getElementById('skill-request-prefix').value.trim();
    const reqSchema = getSchemaEditorValue('se-skill-request-payload-schema');
    if (Object.keys(reqSchema.properties).length > 0) {
      body.request_payload_schema = reqSchema;
    }
  } else if (type === 'http') {
    body.method        = document.getElementById('skill-method').value;
    body.url           = document.getElementById('skill-url').value.trim();
    body.input_schema  = getSchemaEditorValue('se-skill-input-schema');
  } else if (type === 'postgresql.query') {
    body.query = document.getElementById('skill-query').value.trim();
  }
  return body;
}

export async function saveSkill() {
  const body = collectSkillBody();
  if (!body.id) return setMsg('skill-msg', 'ID is required', true);
  try {
    if (editingSkillKey.type) {
      await api('PUT', `/skills/${encodeURIComponent(editingSkillKey.type)}/${encodeURIComponent(editingSkillKey.id)}`, body);
      setMsg('skill-msg', 'Updated ✓');
      toast('Skill "' + body.id + '" updated', 'ok');
      resetSkillEditor();
    } else {
      await api('POST', '/skills', body);
      setMsg('skill-msg', 'Created ✓');
      toast('Skill "' + body.id + '" created', 'ok');
    }
    await loadSkills();
  } catch(e) {
    setMsg('skill-msg', e.message, true);
  }
}

export function editSkill(type, id) {
  const skill = loadedSkills.find(s => s.type === type && s.skill_id === id);
  if (!skill) return;

  editingSkillKey.type = type;
  editingSkillKey.id = id;

  document.getElementById('skill-card-title').textContent = 'Edit Skill — ' + id;
  document.getElementById('skill-cancel-btn').style.display = '';
  document.getElementById('skill-submit-btn').textContent = 'Update';

  document.getElementById('skill-type').value = skill.type;
  document.getElementById('skill-type').disabled = true;
  document.getElementById('skill-id').value = skill.skill_id;
  document.getElementById('skill-id').readOnly = true;
  document.getElementById('skill-desc').value = skill.description ?? '';
  updateSkillForm();

  if (type === 'http') {
    document.getElementById('skill-method').value = skill.method ?? 'post';
    document.getElementById('skill-url').value = skill.url ?? '';
    setSchemaEditorValue('se-skill-input-schema', skill.input_schema || null);
  } else if (type === 'message.publish') {
    document.getElementById('skill-prefix').value = skill.topic_prefix ?? '';
    const legacyPayloadSchema = skill.input_schema?.properties?.payload;
    const payloadSchema = skill.payload_schema ?? legacyPayloadSchema;
    setSchemaEditorValue('se-skill-publish-input', payloadSchema || defaultPublishInputSchema());
  } else if (type === 'message.request') {
    document.getElementById('skill-request-prefix').value = skill.topic_prefix ?? '';
    setSchemaEditorValue('se-skill-request-payload-schema', skill.request_payload_schema || null);
  } else if (type === 'postgresql.query') {
    document.getElementById('skill-query').value = skill.query ?? '';
  }

  document.querySelector('#tab-skills .card').scrollIntoView({ behavior: 'smooth', block: 'start' });
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

export function resetSkillEditor() {
  editingSkillKey.type = null;
  editingSkillKey.id = null;
  document.getElementById('skill-card-title').textContent = 'New Skill';
  document.getElementById('skill-cancel-btn').style.display = 'none';
  document.getElementById('skill-submit-btn').textContent = 'Create';
  document.getElementById('skill-type').disabled = false;
  document.getElementById('skill-id').readOnly = false;
  document.getElementById('skill-type').value = 'message.publish';
  document.getElementById('skill-id').value = '';
  document.getElementById('skill-desc').value = '';
  document.getElementById('skill-prefix').value = '';
  setSchemaEditorValue('se-skill-publish-input', defaultPublishInputSchema());
  document.getElementById('skill-request-prefix').value = '';
  setSchemaEditorValue('se-skill-request-payload-schema', null);
  document.getElementById('skill-method').value = 'post';
  document.getElementById('skill-url').value = '';
  setSchemaEditorValue('se-skill-input-schema', null);
  document.getElementById('skill-query').value = '';
  updateSkillForm();
  setMsg('skill-msg', '');
}

export async function deleteSkill(type, id) {
  if (!confirm(`Delete skill "${id}"?`)) return;
  try {
    await api('DELETE', `/skills/${encodeURIComponent(type)}/${encodeURIComponent(id)}`);
    toast('Deleted ' + id, 'ok');
    await loadSkills();
  } catch(e) {
    toast(e.message, 'err');
  }
}

export function updateSkillForm() {
  const type = document.getElementById('skill-type').value;
  document.getElementById('f-publish').style.display  = type === 'message.publish'  ? '' : 'none';
  document.getElementById('f-request').style.display  = type === 'message.request'  ? '' : 'none';
  document.getElementById('f-http').style.display     = type === 'http'              ? '' : 'none';
  document.getElementById('f-ch').style.display       = type === 'postgresql.query'  ? '' : 'none';
}
