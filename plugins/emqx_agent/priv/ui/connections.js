import { api } from './api.js';
import { esc, setMsg, toast } from './ui_helpers.js';
import { loadedConnections, editingConnectionId } from './state.js';

export async function loadConnections() {
  const list = await api('GET', '/connections');
  const statuses = await loadConnectionStatuses();
  loadedConnections.length = 0;
  loadedConnections.push(...list);
  document.getElementById('cnt-connections').textContent = list.length;
  renderConnectionOptions();
  const tbody = document.getElementById('connections-body');
  if (!list.length) {
    tbody.innerHTML = '<tr class="empty"><td colspan="5">No connections configured</td></tr>';
    return;
  }
  tbody.innerHTML = list.map(c => {
    const configuredStatus = c.enable === true
      ? '<span>enabled</span>'
      : '<span class="tag draft">○ disabled</span>';
    const runtimeStatus = renderRuntimeStatus(statuses?.[c.id]);
    const action = c.enable === true
      ? `<button class="btn sm" onclick="stopConnection('${esc(c.id)}')">stop</button>`
      : `<button class="btn sm" onclick="startConnection('${esc(c.id)}')">start</button>`;
    return `<tr>
      <td><code>${esc(c.id)}</code></td>
      <td><span class="tag ch">${esc(c.type)}</span></td>
      <td><div style="display:flex;gap:4px;flex-wrap:wrap">${configuredStatus}${runtimeStatus}</div></td>
      <td style="color:var(--muted)">${esc(c.config?.server ?? '')}</td>
      <td><div style="display:flex;gap:4px">
        ${action}
        <button class="btn sm" onclick="editConnection('${esc(c.id)}')">edit</button>
        <button class="btn sm danger" onclick="deleteConnection('${esc(c.id)}')">delete</button>
      </div></td>
    </tr>`;
  }).join('');
}

async function loadConnectionStatuses() {
  try {
    return await api('GET', '/connections/statuses');
  } catch(_) {
    return null;
  }
}

function renderRuntimeStatus(status) {
  if (!status) return '<span class="tag danger">unknown</span>';
  const value = status.status ?? 'unknown';
  const cls = value === 'connected' ? 'active' : 'danger';
  return `<span class="tag ${cls}">${esc(value)}</span>`;
}

export function collectConnectionBody() {
  const id = document.getElementById('conn-id').value.trim();
  return {
    id,
    type: 'postgresql',
    enable: document.getElementById('conn-enable').checked,
    config: {
      server: document.getElementById('conn-server').value.trim(),
      database: document.getElementById('conn-database').value.trim(),
      username: document.getElementById('conn-username').value.trim(),
      password: document.getElementById('conn-password').value,
      pool_size: Number(document.getElementById('conn-pool-size').value || 1),
      connect_timeout: Number(document.getElementById('conn-connect-timeout').value || 5000),
      disable_prepared_statements: document.getElementById('conn-disable-prepared').checked,
      ssl: { enable: document.getElementById('conn-ssl').checked }
    }
  };
}

export async function saveConnection() {
  const body = collectConnectionBody();
  if (!body.id) return setMsg('conn-msg', 'Connection ID is required', true);
  if (!body.config.server) return setMsg('conn-msg', 'Server is required', true);
  try {
    if (editingConnectionId.value) {
      await api('PUT', `/connections/${encodeURIComponent(editingConnectionId.value)}`, body);
      toast('Connection "' + body.id + '" updated', 'ok');
    } else {
      await api('POST', '/connections', body);
      toast('Connection "' + body.id + '" created', 'ok');
    }
    setMsg('conn-msg', 'Saved ✓');
    resetConnectionEditor();
    await loadConnections();
  } catch(e) {
    setMsg('conn-msg', e.message, true);
  }
}

export function editConnection(id) {
  const c = loadedConnections.find(x => x.id === id);
  if (!c) return toast('Connection not found — try refreshing', 'err');
  editingConnectionId.value = id;
  document.getElementById('conn-card-title').textContent = 'Edit Connection: ' + id;
  document.getElementById('conn-save-btn').textContent = 'Update Connection';
  document.getElementById('conn-cancel-btn').style.display = '';
  document.getElementById('conn-id').value = c.id;
  document.getElementById('conn-id').readOnly = true;
  document.getElementById('conn-id').style.opacity = '0.6';
  document.getElementById('conn-enable').checked = c.enable === true;
  document.getElementById('conn-server').value = c.config?.server ?? '';
  document.getElementById('conn-database').value = c.config?.database ?? '';
  document.getElementById('conn-username').value = c.config?.username ?? '';
  document.getElementById('conn-password').value = c.config?.password ?? '';
  document.getElementById('conn-pool-size').value = c.config?.pool_size ?? 1;
  document.getElementById('conn-connect-timeout').value = c.config?.connect_timeout ?? 5000;
  document.getElementById('conn-disable-prepared').checked = c.config?.disable_prepared_statements === true;
  document.getElementById('conn-ssl').checked = c.config?.ssl?.enable === true;
  document.getElementById('conn-card-title').closest('.card').scrollIntoView({ behavior: 'smooth', block: 'start' });
}

export function resetConnectionEditor() {
  editingConnectionId.value = null;
  document.getElementById('conn-card-title').textContent = 'New Connection';
  document.getElementById('conn-save-btn').textContent = 'Create Connection';
  document.getElementById('conn-cancel-btn').style.display = 'none';
  document.getElementById('conn-id').value = '';
  document.getElementById('conn-id').readOnly = false;
  document.getElementById('conn-id').style.opacity = '';
  document.getElementById('conn-enable').checked = false;
  document.getElementById('conn-server').value = 'pgsql:5432';
  document.getElementById('conn-database').value = 'mqtt';
  document.getElementById('conn-username').value = 'root';
  document.getElementById('conn-password').value = 'public';
  document.getElementById('conn-pool-size').value = 1;
  document.getElementById('conn-connect-timeout').value = 5000;
  document.getElementById('conn-disable-prepared').checked = true;
  document.getElementById('conn-ssl').checked = false;
  setMsg('conn-msg', '');
}

export async function deleteConnection(id) {
  if (!confirm(`Delete connection "${id}"?`)) return;
  try {
    await api('DELETE', `/connections/${encodeURIComponent(id)}`);
    toast('Deleted ' + id, 'ok');
    await loadConnections();
  } catch(e) {
    toast(e.message, 'err');
  }
}

export async function startConnection(id) {
  try {
    await api('POST', `/connections/${encodeURIComponent(id)}/start`);
    toast('Started ' + id, 'ok');
    await loadConnections();
  } catch(e) {
    toast(e.message, 'err');
  }
}

export async function stopConnection(id) {
  try {
    await api('POST', `/connections/${encodeURIComponent(id)}/stop`);
    toast('Stopped ' + id, 'ok');
    await loadConnections();
  } catch(e) {
    toast(e.message, 'err');
  }
}

export function renderConnectionOptions() {
  const sel = document.getElementById('skill-resource');
  if (!sel) return;
  const current = sel.value;
  const pg = loadedConnections.filter(c => c.type === 'postgresql');
  sel.innerHTML = '<option value="">Select connection...</option>' + pg.map(c => {
    const suffix = c.enable === true ? '' : ' (disabled)';
    return `<option value="${esc(c.id)}">${esc(c.id + suffix)}</option>`;
  }).join('');
  sel.value = current;
}
