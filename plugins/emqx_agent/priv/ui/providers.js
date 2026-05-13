import { api } from './api.js';
import { esc } from './ui_helpers.js';
import { loadedProfiles } from './state.js';

export async function loadProfiles() {
  const list = await api('GET', '/providers');
  loadedProfiles.length = 0;
  loadedProfiles.push(...list);
  document.getElementById('cnt-profiles').textContent = list.length;
  const tbody = document.getElementById('profiles-body');
  if (!list.length) {
    tbody.innerHTML = '<tr class="empty"><td colspan="3">No AI providers configured</td></tr>';
    return;
  }
  tbody.innerHTML = list.map(p => `
    <tr>
      <td><code>${esc(p.name)}</code></td>
      <td style="color:var(--muted)">${esc(p.type ?? '')}</td>
      <td style="color:var(--muted)">${esc(p.base_url ?? '')}</td>
    </tr>`).join('');
}
