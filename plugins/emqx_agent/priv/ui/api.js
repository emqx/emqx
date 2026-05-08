const BASE = window.location.origin + '/api/v5/plugin_api/emqx_agent';

const DASHBOARD_USER_INFO_KEY = 'uKV5';
const DASHBOARD_ENCRYPTION_KEY = 'YWSCVU8Z55N9N1G1';

function getDashboardToken() {
  try {
    const uk = localStorage.getItem(DASHBOARD_USER_INFO_KEY);
    if (!uk) return null;
    const encrypted = localStorage.getItem(uk);
    if (!encrypted) return null;
    const bytes = CryptoJS.AES.decrypt(encrypted, DASHBOARD_ENCRYPTION_KEY);
    const json  = bytes.toString(CryptoJS.enc.Utf8);
    return JSON.parse(json)?.token ?? null;
  } catch(_) {
    return null;
  }
}

export function authHeader() {
  const token = getDashboardToken();
  return token ? 'Bearer ' + token : null;
}

export async function api(method, path, body) {
  const auth = authHeader();
  if (!auth) throw new Error('Not authenticated — please log in to the dashboard first.');
  const opts = {
    method,
    headers: { Authorization: auth, 'Content-Type': 'application/json' }
  };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(BASE + path, opts);
  if (res.status === 204) return null;
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch(_) { data = text; }
  if (!res.ok) throw new Error((data?.message ?? data?.code) || res.statusText);
  return data;
}

export async function coreApi(method, path, body) {
  const auth = authHeader();
  if (!auth) throw new Error('Not authenticated — please log in to the dashboard first.');
  const opts = {
    method,
    headers: { Authorization: auth, 'Content-Type': 'application/json' }
  };
  if (body !== undefined) opts.body = JSON.stringify(body);
  const res = await fetch(window.location.origin + '/api/v5' + path, opts);
  if (res.status === 204) return null;
  const text = await res.text();
  let data;
  try { data = JSON.parse(text); } catch(_) { data = text; }
  if (!res.ok) throw new Error((data?.message ?? data?.code) || res.statusText);
  return data;
}
