const BASE = window.location.origin + '/api/v5/plugin_api/emqx_agent';

export async function api(method, path, body) {
  const opts = {
    method,
    headers: { 'Content-Type': 'application/json' }
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
