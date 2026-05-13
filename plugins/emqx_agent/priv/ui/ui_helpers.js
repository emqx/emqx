export function esc(s) {
  return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

export function typeClass(t) {
  if (t === 'message__publish')  return 'publish';
  if (t === 'message__request')  return 'request';
  if (t === 'http')              return 'http';
  if (t?.startsWith('postgresql')) return 'ch';
  return '';
}

export function stepClass(t) {
  if (t === 'llm_loop')      return 'llm';
  if (t === 'call_skill')    return 'publish';
  if (t === 'break')         return 'kv';
  return '';
}

export function setMsg(id, msg, isErr = false) {
  const el = document.getElementById(id);
  el.textContent = msg;
  el.className = 'msg' + (isErr ? ' err' : '');
  setTimeout(() => { el.textContent = ''; }, 4000);
}

let toastTimer;
export function toast(msg, kind = 'ok') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'show ' + kind;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { el.className = ''; }, 3000);
}

export function setStatus(state) {
  const el = document.getElementById('status-pill');
  el.className = 'pill ' + state;
  el.textContent = state === 'ok' ? '● connected' : '○ error';
}

export function showTab(name, btn) {
  document.querySelectorAll('section').forEach(s => s.classList.remove('active'));
  document.querySelectorAll('nav button').forEach(b => b.classList.remove('active'));
  document.getElementById('tab-' + name).classList.add('active');
  btn.classList.add('active');
}
