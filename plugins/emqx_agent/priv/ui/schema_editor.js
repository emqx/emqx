import { esc } from './ui_helpers.js';

export function createSchemaEditor(target, schema) {
  const container = typeof target === 'string' ? document.getElementById(target) : target;
  if (!container) return;
  container.innerHTML = '';
  container.classList.add('schema-editor');
  const root = document.createElement('div');
  root.className = 'se-root';
  const children = document.createElement('div');
  children.className = 'se-children';
  root.appendChild(children);
  const addBtn = document.createElement('button');
  addBtn.className = 'btn sm';
  addBtn.textContent = '+ Add property';
  addBtn.onclick = () => children.appendChild(buildNode('property', '', null));
  root.appendChild(addBtn);
  container.appendChild(root);
  if (schema && typeof schema === 'object' && schema.type === 'object') {
    const props = schema.properties || {};
    Object.entries(props).forEach(([name, subSchema]) => {
      children.appendChild(buildNode('property', name, subSchema));
    });
  }
}

function buildNode(kind, name, schema) {
  const node = document.createElement('div');
  node.className = 'se-node';
  node.dataset.seKind = kind;

  let type = 'string';
  let nullable = false;
  let desc = '';
  let enumValues = '';
  let childrenSchemas = null;
  let itemsSchema = null;

  if (schema && typeof schema === 'object') {
    const t = schema.type;
    if (Array.isArray(t)) {
      const nonNull = t.filter(x => x !== 'null');
      if (nonNull.length === 1) {
        type = nonNull[0];
        nullable = true;
      }
    } else if (typeof t === 'string') {
      type = t;
    }
    desc = schema.description || '';
    if (Array.isArray(schema.enum)) {
      enumValues = schema.enum.map(v => v === null ? 'null' : String(v)).join(', ');
    }
    if (type === 'object') {
      childrenSchemas = schema.properties || {};
    }
    if (type === 'array') {
      itemsSchema = schema.items || null;
    }
  }

  const nameInput = kind === 'property'
    ? `<input type="text" class="se-name" placeholder="name" value="${esc(name)}">`
    : `<span class="se-label">items</span>`;
  const delBtn = kind === 'property'
    ? `<button class="btn sm danger" onclick="this.closest('.se-node').remove()">×</button>`
    : '';

  node.innerHTML = `
    <div class="se-row">
      ${nameInput}
      <select class="se-type">
        <option value="string"${type==='string'?' selected':''}>string</option>
        <option value="integer"${type==='integer'?' selected':''}>integer</option>
        <option value="number"${type==='number'?' selected':''}>number</option>
        <option value="boolean"${type==='boolean'?' selected':''}>boolean</option>
        <option value="object"${type==='object'?' selected':''}>object</option>
        <option value="array"${type==='array'?' selected':''}>array</option>
      </select>
      <label style="font-size:11px;color:var(--muted);display:flex;align-items:center;gap:4px;white-space:nowrap">
        <input type="checkbox" class="se-nullable"${nullable?' checked':''}> nullable
      </label>
      <input type="text" class="se-desc" placeholder="description" value="${esc(desc)}">
      <input type="text" class="se-enum" placeholder="enum (comma-separated)" value="${esc(enumValues)}"
        style="display:${['string','integer','number','boolean'].includes(type)?'':'none'}">
      ${delBtn}
    </div>
    <div class="se-children" style="display:${type==='object'?'block':'none'}"></div>
    <div class="se-items" style="display:${type==='array'?'block':'none'}"></div>
  `;

  const typeSel = node.querySelector('.se-type');
  typeSel.onchange = () => seTypeChanged(node);

  if (type === 'object' && childrenSchemas) {
    const childrenContainer = node.querySelector('.se-children');
    Object.entries(childrenSchemas).forEach(([childName, childSchema]) => {
      childrenContainer.appendChild(buildNode('property', childName, childSchema));
    });
  }
  if (type === 'array') {
    const itemsContainer = node.querySelector('.se-items');
    itemsContainer.appendChild(buildNode('item', '', itemsSchema || null));
  }
  if (type === 'object') {
    const childrenContainer = node.querySelector('.se-children');
    if (!childrenContainer.querySelector('.btn')) {
      const addBtn = document.createElement('button');
      addBtn.className = 'btn sm';
      addBtn.textContent = '+ Add property';
      addBtn.onclick = () => childrenContainer.appendChild(buildNode('property', '', null));
      childrenContainer.appendChild(addBtn);
    }
  }

  return node;
}

function seTypeChanged(node) {
  const type = node.querySelector('.se-type').value;
  const enumInput = node.querySelector('.se-enum');
  enumInput.style.display = ['string','integer','number','boolean'].includes(type) ? '' : 'none';
  const children = node.querySelector('.se-children');
  const items = node.querySelector('.se-items');
  children.style.display = type === 'object' ? 'block' : 'none';
  items.style.display = type === 'array' ? 'block' : 'none';
  if (type === 'object' && !children.querySelector('.btn')) {
    const addBtn = document.createElement('button');
    addBtn.className = 'btn sm';
    addBtn.textContent = '+ Add property';
    addBtn.onclick = () => children.appendChild(buildNode('property', '', null));
    children.appendChild(addBtn);
  }
  if (type === 'array' && !items.querySelector('.se-node')) {
    items.appendChild(buildNode('item', '', null));
  }
}

function serializeNode(node) {
  const type = node.querySelector('.se-type').value;
  const nullable = node.querySelector('.se-nullable')?.checked;
  const desc = node.querySelector('.se-desc')?.value?.trim();
  const enumStr = node.querySelector('.se-enum')?.value?.trim();
  const out = {};
  out.type = nullable ? [type, 'null'] : type;
  if (desc) out.description = desc;
  if (enumStr && ['string','integer','number','boolean'].includes(type)) {
    const vals = enumStr.split(',').map(s => s.trim()).filter(Boolean);
    if (vals.length) {
      out.enum = vals.map(v => parseEnumValue(v, type));
    }
  }
  if (type === 'object') {
    const childrenContainer = node.querySelector('.se-children');
    const props = {};
    const required = [];
    const childNodes = [...childrenContainer.children].filter(el => el.classList.contains('se-node'));
    childNodes.forEach(child => {
      const name = child.querySelector('.se-name')?.value?.trim();
      if (!name) return;
      props[name] = serializeNode(child);
      required.push(name);
    });
    out.properties = props;
    out.required = required;
    out.additionalProperties = false;
  }
  if (type === 'array') {
    const itemsContainer = node.querySelector('.se-items');
    const itemNode = itemsContainer.querySelector('.se-node');
    if (itemNode) {
      out.items = serializeNode(itemNode);
    }
  }
  return out;
}

function parseEnumValue(raw, type) {
  if (type === 'string') return raw;
  if (type === 'integer') {
    const n = parseInt(raw, 10);
    return isNaN(n) ? raw : n;
  }
  if (type === 'number') {
    const n = Number(raw);
    return isNaN(n) ? raw : n;
  }
  if (type === 'boolean') {
    return raw.toLowerCase() === 'true';
  }
  return raw;
}

export function getSchemaEditorValue(target) {
  const container = typeof target === 'string' ? document.getElementById(target) : target;
  const root = container.querySelector('.se-root');
  const childrenContainer = root.querySelector('.se-children');
  const props = {};
  const required = [];
  const childNodes = [...childrenContainer.children].filter(el => el.classList.contains('se-node'));
  childNodes.forEach(child => {
    const name = child.querySelector('.se-name')?.value?.trim();
    if (!name) return;
    props[name] = serializeNode(child);
    required.push(name);
  });
  return {
    type: 'object',
    properties: props,
    required: required,
    additionalProperties: false
  };
}

export function setSchemaEditorValue(target, schema) {
  createSchemaEditor(target, schema);
}
