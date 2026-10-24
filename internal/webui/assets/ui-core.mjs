export const API = '/api/v1/instances/local';
export const terminalReceipt = state => ['applied', 'refused', 'failed', 'outcome_unknown'].includes(state);
export const label = value => String(value ?? 'Unknown').replaceAll('_', ' ');
export function githubURL(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && url.hostname === 'github.com' && !url.username && !url.password ? url.href : null;
  } catch { return null; }
}
export function time(value) {
  if (!value || String(value).startsWith('0001-')) return 'Not observed';
  const date = new Date(value);
  return Number.isNaN(date.valueOf()) ? 'Unknown time' : date.toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
}
export class SnapshotCache {
  constructor() { this.values = new Map(); this.errors = new Map(); }
  accept(key, value) {
    const previous = this.values.get(key);
    if (previous?.at && value?.at && new Date(value.at) < new Date(previous.at)) {
      this.fail(key, 'An older observation arrived. Showing the last good observation.');
      return false;
    }
    this.values.set(key, value);
    this.errors.delete(key);
    return true;
  }
  fail(key, message) { this.errors.set(key, message); }
  get(key) { return this.values.get(key); }
}
export class ActionTracker {
  constructor(send, lookup, changed = () => {}, storage = null) {
    this.send = send; this.lookup = lookup; this.changed = changed; this.storage = storage; this.requests = new Map(); this.scopes = new Map();
    try {
      for (const { request_id, scope } of JSON.parse(storage?.getItem('factory-ui-requests') || '[]')) {
        if (!/^[a-f0-9-]{36}$/i.test(request_id) || !/^(new-job|[a-z_]+:[a-z0-9_.:-]+)$/i.test(scope)) continue;
        this.requests.set(request_id, { request_id, scope, state: 'pending', recovering: true, message: 'Recovering the original request after reload…' });
        this.scopes.set(scope, request_id);
      }
    } catch { /* Storage may be disabled by the browser. */ }
  }
  persist() {
    try { this.storage?.setItem('factory-ui-requests', JSON.stringify([...this.scopes].map(([scope, request_id]) => ({ request_id, scope })))); } catch { /* In-memory receipts remain available. */ }
  }
  async start(scope, route, payload) {
    if (this.scopes.has(scope)) return this.requests.get(this.scopes.get(scope));
    const id = crypto.randomUUID();
    const record = { request_id: id, state: 'pending', message: 'Sending request…', scope };
    this.scopes.set(scope, id); this.requests.set(id, record); this.persist(); this.changed();
    try {
      this.update(id, await this.send(route, { ...payload, request_id: id }));
    } catch (error) {
      if (error.status >= 400 && error.status < 500) {
        this.update(id, { state: 'refused', message: error.message });
        return this.requests.get(id);
      }
      this.update(id, { state: 'pending', message: 'Response lost. Checking the same request; nothing will be submitted again.', recovering: true });
      await this.recover(id);
    }
    return this.requests.get(id);
  }
  update(id, receipt) {
    const record = { ...this.requests.get(id), ...receipt, request_id: id };
    this.requests.set(id, record);
    if (terminalReceipt(record.state) && record.state !== 'outcome_unknown') this.scopes.delete(record.scope);
    this.persist(); this.changed();
  }
  async recover(id) {
    try { this.update(id, await this.lookup(id)); }
    catch { this.update(id, { recovering: true, message: 'The result is not available. Keep this page open to check the original request. Do not submit it again.' }); }
  }
  async poll() {
    await Promise.all([...this.requests.values()].filter(r => !terminalReceipt(r.state)).map(r => this.recover(r.request_id)));
  }
}
export function captureDrafts(root) {
  const drafts = new Map();
  for (const input of root.querySelectorAll('[data-draft]')) {
    drafts.set(input.dataset.draft, { value: input.value, checked: input.checked, start: input.selectionStart, end: input.selectionEnd, focused: input === root.ownerDocument.activeElement });
  }
  return drafts;
}
export function restoreDrafts(root, drafts) {
  for (const input of root.querySelectorAll('[data-draft]')) {
    const draft = drafts.get(input.dataset.draft);
    if (!draft) continue;
    input.value = draft.value;
    if ('checked' in input) input.checked = draft.checked;
    if (draft.focused) {
      input.focus({ preventScroll: true });
      if (typeof input.setSelectionRange === 'function' && draft.start !== null) {
        try { input.setSelectionRange(draft.start, draft.end); } catch { /* Select and number inputs have no text selection. */ }
      }
    }
  }
}

export function jobEntities(job) {
  return [{ id: job.id, name: `Job · ${job.title || job.id}` }, ...(job.items || []).map(task => ({ id: task.id, name: `Task · ${task.repo || task.id}${task.issue ? ' #' + task.issue : ''} · ${task.id}` })), ...(job.groups || []).map(group => ({ id: group.id, name: `Group · ${group.id}` }))];
}
export function mergeActivity(previous, page) {
  const events = new Map((previous?.events || []).map(event => [`${event.entity_id}:${event.seq}`, event]));
  for (const event of page.events || []) events.set(`${event.entity_id}:${event.seq}`, event);
  return { ...page, events: [...events.values()], next_seq: Math.max(previous?.next_seq || 0, page.next_seq || 0) };
}

export function diagnosticBundle(evidence, health = {}, report = {}, now = new Date()) {
  return { exported_at: now.toISOString(), instance: 'local', source_version: health.version || 'unknown', observation_at: report.at || null, last_tick: report.last_tick || null, manager_source_at: report.health?.source_at || null, account_source_at: report.account?.source_at || null, evidence };
}

export function receiptTarget(entityID, jobs = []) {
  for (const job of jobs) {
    if (job.id === entityID) return { job_id: job.id, entity_id: entityID };
    if ([...(job.items || []), ...(job.groups || [])].some(entity => entity.id === entityID)) return { job_id: job.id, entity_id: entityID };
  }
  return null;
}
