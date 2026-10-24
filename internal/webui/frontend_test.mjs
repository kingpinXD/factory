import test from 'node:test';
import assert from 'node:assert/strict';
import { webcrypto } from 'node:crypto';
import { execFileSync, spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { ActionTracker, SnapshotCache, githubURL, captureDrafts, restoreDrafts, time, jobEntities, mergeActivity, diagnosticBundle, receiptTarget } from './assets/ui-core.mjs';
if (!globalThis.crypto) globalThis.crypto = webcrypto;

test('lost submission response checks its original receipt and never submits twice', async () => {
  let sends = 0;
  const lookedUp = [];
  const tracker = new ActionTracker(async () => { sends++; throw new Error('connection lost'); }, async id => { lookedUp.push(id); return { request_id: id, state: 'queued', entity_id: 'e1' }; });
  const receipt = await tracker.start('new-job', '/jobs', { input: { kind: 'text', value: 'Private goal' } });
  assert.equal(sends, 1);
  assert.equal(receipt.state, 'queued');
  assert.deepEqual(lookedUp, [receipt.request_id]);
  const duplicate = await tracker.start('new-job', '/jobs', { input: { kind: 'text', value: 'Private goal' } });
  assert.equal(duplicate.request_id, receipt.request_id);
  await tracker.poll();
  assert.equal(sends, 1);
  assert.deepEqual(lookedUp, [receipt.request_id, receipt.request_id]);
});

test('simultaneous clicks share one intent until the receipt is resolved', async () => {
  let resolve;
  let sends = 0;
  const tracker = new ActionTracker(() => { sends++; return new Promise(done => { resolve = done; }); }, async () => ({}));
  const first = tracker.start('answer:e1:q1', '/actions', { type: 'answer' });
  const second = await tracker.start('answer:e1:q1', '/actions', { type: 'answer' });
  assert.equal(second.state, 'pending');
  assert.equal(sends, 1);
  resolve({ state: 'applied' });
  const result = await first;
  assert.equal(result.request_id, second.request_id);
  assert.equal(tracker.scopes.size, 0);
});

test('unavailable receipt holds the submission instead of replaying intake', async () => {
  let sends = 0;
  const tracker = new ActionTracker(async () => { sends++; throw new Error('lost'); }, async () => { throw new Error('not yet available'); });
  const receipt = await tracker.start('new-job', '/jobs', {});
  await tracker.poll();
  assert.equal(sends, 1);
  assert.equal(receipt.recovering, true);
  assert.equal(tracker.scopes.get('new-job'), receipt.request_id);
});

test('outcome unknown is displayed as terminal and is never polled into a replay', async () => {
  let lookups = 0;
  const tracker = new ActionTracker(async () => ({ state: 'outcome_unknown', message: 'Inspect partial effects.' }), async () => { lookups++; });
  await tracker.start('retry:e1', '/actions', {});
  await tracker.poll();
  assert.equal(lookups, 0);
  assert.equal([...tracker.requests.values()][0].state, 'outcome_unknown');
});

test('failed and out-of-order reads retain the last good observation', () => {
  const cache = new SnapshotCache();
  const good = { at: '2026-10-24T13:00:00Z', jobs: [{ id: 'e1' }] };
  cache.accept('overview', good);
  cache.fail('overview', 'Disconnected');
  assert.equal(cache.get('overview'), good);
  assert.equal(cache.accept('overview', { at: '2026-10-24T12:59:00Z', jobs: [] }), false);
  assert.equal(cache.get('overview'), good);
  cache.accept('overview', { at: '2026-10-24T13:01:00Z', jobs: [] });
  assert.equal(cache.errors.has('overview'), false);
});

test('poll replacement restores draft text, choice, focus and cursor', () => {
  const text = { dataset: { draft: 'answer:e1' }, value: '<script>private draft</script>', checked: false, selectionStart: 5, selectionEnd: 9 };
  const select = { dataset: { draft: 'decision:e1' }, value: 'keep', checked: false, selectionStart: null, selectionEnd: null };
  const root = { querySelectorAll: () => [text, select], ownerDocument: { activeElement: text } };
  const drafts = captureDrafts(root);
  const restored = [{ ...text, value: '', focus(options) { this.focused = options; }, setSelectionRange(start, end) { this.cursor = [start, end]; } }, { ...select, value: '' }];
  restoreDrafts({ querySelectorAll: () => restored }, drafts);
  assert.equal(restored[0].value, '<script>private draft</script>');
  assert.deepEqual(restored[0].focused, { preventScroll: true });
  assert.deepEqual(restored[0].cursor, [5, 9]);
  assert.equal(restored[1].value, 'keep');
});

test('only credential-free HTTPS GitHub links are navigable', () => {
  assert.equal(githubURL('https://github.com/org/repo/pull/1'), 'https://github.com/org/repo/pull/1');
  for (const value of ['javascript:alert(1)', 'https://github.com.evil.example/repo', 'http://github.com/org/repo', 'https://token@github.com/org/repo', '//github.com/org/repo', '<img src=x onerror=alert(1)>']) assert.equal(githubURL(value), null);
  assert.equal(time('0001-01-01T00:00:00Z'), 'Not observed');
});

test('definite HTTP refusal releases the form and is not treated as a lost write', async () => {
  let lookups = 0;
  const tracker = new ActionTracker(async () => { const error = new Error('Stale question'); error.status = 409; throw error; }, async () => { lookups++; });
  const receipt = await tracker.start('answer:e1:q1', '/actions', {});
  assert.equal(receipt.state, 'refused');
  assert.equal(receipt.message, 'Stale question');
  assert.equal(lookups, 0);
  assert.equal(tracker.scopes.size, 0);
});

test('reload recovers only persisted identity, including an unknown intake, without private payload', async () => {
  const memory = new Map();
  const storage = { getItem: key => memory.get(key), setItem: (key, value) => memory.set(key, value) };
  const tracker = new ActionTracker(async () => ({ state: 'outcome_unknown', message: 'Private response' }), async () => ({}), () => {}, storage);
  const result = await tracker.start('new-job', '/jobs', { input: { value: 'Private goal' }, token: 'secret' });
  const saved = storage.getItem('factory-ui-requests');
  assert.equal(saved.includes('Private'), false);
  assert.equal(saved.includes('secret'), false);
  assert.deepEqual(JSON.parse(saved), [{ request_id: result.request_id, scope: 'new-job' }]);
  let sends = 0;
  const restored = new ActionTracker(async () => { sends++; }, async id => ({ state: 'outcome_unknown', request_id: id }), () => {}, storage);
  await restored.poll();
  await restored.start('new-job', '/jobs', {});
  assert.equal(sends, 0);
  assert.equal(restored.scopes.get('new-job'), result.request_id);
});


test('the embedded browser entrypoint parses before shipping', () => {
  for (const name of ['app.js', 'ui-core.mjs', 'bootstrap.js']) {
    const source = readFileSync(new URL('./assets/' + name, import.meta.url), 'utf8');
    execFileSync(process.execPath, ['--check', '--input-type=module'], { input: source });
  }
  const broken = spawnSync(process.execPath, ['--check', '--input-type=module'], { input: 'import x from "./x.mjs"; function view() { return el("div", {}; }', encoding: 'utf8' });
  assert.notEqual(broken.status, 0, 'syntax gate must reject malformed module bodies');
});


test('debug and activity can target every task and group in a job', () => {
  assert.deepEqual(jobEntities({ id: 'e1', title: 'Goal', items: [{ id: 'e1-w1', repo: 'o/r', issue: 12 }], groups: [{ id: 'e1-s1' }] }).map(entity => entity.id), ['e1', 'e1-w1', 'e1-s1']);
});
test('activity pagination retains entity-local identity and previously loaded events', () => {
  const previous = { events: [{ entity_id: 'e1', seq: 1, summary: 'Job' }, { entity_id: 'e1-w1', seq: 1, summary: 'Task' }], next_seq: 1 };
  const result = mergeActivity(previous, { events: [{ entity_id: 'e1-w1', seq: 1, summary: 'Updated task' }, { entity_id: 'e1-w1', seq: 2, summary: 'New task event' }], next_seq: 2, has_more: false });
  assert.equal(result.events.length, 3);
  assert.equal(result.events[0].summary, 'Job');
  assert.equal(result.events[1].summary, 'Updated task');
  assert.equal(result.next_seq, 2);
  assert.equal(mergeActivity(result, { events: [], next_seq: 0 }).next_seq, 2);
});


test('diagnostics include build and observation provenance without host configuration', () => {
  const result = diagnosticBundle([{ name: 'Selected output', value: { text: 'redacted' } }], { version: 'factory unknown abc123', brain: '/private/brain', executable: '/private/bin' }, { at: '2026-10-24T12:00:00Z', last_tick: '2026-10-24T11:59:00Z', health: { source_at: '2026-10-24T11:59:00Z' }, account: { source_at: '2026-10-24T11:58:00Z' } }, new Date('2026-10-24T12:01:00Z'));
  assert.equal(result.source_version, 'factory unknown abc123');
  assert.equal(result.observation_at, '2026-10-24T12:00:00Z');
  assert.equal(result.account_source_at, '2026-10-24T11:58:00Z');
  assert.equal(JSON.stringify(result).includes('/private'), false);
});

test('embedded app switches from task Debug to first-load Activity', async () => {
  const names = ['document', 'Node', 'window', 'location', 'history', 'fetch', 'setInterval'];
  const saved = new Map(names.map(name => [name, globalThis[name]]));
  const calls = [];
  let doc;
  class FakeNode {
    constructor(tag, text = '') { this.tagName = tag.toUpperCase(); this.children = []; this.attributes = {}; this.dataset = {}; this.events = {}; this._text = text; this.value = ''; this.classList = { toggle() {}, contains() { return false; } }; this.ownerDocument = doc; }
    get textContent() { return this._text + this.children.map(child => child.textContent).join(''); }
    set textContent(value) { this._text = String(value); this.children = []; }
    setAttribute(key, value) { this.attributes[key] = String(value); if (key === 'data-draft') this.dataset.draft = value; if (key === 'data-scope') this.dataset.scope = value; if (key === 'value') this.value = String(value); }
    append(...nodes) { this.children.push(...nodes); if (this.tagName === 'SELECT') this.value = (this.children.find(child => child.attributes.selected !== undefined) || this.children[0])?.value || ''; }
    replaceChildren(...nodes) { this._text = ''; this.children = []; this.append(...nodes); }
    addEventListener(name, handler) { this.events[name] = handler; }
    querySelectorAll(selector) { const attr = selector.slice(1, -1); return this.children.flatMap(child => [child, ...child.querySelectorAll(selector)]).filter(child => child.attributes[attr] !== undefined); }
    focus() { doc.activeElement = this; }
    showModal() { this.open = true; }
    close() { this.open = false; }
  }
  const roots = new Map();
  doc = { activeElement: null, hidden: false, querySelector(selector) { if (!roots.has(selector)) roots.set(selector, new FakeNode('div')); return roots.get(selector); }, querySelectorAll() { return []; }, createElement: tag => new FakeNode(tag), createTextNode: text => new FakeNode('#text', String(text)), addEventListener() {} };
  const job = { id: 'e1', title: 'Fixture goal', state: 'running', health: { category: 'working', reason: 'Recorded work', allowed_actions: [] }, counts: { total: 1 }, items: [{ id: 'e1-w1', repo: 'o/r', state: 'in_review', health: { category: 'waiting', allowed_actions: ['message'] }, pr: {} }], groups: [{ id: 'e1-s1', items: ['e1-w1'] }], questions: [], applied_plan: { name: 'Applied fixture plan', artifact_id: 'fixture', issues: [{ ref: 'o/r#12', result: 'ready', answers: [] }] } };
  const report = { at: '2026-10-24T12:00:00Z', jobs: [job], health: { category: 'working' }, account: {} };
  const settle = async () => { for (let i = 0; i < 5; i++) await new Promise(resolve => setImmediate(resolve)); };
  const descendants = node => [node, ...node.children.flatMap(descendants)];
  const click = async (text, root = '#main') => { const control = descendants(roots.get(root)).find(node => node.tagName === 'BUTTON' && node.textContent === text); assert.ok(control, `missing button ${text}`); await control.events.click({ preventDefault() {} }); await settle(); };
  try {
    Object.assign(globalThis, { document: doc, Node: FakeNode, window: { sessionStorage: { getItem: () => null, setItem() {} } }, location: { hash: '', pathname: '/' }, history: { replaceState() {} }, setInterval() {}, fetch: async (path, options = {}) => {
      calls.push(path);
      let data;
      if (path === '/session') data = { csrf: 'fixture-csrf' };
      else if (path.endsWith('/overview')) data = report;
      else if (path.endsWith('/health')) data = { answer_supported: true, version: 'fixture', prerequisites: [] };
      else if (path.endsWith('/jobs/e1')) data = job;
      else if (path.endsWith('/actions') && options.method === 'POST') data = { request_id: JSON.parse(options.body).request_id, state: 'applied', entity_id: 'e1-w1', message: 'Logged, not delivered.' };
      else if (path.endsWith('/inspection')) data = { id: 'e1-w1', observation: { state: 'idle' }, artifacts: [], conversation_reason: 'Fixture conversation unavailable' };
      else if (path.includes('/events?')) data = { events: [], next_seq: 0, has_more: false };
      else throw new Error(`unexpected fixture read ${path}`);
      return { ok: true, json: async () => data };
    } });
    await import(`./assets/app.js?dom-regression=${Date.now()}`);
    await settle();
    await click('Fixture goal');
    await click('Inspect task');
    assert.ok(calls.some(url => url.endsWith('/entities/e1-w1/inspection')));
    await click('Activity');
    assert.ok(calls.some(url => url.includes('/entities/e1-w1/events?')));
    assert.match(roots.get('#main').textContent, /No recorded activity/);
    await click('Plan & outputs');
    assert.match(roots.get('#main').textContent, /Applied plan/);
    assert.equal(roots.get('#main').textContent.includes('0ready'), false);
    await click('Work');
    assert.match(roots.get('#main').textContent, /Inspect task/);
    await click('Send message');
    const form = descendants(roots.get('#dialog-body')).find(node => node.tagName === 'FORM');
    const input = descendants(form).find(node => node.tagName === 'TEXTAREA');
    input.value = 'Synthetic test message';
    await form.events.submit({ preventDefault() {} });
    await settle();
    await click('Open e1-w1', '#receipts');
    assert.equal(calls.some(url => url.endsWith('/jobs/e1-w1')), false);
    assert.match(roots.get('#main').textContent, /Fixture goal/);
    assert.ok(calls.filter(url => url.includes('/entities/e1-w1/events?')).length >= 2);
  } finally { for (const [name, value] of saved) { if (value === undefined) delete globalThis[name]; else globalThis[name] = value; } }
});


test('receipt navigation resolves actual membership and never guesses from an ID prefix', () => {
  const jobs = [{ id: 'e1', items: [{ id: 'other-work' }], groups: [{ id: 'shared-set' }] }];
  assert.deepEqual(receiptTarget('other-work', jobs), { job_id: 'e1', entity_id: 'other-work' });
  assert.deepEqual(receiptTarget('shared-set', jobs), { job_id: 'e1', entity_id: 'shared-set' });
  assert.equal(receiptTarget('e1-unknown', jobs), null);
});
