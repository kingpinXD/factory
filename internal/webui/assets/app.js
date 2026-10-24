import { API, ActionTracker, SnapshotCache, captureDrafts, restoreDrafts, githubURL, label, time, terminalReceipt, jobEntities, mergeActivity, diagnosticBundle, receiptTarget } from './ui-core.mjs';

const main = document.querySelector('#main');
const dialog = document.querySelector('#dialog');
const cache = new SnapshotCache();
const state = { page: 'overview', job: '', entity: '', tab: 'work', filter: 'active', query: '', csrf: '', busy: false, dismissed: new Set(), activity: new Map(), evidence: new Map(), selected: new Set() };
const actionNames = { answer: 'Answer', retry: 'Retry', replan: 'Request replan', message: 'Send message', deployed: 'Confirm deployment', review_asked: 'Record review request', cancel: 'Cancel' };

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (key === 'class') node.className = value;
    else if (key.startsWith('on')) node.addEventListener(key.slice(2).toLowerCase(), value);
    else if (value !== undefined && value !== null && value !== false) node.setAttribute(key, value === true ? '' : value);
  }
  for (const child of children.flat(Infinity)) {
    if (child === undefined || child === null || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}
const button = (text, handler, cls = '', disabled = false) => el('button', { type: 'button', class: cls, onclick: handler, disabled }, text);
const badge = (category = 'unknown', text = label(category)) => el('span', { class: `badge ${category}` }, text);
const hint = text => el('div', { class: 'hint' }, text);
const notice = (text, blue = false) => el('div', { class: `notice${blue ? ' blue' : ''}` }, text);
const textBlock = value => el('pre', { class: 'log-content', tabindex: '0' }, typeof value === 'string' ? value : JSON.stringify(value, null, 2));
const heading = (title, subtitle, action) => el('div', { class: 'page-heading' }, el('div', {}, el('div', { class: 'eyebrow' }, 'YOUR FACTORY, AT A GLANCE'), el('h1', {}, title), el('p', { class: 'subtitle' }, subtitle)), action);
const sectionHead = (title, action) => el('div', { class: 'section-head' }, el('h2', {}, title), action);
const empty = (title, description, action) => el('div', { class: 'panel empty' }, el('div', { class: 'empty-mark', 'aria-hidden': 'true' }, '◇'), el('h3', {}, title), el('p', {}, description), action);
const block = (title, ...children) => el('section', { class: 'panel block' }, el('h2', {}, title), ...children);
const list = values => el('ul', { class: 'list' }, (values || []).map(value => el('li', {}, typeof value === 'string' ? value : JSON.stringify(value))));
const valuePair = (name, value) => el('div', {}, el('dt', {}, name), el('dd', {}, value ?? 'Not observed'));
function link(text, url) {
  const safe = githubURL(url);
  return safe ? el('a', { href: safe, target: '_blank', rel: 'noopener noreferrer' }, text) : el('span', { class: 'muted' }, text);
}
function field(name, input, help) {
  const id = input.id || `field-${crypto.randomUUID()}`;
  input.id = id;
  return el('div', { class: 'field' }, el('label', { for: id }, name), input, help && hint(help));
}
function errorsPanel(errors) {
  return errors?.length ? notice(['Some sources could not be read. ', ...errors.map(String)].join('\n')) : null;
}
async function request(path, options = {}) {
  const response = await fetch(path, { credentials: 'same-origin', cache: 'no-store', ...options });
  const data = await response.json();
  if (!response.ok) {
    const error = new Error(data.error || `Request failed (${response.status}).`);
    error.status = response.status;
    throw error;
  }
  return data;
}
const api = path => request(API + path);
async function post(path, data) {
  return request(path, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-CSRF-Token': state.csrf }, body: JSON.stringify(data) });
}
const tracker = new ActionTracker((route, payload) => post(API + route, payload), id => api(`/actions/${encodeURIComponent(id)}`), () => { renderReceipts(); updateButtons(); }, window.sessionStorage);
function updateButtons() {
  for (const control of document.querySelectorAll('[data-scope]')) {
    control.disabled = tracker.scopes.has(control.dataset.scope) || Boolean(cache.get('health')?.read_only) || (control.dataset.answer === 'true' && cache.get('health')?.answer_supported !== true);
  }
}
function renderReceipts() {
  const records = [...tracker.requests.values()].filter(r => !state.dismissed.has(r.request_id)).slice(-3);
  document.querySelector('#receipts').replaceChildren(...records.map(record => el('article', { class: 'receipt' }, el('div', { class: 'receipt-top' }, badge(record.state), el('strong', {}, 'Action receipt'), terminalReceipt(record.state) && button('×', () => { state.dismissed.add(record.request_id); renderReceipts(); }, 'receipt-close')), el('p', {}, record.message || 'Waiting for the factory to process this request.'), el('div', { class: 'mono' }, record.request_id), record.entity_id && button(`Open ${record.entity_id}`, () => openReceipt(record.entity_id), 'inline-link'))));
}
async function openReceipt(entityID) {
  let target = receiptTarget(entityID, cache.get('overview')?.jobs);
  if (!target) {
    await observe('overview', '/overview');
    target = receiptTarget(entityID, cache.get('overview')?.jobs);
  }
  if (!target) {
    showDialog('Receipt target unavailable', [notice('This target is not in the current job observation. Refresh the jobs list or inspect the original receipt before acting again.'), hint(entityID)]);
    return;
  }
  await openJob(target.job_id);
  state.entity = target.entity_id;
  if (target.entity_id !== target.job_id) {
    state.tab = 'activity';
    render();
    await loadActivity();
  }
}
async function observe(key, path) {
  try { const data = await api(path); cache.accept(key, data); return data; }
  catch (error) { cache.fail(key, error.message); return null; }
}
function connection() {
  const errors = [...cache.errors.values()];
  const banner = document.querySelector('#connection');
  banner.hidden = errors.length === 0;
  banner.textContent = errors.length ? `Connection or source issue · ${errors[0]} Last good data remains visible.` : '';
}
function navigate(page) {
  state.page = page; state.job = ''; state.entity = ''; state.tab = 'work'; render(); refresh();
}
async function openJob(id) {
  state.page = 'detail'; state.job = id; state.entity = id; state.tab = 'work'; render(); await refresh();
}
function currentJob() { return cache.get(`job:${state.job}`) || cache.get('overview')?.jobs?.find(job => job.id === state.job); }
function counts(count = {}) {
  return el('div', { class: 'counts' }, el('span', {}, el('strong', {}, count.delivered ?? 0), ' delivered'), el('span', {}, el('strong', {}, count.cancelled ?? 0), ' cancelled'), el('span', {}, el('strong', {}, count.cleanup_pending ?? 0), ' cleanup pending'));
}
function metric(title, value, detail, category) {
  return el('section', { class: 'panel metric' }, el('div', { class: 'metric-label' }, title), el('div', { class: 'metric-row' }, el('div', { class: 'metric-value' }, value), category && badge(category)), el('div', { class: 'metric-detail' }, detail));
}
function capacity(account = {}) {
  const known = typeof account.five_hour === 'number' || typeof account.seven_day === 'number';
  const usage = known ? `${account.five_hour == null ? '—' : account.five_hour + '%'} short · ${account.seven_day == null ? '—' : account.seven_day + '%'} weekly` : 'Not observed';
  return metric('CLAUDE CAPACITY · CACHED', usage, `Observed ${time(account.source_at)} · ${label(account.state || 'unknown')} · ${label(account.freshness || 'unknown')} usage observer`);
}
function row(job, header = false) {
  if (header) return el('div', { class: 'job-row table-head', 'aria-hidden': 'true' }, el('div', {}, 'Job'), el('div', {}, 'Status'), el('div', {}, 'Delivery'), el('div', {}, 'Updated'));
  return el('article', { class: 'job-row' }, el('div', {}, button(job.title || job.id, () => openJob(job.id), 'job-name'), el('div', { class: 'job-sub mono' }, job.id)), el('div', {}, badge(job.health?.category), el('div', { class: 'job-reason' }, job.health?.reason || 'Health not observed')), el('div', { class: 'job-count' }, `${job.counts?.delivered ?? 0} / ${job.counts?.total ?? 0} delivered`, el('div', { class: 'job-sub' }, `${job.counts?.cancelled ?? 0} cancelled · ${job.counts?.cleanup_pending ?? 0} cleanup`)), el('div', { class: 'small muted' }, time(job.health?.source_at || job.since)));
}
function jobList(jobs) { return el('section', { class: 'panel job-list', 'aria-label': 'Jobs' }, row(null, true), jobs.map(job => row(job))); }
function overview(report) {
  const jobs = report.jobs || [];
  const attention = jobs.filter(job => ['needs_decision', 'possibly_stuck', 'unknown'].includes(job.health?.category) && !job.end);
  const active = jobs.filter(job => !job.end);
  return [heading('Overview', 'Know what is moving. See where you are needed.', button('Refresh', refresh, 'ghost compact')), sectionHead(el('span', {}, 'Needs attention', el('span', { class: 'count-dot' }, attention.length))), attention.length ? el('div', { class: 'attention-grid' }, attention.slice(0, 6).map(job => el('article', { class: 'panel attention-card' }, el('div', { class: 'eyebrow' }, label(job.health.category)), el('h3', { class: 'attention-title' }, job.title || job.id), el('p', {}, job.questions?.[0]?.text || job.health.reason), button(job.questions?.length ? 'View decision →' : 'Inspect job →', () => openJob(job.id), 'ghost compact')))) : empty('Nothing needs your decision', 'Recorded questions and recovery incidents appear here. Source freshness is shown separately below.'), el('div', { class: 'health-grid' }, metric('MANAGER', label(report.health?.category), report.health?.reason || 'No manager observation', report.health?.category), capacity(report.account), metric('SOURCE FRESHNESS', time(report.last_tick), `Last completed tick · observation ${time(report.at)}`)), sectionHead(`Active jobs · ${active.length}`, button('View all jobs →', () => navigate('jobs'), 'inline-link')), active.length ? jobList(active.slice(0, 8)) : empty('Your next job starts here', 'Submit a goal, an issue, an epic, or a plan. The factory prepares the work and manages it independently.', button('＋ Create a job', newJob, 'primary')), errorsPanel([...(report.errors || []), ...(report.problems || [])])];
}
function jobsView(report) {
  const records = cache.get('jobs')?.jobs || report.jobs || [];
  const filtered = records.filter(job => (state.filter === 'all' || (state.filter === 'completed' ? job.end : !job.end)) && `${job.id} ${job.title}`.toLowerCase().includes(state.query.toLowerCase()));
  const search = el('input', { type: 'search', placeholder: 'Search jobs by title or ID…', value: state.query, 'data-draft': 'job-search', class: 'search', 'aria-label': 'Search jobs', oninput: event => { state.query = event.target.value; render(); } });
  return [heading('Jobs', 'From the first plan to the last cleanup.'), el('div', { class: 'toolbar' }, search, ...['active', 'completed', 'all'].map(filter => button(label(filter), () => { state.filter = filter; render(); }, state.filter === filter ? 'primary compact' : 'ghost compact'))), filtered.length ? jobList(filtered) : empty('No matching jobs', state.query ? 'Try a different title or ID.' : 'There are no jobs in this view.'), cache.get('jobs')?.next_offset > 0 && button('Load more jobs', async () => { const data = await api(`/jobs?offset=${cache.get('jobs').next_offset}&limit=100`); cache.accept('jobs', { ...data, jobs: [...records, ...(data.jobs || [])] }); render(); }, 'ghost')];
}
function allowed(entity) { return entity.health?.allowed_actions || []; }
function actionButtons(entity) {
  return el('div', { class: 'actions' }, allowed(entity).filter(type => type !== 'answer').map(type => {
    const control = button(actionNames[type] || label(type), () => type === 'cancel' ? cancelJob(entity) : actionDialog(type, entity), type === 'cancel' ? 'danger compact' : 'ghost compact', cache.get('health')?.read_only);
    control.dataset.scope = `${type}:${entity.id}`;
    return control;
  }));
}
function questionCard(question) {
  const supported = cache.get('health')?.answer_supported === true;
  const scope = `answer:${question.target_id}:${question.issue}`;
  const input = question.choices?.length ? el('select', { required: true, 'data-draft': scope }, el('option', { value: '' }, 'Choose an answer…'), question.choices.map(choice => el('option', { value: choice }, choice))) : el('textarea', { required: true, placeholder: 'Your answer…', 'data-draft': scope });
  const submit = el('button', { type: 'submit', class: 'primary compact', disabled: !supported || cache.get('health')?.read_only, 'data-scope': scope }, 'Send answer');
  submit.dataset.answer = 'true';
  const form = el('form', { onsubmit: async event => { event.preventDefault(); if (!supported || submit.disabled) return; submit.disabled = true; await tracker.start(scope, '/actions', { type: 'answer', target_id: question.target_id, expected: question.expected, issue: question.issue || '', value: input.value }); await refresh(); } }, field(question.choices?.length ? 'Supported answer' : 'Answer', input), el('div', { class: 'actions' }, submit));
  return el('section', { class: 'panel question' }, badge('needs_decision', 'Your decision'), el('h3', { class: 'wrap' }, question.text), el('div', { class: 'small muted mono' }, `${question.target_id}${question.issue ? ' · ' + question.issue : ''}`), !supported && notice('Answering requires a fresh tick from a manager that supports browser answers. The current manager has not advertised this capability.'), form);
}
function taskCard(task) {
  const pr = task.pr || {};
  return el('article', { class: 'task' }, el('div', { class: 'task-top' }, el('div', {}, el('h3', { class: 'task-title' }, task.repo || task.id, task.issue ? ` #${task.issue}` : ''), el('span', { class: 'mono muted' }, task.id)), badge(task.health?.category, label(task.state))), el('div', { class: 'task-body' }, el('div', {}, task.health?.reason || 'No current health observation'), pr.url && el('div', {}, link('Open pull request ↗', pr.url), ' · ', label(pr.state), ` · ${pr.unanswered ?? 0} unanswered comments`), pr.merge_hold && notice(`Merge hold: ${pr.merge_hold}`), el('div', {}, `Phase since ${time(task.since)} · Meaningful progress ${time(task.progress_at)}`), el('div', {}, `Owner: ${task.owner || 'Not recorded'} · Lease ${task.lease ?? 'unknown'}`), el('div', {}, `Cleanup: ${task.cleanup || 'Not recorded'}`), task.adopted && hint('Adopted pull request. Cancellation uses its preservation rules.'), el('div', { class: 'actions' }, actionButtons(task), button('Inspect task', () => { state.entity = task.id; state.tab = 'debug'; render(); inspect(task.id); }, 'ghost compact'))));
}
function workView(job) {
  const details = el('aside', { class: 'panel aside-panel' }, el('h3', {}, 'Job details'), el('dl', { class: 'key-values' }, valuePair('Recorded state', label(job.state)), valuePair('Executor', job.executor || 'Unknown'), valuePair('Phase since', time(job.since)), valuePair('Meaningful progress', time(job.health?.progress_at)), valuePair('Source observed', time(job.health?.source_at))), sectionHead('Delivery'), counts(job.counts), hint('Cancelled work and cleanup do not count as delivered.'), sectionHead('Available actions'), actionButtons(job));
  const dependencies = job.dependencies?.length ? block('Dependencies', job.dependencies.map(dep => el('div', { class: 'artifact-row' }, el('div', {}, el('div', { class: 'mono' }, `${dep.from} → ${dep.to}`), el('div', { class: 'small muted' }, `${label(dep.gate)} · ${dep.when || 'No release condition recorded'}`)), badge('waiting', dep.release || 'Unknown release')))) : null;
  const groupRows = (job.groups || []).map(group => {
    const identity = el('div', {},
      el('strong', { class: 'small' }, group.id),
      el('div', { class: 'small muted' }, (group.items || []).join(' → ')));
    return el('div', { class: 'artifact-row' }, identity,
      badge(group.health?.category, label(group.state)));
  });
  const groups = groupRows.length ? block('Execution groups', groupRows) : null;
  const tasks = job.items?.length
    ? el('section', { class: 'panel' }, job.items.map(taskCard))
    : empty('No tasks have been applied', 'The planner may still be preparing work. Check the recorded reason above.');
  const content = el('div', {}, tasks, dependencies, groups);
  return [job.questions?.map(questionCard), el('div', { class: 'detail-grid' }, content, details)];
}
async function loadActivity() {
  const id = state.entity || state.job;
  const old = state.activity.get(id);
  const after = old?.next_seq || 0;
  const data = await observe(`events:${id}`, `/entities/${encodeURIComponent(id)}/events?after_seq=${after}&limit=100`);
  if (data) state.activity.set(id, mergeActivity(old, data));
  render();
}
function activityView(job) {
  const activity = state.activity.get(state.entity || job.id);
  if (!activity) return [empty('Activity has not been loaded', 'Events are saved by entity. The timeline shows recorded observations.', button('Load activity', () => loadActivity(), 'ghost'))];
  return [notice('Times show recorded observations. Events from different entities do not establish a strict causal order.', true), activity.events?.length ? el('ol', { class: 'panel timeline' }, activity.events.map(event => el('li', {}, el('div', { class: 'timeline-time' }, time(event.at), el('div', { class: 'mono' }, `#${event.seq}`)), el('div', { class: 'timeline-copy' }, el('span', { class: 'timeline-kind' }, label(event.kind)), event.summary, el('div', { class: 'job-sub mono' }, `${event.entity_id}${event.sender ? ' · ' + event.sender : ''}${event.recipient ? ' → ' + event.recipient : ''}`), (event.delivered || event.acknowledged) && hint(`Delivered: ${event.delivered ? 'yes' : 'no'} · Acknowledged: ${event.acknowledged ? 'yes' : 'no'} · Delivery alone is not progress.`))))) : empty('No recorded activity', 'New observations will appear after the factory writes them.'), activity.has_more && button('Load older / more events', () => loadActivity(), 'ghost')];
}
function rememberEvidence(key, name, value) {
  state.evidence.set(key, { name, value }); state.selected.add(key);
}
async function artifact(jobID, artifactID) {
  try {
    const data = await api(`/entities/${encodeURIComponent(jobID)}/artifacts/${encodeURIComponent(artifactID)}`);
    rememberEvidence(`artifact:${jobID}:${artifactID}`, data.name || artifactID, data);
    showDialog(data.name || 'Output', [badge('waiting', data.status || 'Status unknown'), data.applied && notice('This output is linked to the applied plan.', true), textBlock(data.text || 'No text available.'), data.truncated && notice('This preview is truncated.'), hint(`SHA-256: ${data.sha256 || 'Not recorded'}`)]);
  } catch (error) { showDialog('Output unavailable', [notice(error.message)]); }
}
function planView(job) {
  const planCard = (plan, title) => plan ? block(title, el('div', { class: 'artifact-row' }, el('div', {}, el('strong', { class: 'small' }, plan.name), el('div', { class: 'small muted' }, `${plan.status} · ${time(plan.at)}`)), button('Inspect output', () => artifact(job.id, plan.artifact_id), 'ghost compact')), plan.uat && el('p', {}, `Acceptance coverage: ${plan.uat}`), plan.issues?.map(issue => el('div', { class: 'artifact-row' }, el('div', {}, el('h3', {}, issue.ref), el('p', {}, issue.why), issue.evidence && hint(issue.evidence), issue.answers?.length > 0 && hint(`Supported answers: ${issue.answers.join(', ')}`)), badge('waiting', label(issue.result))))) : block(title, el('p', {}, 'No validated output is linked to this plan. A newer file is not proof of application.'));
  return [planCard(job.applied_plan, 'Applied plan'), planCard(job.pending_plan, 'Pending plan'), notice('Outputs are private. Open only the output you need. Content is rendered as plain text.', true)];
}
async function inspect(id) {
  try {
    const data = await api(`/entities/${encodeURIComponent(id)}/inspection`);
    rememberEvidence(`inspection:${id}`, `Inspection · ${id}`, data); render();
  } catch (error) { showDialog('Inspection unavailable', [notice(error.message)]); }
}
function debugView(job) {
  const id = state.entity || job.id;
  const inspection = state.evidence.get(`inspection:${id}`)?.value;
  return [notice('Private diagnostics can contain code, prompts, messages, and conversations. Credential redaction does not make this content public.'), block('Read-only inspection', el('p', {}, 'Inspection reads saved, bounded evidence. It does not wake agents, deliver messages, or change ownership.'), button(inspection ? 'Refresh private inspection' : 'Load private inspection', () => inspect(id), 'ghost'), inspection && [sectionHead('Saved evidence'), textBlock({ observation: inspection.observation || inspection.saved_observation, open_steps: inspection.open_steps, messages: inspection.messages, recovery: inspection.recovery }), inspection.ownership && [sectionHead('Recorded ownership'), textBlock(inspection.ownership)], inspection.recovery_budget && [sectionHead('Recovery limits'), textBlock(inspection.recovery_budget)], sectionHead('Conversation'), inspection.conversation_available ? textBlock(inspection.conversation || 'No conversation text.') : notice(inspection.conversation_reason || 'No owned conversation source is available.'), inspection.truncated && notice('Inspection is truncated. Absence from this preview does not establish absence from the source.'), sectionHead('Outputs'), (inspection.artifacts || []).map(output => el('div', { class: 'artifact-row' }, el('span', { class: 'small' }, output.name || output.id), button('Open', () => artifact(output.entity_id || inspection.id || id, output.id), 'ghost compact')))]), block('Manager logs', el('p', {}, 'Load one bounded, redacted log tail explicitly.'), el('div', { class: 'actions' }, ...['tick', 'repo-worker'].map(source => button(`Inspect ${source}`, () => inspectLog(source), 'ghost compact')))), block('Selected diagnostic export', el('p', {}, 'Only evidence you explicitly loaded is available. Preview the selection before downloading.'), button('Choose & preview export', exportPreview, 'ghost'))];
}
async function inspectLog(source) {
  try { const data = await api(`/logs/${source}`); rememberEvidence(`log:${source}`, `${source} log`, data); showDialog(`${source} log`, [hint(`Observed ${time(data.source_at)}`), textBlock(data.text || 'No log text available.'), data.truncated && notice('Only a bounded tail is shown.')]); }
  catch (error) { showDialog('Log unavailable', [notice(error.message)]); }
}
function entitySelector(job) {
  const options = jobEntities(job);
  const selected = options.some(entity => entity.id === state.entity) ? state.entity : job.id;
  state.entity = selected;
  const select = el('select', { 'aria-label': 'Inspection and activity target', 'data-draft': `entity:${job.id}`, onchange: event => { state.entity = event.target.value; render(); if (state.tab === 'activity') loadActivity(); } }, options.map(entity => el('option', { value: entity.id, selected: entity.id === selected }, entity.name)));
  return el('div', { class: 'toolbar' }, field('Selected entity', select, 'Activity and private inspection belong to this exact job, task, or execution group.'));
}
function detailView(job) {
  if (!job) return [button('← Jobs', () => navigate('jobs'), 'back'), empty('Loading job', 'Waiting for a readable observation.')];
  return [button('← All jobs', () => navigate('jobs'), 'back'), el('div', { class: 'page-heading' }, el('div', {}, el('div', { class: 'eyebrow mono' }, job.id), el('h1', {}, job.title || job.id), el('p', { class: 'subtitle' }, job.health?.reason || 'No current observation'), el('div', { class: 'detail-meta' }, badge(job.health?.category), counts(job.counts))), button('Refresh', refresh, 'ghost compact')), errorsPanel(job.errors), el('div', { class: 'tabs', role: 'tablist', 'aria-label': 'Job sections' }, ...[['work', 'Work'], ['activity', 'Activity'], ['plan', 'Plan & outputs'], ['debug', 'Debug']].map(([tab, name]) => el('button', { type: 'button', role: 'tab', 'aria-selected': state.tab === tab, class: `tab${state.tab === tab ? ' active' : ''}`, onclick: () => { state.tab = tab; render(); if (tab === 'activity') loadActivity(); } }, name))), ['activity', 'debug'].includes(state.tab) && entitySelector(job), el('div', { role: 'tabpanel', 'aria-label': label(state.tab) }, (state.tab === 'work' ? workView(job) : state.tab === 'activity' ? activityView(job) : state.tab === 'plan' ? planView(job) : debugView(job)))];
}
function healthHolds(report) {
  const repairs = report.repairs || [];
  const paused = report.paused_repos || [];
  if (!repairs.length && !paused.length) return null;
  return block('Repository holds',
    paused.length > 0 && notice(`The manager has paused these repositories: ${paused.join(', ')}.`),
    repairs.map(repair => el('div', { class: 'block' },
      el('h3', {}, `${repair.repo} · ${repair.base}`),
      el('p', {}, repair.waiting_on || (repair.stopped ? 'Repair stopped. A human decision is required.' : 'Base-branch repair is recorded.')),
      hint(`Red since ${time(repair.red_since)} · Head ${repair.red_sha || 'Unknown'}`),
      repair.epic_id && button(`Open repair ${repair.epic_id}`, () => openJob(repair.epic_id), 'inline-link'),
      (repair.checks || []).map(check => el('div', { class: 'artifact-row' }, link(check.name, check.url), badge(check.state === 'passed' ? 'finished' : check.state === 'failed' ? 'failed' : 'waiting', label(check.state)))))));
}
function observerHealth(account = {}) {
  return block('Usage observer',
    el('p', {}, account.freshness_reason || 'No current observer assessment is available.'),
    el('dl', { class: 'key-values' },
      valuePair('Observation freshness', label(account.freshness || 'unknown')),
      valuePair('Cached usage observed', time(account.source_at)),
      valuePair('Last probe', time(account.probed_at)),
      valuePair('Reset observation', time(account.resets_at))),
    errorsPanel(account.errors));
}
function healthView(report) {
  const health = cache.get('health');
  return [heading('Settings & health', 'The selected instance and its saved observations.'), health?.read_only && notice('This browser server is read-only. Job submission and controls are disabled.'), el('div', { class: 'health-layout' }, block('Bound local instance', el('dl', { class: 'key-values' }, valuePair('Instance', health?.instance_id || 'local'), valuePair('Brain', health?.brain || 'Not available'), valuePair('Executable', health?.executable || 'Not available'), valuePair('Build', health?.version || 'Unknown'), valuePair('Browser answers', health?.answer_supported ? 'Supported by a fresh manager tick' : 'Waiting for a compatible manager tick')), notice('The browser does not install a scheduler or start a manager. Closing it leaves factory execution unchanged.', true)), block('Runtime prerequisites', el('p', {}, 'Passive checks only. Login and quota probes never run during refresh.'), (health?.prerequisites || []).map(prerequisite => el('div', { class: 'artifact-row' }, el('div', {}, el('strong', { class: 'small' }, prerequisite.name), el('div', { class: 'small muted' }, prerequisite.detail)), badge(prerequisite.available ? 'finished' : 'unknown', prerequisite.available ? 'Available' : 'Needs setup'))))), el('div', { class: 'health-grid' }, metric('MANAGER OBSERVATION', label(report.health?.category), report.health?.reason), capacity(report.account), metric('LATEST COMPLETED TICK', time(report.last_tick), 'Saved state can lag recorded events.')), observerHealth(report.account), healthHolds(report), block('Configuration boundary', el('p', {}, 'This interface supports the selected local Claude factory. Native executor selection, service control, authentication changes, and model switching are unavailable here.'), el('p', {}, 'A healthy scheduler does not mean every worker is making progress. Use job health, useful output, and explicit debug evidence together.'), el('p', {}, 'Stop browser mutations before downgrading the manager. A previous compatible tick cannot guarantee an older manager will consume bound answers safely.')), errorsPanel([...(report.errors || []), ...(report.problems || [])])];
}
function render() {
  const drafts = captureDrafts(main);
  const report = cache.get('overview');
  document.querySelector('#breadcrumb').textContent = state.page === 'detail' ? 'Job detail' : state.page === 'health' ? 'Settings & health' : label(state.page);
  for (const nav of document.querySelectorAll('[data-nav]')) { nav.classList.toggle('active', nav.dataset.nav === (state.page === 'detail' ? 'jobs' : state.page)); if (nav.tagName === 'BUTTON') nav.setAttribute('aria-current', nav.classList.contains('active') ? 'page' : 'false'); }
  if (!report) main.replaceChildren(empty(cache.errors.size ? 'Cannot read this factory yet' : 'Connecting to your factory', cache.errors.size ? [...cache.errors.values()][0] : 'Reading saved observations…', button('Try again', refresh, 'ghost')));
  else {
    const content = state.page === 'overview' ? overview(report) : state.page === 'jobs' ? jobsView(report) : state.page === 'health' ? healthView(report) : detailView(currentJob());
    main.replaceChildren(...content.flat(Infinity).filter(Boolean));
    document.querySelector('#job-nav-count').textContent = String(report.jobs?.filter(job => !job.end).length || 0);
    document.querySelector('#last-refresh').textContent = `Observation ${time(report.at)}`;
  }
  const health = cache.get('health');
  document.querySelector('#sidebar-instance').textContent = health?.version ? `Build ${health.version}` : 'Bound to this process';
  document.querySelector('#new-job').disabled = Boolean(health?.read_only) || !state.csrf;
  restoreDrafts(main, drafts); connection(); updateButtons();
}
async function loadJobs() {
  try {
    const count = Math.max(100, cache.get('jobs')?.jobs?.length || 0);
    const pages = await Promise.all(Array.from({ length: Math.ceil(count / 100) }, (_, page) => api(`/jobs?offset=${page * 100}&limit=100`)));
    cache.accept('jobs', { ...pages.at(-1), jobs: pages.flatMap(page => page.jobs || []) });
  } catch (error) { cache.fail('jobs', error.message); }
}
async function refresh() {
  if (state.busy || !state.csrf) return;
  state.busy = true;
  try {
    const reads = [observe('overview', '/overview'), observe('health', '/health')];
    if (state.page === 'jobs') reads.push(loadJobs());
    if (state.page === 'detail') reads.push(observe(`job:${state.job}`, `/jobs/${encodeURIComponent(state.job)}`));
    await Promise.all(reads); render(); await tracker.poll();
    if (state.page === 'detail' && state.tab === 'activity') await loadActivity();
  } finally { state.busy = false; }
}
function showDialog(title, children) {
  const body = document.querySelector('#dialog-body');
  body.replaceChildren(el('div', { class: 'dialog-head' }, el('h2', { id: 'dialog-title' }, title), button('×', () => dialog.close(), 'ghost')), ...children.flat(Infinity).filter(Boolean));
  if (!dialog.open) dialog.showModal();
}
function actionDialog(type, entity) {
  const isURL = ['deployed', 'review_asked'].includes(type);
  const input = isURL ? el('input', { type: 'url', required: true, placeholder: 'https://github.com/owner/repo/pull/…' }) : el('textarea', { required: true, placeholder: type === 'message' ? 'Message to the current owner…' : 'Explain the reason for this request…' });
  const submit = el('button', { type: 'submit', class: 'primary' }, actionNames[type]);
  const descriptions = { retry: 'The factory will recheck whether this task is eligible. A request does not promise a new worker.', replan: 'Ask the planner to reconsider this job. The applied plan remains until a new plan is accepted.', message: 'Queue a message for this exact target. Delivery and useful progress are separate observations.', deployed: 'Record that the linked PR has been deployed for this job. This does not deploy software. Dependencies may be released on a later tick.', review_asked: 'Record that a human requested review for this PR. This does not send a review request.' };
  const form = el('form', { onsubmit: async event => { event.preventDefault(); if (submit.disabled) return; submit.disabled = true; const pending = tracker.start(`${type}:${entity.id}`, '/actions', { type, target_id: entity.id, expected: entity.expected, value: input.value }); dialog.close(); await pending; await refresh(); } }, field(isURL ? 'Affected pull request URL' : type === 'message' ? 'Message' : 'Reason', input), el('div', { class: 'dialog-footer' }, button('Back', () => dialog.close(), 'ghost'), submit));
  showDialog(actionNames[type], [notice(descriptions[type] || 'This request is checked by the factory before application.', true), hint(`Exact target: ${entity.id}`), form]);
}
async function cancelJob(entity) {
  try {
    const preview = await api(`/entities/${encodeURIComponent(entity.id)}/cancel-preview`);
    const input = el('textarea', { required: true, placeholder: 'Why are you cancelling this work?' });
    const checked = el('input', { type: 'checkbox', required: true, id: 'cancel-ack' });
    const confirmText = el('input', { required: true, autocomplete: 'off', placeholder: preview.target_id });
    const submit = el('button', { type: 'submit', class: 'danger' }, 'Cancel this work');
    const error = el('div', { class: 'error-text small', role: 'alert' });
    const form = el('form', { onsubmit: async event => { event.preventDefault(); if (confirmText.value !== preview.target_id) { error.textContent = 'The confirmation text must match exactly.'; return; } if (submit.disabled) return; submit.disabled = true; const pending = tracker.start(`cancel:${entity.id}`, '/actions', { type: 'cancel', target_id: entity.id, expected: preview.expected, value: input.value, confirmation: preview.confirmation }); dialog.close(); await pending; await refresh(); } }, field('Cancellation reason', input), field(`Type “${preview.target_id}” to confirm`, confirmText), el('div', { class: 'check-row' }, checked, el('label', { for: 'cancel-ack' }, 'I understand the scope and the PR, branch, and worktree consequences shown above.')), error, el('div', { class: 'dialog-footer' }, button('Keep working', () => dialog.close(), 'ghost'), submit));
    showDialog('Cancel work', [notice(preview.message || 'Factory-owned pull requests, branches, and worktrees can be removed. This is cancellation, not pause.'), hint(`Exact target: ${preview.target_id}`), el('ul', { class: 'list' }, (preview.descendants || []).map(child => el('li', {}, `${child.id} · ${label(child.state)} · ${child.preserve_pr ? 'Adopted PR preserved' : child.pr ? 'Factory PR subject to cancellation' : 'No PR recorded'}`, child.pr?.url ? link(' Open PR', child.pr.url) : typeof child.pr === 'string' && githubURL(child.pr) ? link(' Open PR', child.pr) : null))), form]);
  } catch (error) { showDialog('Cancellation unavailable', [notice(error.message)]); }
}
function newJob() {
  const kind = el('select', {}, ...[['text', 'Written goal'], ['issue', 'GitHub issue'], ['epic', 'GitHub epic'], ['plan_upload', 'Upload a plan']].map(([value, name]) => el('option', { value }, name)));
  const input = el('textarea', { required: true, placeholder: 'What would you like the factory to build?' });
  const upload = el('input', { type: 'file', accept: '.yaml,.yml,.txt', class: 'upload-file' });
  const inputField = field('Goal', input);
  const uploadField = field('Plan file', upload, 'A bounded UTF-8 plan is copied to this factory instance. No local file path is sent.'); uploadField.hidden = true;
  kind.addEventListener('change', () => { const isUpload = kind.value === 'plan_upload'; uploadField.hidden = !isUpload; inputField.hidden = isUpload; input.required = !isUpload; upload.required = isUpload; input.placeholder = kind.value === 'text' ? 'What would you like the factory to build?' : 'https://github.com/owner/repo/issues/…'; inputField.querySelector('label').textContent = kind.value === 'text' ? 'Goal' : 'GitHub link'; });
  const submit = el('button', { type: 'submit', class: 'primary', disabled: tracker.scopes.has('new-job') }, 'Submit to factory');
  const error = el('div', { class: 'error-text small', role: 'alert' });
  const form = el('form', { onsubmit: async event => {
    event.preventDefault(); if (submit.disabled) return; submit.disabled = true; error.textContent = '';
    let payload = { kind: kind.value, value: input.value };
    try {
      if (kind.value === 'plan_upload') { const file = upload.files[0]; if (!file) throw new Error('Choose a plan file.'); if (file.size > 512 * 1024) throw new Error('Choose a plan smaller than 512 KiB.'); const result = await post(API + '/uploads', { name: file.name, text: await file.text() }); payload = { kind: 'plan_upload', upload_id: result.upload_id }; }
      const pending = tracker.start('new-job', '/jobs', { input: payload }); dialog.close(); const receipt = await pending; if (receipt?.entity_id) await openJob(receipt.entity_id); else await refresh();
    } catch (failure) { error.textContent = failure.message; submit.disabled = false; }
  } }, field('Input type', kind), inputField, uploadField, error, el('div', { class: 'dialog-footer' }, button('Back', () => dialog.close(), 'ghost'), submit));
  showDialog('Create a job', [notice('Submitting starts factory planning and may later create issues, branches, pull requests, and agents. The factory may merge pull requests under the repository policy. Execution continues after this browser closes.', true), hint(`Bound instance: local · ${cache.get('health')?.brain || 'Current factory brain'} · Claude executor`), tracker.scopes.has('new-job') && notice('The previous submission has an unresolved result. Inspect its original receipt before submitting another job.'), form]);
}
function exportPreview() {
  const content = el('div');
  const preview = el('div');
  const download = button('Download selected JSON', () => {
    const selected = [...state.selected].filter(key => state.evidence.has(key)).map(key => state.evidence.get(key));
    const blob = new Blob([JSON.stringify(diagnosticBundle(selected, cache.get('health'), cache.get('overview')), null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob); const anchor = el('a', { href: url, download: 'factory-diagnostics.json' }); anchor.click(); URL.revokeObjectURL(url);
  }, 'primary', state.selected.size === 0);
  function update() { const selected = [...state.selected].filter(key => state.evidence.has(key)).map(key => state.evidence.get(key)); preview.replaceChildren(textBlock(diagnosticBundle(selected, cache.get('health'), cache.get('overview')))); download.disabled = selected.length === 0; }
  for (const [key, evidence] of state.evidence) { const id = `export-${crypto.randomUUID()}`; const check = el('input', { type: 'checkbox', id, checked: state.selected.has(key), onchange: () => { check.checked ? state.selected.add(key) : state.selected.delete(key); update(); } }); content.append(el('div', { class: 'check-row' }, check, el('label', { for: id }, evidence.name))); }
  update();
  showDialog('Preview diagnostic export', [notice('This selection is server-redacted but remains private. It can contain code, issue text, prompts, and conversations. Nothing is uploaded.'), state.evidence.size ? content : hint('Load an inspection, output, or log before exporting.'), preview, el('div', { class: 'dialog-footer' }, button('Back', () => dialog.close(), 'ghost'), download)]);
}
for (const nav of document.querySelectorAll('[data-nav]')) nav.addEventListener('click', event => { event.preventDefault(); navigate(nav.dataset.nav); });
document.querySelector('#new-job').addEventListener('click', newJob);
document.addEventListener('visibilitychange', () => { if (!document.hidden) refresh(); });
async function start() {
  const fragment = location.hash.slice(1);
  const token = fragment.startsWith('token=') ? new URLSearchParams(fragment).get('token') : fragment;
  history.replaceState(null, '', location.pathname);
  try {
    const session = token ? await post('/session', { token }) : await request('/session');
    state.csrf = session.csrf;
    await refresh();
  } catch (error) { cache.fail('session', `Local session unavailable: ${error.message} Open the authenticated URL printed by factory ui.`); render(); }
}
start();
setInterval(() => { if (document.hidden) return; if (state.tab === 'debug' || dialog.open) tracker.poll(); else refresh(); }, 6000);
