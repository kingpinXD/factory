window.addEventListener('error', event => {
  const main = document.querySelector('#main');
  if (!main) return;
  const message = document.createElement('p');
  message.className = 'notice';
  const file = event.filename ? new URL(event.filename).pathname : 'browser';
  message.textContent = `The interface could not start: ${event.message || 'Unknown browser error'} (${file}:${event.lineno || 0}:${event.colno || 0}). Refresh after checking the UI build.`;
  main.replaceChildren(message);
});
window.addEventListener('unhandledrejection', event => {
  const banner = document.querySelector('#connection');
  if (!banner) return;
  banner.hidden = false;
  banner.textContent = `A browser operation failed: ${event.reason?.message || 'Unknown error'}. Last good data remains visible.`;
});
