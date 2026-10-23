// Native disclosures work on their own; these controls add bulk and deep-link support.
document.querySelectorAll('[data-publications]').forEach((section) => {
  const papers = Array.from(section.querySelectorAll('.publication-disclosure'));
  const button = section.querySelector('.publications-toggle');
  if (!button || !papers.length) return;

  const updateButton = () => {
    const allOpen = papers.every((paper) => paper.open);
    button.textContent = allOpen ? 'Collapse all' : 'Expand all';
    button.setAttribute('aria-label', allOpen ? 'Collapse all publication details' : 'Expand all publication details');
  };
  button.hidden = false;
  button.addEventListener('click', () => {
    const expand = papers.some((paper) => !paper.open);
    papers.forEach((paper) => { paper.open = expand; });
    updateButton();
  });
  papers.forEach((paper) => paper.addEventListener('toggle', updateButton));
  updateButton();
});

document.querySelectorAll('.publication-disclosure, .publication-video').forEach((disclosure) => {
  disclosure.addEventListener('toggle', () => {
    if (!disclosure.open) disclosure.querySelectorAll('video').forEach((video) => video.pause());
  });
});

function revealPublication() {
  let id;
  try {
    id = decodeURIComponent(window.location.hash.slice(1));
  } catch (_) {
    return;
  }
  const target = document.getElementById(id);
  const paper = target && target.closest('.publication-entry');
  if (!paper) return;
  paper.querySelector('.publication-disclosure').open = true;
  requestAnimationFrame(() => paper.scrollIntoView({ block: 'start' }));
}

window.addEventListener('hashchange', revealPublication);
// A second click on the same anchor should also reopen a manually collapsed paper.
document.addEventListener('click', (event) => {
  const link = event.target.closest('a[href]');
  if (!link || event.defaultPrevented || event.button !== 0 || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  if (link.hash && link.href === window.location.href) revealPublication();
});
revealPublication();
