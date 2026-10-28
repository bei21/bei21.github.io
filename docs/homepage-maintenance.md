# Updating this homepage

The site uses al-folio / Jekyll. Publication content lives in
`_bibliography/papers.bib`; the homepage renders selected entries by topic.
The top navigation contains About and News; the homepage selection is the only
publication display. The full publication record is linked through Google
Scholar beside the Selected Publications heading. The old `/publications/` URL
redirects to the homepage selection and preserves paper fragments when JavaScript
is enabled; without JavaScript it redirects to the selection heading.

## Curated publications

Keep `selected = {true}` only on works that Bei chooses to feature. The homepage
currently features fourteen papers, grouped using `_data/research_topics.yml` and
each entry's `topic` field:

| Topic | Representative papers |
| --- | --- |
| `document_agents` | OfficeTown, DocAtlas, XL-DocBench |
| `embodied` | STP, TPM (Transferring Foundation Models), RoLD |
| `vision_language` | CLIP-ViP, LF-VILA, HD-VILA, SOHO, Probing Inter-modality |
| `generation` | ViCo, FGLA (landscape animation), Beyond Narrative Description (image-to-poem) |

The shared selection note describes first-author work and projects Bei led with
students Bei mentored, as confirmed by Bei on October 28, 2026. It explains the
scope of the whole selection without repeating role labels on each paper.

The selection includes the three recent document/agent projects, award-winning
papers, and earlier work with substantial citation impact. ViCo represents
award-winning video comment generation; HD-VILA and Probing Inter-modality
broaden the representation-learning group. Citation impact was assessed using
the archived May 4, 2026 data, rather than adding a live citation dependency.

Unifying Multimodal Transformer remains in the source bibliography with
`selected = {false}`. Other recent coauthored papers are not automatically
selected. The local bibliography currently has fifteen entries and is not a
complete publication record. To remove a homepage paper after review, set only
its `selected` field to `false`; keep the entry and its key.

- OfficeTown: arXiv preprint, 2026.
- DocAtlas and XL-DocBench: NeurIPS 2026, as confirmed by Bei and the project pages.
- STP: IJCV 2026, volume 134, issue 8, article 354;
  [DOI 10.1007/s11263-026-02948-3](https://doi.org/10.1007/s11263-026-02948-3).
  The publisher records acceptance on July 1 and online publication on July 20,
  2026; the acceptance news uses July 1.
- News: four papers accepted to NeurIPS 2026, dated September 25, 2026, as
  confirmed by Bei.
- TPM: WACV 2025, pages 1999–2010, with the Oral distinction stated in the
  [official repository](https://github.com/MCG-NJU/TPM). The final title and nine
  authors follow the CVF proceedings; the older arXiv version shares ID
  `2306.05716` and was titled Pave the Way to Grasp Anything.
- LF-VILA: NeurIPS 2022, volume 35, pages 38032–38045, verified against the
  [conference record](https://proceedings.neurips.cc/paper_files/paper/2022/hash/f8290ccc2905538be1a7f7914ccef629-Abstract-Conference.html).
- FGLA: ACM Multimedia 2021 Oral, pages 291–299;
  [DOI 10.1145/3474085.3475421](https://doi.org/10.1145/3474085.3475421).
  The six authors and page range follow the publisher's Crossref record and the
  paper. The [arXiv record](https://arxiv.org/abs/2109.02216) also explicitly
  confirms its Oral acceptance. It is selected under `generation`.

See the [publication recognition audit](publication-recognition.md) for verified
Oral distinctions, official Poster records, and unresolved cases. In particular,
RoLD is MMM 2025 Oral, and CLIP-ViP's ICLR 2023 decision is `Accept: poster`.

Useful fields for each BibTeX entry:

| Field | Purpose |
| --- | --- |
| `topic` | A research topic ID from `_data/research_topics.yml`; required for a selected paper. |
| `teaser` | One short sentence visible in the compact publication row. |
| `summary` | One or two readable sentences about the contribution, shown when expanded. |
| `preview` | Local filename under `assets/img/publication_preview/`, or an image URL. |
| `preview_alt` | Meaningful description of the figure. |
| `preview_width`, `preview_height` | Image dimensions to prevent layout shifts. |
| `preview_source` | Original source, linked beside the full-size local figure. |
| `video`, `video_poster` | Optional MP4 recording and local poster; click to play, with no autoplay or preloading. |
| `arxiv`, `pdf`, `code`, `models`, `website`, `dataset` | Public resource links. Omit unavailable resources. |
| `demo`, `demo_label`, `leaderboard` | Demo, examples, and benchmark links. |
| `note`, `award` | Verified presentation distinction and paper award; keep their supporting sources in the recognition audit. |
| `selected`, `bibtex_show` | Homepage selection and expandable BibTeX citation. |

Presentation fields are excluded from the downloadable/copyable citation.
Preserve existing BibTeX keys: links in the introduction use them as anchors.
Record new announcements in `_news/`, with `inline: true` and an explicit date.

Each paper uses a native, initially closed `<details>` disclosure. Its compact
row shows the venue, full title, teaser, thumbnail, and any award. Opening it
reveals all authors, the longer summary, resource links, figure, and optional
demo. This works with a mouse, keyboard, or touch, including without JavaScript.
`assets/js/publications.js` adds Expand all / Collapse all, opens papers targeted
by URL fragments (including existing intro/news links), and pauses videos when
their paper or demo is closed. Videos use `preload="none"` and never autoplay.

The homepage shows the latest three news items; `/news/` shows all announcements.
Awards shows three paper awards first, with the other honors under More honors.
The header uses a text name and navigation, with readable contact links below
the introduction. Profile images keep their natural aspect ratio on all screens.
On desktop, the portrait floats beside the heading and opening text, so later
paragraphs use the space beneath it. On smaller screens, the portrait and name
sit beside one another, with the biography occupying the full width below.

## Sources and media

The figures are from the papers or official project pages, compressed to WebP
without changing their scientific content. The original source links are also
stored alongside each publication.

| Asset | Source |
| --- | --- |
| `officetown.webp` | [OfficeTown, Figure 1](https://arxiv.org/html/2610.23194v1/fig1_officetown_evidence.png) |
| `docatlas.webp` | [DocAtlas framework](https://officeintelligence.github.io/docatlas/assets/figures/framework.png) |
| `docatlas-demo.webp` | [Official web demo poster](https://officeintelligence.github.io/assets/homepage/docatlas-web-poster.webp) |
| `xl-docbench.webp` | [XL-DocBench construction pipeline](https://officeintelligence.github.io/xl-docbench/assets/figures/pipeline.png) |
| `rold.webp` | [RoLD, Figure 1](https://arxiv.org/html/2403.07312v3/x1.png) |
| `stp.webp` | [STP, Figure 1: pretraining and downstream control](https://arxiv.org/html/2403.05304v4/models.png) |
| `clip-vip.webp` | [CLIP-ViP, Figure 2 on PDF page 4](https://arxiv.org/pdf/2209.06430#page=4), rendered from the original vector figure at 4× scale; crop `(108, 78, 504, 311)` in PDF points |
| `soho.webp` | [SOHO official architecture](https://raw.githubusercontent.com/researchmm/soho/master/resources/soho.png) |
| `image-to-poem.webp` | [Official image-to-poem example](https://raw.githubusercontent.com/researchmm/img2poem/master/img/example.png) |
| `vico.webp` | [ViCo, Figure 2 on PDF page 3](https://arxiv.org/pdf/2308.11171#page=3), crop `(54, 52, 558, 267)` in PDF points |
| `hd-vila.webp` | [HD-VILA official framework](https://raw.githubusercontent.com/microsoft/XPretrain/main/hd-vila/figs/framework.png) |
| `probing.webp` | [Probing Inter-modality, Figure 1 on PDF page 4](https://arxiv.org/pdf/2106.13488#page=4), crop `(108, 74, 504, 325)` in PDF points |
| `tpm.webp` | [TPM official framework](https://raw.githubusercontent.com/MCG-NJU/TPM/main/frame_work.png), the 1429×640 figure from the authors' repository |
| `lf-vila.webp` | [LF-VILA official framework](https://raw.githubusercontent.com/microsoft/XPretrain/main/LF-VILA/figs/framework.png) |
| `fgla.webp` | [FGLA, Figure 2](https://arxiv.org/html/2109.02216v2/pipline.png), the original pipeline figure resized from 1680×984 to 1600×937 |

The DocAtlas MP4 is served from the [official project site](https://officeintelligence.github.io/assets/homepage/docatlas-web-loop.mp4).
OfficeTown links to the [interactive task environment](https://officeintelligence.github.io/#task-environments).
XL-DocBench links to its [dataset](https://huggingface.co/datasets/microsoft/XL-DocBench),
[examples](https://officeintelligence.github.io/xl-docbench/#cases), and
[leaderboard](https://officeintelligence.github.io/xl-docbench/#leaderboard).
STP links to its [official code](https://github.com/MCG-NJU/STP) and
[released model weights](https://huggingface.co/yangjiange/STP).
TPM links to its [official code](https://github.com/MCG-NJU/TPM) and
[robot demonstrations](https://www.youtube.com/watch?v=MAcUPFBfRIw).
LF-VILA links to its [official code](https://github.com/microsoft/XPretrain/tree/main/LF-VILA)
and [data/model preparation instructions](https://github.com/microsoft/XPretrain/tree/main/LF-VILA#prepare-data-and-pretrained-model).

## Scholar sync

`scripts/sync_scholar_to_bib.py` preserves local entries, manual selection,
publication venues, descriptions, media, links, awards, and existing BibTeX keys.
Newly discovered publications are added with `selected = {false}`. Entries
missing from a scrape are retained. Empty, malformed, or incomplete exports
stop before any replacement; successful updates replace the file atomically.

The scheduled workflow runs the regression checks before importing records:

```sh
python -m pip install scholarly 'bibtexparser>=1.4,<2'
python -m unittest discover -s tests -v
```

Per-paper citation counts are no longer displayed. The citation archive workflow
is manual-only, with no scheduled scrape. Existing citation data and Scholar IDs
are retained for reference and stable matching; Google Scholar remains the
public entry point for up-to-date citation information.

## Visitor analytics

Google Analytics 4 uses the web-stream measurement ID `G-Z82WY9SRK2`, supplied
by Bei. `_config.yml` enables the existing Google tag integration. The tag is
rendered only when `JEKYLL_ENV=production`, as in the deployment workflow, and
loads only on the hostname in `site.url` (`bei21.github.io`). Local previews do
not load the tracker or contribute visits, even when using a production build.

Sign in to [Google Analytics](https://analytics.google.com/), select the homepage
property, and open Reports → Realtime to check incoming activity. Standard
reports show traffic trends, referrals, and approximate visitor locations.
Initial data collection can take up to 30 minutes, and standard reports update
later. Analytics does not recover visits from before the tag was installed.

In Admin → Data streams → the homepage web stream, keep Enhanced measurement
and Outbound clicks enabled to collect visits to external paper, project, and
code links. GA4 records these as `click` events with the destination in
`link_url`. These options are managed in the Google Analytics account; the
website integration does not change those account settings. No additional
custom click handler is needed for these outbound links.

## Preview

With Ruby 3.2+, Bundler, ImageMagick, and Jupyter installed:

```sh
bundle install
bundle exec jekyll serve --host 127.0.0.1 --port 4000
```

For the current remote workspace, the running Docker container is
`homepage-preview` and serves the site on `127.0.0.1:4000`. Source edits trigger
a rebuild; refresh the browser to see the result. Configuration changes require
restarting Jekyll.

Forward port 4000 using VS Code's Ports panel, or run this on your own computer:

```sh
ssh -N -L 4000:127.0.0.1:4000 YOUR_SSH_HOST
```

Then open <http://localhost:4000>. This is a local preview; publication to the
live site still follows the repository's GitHub Pages deployment workflow.
