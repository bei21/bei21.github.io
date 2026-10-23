# Publication recognition audit

Checked on October 23, 2026. This audit covers the current homepage bibliography
and several additional coauthored papers; it is not a complete career-wide list.
Presentation distinctions and paper awards are separate metadata. Add an Oral or
Spotlight label only when the conference or the authors explicitly identify it.
An unverified distinction does not mean that a paper was a Poster.

## Confirmed Oral presentations

| Paper | Recognition and evidence | Homepage action |
| --- | --- | --- |
| RoLD: Robot Latent Diffusion for Multi-task Policy Modeling | **MMM 2025 Oral.** The [official program](https://mmm2025.net/conference/#oral-sessions) lists paper 196 under Oral Sessions, January 8, 11:00–12:00, Best Paper Session. The same page separately confirms its Best Paper Award. | Added `note = {Oral}` to `tan2025rold`; retained the existing award. |
| Seeing Out of tHe bOx: End-to-End Pre-training for Vision-Language Representation Learning (SOHO) | **CVPR 2021 Oral.** The [Microsoft Research publication page](https://www.microsoft.com/en-us/research/publication/seeing-out-of-the-box-end-to-end-pre-training-for-vision-language-representation-learning/) explicitly says “CVPR 2021 \| June 2021 Oral”; the [authors' repository](https://github.com/researchmm/soho) also marks it Oral. | Existing Oral annotation retained. |
| Transferring Foundation Models for Generalizable Robotic Manipulation (TPM) | **WACV 2025 Oral.** The [authors' repository](https://github.com/MCG-NJU/TPM) labels the paper “[WACV 2025 Oral]”. | Existing Oral annotation retained. |
| Learning Fine-Grained Motion Embedding for Landscape Animation | **ACM Multimedia 2021 Oral.** Paper 1465 appears under Oral Papers in the [official main-track list](https://2021.acmmm.org/main-track-list). The [Microsoft Research page](https://www.microsoft.com/en-us/research/publication/learning-fine-grained-motion-embedding-for-landscape-animation/) explicitly marks it Oral. The [official program, PDF page 17](https://2021.acmmm.org/documents/ACMMM21_Full_Technical_Program.pdf#page=17), schedules its talk on October 21, 11:00–11:15. | Added as `xue2021learning` under Generation at Bei's request, with its Oral distinction, summary, Figure 2, and paper links. |

## Poster records

**CLIP-ViP was accepted as a Poster, not a Spotlight.** The decisive source is
the ICLR conference data's acceptance decision, rather than the `/poster/` URL:
ICLR can also use poster pages for papers with other distinctions.

| Paper | Official evidence |
| --- | --- |
| CLIP-ViP: Adapting Pre-trained Image-Text Model to Video-Language Alignment | [ICLR 2023 conference data](https://iclr.cc/static/virtual/data/iclr-2023-orals-posters.json), record `11587`: `decision: "Accept: poster"`, `eventtype: "Poster"`, `type_display_name: "Virtual presentation / poster accept"`. [Paper page](https://iclr.cc/virtual/2023/poster/11587). |
| Long-Form Video-Language Pre-Training with Multimodal Temporal Contrastive Learning (LF-VILA) | [NeurIPS 2022 conference data](https://neurips.cc/static/virtual/data/neurips-2022-orals-posters.json), record `53633`: `eventtype: "Poster"`. The dataset explicitly distinguishes Poster from Spotlight entries. [Paper page](https://neurips.cc/virtual/2022/poster/53633). |
| Probing Inter-modality: Visual Parsing with Self-Attention for Vision-and-Language Pre-training | [NeurIPS 2021 conference data](https://neurips.cc/static/virtual/data/neurips-2021-orals-posters.json), record `27599`: `decision: "Accept (Poster)"`. [Paper page](https://neurips.cc/virtual/2021/poster/27599). |
| Unifying Multimodal Transformer for Bi-directional Image and Text Generation | [ACM Multimedia 2021 official program, PDF page 31](https://2021.acmmm.org/documents/ACMMM21_Full_Technical_Program.pdf#page=31): October 21, 9:00–12:00, Industrial Session Poster, paper `P068`, with the matching title and authors. This entry is retained in the source bibliography and is not selected. |
| Searching the Search Space of Vision Transformer | [NeurIPS 2021 conference data](https://neurips.cc/static/virtual/data/neurips-2021-orals-posters.json), record `27152`: `decision: "Accept (Poster)"`. Outside the current bibliography. |
| MM-Diffusion: Learning Multi-Modal Diffusion Models for Joint Audio and Video Generation | [CVPR 2023 conference data](https://cvpr.thecvf.com/static/virtual/data/cvpr-2023-orals-posters.json), record `22129`: `eventtype: "Poster"`, `type_display_name: "Poster"`. [Paper page](https://cvpr.thecvf.com/virtual/2023/poster/22129). No separate Highlight or Oral distinction was verified. Outside the current bibliography. |

Poster labels were not added to the homepage; it continues to highlight verified
Oral distinctions and awards. No new Spotlight designation was verified in this
audit.

## Unverified or inapplicable distinctions

- **HD-VILA (CVPR 2022):** no independent Oral or Spotlight evidence was obtained.
  Keep its venue without a presentation distinction.
- **ViCo (ACM MM Asia 2024)** and **Beyond Narrative Description / image-to-poem
  (ACM Multimedia 2018):** retain the confirmed awards. No separate presentation
  distinction was independently verified in this audit.
- **DocAtlas and XL-DocBench (NeurIPS 2026):** acceptance is confirmed by Bei and
  the project pages. The titles were not found in the conference data retrieved
  during this audit, so their presentation distinctions remain unverified. This
  does not change their accepted venue.
- **OfficeTown:** currently an arXiv preprint, without a conference presentation
  distinction.
- **STP:** the current publication record is IJCV, a journal, so no conference
  Oral or Spotlight annotation applies to that record.

Following Bei's request to include FGLA, the homepage selection has fourteen
papers, and the local bibliography contains fifteen entries. The recognition
changes are RoLD's Oral annotation and the new FGLA entry's Oral annotation.
