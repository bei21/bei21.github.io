import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts import sync_scholar_to_bib as sync


class ScholarSyncTests(unittest.TestCase):
    def setUp(self):
        self.paper = {
            "ENTRYTYPE": "inproceedings", "ID": "wei2026docatlas",
            "title": "{DocAtlas}: Long-Document Understanding",
            "author": "Wei, Hongchen and Liu, Bei", "year": "2026",
            "selected": "true", "abbr": "NeurIPS", "booktitle": "NeurIPS",
            "summary": "Curated introduction.", "preview": "docatlas.webp",
            "code": "https://github.com/microsoft/DocAtlas-Harness",
            "video": "https://example.org/demo.mp4", "award": "An existing award",
            "google_scholar_id": "stable-scholar-id",
        }

    def test_local_venue_selection_media_and_key_survive_refresh(self):
        fetched = {
            **self.paper, "ENTRYTYPE": "article", "ID": "scholar-generated-key",
            "title": "DocAtlas: Long-Document Understanding", "year": "2025",
            "abbr": "arXiv", "booktitle": "", "selected": "false",
            "summary": "", "preview": "", "code": "", "video": "", "award": "",
        }
        self.assertEqual(sync.merge_publications([self.paper], [fetched]), [self.paper])

    def test_missing_records_are_retained_and_new_work_is_not_selected(self):
        new = {**self.paper, "ID": "other", "title": "Another project", "google_scholar_id": "other"}
        entries = sync.merge_publications([self.paper], [new])
        self.assertEqual(entries[0], self.paper)
        self.assertEqual(entries[1]["selected"], "false")

    def test_explicitly_unselected_entry_stays_unselected(self):
        local = {**self.paper, "selected": "false"}
        self.assertEqual(sync.merge_publications([local], [self.paper])[0]["selected"], "false")

    def test_new_key_collision_does_not_overwrite_existing_paper(self):
        new = {**self.paper, "title": "Different paper", "google_scholar_id": "different"}
        entries = sync.merge_publications([self.paper], [new])
        self.assertEqual(entries[0], self.paper)
        self.assertNotEqual(entries[0]["ID"], entries[1]["ID"])

    def test_empty_and_partial_exports_leave_original_bytes_unchanged(self):
        original = "@article{keep, title={Keep me}, author={Liu, Bei}, year={2026}}\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "papers.bib"
            path.write_text(original)
            for incoming in ([], [self.paper, {"ID": "broken", "title": "No author"}]):
                with self.assertRaises(ValueError):
                    sync.update_bibliography(path, incoming)
                self.assertEqual(path.read_text(), original)

    def test_invalid_existing_bibliography_is_not_replaced(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "papers.bib"
            original = "@article{broken, title={An unclosed entry\n"
            path.write_text(original)
            with self.assertRaises(ValueError):
                sync.update_bibliography(path, [self.paper])
            self.assertEqual(path.read_text(), original)

    def test_failed_export_after_a_successful_export_never_writes(self):
        provider = Mock()
        provider.fill.side_effect = [{"publications": [{}, {}]}, {}, {}]
        provider.bibtex.side_effect = [
            "@article{ok, title={A paper}, author={Liu, Bei}, year={2026}}", "",
        ]
        with patch.object(sync, "update_bibliography") as update, patch.object(sync.time, "sleep"):
            with self.assertRaises(ValueError):
                sync.main(provider)
            update.assert_not_called()

    def test_successful_sync_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "papers.bib"
            self.assertTrue(sync.update_bibliography(path, [self.paper]))
            first = path.read_bytes()
            self.assertFalse(sync.update_bibliography(path, [self.paper]))
            self.assertEqual(path.read_bytes(), first)


if __name__ == "__main__":
    unittest.main()
