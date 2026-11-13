from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from datamak_lite.core.literature import (
    default_literature_path,
    find_duplicates,
    find_papers,
    list_reviews,
    normalize_arxiv,
    normalize_doi,
    register_paper,
    register_review,
)


class LiteratureCoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / "index" / "literature.sqlite"
        self.a = self.root / "review-a"
        self.b = self.root / "review-b"
        self.a.mkdir()
        self.b.mkdir()

    def review(self, path: Path) -> str:
        return register_review(path, db_path=self.db)["uid"]

    def paper(self, review: str, **kwargs):
        return register_paper(review, db_path=self.db, **kwargs)

    def test_read_only_queries_do_not_create_index(self):
        self.assertEqual(find_papers(db_path=self.db), [])
        self.assertEqual(list_reviews(self.db), [])
        self.assertEqual(find_duplicates(self.db), [])
        self.assertFalse(self.db.parent.exists())
        with patch.dict(os.environ, {"DATAMAK_HOME": str(self.root / "configured")}):
            self.assertEqual(default_literature_path(), self.root / "configured" / "literature.sqlite")

    def test_normalization_and_cross_review_identity(self):
        first, second = self.review(self.a), self.review(self.b)
        note_a, note_b = self.a / "paper.md", self.b / "paper.md"
        note_a.write_text("existing notes A")
        note_b.write_text("existing notes B")
        pdf = self.a / "paper.pdf"
        pdf.write_bytes(b"preserve source file")
        one = self.paper(first, title="Transport study", doi="https://doi.org/10.1234/ABC", note_path=note_a,
                         pdf_path=pdf, depth="deep")
        two = self.paper(second, title="Transport study", doi="DOI:10.1234/abc", note_path=note_b,
                         depth="seed", corpus_status="provisional")
        self.assertEqual(one["uid"], two["uid"])
        self.assertEqual(len(find_papers(db_path=self.db)), 1)
        memberships = {r["uid"]: r for r in two["reviews"]}
        self.assertEqual(memberships[first]["depth"], "deep")
        self.assertEqual(memberships[second]["depth"], "seed")
        self.assertEqual(memberships[second]["corpus_status"], "provisional")
        self.assertEqual(pdf.read_bytes(), b"preserve source file")
        self.assertEqual(note_a.read_text(), "existing notes A")
        self.assertEqual(note_b.read_text(), "existing notes B")
        self.assertEqual(len(find_papers("https://doi.org/10.1234/ABC", self.db)), 1)

    def test_idempotent_registration_and_later_enrichment(self):
        review = self.review(self.a)
        self.assertEqual(review, self.review(self.a))
        pdf = self.a / "weak-label.pdf"
        pdf.touch()
        one = self.paper(review, title="weak-label", pdf_path=pdf, metadata_incomplete=True)
        enriched = self.paper(review, title="A carefully specified title", doi="10.1234/xyz", pdf_path=pdf,
                              authors="A. Author", year="2020")
        self.assertEqual(one["uid"], enriched["uid"])
        self.assertFalse(enriched["metadata_incomplete"])
        again = self.paper(review, title="weak-label", pdf_path=pdf, metadata_incomplete=True)
        self.assertEqual(again["title"], "A carefully specified title")
        self.assertFalse(again["metadata_incomplete"])
        self.assertEqual(again["reviews"][0]["depth"], "")
        self.assertEqual(list_reviews(self.db)[0]["paper_count"], 1)
        with sqlite3.connect(self.db) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM artifact").fetchone()[0], 1)

    def test_enrichment_preserves_provisional_membership(self):
        review = self.review(self.a)
        pdf = self.a / "filename-only.pdf"
        initial = self.paper(review, title="filename-only", pdf_path=pdf,
                             metadata_incomplete=True, corpus_status="provisional")
        enriched = self.paper(review, uid=initial["uid"], title="A bibliographically identified paper",
                              doi="10.1234/identified")
        self.assertFalse(enriched["metadata_incomplete"])
        self.assertEqual(enriched["reviews"][0]["corpus_status"], "provisional")
        repeated = self.paper(review, title="filename-only", pdf_path=pdf, metadata_incomplete=True)
        self.assertEqual(repeated["title"], "A bibliographically identified paper")
        self.assertFalse(repeated["metadata_incomplete"])
        self.assertEqual(repeated["reviews"][0]["corpus_status"], "provisional")

    def test_arxiv_versions_and_published_paper_are_not_title_merged(self):
        review = self.review(self.a)
        one = self.paper(review, title="The same research title", arxiv="https://arxiv.org/pdf/2301.12345v2.pdf")
        two = self.paper(review, title="The same research title", arxiv="arXiv:2301.12345v3")
        published = self.paper(review, title="The same research title", doi="10.1234/published")
        self.assertEqual(one["uid"], two["uid"])
        self.assertEqual(two["arxiv_versions"], ["v2", "v3"])
        self.assertNotEqual(published["uid"], two["uid"])
        self.assertEqual(len(find_duplicates(self.db)), 1)
        self.assertEqual(len(find_papers(db_path=self.db)), 2)
        self.assertEqual(normalize_arxiv("arXiv:hep-th/9901001v1"), ("hep-th/9901001", "v1"))
        self.assertEqual(normalize_doi("DOI:10.1234/ABC"), "10.1234/abc")

    def test_conflicting_identity_rolls_back_before_updating_anything(self):
        review = self.review(self.a)
        first = self.paper(review, title="One independent paper", doi="10.1234/one")
        second = self.paper(review, title="Another independent paper", arxiv="2301.12345")
        before = find_papers(db_path=self.db)
        with self.assertRaisesRegex(ValueError, "multiple papers"):
            self.paper(review, title="Incorrect attempted merge", doi="10.1234/one", arxiv="2301.12345")
        self.assertEqual(find_papers(db_path=self.db), before)
        with self.assertRaisesRegex(ValueError, "Conflicting doi"):
            self.paper(review, title="Incorrect attempted overwrite", uid=first["uid"], doi="10.1234/different")
        self.assertEqual(find_papers(db_path=self.db), before)
        self.assertNotEqual(first["uid"], second["uid"])

    def test_explicit_identifiers_link_when_unambiguous_and_keep_locations(self):
        review = self.review(self.a)
        first = self.paper(review, title="A known paper", doi="10.1234/one", pdf_path=self.a / "v1.pdf")
        linked = self.paper(review, title="A known paper", doi="10.1234/one", arxiv="2301.12345v2",
                            pdf_path=self.a / "v2.pdf")
        self.assertEqual(first["uid"], linked["uid"])
        self.assertEqual(len(linked["artifacts"]), 2)
        self.assertEqual(len(linked["reviews"][0]["pdf_paths"]), 2)


if __name__ == "__main__":
    unittest.main()
