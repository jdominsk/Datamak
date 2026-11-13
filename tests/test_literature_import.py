from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from datamak_lite.adapters.literature_review import discover_reviews, import_review
from datamak_lite.core.literature import find_papers, list_reviews


class LiteratureImportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.db = self.root / "catalogue.sqlite"

    def write(self, path, text=""):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        return target

    def test_discovery_finds_topics_scopes_and_legacy_without_derivative_outputs(self):
        self.write("topics/a/topic_overview.md")
        self.write("topics/a/reports/review_report.tex")
        self.write("topics/a/impurity_transport_review/impurity_transport_review.tex")
        self.write("topics/a/scoped_reviews/question/scope.md", "# Narrow question")
        self.write("topics/b/old_review.tex")
        self.write("topics/c/references/paper.pdf")
        self.write("topics/old_skill/SKILL.md")
        self.write("topics/old_skill/review_template.tex")
        self.write("topics/.hidden/hidden_review.md")
        found = discover_reviews([self.root / "topics"])
        self.assertEqual(
            {item["path"] for item in found},
            {str(self.root / "topics" / name) for name in ("a", "a/scoped_reviews/question", "b", "c")},
        )
        self.assertFalse(self.db.exists())

    def test_dry_run_pairs_note_with_pdf_and_leaves_everything_unchanged(self):
        self.write("topic/references/example.pdf", "%PDF-placeholder")
        self.write("topic/paper_notes/example.md", "# Example paper\n- DOI: 10.1234/example\n- Review depth: deep\n")
        before = {path: path.read_bytes() for path in (self.root / "topic").rglob("*") if path.is_file()}
        plan = import_review(self.root / "topic", db_path=self.db, dry_run=True)
        self.assertEqual(plan["counts"]["paper_records"], 1)
        self.assertEqual(plan["papers"][0]["pdf_path"], str(self.root / "topic/references/example.pdf"))
        self.assertFalse(self.db.exists())
        self.assertEqual(before, {path: path.read_bytes() for path in (self.root / "topic").rglob("*") if path.is_file()})

    def test_repeated_import_links_same_paper_across_reviews_without_losing_depth(self):
        for topic, depth in (("one", "deep"), ("two", "seed pass")):
            self.write(f"{topic}/References/example.pdf", "%PDF-placeholder")
            self.write(f"{topic}/paper_notes/example.md", f"# Example paper\n- DOI: https://doi.org/10.1234/example\n- Local path: References/example.pdf\n- Review depth: {depth}\n")
            import_review(self.root / topic, db_path=self.db)
            import_review(self.root / topic, db_path=self.db)
        papers = find_papers(db_path=self.db)
        self.assertEqual(len(papers), 1)
        self.assertEqual(len(list_reviews(db_path=self.db)), 2)
        self.assertEqual({item["name"]: item["depth"] for item in papers[0]["reviews"]}, {"one": "deep", "two": "seed"})
        self.assertEqual(len([item for item in papers[0]["artifacts"] if item["kind"] == "pdf"]), 2)

    def test_filename_only_pdf_has_unknown_depth_and_incomplete_metadata(self):
        self.write("topic/References/name_only.pdf", "%PDF-placeholder")
        outcome = import_review(self.root / "topic", db_path=self.db)
        self.assertEqual(outcome["counts"]["metadata_incomplete"], 1)
        paper = find_papers(db_path=self.db)[0]
        self.assertTrue(paper["metadata_incomplete"])
        self.assertEqual(paper["reviews"][0]["depth"], "")

    def test_unfilled_optional_identifiers_do_not_merge_different_papers(self):
        for key in ("a", "b"):
            self.write(f"topic/paper_notes/{key}.md",
                       f"# Paper {key}\n- Title: ...\n- Paper ID: ...\n- DOI: ...\n- arXiv: unknown\n")
        import_review(self.root / "topic", db_path=self.db)
        papers = find_papers(db_path=self.db)
        self.assertEqual({paper["title"] for paper in papers}, {"Paper a", "Paper b"})
        self.assertEqual(len({paper["uid"] for paper in papers}), 2)

    def test_project_relative_paths_and_scoped_provisional_notes(self):
        self.write("project/Reviewing/topic/References/a.pdf", "%PDF-placeholder")
        self.write("project/Reviewing/topic/paper_notes/a.md", "# A paper\n- Local path: `Reviewing/topic/References/a.pdf`\n- Review depth: comparative pass\n")
        root = self.root / "project/Reviewing/topic"
        outcome = import_review(root, db_path=self.db)
        self.assertEqual(outcome["counts"]["paper_records"], 1)
        self.assertEqual(outcome["papers"][0]["pdf_path"], str(root / "References/a.pdf"))
        self.write("scope/scope.md", "# A question")
        self.write("scope/web_pdfs/b.pdf", "%PDF-placeholder")
        self.write("scope/web_notes/b.md", "# B paper\n- Local path: ../web_pdfs/b.pdf\n- Review depth: seed pass\n")
        import_review(self.root / "scope", db_path=self.db)
        paper = next(item for item in find_papers(db_path=self.db) if item["title"] == "B paper")
        self.assertEqual(paper["reviews"][0]["corpus_status"], "provisional")
        self.assertEqual(paper["reviews"][0]["depth"], "seed")

    def test_bibliographic_fields_after_evidence_heading_are_not_imported(self):
        self.write("topic/paper_notes/a.md", "# A paper\n- Review depth: seed\n\n## Main claims\n- DOI: 10.1234/some_other_paper\n")
        plan = import_review(self.root / "topic", db_path=self.db, dry_run=True)
        self.assertEqual(plan["papers"][0]["doi"], "")
        self.assertEqual(plan["counts"]["missing_identifiers"], 1)

    def test_missing_reference_path_is_reported_in_existing_corpus_directory(self):
        (self.root / "topic/References").mkdir(parents=True)
        self.write("topic/paper_notes/a.md", "# A paper\n- Local path: References/missing.pdf\n- Review depth: comparative, source-specific extraction\n")
        plan = import_review(self.root / "topic", db_path=self.db, dry_run=True)
        self.assertEqual(plan["papers"][0]["pdf_path"], str(self.root / "topic/References/missing.pdf"))
        self.assertEqual(plan["papers"][0]["depth"], "comparative")
        self.assertTrue(any("PDF path in note is missing" in gap for gap in plan["gaps"]))


if __name__ == "__main__":
    unittest.main()
