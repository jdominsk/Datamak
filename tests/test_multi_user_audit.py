import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TARGETS = [
    PROJECT_ROOT / "dtwin_config.py",
    PROJECT_ROOT / "tools" / "resolve_dtwin_env.py",
    PROJECT_ROOT / "README",
    PROJECT_ROOT / "gui",
    PROJECT_ROOT / "batch",
    PROJECT_ROOT / "db_update",
]
EXCLUDED_PARTS = {
    "docs",
    "tests",
    "transp_full_auto",
    "Transp_full_auto_remote",
    "__pycache__",
}
EXCLUDED_FILES = {
    "CODEOWNERS",
    "show_transp_time_array.py",
}
BANNED_LITERALS = [
    "jdominsk",
    "/Users/jdominsk",
    "/u/jdominsk",
]


class MultiUserAuditTests(unittest.TestCase):
    def test_active_scope_contains_no_personal_path_literals(self) -> None:
        offenders: list[str] = []
        for target in TARGETS:
            if target.is_file():
                candidates = [target]
            else:
                candidates = [path for path in target.rglob("*") if path.is_file()]
            for path in candidates:
                relative = path.relative_to(PROJECT_ROOT)
                if any(part in EXCLUDED_PARTS for part in relative.parts):
                    continue
                if path.name in EXCLUDED_FILES:
                    continue
                try:
                    text = path.read_text(encoding="utf-8")
                except UnicodeDecodeError:
                    continue
                for literal in BANNED_LITERALS:
                    if literal in text:
                        offenders.append(f"{relative}: {literal}")
        self.assertEqual(offenders, [])
