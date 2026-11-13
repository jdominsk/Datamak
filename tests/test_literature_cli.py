from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class LiteratureCliTest(unittest.TestCase):
    def test_existing_reviews_shared_paper_and_read_only_discovery(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / 'config'
            env = dict(os.environ, DATAMAK_HOME=str(config), PYTHONDONTWRITEBYTECODE='1')

            def cli(*args):
                run = subprocess.run(
                    [sys.executable, '-m', 'datamak_lite.cli', *map(str, args)],
                    env=env, text=True, capture_output=True,
                )
                self.assertEqual(run.returncode, 0, run.stderr)
                return run.stdout

            self.assertEqual(json.loads(cli('literature-reviews', '--json')), [])
            self.assertFalse(config.exists())
            for name, depth in [('first', 'deep'), ('second', 'seed')]:
                folder = root / name
                (folder / 'paper_notes').mkdir(parents=True)
                (folder / 'References').mkdir()
                (folder / 'topic_overview.md').write_text('# Topic\n')
                (folder / 'References/paper.pdf').write_bytes(b'placeholder; no PDF parsing required')
                (folder / 'paper_notes/paper.md').write_text(
                    '# Shared paper\n\n- DOI: https://doi.org/10.1234/EXAMPLE\n'
                    '- Local path: ../References/paper.pdf\n'
                    f'- Review depth: {depth}\n'
                )
            before = {str(p): p.read_bytes() for p in root.rglob('*') if p.is_file()}
            discovered = json.loads(cli('literature-discover', root, '--json'))
            self.assertEqual({Path(item['path']).name for item in discovered}, {'first', 'second'})
            cli('literature-import', root / 'first', '--dry-run')
            self.assertFalse(config.exists())
            cli('literature-import', root / 'first')
            cli('literature-import', root / 'second')
            cli('literature-import', root / 'first')
            papers = json.loads(cli('literature-find', 'https://doi.org/10.1234/example', '--json'))
            self.assertEqual(len(papers), 1)
            self.assertEqual({r['depth'] for r in papers[0]['reviews']}, {'deep', 'seed'})
            self.assertEqual(len(json.loads(cli('literature-reviews', '--json'))), 2)
            self.assertEqual(before, {path: Path(path).read_bytes() for path in before})
            cli('literature-find', 'shared')
            cli('literature-duplicates')


if __name__ == '__main__':
    unittest.main()
