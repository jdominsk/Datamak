#!/usr/bin/env python3
import shutil
from pathlib import Path


def next_run_index(parent: Path) -> int:
    existing = []
    for path in parent.iterdir():
        if path.is_dir() and path.name.startswith("run"):
            suffix = path.name[3:]
            if suffix.isdigit():
                existing.append(int(suffix))
    return max(existing, default=0) + 1


def main() -> None:
    newbatch_dir = Path.cwd()
    parent_dir = newbatch_dir.parent
    script_dir = Path(__file__).resolve().parent
    scripts = [script_dir / "job_submit.sh", script_dir / "job_execute.sh"]

    missing = [str(path) for path in scripts if not path.is_file()]
    if missing:
        raise SystemExit(f"Missing job scripts: {', '.join(missing)}")

    db_files = sorted(newbatch_dir.glob("batch_database_*.db"))
    if not db_files:
        print(f"No batch_database_*.db files found in {newbatch_dir}")
        return

    run_index = next_run_index(parent_dir)
    for db_path in db_files:
        run_dir = parent_dir / f"run{run_index:04d}"
        run_dir.mkdir(parents=True, exist_ok=False)
        shutil.copy2(db_path, run_dir / db_path.name)
        for script_path in scripts:
            shutil.copy2(script_path, run_dir / script_path.name)
        db_path.unlink()
        print(f"Created {run_dir} from {db_path.name}")
        run_index += 1


if __name__ == "__main__":
    main()
