"""Stage and apply source updates for the Datamak Lite User app."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import ctypes
import fcntl
import json
import os
from pathlib import Path
import plistlib
import re
import shutil
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen


REPOSITORY = "jdominsk/Datamak"
ORIGIN = f"https://github.com/{REPOSITORY}.git"
API = f"https://api.github.com/repos/{REPOSITORY}"
SOURCE = Path("datamak_lite/menubar")
REVISION = "DatamakSourceRevision"


class UpdateError(Exception):
    pass


def result(state, message, **details):
    return {"state": state, "message": message, **details}


def revision_is_valid(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{40}", value) is not None


def github_json(path, **query):
    url = f"{API}/{path}"
    if query:
        url += "?" + urlencode(query)
    request = Request(url, headers={"Accept": "application/vnd.github+json", "User-Agent": "Datamak-Lite"})
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def newest_app_revision():
    commits = github_json("commits", sha="main", path=str(SOURCE), per_page=1)
    if commits == []:
        return None
    if (
        not isinstance(commits, list) or not isinstance(commits[0], dict)
        or not revision_is_valid(commits[0].get("sha"))
    ):
        raise UpdateError("GitHub did not return an app source revision.")
    return commits[0]["sha"]


def bundle_info(bundle):
    with (bundle / "Contents/Info.plist").open("rb") as stream:
        info = plistlib.load(stream)
    if not isinstance(info, dict):
        raise UpdateError("The app bundle has invalid metadata.")
    return info


def user_bundle(info, window):
    flavor = "dashboard" if window else "menubar"
    return (
        info.get("DatamakRole") == "user"
        and not info.get("DatamakDemo", False)
        and info.get("CFBundleIdentifier") == f"org.datamak.lite.{flavor}.user"
    )


def command(*arguments, timeout, env=None):
    completed = subprocess.run(
        arguments, env=env, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True, errors="replace", timeout=timeout, check=False,
    )
    if completed.returncode:
        lines = completed.stdout.strip().splitlines()
        diagnostic = lines[-1][:240] if lines else f"{Path(arguments[0]).name} failed."
        raise UpdateError(diagnostic)
    return completed.stdout.strip()


def update_folder(app):
    return app.parent / ".datamak-updates"


def staged_path(app, current):
    return update_folder(app) / current / app.name


@contextmanager
def update_lock(app):
    folder = update_folder(app)
    folder.mkdir(exist_ok=True, mode=0o700)
    with (folder / f"{app.name}.lock").open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        yield


def validate_stage(candidate, expected_revision, window):
    info = bundle_info(candidate)
    if not user_bundle(info, window) or info.get(REVISION) != expected_revision:
        raise UpdateError("The staged bundle does not match the expected User app and revision.")
    command("/usr/bin/codesign", "--verify", "--strict", str(candidate), timeout=15)


def checkout(source, revision):
    env = {key: value for key, value in os.environ.items() if not key.startswith("GIT_")}
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull, GIT_TERMINAL_PROMPT="0")
    command("/usr/bin/git", "init", "--quiet", str(source), timeout=15, env=env)
    git = ("/usr/bin/git", "-C", str(source))
    command(*git, "fetch", "--quiet", "--depth=1", ORIGIN, revision, timeout=120, env=env)
    command(*git, "checkout", "--quiet", "--detach", "FETCH_HEAD", timeout=30, env=env)
    if command(*git, "rev-parse", "HEAD", timeout=15, env=env) != revision:
        raise UpdateError("The downloaded source did not match the selected GitHub revision.")
    return env


def swap_bundles(left, right):
    rename = ctypes.CDLL(None, use_errno=True).renamex_np
    rename.argtypes = (ctypes.c_char_p, ctypes.c_char_p, ctypes.c_uint)
    rename.restype = ctypes.c_int
    # Darwin renamex_np exchanges the directories atomically (RENAME_SWAP).
    if rename(os.fsencode(left), os.fsencode(right), 0x00000002):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def stage_source(args, app, installed, newest):
    current = installed[REVISION]
    staged = staged_path(app, current)
    with update_lock(app):
        if bundle_info(app).get(REVISION) != current:
            raise UpdateError("The installed app changed. Open it again and check for updates.")
        staged.parent.mkdir(exist_ok=True, mode=0o700)
        if staged.exists() and bundle_info(staged).get(REVISION) == newest:
            validate_stage(staged, newest, args.window)
            return staged

        with tempfile.TemporaryDirectory(prefix=".datamak-source-", dir=staged.parent) as directory:
            source = Path(directory) / "source"
            env = checkout(source, newest)
            env.update(DATAMAK_LITE_ROOT=args.lite_root, DATAMAK_PYTHON=args.python)
            if installed.get("DatamakHome"):
                env["DATAMAK_HOME"] = installed["DatamakHome"]
            flags = ["--role", "user", "--build-only"]
            if args.window:
                flags.append("--window")
            command("/bin/bash", str(source / SOURCE / "launch.sh"), *flags, timeout=300, env=env)
            name = "Datamak Lite User Dashboard.app" if args.window else "Datamak Lite User.app"
            candidate = source / "tmp/datamak-menubar" / name
            validate_stage(candidate, newest, args.window)
            if staged.exists():
                swap_bundles(staged, candidate)
            else:
                candidate.rename(staged)
    return staged


def check(args, app, installed):
    newest = newest_app_revision()
    if newest is None:
        return result("unpublished", "The Datamak Lite app source has not been published on GitHub.")
    current = installed.get(REVISION)
    if not revision_is_valid(current) or args.revision != current:
        return result("local", "This local build has no published source revision to update from.")
    if newest == current:
        return result("current", "Datamak Lite is up to date.", revision=current)
    comparison = github_json(f"compare/{current}...{newest}")
    if (not isinstance(comparison, dict) or comparison.get("status") != "ahead"
            or comparison.get("behind_by") != 0):
        return result("unavailable", "GitHub has no newer app source descending from this build.")
    staged = stage_source(args, app, installed, newest)
    return result(
        "staged", "A Datamak Lite update is ready. Relaunch to install it.",
        revision=newest, staged_app=str(staged),
    )


def process_exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def wait_for_exit(pid):
    deadline = time.monotonic() + 30
    while process_exists(pid):
        if time.monotonic() >= deadline:
            raise UpdateError("The running Datamak Lite app did not close for the update.")
        time.sleep(0.2)


def apply(args, app, installed):
    current = installed.get(REVISION)
    if (
        not revision_is_valid(current) or args.revision != current
        or args.wait_pid is None or args.wait_pid < 2 or args.wait_pid == os.getpid()
    ):
        raise UpdateError("The installed revision or relaunch request is invalid.")
    staged = staged_path(app, current)
    if args.apply != staged:
        raise UpdateError("The pending update was prepared for a different installed version.")
    with update_lock(app):
        newest = bundle_info(staged).get(REVISION)
        if not revision_is_valid(newest) or newest == current:
            raise UpdateError("The staged revision is invalid.")
        validate_stage(staged, newest, args.window)
        print(json.dumps(result("ready", "Datamak Lite is ready to relaunch.")), flush=True)
        wait_for_exit(args.wait_pid)
        try:
            if bundle_info(app).get(REVISION) != current:
                raise UpdateError("The installed app changed during relaunch.")
            swap_bundles(app, staged)
            try:
                command("/usr/bin/open", "-n", str(app), timeout=15)
            except (OSError, subprocess.SubprocessError, UpdateError):
                swap_bundles(app, staged)
                raise
        except (OSError, ValueError, subprocess.SubprocessError, UpdateError):
            try:
                command("/usr/bin/open", str(app), timeout=15)
            except (OSError, subprocess.SubprocessError, UpdateError):
                pass
            raise
        shutil.rmtree(staged, ignore_errors=True)
    return result("installed", "Datamak Lite updated.", revision=newest)


def run(args):
    app = args.app.resolve(strict=True)
    installed = bundle_info(app)
    if not user_bundle(installed, args.window):
        return result("disabled", "Automatic updates are available in the Datamak Lite User app.")
    if args.apply is None:
        return check(args, app, installed)
    return apply(args, app, installed)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--app", required=True, type=Path)
    parser.add_argument("--revision")
    parser.add_argument("--lite-root", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--window", action="store_true")
    parser.add_argument("--apply", type=Path)
    parser.add_argument("--wait-pid", type=int)
    try:
        outcome = run(parser.parse_args())
    except (OSError, ValueError, subprocess.SubprocessError, UpdateError) as error:
        outcome = result("unavailable", f"Datamak Lite update failed: {error}")
    if outcome["state"] != "installed":
        print(json.dumps(outcome))


if __name__ == "__main__":
    main()
