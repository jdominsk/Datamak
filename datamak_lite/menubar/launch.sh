#!/bin/bash
set -euo pipefail

demo=false
window=false
build_only=false
role=developer
while (( $# )); do
    case "$1" in
        --demo) demo=true; window=true ;;
        --window) window=true ;;
        --build-only) build_only=true ;;
        --role)
            if (( $# < 2 )) || [[ "$2" != developer && "$2" != user ]]; then
                printf '%s\n' 'Choose developer or user with --role.' >&2
                exit 2
            fi
            role=$2
            shift
            ;;
        -h|--help)
            printf '%s\n' 'Usage: launch.sh [--role developer|user] [--window | --demo] [--build-only]'
            exit 0
            ;;
        *)
            printf 'Unknown option: %s\n' "$1" >&2
            exit 2
            ;;
    esac
    shift
done

if "$demo" && [[ "$role" == user ]]; then
    printf '%s\n' 'Demo builds use the developer role.' >&2
    exit 2
fi

source_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
repo=$(cd "$source_dir/../.." && pwd -P)
build_dir="$repo/tmp/datamak-menubar"
canonical_lite="$repo/../../Tungsten_transport/Tungsten_turbulent_transport/Datamak_lite"
if [[ "$role" == user && ! -f "$canonical_lite/datamak_lite/cli.py" ]]; then
    canonical_lite="$repo"
fi
lite_root=${DATAMAK_LITE_ROOT:-$canonical_lite}
if [[ ! -f "$lite_root/datamak_lite/cli.py" ]]; then
    printf 'Could not find a Datamak Lite checkout at %s\n' "$lite_root" >&2
    printf '%s\n' 'Set DATAMAK_LITE_ROOT to the Datamak Lite checkout.' >&2
    exit 1
fi
lite_root=$(cd "$lite_root" && pwd -P)

python_executable=${DATAMAK_PYTHON:-python3}
if ! python_path=$("$python_executable" -c '
import sys
if sys.version_info < (3, 10):
    raise SystemExit(1)
print(sys.executable)
'); then
    printf '%s\n' 'Choose a Python 3.10+ executable with DATAMAK_PYTHON.' >&2
    exit 1
fi

app_name='Datamak Lite'
bundle_id='org.datamak.lite.menubar'
if "$demo"; then
    app_name='Datamak Lite Demo'
    bundle_id='org.datamak.lite.menubar.demo'
elif "$window"; then
    app_name='Datamak Lite Dashboard'
    bundle_id='org.datamak.lite.dashboard'
fi
if [[ "$role" == user ]]; then
    app_name='Datamak Lite User'
    if "$window"; then app_name='Datamak Lite User Dashboard'; fi
    bundle_id="$bundle_id.user"
fi
app="$build_dir/$app_name.app"
mkdir -p "$build_dir"
staging_dir=$(mktemp -d "$build_dir/.build.XXXXXX")
trap 'rm -rf "$staging_dir"' EXIT
staging_app="$staging_dir/$app_name.app"
contents="$staging_app/Contents"
mkdir -p "$contents/MacOS" "$contents/Resources"

/usr/bin/swiftc -swift-version 5 -O -target "$(uname -m)-apple-macosx14.0" \
    -module-cache-path "$build_dir/module-cache" \
    "$source_dir/DatamakMenuBar.swift" "$source_dir/Dashboard.swift" \
    "$source_dir/ArchiveQueue.swift" "$source_dir/AppUpdate.swift" \
    "$source_dir/RemoteQuota.swift" \
    -o "$contents/MacOS/DatamakMenuBar"
/bin/cp "$source_dir/snapshot.py" "$contents/Resources/snapshot.py"
if ! "$demo"; then
    /bin/cp "$source_dir/remote_quota.py" "$contents/Resources/remote_quota.py"
fi

plist="$contents/Info.plist"
/usr/bin/plutil -create xml1 "$plist"
/usr/bin/plutil -insert CFBundleIdentifier -string "$bundle_id" "$plist"
/usr/bin/plutil -insert CFBundleExecutable -string DatamakMenuBar "$plist"
/usr/bin/plutil -insert CFBundlePackageType -string APPL "$plist"
/usr/bin/plutil -insert CFBundleName -string "$app_name" "$plist"
/usr/bin/plutil -insert CFBundleDisplayName -string "$app_name" "$plist"
/usr/bin/plutil -insert CFBundleVersion -string 1 "$plist"
/usr/bin/plutil -insert CFBundleShortVersionString -string 0.1.0 "$plist"
/usr/bin/plutil -insert LSMinimumSystemVersion -string 14.0 "$plist"
if "$window"; then
    /usr/bin/plutil -insert DatamakDashboardWindow -bool YES "$plist"
else
    /usr/bin/plutil -insert LSUIElement -bool YES "$plist"
fi
/usr/bin/plutil -insert DatamakLiteRoot -string "$lite_root" "$plist"
/usr/bin/plutil -insert DatamakPython -string "$python_path" "$plist"
/usr/bin/plutil -insert DatamakDemo -bool "$demo" "$plist"
/usr/bin/plutil -insert DatamakRole -string "$role" "$plist"
if [[ "$role" == user ]]; then
    /bin/cp "$source_dir/app_update.py" "$contents/Resources/app_update.py"
    # An uncommitted prototype has no source revision the updater can compare.
    if source_changes=$(/usr/bin/git -C "$repo" status --porcelain --untracked-files=all -- datamak_lite/menubar 2>/dev/null) &&
       [[ -z "$source_changes" ]] &&
       source_revision=$(/usr/bin/git -C "$repo" log -1 --format=%H -- datamak_lite/menubar 2>/dev/null) &&
       [[ "$source_revision" =~ ^[0-9a-f]{40}$ ]]; then
        /usr/bin/plutil -insert DatamakSourceRevision -string "$source_revision" "$plist"
    fi
fi
if [[ -n "${DATAMAK_HOME:-}" ]]; then
    config_dir=$DATAMAK_HOME
    if [[ "$config_dir" != /* ]]; then config_dir="$PWD/$config_dir"; fi
    /usr/bin/plutil -insert DatamakHome -string "$config_dir" "$plist"
fi

/usr/bin/codesign --force --sign - "$staging_app"
/usr/bin/ditto "$staging_app" "$app"
printf '%s\n' "$app"
if ! "$build_only"; then
    /usr/bin/open "$app"
fi
