# Datamak Lite in the Mac menu bar

## Try the prototype

On macOS 14 or later with Python 3.10 or later and the Xcode Command Line Tools, run this from the workspace root.

```bash
./datamak_lite/menubar/launch.sh
```

Hover over the Datamak icon for a status summary, then click it to open the panel. A ring marks changes since your last visit. A dot signals something to review. Use **Hide** to dismiss the panel and keep Datamak running. Right-click the icon to find **Quit Datamak Lite**. The app checks the local catalog every minute while it runs. Click the refresh icon to check immediately.

- **Campaigns** shows running and pending work as last recorded in the registered Datamak Lite databases. Click a counter to filter the list or a campaign to see its details.
- **Storage** groups recorded remote paths by machine. Click a machine to browse run paths, archive entries, and recorded sizes. Quotas carry an observation date when available.

Click the refresh icon beside a machine in **Storage** to query its quota.
Datamak uses the existing SSH configuration and shares an authenticated connection
when one is available. If sign-in is needed, choose **Connect…**, finish signing
in through Terminal, then click Refresh again. A successful check is cached
locally with its observation time and filesystem or project scope.

Use `./datamak_lite/menubar/launch.sh --window` to see your registered campaigns in a standalone window, or `./datamak_lite/menubar/launch.sh --demo` to explore a sample scheduler mismatch and storage records. Use `--build-only` to build the app without opening it. From the workspace root, reopen the dashboard with `open "tmp/datamak-menubar/Datamak Lite Dashboard.app"` or double-click it in Finder.

The prototype uses the campaign registry under `$DATAMAK_HOME` or `~/.datamak`. `DATAMAK_LITE_ROOT` selects the Datamak Lite checkout and `DATAMAK_PYTHON` selects the Python interpreter. The app reads local metadata and flags recorded running or queued activity for review when its last observation is missing or older than 24 hours. Live HPC checks and catalog updates are future work.

## Developer and User builds

The launcher defaults to **Developer** for local work. Run
`./datamak_lite/menubar/launch.sh --role user` to build the separate **Datamak Lite User** app.
Add `--window` for a standalone User dashboard. The two builds use the same
local campaign registry and archive flags.

The User app checks `jdominsk/Datamak` for a newer published menu-bar source
revision once a day while it runs. It builds an available update in a separate
directory and offers **Relaunch to Update** when the build is ready. This updates
the installed User app. Your Git checkouts remain separate.

Source updates require a User build that records a committed menu-bar source
revision and a newer revision on GitHub. Use two published revisions to
validate an end-to-end update.

## Flag runs for archive

Open **Storage**, choose a machine, and select **Remote runs**. Hover over the icon beside a run to see its archive state and the supporting record.

- **Unarchived** means Datamak has no archive record for this run path.
- **To be archived** means you flagged the run for archive.
- **Archived** means a verified full archive matches the recorded source snapshot.
- **Archive needs review** can mean a newer source inventory, incomplete archive coverage, or archive freshness still to verify. The tooltip reports which evidence is available.

Click an unarchived or review icon to flag a run, and click its flag to remove it. **Archive queue** in Storage lists your selections across machines. Flags are saved in your Mac's Application Support folder.

**Export GX queue…** writes one exact directory path per line for flagged GX runs on Perlmutter that are eligible for planning. Runs with a verified current archive, unavailable records, or unsupported paths remain outside the export. Export is available with your registered data.

To turn that file into an archive plan, stage it and `plan_gx_archive.py` from the GX Archiver skill in your Perlmutter scratch workspace. From that workspace, use the GX Python environment and the approved GX root for your selection.

```bash
"$GX_PYTHON" ./plan_gx_archive.py \
  --queue-file gx-archive-queue.txt \
  --allowed-root "$GX_ROOT" \
  --output gx-archive-plan.json
```

For a campaign-specific plan, use `--datamak-db` and `--campaign-uid` with the database and UID from that campaign's registry entry. Review the plan's exact paths, file sizes, and issues. The dashboard ends at export. A compatible Perlmutter transfer worker and a separately reviewed Slurm submission are needed to send the complete directories to HPSS. Before submitting, confirm producer state, source stability, and HPSS capacity. An imported verification receipt provides the evidence to mark a run archived.

## Target

The menu bar should answer two questions quickly. What changed since I last looked, and what needs my attention? Clicking an alert should take you to the affected campaign or storage location and show the evidence behind it.

### Campaigns and reconciliation

Show running simulations, queued jobs, and cases ready for a worker at the campaign level. A campaign can share a pool with another campaign, so use direct campaign membership for case counts and list a shared allocation only once in the scheduler view. Show the source and observation time for each status.

Start with one manual **Check Perlmutter** action and then extend the same interface to Polaris and Aurora. Use one read-only scheduler check per machine, followed by accounting lookups for tracked jobs that have left the queue. The existing Perlmutter GUI has Slurm parsing and connection code to draw from. Link observations by machine and recorded job or array task ID. Read pool snapshots for worker claims and case results. A scheduler reports the allocation or job, and a pool reports individual cases.

Flag a mismatch when a newer observation and the catalog disagree about the same run or attempt. For example, a confirmed time limit in scheduler accounting alongside a catalogued running status warrants review. A job missing from the active queue calls for an accounting check. Show unknown or stale evidence when a host is unavailable, history has expired, or a match is uncertain. Let the user review the proposed change before running the existing campaign refresh workflow.

### Storage and archive review

Group storage by machine and filesystem, with account or project scope on each quota. Show measured on-disk usage, the applicable quota, and when they were checked. A campaign view should list known simulation directories by measured size and show whether each has a local copy or a recorded archive. Count a shared path once per filesystem and label partial size totals.

Use Datamak `archived_in` relations and the associated receipts to show the archive destination, verification date, and coverage. A historical archive may cover outputs while omitting restarts. A verified GX directory revision should point to the receipt for that specific source snapshot.

For archive and cleanup review, show the largest eligible directories together with active jobs, restart and history dependencies, and archive coverage. Reuse the existing GX archive planner and receipt importer for any explicitly requested archive. Review removal of a particular copy separately, with its exact path and the current evidence on dependent runs and archive coverage.
