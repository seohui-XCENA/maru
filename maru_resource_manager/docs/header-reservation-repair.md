# Repairing an empty legacy DEV_DAX checkpoint

The RM reserves the first device alignment unit for its UUID header. Older
checkpoints can contain an extent starting at zero even when their total size
already excludes that unit. The fixed RM rejects these checkpoints with a
`checkpoint data range (offline repair required)` error. It does not silently
discard allocations or relocate handles.

The v6 pool/v2 global metadata and v6 WAL formats are unchanged. Correct existing
checkpoints and WAL remain supported. Corrupt/truncated records, invalid ranges,
or missing pieces of a checkpoint require investigation; they are not treated
as empty pools. Startup recovery failure exits before listening. A failed rescan
keeps serving existing valid pools and retains WAL instead of checkpointing an
incomplete recovery. An unavailable device referenced by recovery state also
keeps WAL until that device can be recovered.

## Preconditions

This procedure only handles an **empty, stopped RM** whose pool checkpoint has
the known `(offset=0, length=deviceSize)` layout. It also accepts already-correct
empty pools without changing their content.

1. Stop new client allocations, finish GPU transfers, release registrations and
   mappings, and free all allocations. Include remote clients and workers that
   may hold a mapping independently of their RM connection.
2. Confirm the RM's allocation ledger is empty. STATS/free bytes alone are not
   evidence of this: the legacy free-list is the reason for this repair.
3. Stop the existing RM gracefully and ensure it stays stopped. Inspect device
   users/mappings with root privileges. The RM closing its TCP listener does not
   revoke client mmaps.
4. Take a permission-preserving backup of the entire state directory. Checkpoint
   must have left `global.meta` with zero allocations and no nonempty WAL. If
   either WAL filename contains records, stop here and investigate recovery;
   do not delete the log to make the preparation tool accept the snapshot.
5. Check that each target device has a valid, expected Maru UUID header. The
   preparation tool does not open devices or repair their headers. A missing
   header after a legacy offset-zero allocation requires a separate identity
   repair after confirming that no old users remain.

## Inspect open descriptors and mappings

Run this on the **host**, with root privileges and visibility of all host PIDs,
after preventing new client starts/allocations. Repeat on every host that can
access the same physical CXL memory; device names can differ across hosts.

Start with a quick inventory (this command does not kill processes):

```bash
sudo fuser -v /dev/dax0.0 /dev/dax1.0 /dev/dax2.0
```

Then inspect both open descriptors and file-backed mappings. Closing a device
descriptor does not unmap it. The following read-only scan follows `/proc` links
and compares character-device major/minor numbers, so it also finds mappings
through differently named device nodes. It scans threads as well as process
leaders, and never opens the DAX devices themselves.

```bash
sudo python3 - /dev/dax0.0 /dev/dax1.0 /dev/dax2.0 <<'PY'
import os
import stat
import sys
from pathlib import Path

targets = {}
for name in sys.argv[1:]:
    info = os.stat(name)
    if not stat.S_ISCHR(info.st_mode):
        raise SystemExit(f"Not a character device: {name}")
    targets[info.st_rdev] = name

matches = set()
errors = set()

def entries(path):
    try:
        return list(path.iterdir())
    except (FileNotFoundError, ProcessLookupError):
        return []  # Process/thread exited during this snapshot.
    except OSError as error:
        errors.add(f"{path}: {error}")
        return []

for process in entries(Path("/proc")):
    if not process.name.isdigit():
        continue
    for thread in entries(process / "task"):
        # map_files is exposed at /proc/<tid>, not /proc/<pid>/task/<tid>.
        task = Path("/proc") / thread.name
        for kind in ("fd", "map_files"):
            for link in entries(task / kind):
                try:
                    info = link.stat()
                except (FileNotFoundError, ProcessLookupError):
                    continue  # FD/mapping closed, or thread exited.
                except OSError as error:
                    errors.add(f"{link}: {error}")
                    continue
                if stat.S_ISCHR(info.st_mode) and info.st_rdev in targets:
                    matches.add((int(process.name), kind, link.name,
                                 targets[info.st_rdev]))

for pid, kind, entry, device in sorted(matches):
    print(f"PID={pid} {kind}/{entry} -> {device}")
for error in sorted(errors):
    print(f"INCOMPLETE: {error}", file=sys.stderr)
print(f"Found {len(matches)} distinct process references; {len(errors)} scan errors.")
raise SystemExit(2 if errors else 1 if matches else 0)
PY
```

`map_files/<start>-<end>` identifies a virtual address range; inspect the
corresponding `/proc/<pid>/maps` entry for its device offset and permissions.
Use `ps -fp <pid>` to identify the owner, then shut down that worker/service
normally and repeat the scan. Permission errors mean **incomplete inspection**,
not zero users. Restricted containers or security policies may prevent even
UID 0 from inspecting `map_files`; run with the necessary host capabilities.
See the [kernel `/proc` documentation](https://docs.kernel.org/filesystems/proc.html)
and [`proc_pid_map_files(5)`](https://man7.org/linux/man-pages/man5/proc_pid_map_files.5.html).

This is a non-atomic snapshot of userspace references, not proof that all device
activity has stopped. Keep new workers blocked throughout the maintenance:

- Complete asynchronous GPU/DMA work and release CUDA host registrations before
  unmapping (for example, synchronize the relevant CUDA work, then
  `cudaHostUnregister`, then `munmap`/close). An empty `/proc` scan does not itself
  prove that a driver has released every pinned page or DMA reference.
- Verify the RM allocation ledger separately: after graceful RM shutdown, the
  repair preparation tool below requires zero saved allocations and no nonempty
  WAL. Neither `fuser` nor free-capacity STATS validates the allocation ledger.
- For a later ROOF takeover, explicitly exclude dax2.0 from RM discovery as well.
  RM need not hold a persistent device fd to keep advertising/allocating a pool;
  an empty process scan does not establish device ownership.

## Prepare files for review

Build and run unit tests before deploying the new binary:

```bash
cmake -S maru_resource_manager -B /tmp/maru-rm-header-fix-build \
  -DMARU_BUILD_TESTS=ON -DMARU_BUILD_TOOLS=OFF
cmake --build /tmp/maru-rm-header-fix-build -j 4
ctest --test-dir /tmp/maru-rm-header-fix-build --output-on-failure
python3 -m unittest discover -s maru_resource_manager/tests \
  -p test_prepare_empty_pool_repair.py
```

The following is an operator example after completing the preconditions. Use
fresh backup/output paths. It is not an automatic stop/drain/repair script.

```bash
sudo systemctl stop maru-resource-manager.service
sudo cp -a /var/lib/maru-resourced /var/lib/maru-resourced.before-header-repair

python3 maru_resource_manager/tools/prepare_empty_pool_repair.py \
  --state-dir /var/lib/maru-resourced.before-header-repair \
  --output /tmp/maru-rm-prepared-checkpoints \
  --device /dev/dax0.0 --device /dev/dax1.0 --device /dev/dax2.0
```

The tool reads device size/alignment from sysfs. It writes **only repaired copies
and a manifest in the new output directory**. It never modifies the source
state, UUID headers, global metadata, secrets, or WAL. It rejects nonempty
allocation ledgers/WAL, invalid CRCs, unexpected extents, and ambiguous pool IDs.

Review `manifest.json`: pool/device correspondence, source hashes, geometry,
proposed data ranges, and preserved next region ID. Verify the live state files
still match the backed-up source hashes and the RM is still stopped. On this
node all three legacy pool files need correction, including dax2.0 while it is
still RM-owned. ROOF device exclusion is a later change.

## Apply and verify

After reviewing the generated files and verifying the preconditions, replace
only the affected `pool_N.meta` files, preserving their ownership/permissions.
Leave `global.meta` (including `nextRegionId`), `secret`, and any other state
unchanged. Do not apply part of a prepared repair while starting RM concurrently.

For the default root-owned 0644 pool metadata on this node:

```bash
sudo install -o root -g root -m 0644 \
  /tmp/maru-rm-prepared-checkpoints/pool_0.meta \
  /tmp/maru-rm-prepared-checkpoints/pool_1.meta \
  /tmp/maru-rm-prepared-checkpoints/pool_2.meta \
  /var/lib/maru-resourced/
sudo cmake --install /tmp/maru-rm-header-fix-build
sudo systemctl start maru-resource-manager.service
```

Check logs/STATS and perform controlled allocation/free tests: the minimum
DEV_DAX allocation offset must be at least the device alignment, free capacity
must not exceed total, and a clean restart must retain these properties. Verify
an active allocation survives RM restart without allowing an overlapping second
allocation. Hardware/client tests require the maintenance window; passing
temporary-file tests alone does not validate deployment.

Keep the backup. Do not restore the legacy broken free-list and resume service
as a rollback. If startup rejects another inconsistency, leave the service down
and investigate the saved state.
