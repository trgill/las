#!/usr/bin/env python3
#
# Copyright Red Hat
#
# las.py - Lift and Shift main interface
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
"""
las main.
"""
import os
import argparse
import sys
import subprocess
import re
import time

import raid
import utils
import database
from dm import RAIDEngine
from storage import StorageBackend


def update_sync_throttle(name, throttle_kibs):
    """
    Updates the RAID throttle on a live mapper device by staging
    the table before resuming.
    """
    try:
        current_table = subprocess.check_output(
            ['sudo', 'dmsetup', 'table', name], text=True
        ).strip()

        new_max = throttle_kibs * 2
        new_table = re.sub(
            r'min_recovery_rate \d+',
            f'min_recovery_rate {throttle_kibs}', current_table
        )
        new_table = re.sub(
            r'max_recovery_rate \d+',
            f'max_recovery_rate {new_max}', new_table
        )

        print(f"[*] Staging new throttle: {throttle_kibs} KiB/s...")

        load_proc = subprocess.Popen(
            ['sudo', 'dmsetup', 'load', name], stdin=subprocess.PIPE
        )
        load_proc.communicate(input=new_table.encode())

        subprocess.run(['sudo', 'dmsetup', 'suspend', name], check=True)
        subprocess.run(['sudo', 'dmsetup', 'resume', name], check=True)

        print("[SUCCESS] Sync speed updated.")
    except Exception as e:
        print(f"[!] Sync update failed. Attempting emergency resume...")
        subprocess.run(
            ['sudo', 'dmsetup', 'resume', name], stderr=subprocess.DEVNULL
        )


def list_migrations():
    migrations = database.list_all_migrations()
    if not migrations:
        print("[*] No migrations found in database.")
        return
    print(f"{'Name':<15} {'Type':<12} {'Source':<15} {'Dest':<18} {'Progress'}")
    print("-" * 70)
    for m in migrations:
        backend = StorageBackend.from_record(m)
        _, pct, _ = backend.check_sync(m['name'], m)
        mtype = m.get('migration_type', 'partition')
        print(f"{m['name']:<15} {mtype:<12} {m['orig']:<15} {m['dest']:<18} {pct}")


def check_boot_state(name):
    root_dev = "Unknown"
    is_on_raid = False

    try:
        root_dev_raw = subprocess.check_output(
            ["findmnt", "-n", "-o", "SOURCE", "/"], text=True
        ).strip()
        root_dev = root_dev_raw.split('[')[0]

        if "/dev/mapper/" in root_dev and name in root_dev:
            is_on_raid = True

    except Exception as e:
        root_dev = f"Error: {str(e)}"

    return is_on_raid, root_dev


def show_status(name):
    print(f"🔍 Checking LAS Migration Status: {name}")
    print("-" * 40)

    is_on_raid, root_dev = check_boot_state(name)

    state_str = "RUNNING ON RAID MIRROR" if is_on_raid else "RUNNING ON ORIGIN"
    print(f"[*] Boot State:    {state_str}")
    print(f"[*] Root Device:   {root_dev}")

    rec = database.get_migration(name)
    if not rec:
        print(f"[*] No migration record found for: {name}")
        print("-" * 40)
        print("👉 STATUS: No active migration found.")
        print("   Run 'las prepare-root' to begin a new migration.")
        return

    backend = StorageBackend.from_record(rec)
    mtype = rec.get('migration_type', backend.backend_type)
    print(f"[*] Migration Type: {mtype}")

    raw, pct, is_synced = backend.check_sync(name, rec)

    if mtype == 'lvm':
        print(f"[*] LV Sync Status:")
        for line in raw.split('\n'):
            if line.strip():
                print(f"    {line.strip()}")
        if is_synced:
            print(f"[SUCCESS] Status:   ALL LVs FULLY SYNCED")
        else:
            print(f"[!] Status:        SYNCING")
    else:
        if os.path.exists(f"/dev/mapper/{name}"):
            try:
                dm_status = subprocess.check_output(
                    ["sudo", "dmsetup", "status", name], text=True
                ).strip()
                parts = dm_status.split()

                health = next(
                    (p for p in parts if len(p) == 2 and p.strip('AaR ') == ''),
                    "??"
                )
                sync_info = next(
                    (p for p in parts if '/' in p), None
                )

                if sync_info:
                    curr, total = map(int, sync_info.split('/'))
                    percent = (curr / total) * 100 if total > 0 else 0

                    print(f"[*] RAID Health:   {health} (A=Alive, a=Syncing)")
                    print(f"[*] Sync Progress: {percent:.2f}% "
                          f"({curr} / {total} sectors)")

                    if "idle" in parts or health == "AA":
                        print("[SUCCESS] Status:   FULLY SYNCED")
                    else:
                        print("[!] Status:        REBUILDING/SYNCING")
            except Exception as e:
                print(f"[!] Error parsing RAID: {e}")
        else:
            print(f"[*] RAID Device:   {name} is NOT yet active.")

    print("-" * 40)
    if is_on_raid:
        if is_synced:
            print("👉 SUCCESS: Migration synced! Run 'las break' to finalize.")
        else:
            print("👉 STATUS: Mirroring in progress. Stay booted into this entry.")
    elif mtype == 'lvm':
        if is_synced:
            print("👉 SUCCESS: All LVs synced! Run 'las break' to finalize.")
        else:
            print("👉 STATUS: LVM RAID1 sync in progress.")
    else:
        raid_exists = os.path.exists(f"/dev/mapper/{name}")
        if raid_exists:
            print("   dm-raid active.")
        else:
            boom_check = subprocess.run(
                ['sudo', 'boom', 'entry', 'list'],
                capture_output=True, text=True
            )
            has_boom_entry = (
                boom_check.returncode == 0
                and f'/dev/mapper/{name}' in boom_check.stdout
            )

            if rec and has_boom_entry:
                print("👉 STATUS: Migration prepared but not yet active.")
                print(f"   Run 'sudo grub2-reboot \"LAS-{name}\" "
                      f"&& sudo reboot' to activate.")
            else:
                print("👉 STATUS: No active migration found in the kernel.")
                print("   Run 'las prepare-root' to begin a new migration.")


def main():
    parser = argparse.ArgumentParser(
        description="las: Lift and Shift - Block Device Migration Tool"
    )
    subparsers = parser.add_subparsers(dest='command', help='Migration commands')

    def add_common_args(p):
        p.add_argument('--name', default='migration',
                       help='Unique name for the migration')
        p.add_argument('--orig', required=True,
                       help='Source partition or LVM PV')
        p.add_argument('--dest', required=True,
                       help='Destination partition or LVM PV')
        p.add_argument('--meta-orig',
                       help='Source metadata partition '
                            '(required for partition-based, not used for LVM)')
        p.add_argument('--meta-dest',
                       help='Destination metadata partition '
                            '(required for partition-based, not used for LVM)')

    # --- 1. Command: activate ---
    act = subparsers.add_parser('activate', help='Adopt LUNs into a live mirror')
    add_common_args(act)
    act.add_argument('--hook', help='Path to quiesce script')
    act.add_argument('--throttle', default=None, type=int,
                     help='KiB/s speed limit')

    # --- 2. Command: prepare-root ---
    proot = subparsers.add_parser('prepare-root',
                                  help='Stage a root migration via Boom')
    proot.add_argument('--fix-boot', action='store_true',
                       help='Automatically rebuild Initramfs with RAID drivers '
                            'if verification fails')
    proot.add_argument('--throttle', default=None, type=int,
                       help='KiB/s speed limit')
    add_common_args(proot)

    # --- 3. Command: sync ---
    syn = subparsers.add_parser('sync', help='Start or update sync throttle')
    syn.add_argument('--name', default='migration')
    syn.add_argument('--throttle', default=None, type=int,
                     help='KiB/s speed limit')

    # --- 4. Command: status ---
    stat = subparsers.add_parser('status', help='Check sync progress')
    stat.add_argument('--name', default='migration')
    stat.add_argument('--wait', action='store_true',
                      help='Monitor in real-time')

    # --- 5. Command: list ---
    subparsers.add_parser('list', help='List migrations in database')

    # --- 6. Command: check ---
    chk = subparsers.add_parser('check',
                                help='Validate migration readiness '
                                     'without making changes')
    add_common_args(chk)

    # --- 7. Command: break ---
    brk = subparsers.add_parser('break', help='Finalize and remove mirror')
    brk.add_argument('--name', default='migration')
    brk.add_argument('--commit', action='store_true',
                     help='Regenerate UUID on origin disk to prevent conflicts')

    # --- 8. Command: revert ---
    rvt = subparsers.add_parser('revert',
                                help='Revert to origin and cleanup '
                                     'migration metadata')
    rvt.add_argument('--name', required=True,
                     help='Name of the migration to revert')

    # --- 9. Command: dump-metadata ---
    dump = subparsers.add_parser('dump-metadata',
                                 help='Display RAID metadata for debugging')
    dump.add_argument('device', help='Metadata device path (e.g., /dev/sdb)')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    name = getattr(args, 'name', 'migration')

    # --- COMMAND LOGIC ---

    if args.command == 'list':
        list_migrations()

    elif args.command == 'status':
        show_status(args.name)

    elif args.command == 'dump-metadata':
        if not raid.dump_raid_metadata(args.device):
            print(f"[!] Failed to read metadata from {args.device}")
            sys.exit(1)

    elif args.command == 'check':
        if utils.check_migration_readiness(
            args.orig, args.dest, args.meta_orig, args.meta_dest
        ):
            print("[SUCCESS] All pre-flight checks passed "
                  "- ready to proceed with migration")
            sys.exit(0)
        else:
            print("[FAILED] Pre-flight checks failed "
                  "- resolve issues before proceeding")
            sys.exit(1)

    elif args.command == 'prepare-root':
        backend = StorageBackend.detect(args.orig)
        print(f"[*] Detected {backend.backend_type} storage on {args.orig}")

        if backend.backend_type == 'lvm':
            if args.meta_orig or args.meta_dest:
                print("[*] Note: --meta-orig and --meta-dest "
                      "ignored for LVM migrations")
            backend.prepare(name, args.orig, args.dest)
        else:
            if not args.meta_orig or not args.meta_dest:
                print("[!] ERROR: Partition-based migrations require "
                      "--meta-orig and --meta-dest")
                print("[!] These devices store RAID metadata "
                      "(minimum 1MB each)")
                sys.exit(1)
            backend.prepare(
                name, args.orig, args.dest,
                meta_orig=args.meta_orig, meta_dest=args.meta_dest
            )

    elif args.command == 'activate':
        engine = RAIDEngine(name)
        if engine.activate_passive(
            args.orig, args.dest, args.meta_orig, args.meta_dest
        ):
            mnt = engine.remount_to_mapper(args.orig, args.hook)
            database.record_migration(
                args.name, args.orig, args.dest,
                args.meta_orig, args.meta_dest, None
            )
            print(f"[SUCCESS] Activated. Mounted at: {mnt if mnt else 'N/A'}")

    elif args.command == 'sync':
        rec = database.get_migration(args.name)
        if rec:
            engine = RAIDEngine(name)
            success, actual_throttle = engine.start_sync(
                args.name, args.throttle
            )
            if success and actual_throttle:
                database.update_throttle(args.name, actual_throttle)
                print(f"[SUCCESS] Sync speed set to "
                      f"{actual_throttle} KiB/s")

    elif args.command == 'break':
        rec = database.get_migration(args.name)
        if not rec:
            print("[!] No record found.")
            sys.exit(1)

        backend = StorageBackend.from_record(rec)
        backend.break_mirror(args.name, rec, commit=args.commit)

    elif args.command == 'revert':
        rec = database.get_migration(args.name)
        if not rec:
            print(f"[!] No migration record found for: {args.name}")
            sys.exit(1)

        backend = StorageBackend.from_record(rec)
        backend.revert(args.name, rec)


if __name__ == "__main__":
    main()
