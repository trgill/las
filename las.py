#!/usr/bin/env python3
#
# Copyright Red Hat
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
"""
las main.
"""
#!/usr/bin/env python3
import os
import argparse
import sys
import subprocess
import time

# Project modules
import raid
import utils
import database
from dm import RAIDEngine


def prepare_root(engine, name, origin, dest, meta_orig, meta_dest):
    """
    Main workflow for the Lift and Shift (LAS) root migration.
    """
    print(f"[*] Starting Lift and Shift (LAS) preparation for: {name}")

    # 1. Get exact size of the source disk for metadata priming
    try:
        origin_sz = int(subprocess.check_output(['blockdev', '--getsz', origin], text=True).strip())
    except Exception as e:
        print(f"[!] Could not determine size of {origin}: {e}")
        return False

    # 2. Prime Source Metadata (Leg 0)
    # Writes the binary identity to the metadata disk so the boot hook recognizes it.
    raid.wipe_metadata(meta_orig)
    if not raid.write_dm_raid_superblock(meta_orig, origin_sz):
        print("[!] Failed to prime source metadata.")
        return False

    # 3. Resolve Persistent Paths (by-id)
    # Used in the Dracut hook to ensure we find the right disks after reboot.
    p_orig = utils.get_persistent_path(origin)
    p_dest = utils.get_persistent_path(dest)
    p_m_orig = utils.get_persistent_path(meta_orig)
    p_m_dest = utils.get_persistent_path(meta_dest)

    # 4. Inject Assembly Hook & Create Initramfs
    # Generates the self-assembling /boot/initramfs-las-{name}.img
    img_path = utils.inject_las_assembly_hook(name, p_orig, p_dest, p_m_orig, p_m_dest)
    if not img_path:
        return False

    # 5. Dynamic Filesystem Discovery
    # We pull the FSTYPE and OPTIONS directly from the live '/' mount.
    # This prevents hard-coding for Btrfs, XFS, or Ext4.
    try:
        # findmnt -n (no headings) -o (output columns)
        cmd = ["findmnt", "-n", "-o", "FSTYPE,OPTIONS", "/"]
        fs_info = subprocess.check_output(cmd, text=True).strip().split()
        current_fstype = fs_info[0]
        current_fsflags = fs_info[1]
        
        # Get the partition index (e.g., '3' if root is on /dev/sda3)
        _, part_idx = utils.get_root_partition_info()
        
        print(f"[*] Detected {current_fstype} on partition {part_idx}")
        print(f"[*] Using mount flags: {current_fsflags}")
    except Exception as e:
        print(f"[!] Could not detect live filesystem info: {e}")
        return False

    # 6. Register Boot Entry with the Engine
    # Passing the img_path, fstype, fsflags, and part_index as requested.
    if not engine.setup_boom_entry(img_path, current_fstype, current_fsflags, part_idx):
        print("[!] Failed to register Boom boot entry.")
        return False

    print(f"\n[SUCCESS] Lift and Shift prepared for '{name}'.")
    print(f"[ACTION] Run: grub2-reboot 'LAS: {name}' && reboot")
    return True

def main():
    parser = argparse.ArgumentParser(
        description="LAS: Lift and Shift (Logical Adoption System) - Block Migration Tool"
    )
    subparsers = parser.add_subparsers(dest='command', help='Migration commands')

    # --- Shared Arguments Helper ---
    def add_common_args(p):
        p.add_argument('--name', default='migration', help='Unique name for the migration')
        p.add_argument('--orig', required=True, help='Source partition')
        p.add_argument('--dest', required=True, help='Destination partition')
        p.add_argument('--meta-orig', required=True, help='Source metadata partition')
        p.add_argument('--meta-dest', required=True, help='Destination metadata partition')

    # --- 1. Command: activate ---
    act = subparsers.add_parser('activate', help='Adopt LUNs into a live mirror')
    add_common_args(act)
    act.add_argument('--hook', help='Path to quiesce script')
    act.add_argument('--throttle', default=None, type=int, help='KiB/s speed limit')


    # --- 2. Command: prepare-root ---
    proot = subparsers.add_parser('prepare-root', help='Stage a root migration via Boom')
    proot.add_argument(
        '--fix-boot', 
        action='store_true', 
        help='Automatically rebuild Initramfs with RAID drivers if verification fails'
        )
    proot.add_argument('--throttle', default=None, type=int, help='KiB/s speed limit')
    add_common_args(proot)

    # --- 3. Command: sync ---
    syn = subparsers.add_parser('sync', help='Start or update sync throttle')
    syn.add_argument('--name', default='migration')
    syn.add_argument('--throttle', default=None, type=int, help='KiB/s speed limit')

    # --- 4. Command: status ---
    stat = subparsers.add_parser('status', help='Check sync progress')
    stat.add_argument('--name', default='migration')
    stat.add_argument('--wait', action='store_true', help='Monitor in real-time')

    # --- 5. Command: list ---
    subparsers.add_parser('list', help='List migrations in database')

    # --- 6. Command: break ---
    brk = subparsers.add_parser('break', help='Finalize and remove mirror')
    brk.add_argument('--name', default='migration')

    # Parse arguments after ALL subparsers are added
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    # Initialize Engine
    name = getattr(args, 'name', 'migration')
    engine = RAIDEngine(name)

    # --- COMMAND LOGIC ---

    if args.command == 'list':
        migrations = database.list_all_migrations()
        if not migrations:
            print("[*] No migrations found in database.")
        else:
            print(f"{'Name':<15} {'Source':<15} {'Dest':<18} {'Progress'}")
            print("-" * 60)
            for m in migrations:
                temp_engine = RAIDEngine(m['name'])
                _, pct = temp_engine.get_status()
                print(f"{m['name']:<15} {m['orig']:<15} {m['dest']:<18} {pct}")

    elif args.command == 'status':
        try:
            while True:
                raw, pct = engine.get_status()
                print(f"[{name}] Progress: {pct} | Kernel info: {raw}")
                if not args.wait or "100.00%" in pct:
                    break
                time.sleep(5)
        except KeyboardInterrupt:
            print("\n[*] Monitoring stopped.")

    # --- LOGIC: prepare-root ---
    # This command prepares a system for a "Pivot-on-Reboot" migration.
    elif args.command == 'prepare-root':
        prepare_root(engine, name, args.orig, args.dest, args.meta_orig, args.meta_dest)

    elif args.command == 'activate':
        if engine.activate_passive(args.orig, args.dest, args.meta_orig, args.meta_dest):
            mnt = engine.remount_to_mapper(args.orig, args.hook)
            database.record_migration(args.name, args.orig, args.dest, args.meta_orig, args.meta_dest, None)
            print(f"[SUCCESS] Activated. Mounted at: {mnt if mnt else 'N/A'}")

    elif args.command == 'sync':
        rec = database.get_migration(args.name)
        if rec and engine.start_sync(rec['orig'], rec['dest'], rec['meta_orig'], rec['meta_dest'], args.throttle):
            database.update_throttle(args.name, args.throttle)
            print(f"[SUCCESS] Sync speed set to {args.throttle or 'default'} KiB/s")

    elif args.command == 'break':
        rec = database.get_migration(args.name)
        if not rec:
            print("[!] No record found."); sys.exit(1)
        
        _, pct = engine.get_status()
        if "100.00%" not in pct:
            if input(f"[!] Sync incomplete ({pct}). Finalize anyway? (y/N): ").lower() != 'y': sys.exit(0)

        engine.cleanup_boom_entry()
        engine.stop()
        database.delete_migration(args.name)
        print("[SUCCESS] Finalized.")

if __name__ == "__main__":
    main()