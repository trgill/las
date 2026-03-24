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

import subprocess
import database
import os

def revert_migration(name):
    """
    Cleans up LAS metadata and boot entries. 
    User must be booted into the original /dev/sda (non-DM) to run this safely.
    """
    import database
    import subprocess
    import os

    # 1. Safety Check: Are we currently running on the RAID?
    # We check if the root mount is a DM device named after the migration
    try:
        root_dev = subprocess.check_output(["findmnt", "-n", "-o", "SOURCE", "/"], text=True).strip()
        if name in root_dev:
            print(f"[!] ERROR: You are currently booted into the RAID device ({root_dev}).")
            print("[!] Please reboot and select your ORIGINAL boot entry before reverting.")
            return False
    except Exception as e:
        print(f"[!] Warning: Could not verify current root device: {e}")

    # 2. Fetch the record
    record = database.get_migration(name)
    if not record:
        print(f"[!] No migration record found for: {name}")
        return False

    print(f"[*] Reverting migration '{name}'...")

    # 3. Remove Boom Boot Profile
    try:
        title = f"LAS: {name}"
        print(f"[*] Removing Boom entry: {title}")
        subprocess.run(["sudo", "boom", "profile", "delete", "--title", title], check=True, capture_output=True)
    except subprocess.CalledProcessError:
        print("[!] Note: Boom profile already removed or not found.")

    # 4. Delete the custom Initramfs
    img_path = f"/boot/initramfs-las-{name}.img"
    if os.path.exists(img_path):
        print(f"[*] Removing Initramfs: {img_path}")
        os.remove(img_path)

    # 5. Wipe Metadata and Destination Disks
    # We leave 'orig' alone! We only wipe the secondary 'dest' and the meta disks.
    target_disks = [record['dest'], record['meta_orig'], record['meta_dest']]
    for disk in target_disks:
        if os.path.exists(disk):
            print(f"[*] Wiping signatures on {disk}...")
            # This clears RAID superblocks so the disk looks 'empty' again
            subprocess.run(["sudo", "wipefs", "-a", disk], check=True)

    # 6. Final Database Cleanup
    database.delete_migration(name)
    
    print(f"\n[SUCCESS] Revert complete. System is back to its original state.")
    return True

def list_migrations():
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

def show_status(name):
    """
    Displays the live status of a migration, including sync progress
    and current boot state.
    """
    print(f"🔍 Checking LAS Migration Status: {name}")
    print("-" * 40)

    # 1. Check Database Record
    record = database.get_migration(name)
    if not record:
        print(f"[!] No record found in database for '{name}'.")
        return

    # 2. Check Current Boot State
    try:
        root_dev = subprocess.check_output(["findmnt", "-n", "-o", "SOURCE", "/"], text=True).strip()
        is_on_raid = name in root_dev
        print(f"[*] Boot State: {'RUNNING ON RAID' if is_on_raid else 'RUNNING ON ORIGIN'}")
        print(f"[*] Root Device: {root_dev}")
    except:
        print("[!] Could not determine current boot state.")

# 3. Check RAID Sync Progress (from the kernel)
    try:
        dm_status = subprocess.check_output(["sudo", "dmsetup", "status", name], text=True).strip()
        parts = dm_status.split()
        
        # Find the part that looks like '83886080/83886080'
        sync_info = next((p for p in parts if '/' in p), None)
        
        if sync_info:
            curr, total = map(int, sync_info.split('/'))
            percent = (curr / total) * 100
            
            # Health is usually at index 5 (e.g., 'AA')
            health = parts[5] if len(parts) > 5 else "unknown"
            
            print(f"[*] RAID Health: {health} (A=Alive, D=Dead/Down)")
            print(f"[*] Sync Progress: {percent:.2f}% ({curr} / {total} sectors)")
            
            # Check if we are idle vs rebuilding
            if "idle" in parts:
                print("[SUCCESS] Status: FULLY SYNCED (Idle)")
            else:
                print("[!] Status: REBUILDING/SYNCING")
    except StopIteration:
        print("[!] Could not find sync progress in dmsetup output.")
    except Exception as e:
        print(f"[!] Error parsing RAID status: {e}")

    # 4. Actionable Advice
    print("-" * 40)
    if is_on_raid:
        if "percent" in locals() and percent >= 100:
            print("👉 Recommendation: Run 'las break' to finalize migration.")
            print("   Or reboot to origin if you want to revert migration.")
        else:
            print("👉 Recommendation: Wait for sync to hit 100% or adjust throttle.")
    else:
        print("👉 Recommendation: Reboot and select the 'LAS' entry to start migration.")
        print("   Or run 'las revert' to clean up if you've changed your mind.")


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

    # 6. DATABASE UPDATE
    # We record the state before rebooting so the 'sync' command 
    # knows which devices belong to this migration.
    print("[*] Updating migration database...")
    database.record_migration(
        name=name,
        orig=p_orig,
        dest=p_dest,
        meta_orig=p_m_orig,
        meta_dest=p_m_dest,
        throttle=0,
        fstype=current_fstype,
        fsflags=current_fsflags
    )
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

    rvt = subparsers.add_parser('revert', help='Revert to origin and cleanup migration metadata')
    rvt.add_argument('--name', required=True, help='Name of the migration to revert')

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
        list_migrations()

    elif args.command == 'status':
        show_status(args.name)

    elif args.command == 'revert':
        revert_migration(args.name)
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