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

def update_sync_throttle(name, throttle_kibs):
    """
    Updates the RAID throttle on a live mapper device by staging 
    the table before resuming.
    """
    try:
        # 1. Get current table to ensure we preserve device majors/minors
        current_table = subprocess.check_output(['sudo', 'dmsetup', 'table', name], text=True).strip()
        
        # 2. Prepare the new table string
        # We preserve everything but swap the rate numbers
        import re
        new_max = throttle_kibs * 2
        new_table = re.sub(r'min_recovery_rate \d+', f'min_recovery_rate {throttle_kibs}', current_table)
        new_table = re.sub(r'max_recovery_rate \d+', f'max_recovery_rate {new_max}', new_table)

        print(f"[*] Staging new throttle: {throttle_kibs} KiB/s...")

        # 3. THE FIX: Load into the INACTIVE slot first
        # This avoids the 'Invalid Argument' error on a live 'reload'
        load_proc = subprocess.Popen(['sudo', 'dmsetup', 'load', name], stdin=subprocess.PIPE)
        load_proc.communicate(input=new_table.encode())

        # 4. Suspend/Resume to flip the slots
        # Suspend flushes I/O so the superblock can be updated
        subprocess.run(['sudo', 'dmsetup', 'suspend', name], check=True)
        subprocess.run(['sudo', 'dmsetup', 'resume', name], check=True)

        print("[SUCCESS] Sync speed updated.")
    except Exception as e:
        print(f"[!] Sync update failed. Attempting emergency resume...")
        subprocess.run(['sudo', 'dmsetup', 'resume', name], stderr=subprocess.DEVNULL)


def sync_partition_table(src, dest):
    print(f"[*] Syncing partition table from {src} to {dest}...")
    try:
        # Dump from source and pipe to destination
        dump = subprocess.check_output(['sfdisk', '-d', src])
        process = subprocess.Popen(['sfdisk', dest], stdin=subprocess.PIPE)
        process.communicate(input=dump)
        
        # Move backup GPT headers to the end of the disk
        subprocess.run(['sgdisk', '-e', dest], check=True)
        # Force kernel to re-scan
        subprocess.run(['partprobe', dest], check=True)
    except Exception as e:
        print(f"[!] Failed to sync geometry: {e}")
        return False
    subprocess.run(['udevadm', 'settle'], check=False) # Wait for /dev/sdd3 to appear
    return True

def revert_migration(name):
    """
    Cleans up LAS metadata and boot entries and ensures 
    the original partitions are remounted correctly.
    """
    record = database.get_migration(name)
    if not record:
        print(f"[!] No migration record found for: {name}")
        return False

    # 1. Safety Check: Ensure we aren't currently running ON the RAID
    root_dev = subprocess.check_output(["findmnt", "-n", "-o", "SOURCE", "/"], text=True).strip()
    if name in root_dev:
        print(f"[!] ERROR: You are still booted into the RAID ({root_dev}).")
        return False

    print(f"[*] Starting revert for migration: {name}")

    # 3. Cleanup: Boom Profile and Initramfs
    try:
        subprocess.run(["sudo", "boom", "profile", "delete", "--title", f"LAS: {name}"], check=True, capture_output=True)
        print("[OK] Removed Boom entry.")
    except:
        print("[!] Note: Boom profile not found.")

    img_path = f"/boot/initramfs-las-{name}.img"
    if os.path.exists(img_path):
        os.remove(img_path)
        print("[OK] Deleted custom Initramfs.")

    # 4. Wipe Metadata and Destination Disks
    subprocess.run(["sudo", "udevadm", "settle"], check=False)
    target_disks = [record['dest'], record['meta_orig'], record['meta_dest']]
    for disk in target_disks:
        if os.path.exists(disk):
            print(f"[*] Wiping signatures on {disk}...")
            try:
                subprocess.run(["sudo", "wipefs", "-a", "-f", disk], check=True)
            except:
                subprocess.run(["sudo", "dd", "if=/dev/zero", f"of={disk}", "bs=1M", "count=1", "oflag=direct"], check=True)


    # 6. Database Cleanup
    database.delete_migration(name)
    
    print(f"\n[SUCCESS] Revert complete. System is stable on {root_dev}.")
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

def check_boot_state(name):
    import subprocess
    import re
    
    root_dev = "Unknown"
    is_on_raid = False
    
    try:
        # Get the source for /
        root_dev_raw = subprocess.check_output(["findmnt", "-n", "-o", "SOURCE", "/"], text=True).strip()
        
        # Strip Btrfs subvolume notation: /dev/sda3[/root] -> /dev/sda3
        root_dev = re.sub(r'[\[/].*$', '', root_dev_raw)
        
        # Check for mapper and name match
        if "/dev/mapper/" in root_dev and name in root_dev:
            is_on_raid = True
            
    except Exception as e:
        root_dev = f"Error: {str(e)}"
        
    return is_on_raid, root_dev

def show_status(name):
    import subprocess
    import os

    print(f"🔍 Checking LAS Migration Status: {name}")
    print("-" * 40)

    # 1. Use the dedicated check_boot_state
    is_on_raid, root_dev = check_boot_state(name)
    
    state_str = "RUNNING ON RAID MIRROR" if is_on_raid else "RUNNING ON ORIGIN"
    print(f"[*] Boot State:    {state_str}")
    print(f"[*] Root Device:   {root_dev}")

    # 2. Check RAID Sync (Parent Device)
    is_synced = False
    if os.path.exists(f"/dev/mapper/{name}"):
        try:
            dm_status = subprocess.check_output(["sudo", "dmsetup", "status", name], text=True).strip()
            parts = dm_status.split()
            
            # Health characters (e.g., 'AA', 'Aa') 
            health = next((p for p in parts if len(p) == 2 and p.strip('AaR ') == ''), "??")
            
            # Sync info (e.g., 31613440/83886080)
            sync_info = next((p for p in parts if '/' in p), None)
            
            if sync_info:
                curr, total = map(int, sync_info.split('/'))
                percent = (curr / total) * 100 if total > 0 else 0
                
                print(f"[*] RAID Health:   {health} (A=Alive, a=Syncing)")
                print(f"[*] Sync Progress: {percent:.2f}% ({curr} / {total} sectors)")
                
                # 'idle' or 'AA' means the heavy lifting is done
                if "idle" in parts or health == "AA":
                    print("[SUCCESS] Status:   FULLY SYNCED")
                    is_synced = True
                else:
                    print("[!] Status:        REBUILDING/SYNCING")
        except Exception as e:
            print(f"[!] Error parsing RAID: {e}")
    else:
        print(f"[*] RAID Device:   {name} is NOT yet active.")

    print("-" * 40)
    if is_on_raid:
        if 'is_synced' in locals() and is_synced:
            print("👉 SUCCESS: Migration synced! Run 'las break' to finalize.")
        else:
            print("👉 STATUS: Mirroring in progress. Stay booted into this entry.")
    else:
        # Check if the RAID device actually exists even if we aren't booted to it
        raid_exists = os.path.exists(f"/dev/mapper/{name}")
        
        if raid_exists:
            print("   dm-raid active.")
        else:
            print("👉 STATUS: No active migration found in the kernel.")
            print("   Run 'las prepare' to begin a new migration.")


def prepare_root(engine, name, origin, dest, meta_orig, meta_dest):
    """
    Main workflow for the Lift and Shift (LAS) root migration.
    Updated to work with dynamic Boom entry mapping.
    """
    print(f"[*] Starting Lift and Shift (LAS) preparation for: {name}")

    if not utils.validate_migration_geometry(origin, dest, meta_orig, meta_dest):
        sys.exit(1)

    # 1. Get exact size of the source disk for metadata priming
    try:
        origin_sz = int(subprocess.check_output(['blockdev', '--getsz', origin], text=True).strip())
    except Exception as e:
        print(f"[!] Could not determine size of {origin}: {e}")
        return False

    if not sync_partition_table(origin, dest):
        print("[!] Geometry sync failed. Cannot proceed.")
        return False

    # 2. Parse partition table for dynamic hook generation
    partitions = utils.parse_partition_table(origin)
    if not partitions:
        print("[!] Failed to parse partition table. Cannot proceed.")
        return False

    # Validate we have at least 2 partitions (typically boot + root minimum)
    if len(partitions) < 2:
        print(f"[!] Expected at least 2 partitions, found {len(partitions)}")
        return False

    print(f"[*] Detected {len(partitions)} partitions:")
    for part in partitions:
        # Display partition info with size in GB for readability
        size_gb = (part['size'] * 512) / (1024**3)  # sectors to GB
        print(f"    Partition {part['num']}: start={part['start']}, size={part['size']} sectors ({size_gb:.2f} GB)")

    # 3. Prime Source Metadata (Leg 0)
    raid.wipe_metadata(meta_orig)
    if not raid.write_dm_raid_superblock(meta_orig, origin_sz):
        print("[!] Failed to prime source metadata.")
        return False

    # 3. Resolve Persistent Paths (by-id)
    p_orig = utils.get_persistent_path(origin)
    p_dest = utils.get_persistent_path(dest)
    p_m_orig = utils.get_persistent_path(meta_orig)
    p_m_dest = utils.get_persistent_path(meta_dest)

    # 4. Inject Assembly Hook & Create Initramfs
    img_path = utils.inject_las_assembly_hook(name, p_orig, p_dest, p_m_orig, p_m_dest, partitions)
    if not img_path:
        return False

    # 5. Dynamic Filesystem Discovery for Root (/)
    # We still need current_fstype/flags for the database and the root mount.
    try:
        # findmnt -n (no headings) -o (output columns)
        cmd = ["findmnt", "-n", "-o", "FSTYPE,OPTIONS", "/"]
        fs_info = subprocess.check_output(cmd, text=True).strip().split()
        current_fstype = fs_info[0]
        current_fsflags = fs_info[1]
        
        print(f"[*] Detected {current_fstype} for migration.")
        print(f"[*] Using root mount flags: {current_fsflags}")
    except Exception as e:
        print(f"[!] Could not detect live filesystem info: {e}")
        return False

    # 6. DATABASE UPDATE
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

    # 7. Register Boot Entry with the Engine
    # Note: part_idx is no longer passed; setup_boom_entry will detect all mounts.
    if not engine.setup_boom_entry(img_path, current_fstype, current_fsflags):
        print("[!] Failed to register Boom boot entry.")
        return False

    print(f"\n[SUCCESS] Lift and Shift prepared for '{name}'.")
    # Updated the suggestion to match the Boom Title we set in the function
    print(f"[ACTION] Run: grub2-reboot 'LAS-{name}' && reboot")
    return True

def main():
    parser = argparse.ArgumentParser(
        description="las: Lift and Shift - Block Device Migration Tool"
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

    # --- 7. Command: dump-metadata ---
    dump = subparsers.add_parser('dump-metadata', help='Display RAID metadata for debugging')
    dump.add_argument('device', help='Metadata device path (e.g., /dev/sdb)')

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

    elif args.command == 'dump-metadata':
        # Display RAID metadata from specified device
        if not raid.dump_raid_metadata(args.device):
            print(f"[!] Failed to read metadata from {args.device}")
            sys.exit(1)

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
        if rec and engine.start_sync(args.name, args.throttle):
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
