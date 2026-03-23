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
import time

# Project modules
import utils
import database
from dm import RAIDEngine


def prepare_root(engine, name, origin, dest, meta_orig, meta_dest, throttle=None):
    """
    Main workflow to prepare the system for migration.
    """
    import os
    import utils
    import database

    # 1. Initialize RAID Metadata (The "Missing Leg" Strategy)
    # This prepares the disks and calculates the usable sector count
    if not engine.init_raid_metadata(origin, dest, meta_orig, meta_dest):
        print("[!] RAID metadata initialization failed.")
        return False

    # 2. Resolve ALL persistent paths (by-id)
    # These are used for the RAID table and the 'wait' loop in the hook
    p_orig = utils.get_persistent_path(origin)
    p_dest = utils.get_persistent_path(dest)
    p_m_orig = utils.get_persistent_path(meta_orig)
    p_m_dest = utils.get_persistent_path(meta_dest)

    # 3. Construct the DM-RAID Table String
    # 2 = optional parameter count (1024 and nosync)
    raw_table = (
        f"0 {engine.sectors} raid raid1 2 1024 nosync 2 "
        f"{p_m_orig} {p_orig} {p_m_dest} {p_dest}"
    )

    # 4. Inject Hook and Create the ISOLATED Initramfs
    # We pass the required disks so the hook knows exactly what to wait for
    print(f"[*] Creating isolated migration Initramfs for '{name}'...")
    required_disks = [p_m_orig, p_orig, p_m_dest, p_dest]
    
    custom_img = utils.inject_las_assembly_hook(name, raw_table, required_disks)
    if not custom_img:
        print("[!] Failed to create specialized Initramfs.")
        return False

    # 5. Detect Filesystem Info from the ACTIVE partition
    # This finds if we are on Btrfs/XFS and gets the subvolume flags
    root_partition = utils.get_root_device()
    if not root_partition:
        print("[!] Could not determine active root partition.")
        return False

    print(f"[*] Detecting filesystem on {root_partition}...")
    fstype, fsflags = utils.get_root_filesystem_info(root_partition)
    print(f"[*] FS Info: type='{fstype}', flags='{fsflags}'")

    # 6. CALL SETUP_BOOM_ENTRY
    # This is the call you were looking for. It passes the custom image 
    # and the detected filesystem metadata to the Boom CLI logic.
    if not engine.setup_boom_entry(custom_img, fstype, fsflags):
        print("[!] Failed to create Boom boot entry.")
        # Cleanup the orphaned image if Boom failed to write the config
        if os.path.exists(custom_img):
            os.remove(custom_img)
        return False

    # 7. Record the Migration to the Database
    # This allows 'las list' and 'las break' to function later
    database.record_migration(name, origin, dest, meta_orig, meta_dest, throttle)

    print(f"\n[SUCCESS] Preparation complete for '{name}'.")
    print(f"[INFO] Filesystem: {fstype} ({fsflags})")
    print("[ACTION] Reboot and select the 'LAS-migration' entry from the GRUB menu.")
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