#!/usr/bin/env python3
#
# Copyright Red Hat
#
# snapm/_snapm.py - Snapshot Manager global definitions
#
# This file is part of the snapm project.
#
# SPDX-License-Identifier: Apache-2.0
"""
las main.
"""
#!/usr/bin/env python3
import argparse
import sys
import time

# Project modules
import utils
import database
from dm import RAIDEngine

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
        print(f"[*] Preparing Pivot-Root migration: {args.name}")
        print(f"[*] Source: {args.orig} | Destination: {args.dest}")
        # Inside the prepare-root block...
        supported, msg = utils.verify_initramfs_dm_support()
        
        if not supported:
            print(f"\n[!] {msg}")
            
            # Determine if we should fix automatically or ask
            do_fix = False
            if args.fix_boot:
                print("[*] --fix-boot detected. Proceeding with automatic repair...")
                do_fix = True
            else:
                choice = input("\n[?] Attempt to rebuild Initramfs now? (y/N): ")
                if choice.lower() == 'y':
                    do_fix = True

            if do_fix:
                if utils.rebuild_initramfs():
                    # Re-verify after the fix
                    final_check, _ = utils.verify_initramfs_dm_support()
                    if final_check:
                        print("[SUCCESS] Initramfs is now RAID-capable.")
                    else:
                        print("[!] Rebuild completed but verification still fails. Check dracut logs.")
                else:
                    print("[!] Auto-fix failed. Manual intervention required.")
            
        # 1. Primes the metadata devices with RAID1 superblocks
        print(f"[*] Initializing RAID headers on {args.meta_orig} and {args.meta_dest}...")
        if not engine.init_raid_metadata(args.orig, args.dest, args.meta_orig, args.meta_dest):
            print("[!] CRITICAL: Failed to clone RAID headers. Metadata is uninitialized.")
            print("[!] The kernel will not be able to assemble the RAID at boot. Aborting.")
            sys.exit(1)

        # 2. BOOTLOADER PREP: Create the Boom BLS entry.
        # This adds the 'LAS-migration' option to your GRUB menu.
        # It includes 'rd.driver.pre' to ensure modules load before table parsing.
        if engine.setup_boom_entry(args.orig, args.dest, args.meta_orig, args.meta_dest):
            # Record the intent in our central database in /etc/
            database.record_migration(
                args.name, args.orig, args.dest, 
                args.meta_orig, args.meta_dest, args.throttle
            )
            
            print("\n[+] Boom entry created successfully.")
            
            # 3. DRIVER PREP: Verify the Initramfs actually contains the RAID drivers.
            # Fedora 43 often strips these out if the host isn't currently using RAID.
            supported, msg = utils.verify_initramfs_dm_support()
            
            if not supported:
                print(f"\n[!] BOOT CAPABILITY WARNING:")
                print(f"    {msg}")
                
                # Interactive Auto-Fix
                confirm = input("\n[?] Would you like to rebuild Initramfs with RAID drivers now? (y/N): ")
                if confirm.lower() == 'y':
                    if utils.rebuild_initramfs():
                        # Final verification post-rebuild
                        reverify, _ = utils.verify_initramfs_dm_support()
                        if reverify:
                            print("[SUCCESS] Initramfs is now RAID-capable.")
                        else:
                            print("[!] Rebuild completed but drivers still not detected. Check logs.")
                    else:
                        print("[!] Failed to rebuild Initramfs. Manual intervention required.")
                else:
                    print("[!] WARNING: System may hang on reboot if drivers are missing.")
            else:
                print(f"[*] {msg}")

            print("\n" + "="*60)
            print(" PREPARATION COMPLETE ")
            print("="*60)
            print(" 1. Reboot.")
            print(f" 2. Select 'LAS-{args.name}' from the GRUB menu.")
            print("="*60)

        else:
            print("[!] Error: Could not create Boom boot entry.")
            sys.exit(1)

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
        if engine.stop():
            database.delete_migration(args.name)
            print("[SUCCESS] Finalized.")

if __name__ == "__main__":
    main()