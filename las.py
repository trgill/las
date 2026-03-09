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
import argparse, os, sys, subprocess, dm, database

def main():
    if os.geteuid() != 0:
        print("Error: Root privileges required.")
        sys.exit(1)

    database.init_db()
    parser = argparse.ArgumentParser(description="LAS: LUN Migration Utility")
    subparsers = parser.add_subparsers(dest="command")

    # Command: wipe (Now handles its own logic)
    wp = subparsers.add_parser("wipe", help="Deep wipe metadata partitions")
    wp.add_argument("--devs", nargs="+", required=True, help="Partitions to wipe")

    # Command: activate
    act = subparsers.add_parser("activate")
    act.add_argument("--hook", help="Path to quiesce script (receives suspend/resume)")
    act.add_argument("--orig", required=True)
    act.add_argument("--dest", required=True)
    act.add_argument("--meta_orig", required=True)
    act.add_argument("--meta_dest", required=True)
    act.add_argument("--name", default="las_migration")

    # Standard commands
    syn = subparsers.add_parser("sync")
    syn.add_argument("--name", default="las_migration")
    syn.add_argument(
        "--throttle", type=int, help="Sync speed in KiB/s (e.g., 10000 for 10MB/s)"
    )
    subparsers.add_parser("status").add_argument("--name", default="las_migration")
    subparsers.add_parser("break").add_argument("--name", default="las_migration")
    subparsers.add_parser("list")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)

    # --- COMMAND LOGIC ---

    if args.command == "wipe":
        print(f"[*] WARNING: This will permanently erase signatures on {args.devs}")
        if input("Proceed? (y/N): ").lower() == "y":
            for d in args.devs:
                print(f"[*] Wiping {d}...")
                subprocess.run(["wipefs", "-a", d], check=True)
                # Zeroing the start of the disk to clear RAID superblocks
                subprocess.run(
                    ["dd", "if=/dev/zero", f"of={d}", "bs=1M", "count=10"],
                    capture_output=True,
                )
            print("[SUCCESS] Metadata partitions are clean.")
        return  # Exit early after wipe
    elif args.command == "list":
        with database.sqlite3.connect(database.DB_PATH) as conn:
            rows = conn.execute("SELECT * FROM migrations").fetchall()
            for r in rows:
                print(r)
        return  # Exit early after wipe

    # Initialize engine ONLY for commands that use it
    engine = dm.RAIDEngine(args.name)

    if args.command == "activate":
        # Pass the hook script to the remount logic
        new_mnt = engine.remount_to_mapper(args.orig, args.name, hook_script=args.hook)

        if new_mnt:
            database.record_migration(
                args.name,
                args.orig,
                args.dest,
                args.meta_orig,
                args.meta_dest,
                None,
                active=False,
            )
            print(f"[SUCCESS] {args.name} is fully adopted and live.")
        else:
            print("[!] Handover failed. Cleaning up Device Mapper...")
            engine.stop()  # Remove the DM device since we rolled back to physical
            sys.exit(1)

        # 1. Baseline Magic Check
        is_xfs = engine.verify_xfs_magic(args.orig)

        # 2. Size Validation
        if not engine.validate_sizes(args.orig, args.dest):
            sys.exit(1)

        # 3. MANUAL HEADER CLONE
        if not engine.clone_header(args.orig, args.dest):
            print("[!] CRITICAL: Failed to clone disk label.")
            sys.exit(1)

        # 4. UPDATE TARGET UUID (The Fix)
        # if is_xfs:
        #     if not engine.update_xfs_uuid(args.dest):
        #         print("[!] WARNING: Could not update XFS UUID. Collisions may occur.")

        # 5. DM Activation
        if engine.activate_passive(
            args.orig, args.dest, args.meta_orig, args.meta_dest
        ):
            database.record_migration(
                args.name,
                args.orig,
                args.dest,
                args.meta_orig,
                args.meta_dest,
                active=False,
            )
            print(f"[SUCCESS] Migration {args.name} activated with unique UUID.")

    elif args.command == "sync":
        rec = database.get_migration(args.name)
        if rec and engine.start_sync(*rec):
            database.record_migration(args.name, *rec, active=True)
            print("[SUCCESS] Sync started.")

    elif args.command == "status":
        raw, pct = engine.get_status()
        if raw:
            print(f"Sync Progress: {pct}\nKernel Status: {raw}")

    elif args.command == "break":
        rec = database.get_migration(args.name)
        if not rec:
            print("Error: Record not found.")
            sys.exit(1)

        if not engine.verify_integrity(rec[0], rec[1]):
            print("[!] INTEGRITY MISMATCH DETECTED.")
            if input("Override and break mirror? (y/N): ").lower() != "y":
                sys.exit(0)

        if engine.stop():
            database.delete_migration(args.name)
            print(f"[SUCCESS] Migration complete. Database cleared.")


if __name__ == "__main__":
    main()
