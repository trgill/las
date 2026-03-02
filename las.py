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
    act.add_argument("--orig", required=True)
    act.add_argument("--dest", required=True)
    act.add_argument("--meta_orig", required=True)
    act.add_argument("--meta_dest", required=True)
    act.add_argument("--throttle", type=int)
    act.add_argument("--name", default="las_migration")

    # Standard commands
    subparsers.add_parser("sync").add_argument("--name", default="las_migration")
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

    # Initialize engine ONLY for commands that use it
    engine = dm.RAIDEngine(args.name)

    if args.command == "activate":
        # 1. Baseline XFS check
        if not engine.verify_xfs_magic(args.orig):
            print(
                f"[!] WARNING: Source {args.orig} does not have a valid XFS magic number."
            )
            if input("Continue anyway? (y/N): ").lower() != "y":
                sys.exit(1)

        # 2. Size validation check
        if not engine.validate_sizes(args.orig, args.dest):
            print("[!] CRITICAL: Migration aborted due to size mismatch.")
            sys.exit(1)

        # 3. Proceed to activation
        if engine.activate_passive(
            args.orig, args.dest, args.meta_orig, args.meta_dest
        ):
            database.record_migration(
                args.name,
                args.orig,
                args.dest,
                args.meta_orig,
                args.meta_dest,
                args.throttle,
                active=False,
            )
            print(f"[SUCCESS] Migration {args.name} activated.")

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

    elif args.command == "list":
        with database.sqlite3.connect(database.DB_PATH) as conn:
            rows = conn.execute("SELECT * FROM migrations").fetchall()
            for r in rows:
                print(r)

if __name__ == "__main__":
    main()
