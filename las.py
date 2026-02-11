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
import os
import sys
import dm
import database


def main():
    if os.geteuid() != 0:
        print("Error: Root privileges required.")
        sys.exit(1)

    database.init_db()
    parser = argparse.ArgumentParser(description="LAS: LUN Migration Utility")
    subparsers = parser.add_subparsers(dest="command")

    # Command: activate
    act = subparsers.add_parser("activate", help="Adopt LUNs into a passive mirror")
    act.add_argument("--orig", required=True)
    act.add_argument("--dest", required=True)
    act.add_argument("--meta_orig", required=True)
    act.add_argument("--meta_dest", required=True)
    act.add_argument("--throttle", type=int)
    act.add_argument("--name", default="las_migration")

    subparsers.add_parser("sync", help="Start background synchronization").add_argument(
        "--name", default="las_migration"
    )
    subparsers.add_parser("status", help="Check synchronization status").add_argument(
        "--name", default="las_migration"
    )
    subparsers.add_parser("break", help="Cut over to destination device").add_argument(
        "--name", default="las_migration"
    )
    subparsers.add_parser("list", help="List all migration tasks").add_argument(
        "--name", default="las_migration"
    )
    subparsers.add_parser("delete", help="Delete a migration task").add_argument(
        "--name", default="las_migration"
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()
    engine = dm.RAIDEngine(args.name)

    if args.command == "activate":
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
            print(f"[SUCCESS] {args.name} established in passive mode (no sync).")

    elif args.command == "sync":
        rec = database.get_migration(args.name)
        if rec and engine.start_sync(*rec):
            database.record_migration(args.name, *rec, active=True)
            print(f"[SUCCESS] Background synchronization initiated.")

    elif args.command == "status":
        raw, pct = engine.get_status()
        if pct:
            print(f"Status for {args.name} sync : {pct}")

    elif args.command == "break":
        raw, pct = engine.get_status()
        if pct != "100.00%":
            print(f"WARNING: Sync is incomplete ({pct}).")
            confirm = input(
                "Confirm break (data on destination may be partial)? (y/N): "
            )
            if confirm.lower() != "y":
                sys.exit(0)
        engine.stop()
        rec = database.get_migration(args.name)

        database.mark_complete(args.name)
        print(f"[SUCCESS] Finalized. {args.name} is now a linear device on {rec[1]}.")

    elif args.command == "list":
        with database.sqlite3.connect(database.DB_PATH) as conn:
            for row in conn.execute("SELECT * FROM migrations"):
                print(row)
    elif args.command == "delete":
        database.delete_migration(args.name)
        print(f"[SUCCESS] Deleted migration record for {args.name}.")


if __name__ == "__main__":
    main()
