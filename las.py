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
import os
import subprocess
import migration
import database as db
import hashlib


def calculate_hash(path, sectors):
    sha256 = hashlib.sha256()
    bytes_to_read = int(sectors) * 512
    chunk = 1024 * 1024
    with open(path, "rb") as f:
        read = 0
        while read < bytes_to_read:
            data = f.read(min(chunk, bytes_to_read - read))
            if not data:
                break
            sha256.update(data)
            read += len(data)
    return sha256.hexdigest()


def update_boot_environment():
    print("Regenerating boot configurations...")
    try:
        if os.path.exists("/etc/debian_version"):
            subprocess.run(["update-initramfs", "-u"], check=True)
            subprocess.run(["update-grub"], check=True)
        else:
            subprocess.run(["dracut", "-f"], check=True)
            subprocess.run(["grub2-mkconfig", "-o", "/boot/grub2/grub.cfg"], check=True)
    except Exception as e:
        print(f"Warning: Boot update failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="LAS: Lift and Shift (EXPERIMENTAL)")
    subparsers = parser.add_subparsers(dest="command")

    p_cmd = subparsers.add_parser("pair")
    p_cmd.add_argument("--name", required=True)
    p_cmd.add_argument("--origin", required=True)
    p_cmd.add_argument("--dest", required=True)
    p_cmd.add_argument("--boot", action="store_true")

    subparsers.add_parser("status").add_argument("--name", required=True)
    subparsers.add_parser("list")
    subparsers.add_parser("verify").add_argument("--name", required=True)
    subparsers.add_parser("finish").add_argument("--name", required=True)

    args = parser.parse_args()
    db.init_db()

    if os.getuid() != 0:
        sys.exit("Error: Root privileges required.")

    if args.command == "pair":
        if not migration.check_dependencies(args.boot):
            sys.exit(1)
        engine = migration.get_engine(args.boot)
        if engine.pair(args.name, args.origin, args.dest):
            pair_obj = db.LunPair(args.name, args.origin, args.dest)
            pair_obj.is_boot = args.boot
            db.save_pair(pair_obj)
            if args.boot:
                update_boot_environment()
            print(f"Pair '{args.name}' established.")

    elif args.command == "status":
        p = db.get_pair(args.name)
        engine = migration.get_engine(p.is_boot if p else False)
        print(f"Sync Progress: {engine.get_status(args.name)}")

    elif args.command == "verify":
        p = db.get_pair(args.name)
        if not p:
            sys.exit("Pair not found.")
        size = (
            subprocess.check_output(["blockdev", "--getsz", p.origin]).decode().strip()
        )
        print("Calculating hashes...")
        h1 = calculate_hash(p.origin, size)
        h2 = calculate_hash(p.destination, size)
        print(f"Origin: {h1}\nDest:   {h2}\nMatch:  {h1 == h2}")


if __name__ == "__main__":
    main()
