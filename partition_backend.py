#!/usr/bin/env python3
#
# Copyright Red Hat
#
# partition_backend.py - Partition-based (dm-raid) storage backend
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
import os
import re
import subprocess
import sys

import database
import raid
import utils
from dm import RAIDEngine
from storage import StorageBackend


class PartitionBackend(StorageBackend):
    backend_type = 'partition'

    def validate(self, origin, dest, *, meta_orig, meta_dest):
        return utils.validate_migration_geometry(origin, dest, meta_orig, meta_dest)

    def _detect_root_partition_index(self):
        """Detect which partition number the current root (/) lives on."""
        try:
            root_src = subprocess.check_output(
                ['findmnt', '-n', '-o', 'SOURCE', '/'], text=True
            ).strip().split('[')[0]
            match = re.search(r'(\d+)$', root_src)
            if match:
                return int(match.group(1))
        except Exception as e:
            print(f"[!] Could not detect root partition index: {e}")
        return None

    def prepare(self, name, origin, dest, *, meta_orig, meta_dest):
        print(f"[*] Starting Lift and Shift (LAS) preparation for: {name}")

        if not self.validate(origin, dest, meta_orig=meta_orig, meta_dest=meta_dest):
            sys.exit(1)

        try:
            origin_sz = int(subprocess.check_output(
                ['blockdev', '--getsz', origin], text=True
            ).strip())
        except Exception as e:
            print(f"[!] Could not determine size of {origin}: {e}")
            return False

        if not utils.sync_partition_table(origin, dest):
            print("[!] Geometry sync failed. Cannot proceed.")
            return False

        partitions = utils.parse_partition_table(origin)
        if not partitions:
            print("[!] Failed to parse partition table. Cannot proceed.")
            return False

        if len(partitions) < 2:
            print(f"[!] Expected at least 2 partitions, found {len(partitions)}")
            return False

        print(f"[*] Detected {len(partitions)} partitions:")
        for part in partitions:
            size_gb = (part['size'] * 512) / (1024**3)
            print(f"    Partition {part['num']}: start={part['start']}, "
                  f"size={part['size']} sectors ({size_gb:.2f} GB)")

        root_part_num = self._detect_root_partition_index()
        if root_part_num is not None:
            part_nums = [p['num'] for p in partitions]
            if root_part_num not in part_nums:
                print(f"[!] Detected root partition {root_part_num} "
                      f"not in partition table {part_nums}")
                return False
            print(f"[*] Root filesystem is on partition {root_part_num}")
        else:
            root_part_num = partitions[-1]['num']
            print(f"[*] Could not auto-detect root partition, "
                  f"assuming partition {root_part_num}")

        raid.wipe_metadata(meta_orig)
        if not raid.write_dm_raid_superblock(meta_orig, origin_sz):
            print("[!] Failed to prime source metadata.")
            return False

        p_orig = utils.get_persistent_path(origin)
        p_dest = utils.get_persistent_path(dest)
        p_m_orig = utils.get_persistent_path(meta_orig)
        p_m_dest = utils.get_persistent_path(meta_dest)

        img_path = utils.inject_las_assembly_hook(
            name, p_orig, p_dest, p_m_orig, p_m_dest, partitions,
            root_part_num=root_part_num
        )
        if not img_path:
            return False

        try:
            current_fstype, current_fsflags = self._detect_fsinfo()
            print(f"[*] Detected {current_fstype} for migration.")
            print(f"[*] Using root mount flags: {current_fsflags}")
        except Exception as e:
            print(f"[!] Could not detect live filesystem info: {e}")
            return False

        print("[*] Updating migration database...")
        database.record_migration(
            name=name, orig=p_orig, dest=p_dest,
            meta_orig=p_m_orig, meta_dest=p_m_dest,
            throttle=0, fstype=current_fstype, fsflags=current_fsflags,
            migration_type='partition'
        )

        engine = RAIDEngine(name)
        if not engine.setup_boom_entry(img_path, current_fstype, current_fsflags):
            print("[!] Failed to register Boom boot entry.")
            return False

        print(f"\n[SUCCESS] Lift and Shift prepared for '{name}'.")
        print(f"[ACTION] Run: grub2-reboot 'LAS-{name}' && reboot")
        return True

    def check_sync(self, name, record):
        engine = RAIDEngine(name)
        if not os.path.exists(f"/dev/mapper/{name}"):
            return "Offline", "N/A", False
        raw, pct = engine.get_status()
        is_synced = "100.00%" in pct
        return raw, pct, is_synced

    def break_mirror(self, name, record, commit=False):
        print(f"[*] Detected partition-based migration")
        engine = RAIDEngine(name)

        _, pct = engine.get_status()
        if "100.00%" not in pct:
            if input(f"[!] Sync incomplete ({pct}). Finalize anyway? (y/N): ").lower() != 'y':
                sys.exit(0)

        engine.cleanup_boom_entry()
        engine.stop()

        if commit:
            print(f"\n[*] Committing migration: regenerating origin UUID...")
            origin_dev = record['orig']
            fstype = record.get('fstype', 'xfs')

            if origin_dev.startswith('/dev/disk/by-id/'):
                try:
                    actual_dev = os.path.realpath(origin_dev)
                    print(f"[*] Origin device: {origin_dev} -> {actual_dev}")
                    origin_dev = actual_dev
                except Exception as e:
                    print(f"[!] Warning: Could not resolve device path: {e}")

            if not utils.regenerate_filesystem_uuid(origin_dev, fstype):
                print("[!] WARNING: Failed to regenerate origin UUID")
                print("[!] Manual intervention may be needed to prevent UUID conflicts")
            else:
                print(f"[*] Origin disk {origin_dev} now has a unique UUID")
                print(f"[*] Destination disk has taken over with the original UUID")

        database.delete_migration(name)
        print("[SUCCESS] Partition migration finalized.")
        return True

    def revert(self, name, record):
        root_dev = subprocess.check_output(
            ["findmnt", "-n", "-o", "SOURCE", "/"], text=True
        ).strip()
        if name in root_dev:
            print(f"[!] ERROR: You are still booted into the RAID ({root_dev}).")
            return False

        print(f"[*] Starting revert for migration: {name}")

        try:
            subprocess.run(
                ["sudo", "boom", "profile", "delete", "--title", f"LAS: {name}"],
                check=True, capture_output=True
            )
            print("[OK] Removed Boom entry.")
        except Exception:
            print("[!] Note: Boom profile not found.")

        img_path = f"/boot/initramfs-las-{name}.img"
        if os.path.exists(img_path):
            os.remove(img_path)
            print("[OK] Deleted custom Initramfs.")

        subprocess.run(["sudo", "udevadm", "settle"], check=False)
        target_disks = [record['dest'], record.get('meta_orig', ''), record.get('meta_dest', '')]
        for disk in target_disks:
            if disk and os.path.exists(disk):
                print(f"[*] Wiping signatures on {disk}...")
                try:
                    subprocess.run(["sudo", "wipefs", "-a", "-f", disk], check=True)
                except Exception:
                    subprocess.run([
                        "sudo", "dd", "if=/dev/zero", f"of={disk}",
                        "bs=1M", "count=1", "oflag=direct"
                    ], check=True)

        database.delete_migration(name)
        print(f"\n[SUCCESS] Revert complete. System is stable on {root_dev}.")
        return True
