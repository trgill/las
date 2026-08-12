#!/usr/bin/env python3
#
# Copyright Red Hat
#
# storage.py - Unified storage backend abstractions for LAS
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
"""
Storage backend abstraction layer.

Provides a common interface for partition-based (dm-raid) and LVM-based
(lvconvert RAID1) migrations, replacing the parallel code paths that
previously lived in las.py.
"""
from abc import ABC, abstractmethod
import os
import subprocess
import sys

import database
import raid
import utils
from dm import RAIDEngine


class StorageBackend(ABC):
    backend_type = None

    @abstractmethod
    def validate(self, origin, dest, **kwargs):
        """Pre-flight validation. Returns True if ready to proceed."""

    @abstractmethod
    def prepare(self, name, origin, dest, **kwargs):
        """Execute full preparation workflow. Returns True on success."""

    @abstractmethod
    def check_sync(self, name, record):
        """Returns (raw_status, percent_string, is_synced)."""

    @abstractmethod
    def break_mirror(self, name, record, commit=False):
        """Finalize migration by removing origin. Returns True on success."""

    @abstractmethod
    def revert(self, name, record):
        """Revert migration and clean up. Returns True on success."""

    @staticmethod
    def detect(origin):
        """Auto-detect backend from origin device."""
        lvm_info = utils.detect_lvm_info(origin)
        if lvm_info and lvm_info['is_pv']:
            return LVMBackend()
        return PartitionBackend()

    @staticmethod
    def from_record(record):
        """Reconstruct backend from a database migration record."""
        mtype = record.get('migration_type', '')
        if mtype == 'lvm':
            return LVMBackend()
        if mtype == 'partition':
            return PartitionBackend()
        # Backward compat for records created before migration_type column
        if not record.get('meta_orig') or record['meta_orig'] == '':
            return LVMBackend()
        return PartitionBackend()

    def _detect_fsinfo(self):
        cmd = ["findmnt", "-n", "-o", "FSTYPE,OPTIONS", "/"]
        fs_info = subprocess.check_output(cmd, text=True).strip().split()
        return fs_info[0], fs_info[1]


class PartitionBackend(StorageBackend):
    backend_type = 'partition'

    def validate(self, origin, dest, *, meta_orig, meta_dest):
        return utils.validate_migration_geometry(origin, dest, meta_orig, meta_dest)

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

        raid.wipe_metadata(meta_orig)
        if not raid.write_dm_raid_superblock(meta_orig, origin_sz):
            print("[!] Failed to prime source metadata.")
            return False

        p_orig = utils.get_persistent_path(origin)
        p_dest = utils.get_persistent_path(dest)
        p_m_orig = utils.get_persistent_path(meta_orig)
        p_m_dest = utils.get_persistent_path(meta_dest)

        img_path = utils.inject_las_assembly_hook(
            name, p_orig, p_dest, p_m_orig, p_m_dest, partitions
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


class LVMBackend(StorageBackend):
    backend_type = 'lvm'

    def validate(self, origin, dest, **kwargs):
        lvm_info = utils.detect_lvm_info(origin)
        if not lvm_info or not lvm_info['is_pv']:
            print(f"[!] {origin} is not an LVM Physical Volume")
            return False

        if not utils.validate_lvm_migration(lvm_info, dest):
            return False

        try:
            origin_sz = int(subprocess.check_output(
                ['blockdev', '--getsz', origin], text=True
            ).strip())
            dest_sz = int(subprocess.check_output(
                ['blockdev', '--getsz', dest], text=True
            ).strip())
            if dest_sz < origin_sz:
                print(f"[!] Destination ({dest_sz} sectors) smaller than origin ({origin_sz} sectors)")
                return False
        except Exception as e:
            print(f"[!] Could not determine device sizes: {e}")
            return False

        return True

    def prepare(self, name, origin, dest, **kwargs):
        print(f"[*] Starting LVM native RAID1 migration for: {name}")

        lvm_info = utils.detect_lvm_info(origin)
        if not lvm_info or not lvm_info['is_pv']:
            print(f"[!] {origin} is not an LVM Physical Volume")
            return False

        vg_name = lvm_info['vg_name']
        lvs = lvm_info['lvs']

        print(f"[*] Volume Group: {vg_name}")
        print(f"[*] Origin PV: {origin}")
        print(f"[*] Destination PV: {dest}")
        print(f"[*] Logical Volumes: {', '.join([lv['lv_name'] for lv in lvs])}")

        if not utils.validate_lvm_migration(lvm_info, dest):
            return False

        try:
            origin_sz = int(subprocess.check_output(
                ['blockdev', '--getsz', origin], text=True
            ).strip())
            dest_sz = int(subprocess.check_output(
                ['blockdev', '--getsz', dest], text=True
            ).strip())
            if dest_sz < origin_sz:
                print(f"[!] Destination ({dest_sz} sectors) smaller than origin ({origin_sz} sectors)")
                return False
        except Exception as e:
            print(f"[!] Could not determine device sizes: {e}")
            return False

        print(f"\n[*] Adding {dest} as Physical Volume...")
        try:
            subprocess.run(
                ['sudo', 'pvcreate', dest],
                check=True, capture_output=True, text=True
            )
            print(f"[SUCCESS] PV created on {dest}")
        except subprocess.CalledProcessError as e:
            print(f"[!] pvcreate failed: {e.stderr}")
            return False

        print(f"[*] Extending VG {vg_name} to include {dest}...")
        try:
            subprocess.run(
                ['sudo', 'vgextend', vg_name, dest],
                check=True, capture_output=True, text=True
            )
            print(f"[SUCCESS] VG extended")
        except subprocess.CalledProcessError as e:
            print(f"[!] vgextend failed: {e.stderr}")
            subprocess.run(['sudo', 'pvremove', dest], capture_output=True)
            return False

        print(f"\n[*] Converting Logical Volumes to RAID1...")
        converted_lvs = []

        for lv in lvs:
            lv_name = lv['lv_name']
            lv_path = f"{vg_name}/{lv_name}"

            print(f"[*] Converting {lv_name} to RAID1...")
            try:
                result = subprocess.run([
                    'sudo', 'lvconvert', '--type', 'raid1', '-m', '1',
                    f'/dev/{lv_path}', dest
                ], capture_output=True, text=True, input='y\n')

                if result.returncode != 0:
                    print(f"[!] lvconvert failed for {lv_name}: {result.stderr}")
                    print(f"[!] Rolling back conversions...")
                    for converted in converted_lvs:
                        subprocess.run([
                            'sudo', 'lvconvert', '-m', '0',
                            f'/dev/{vg_name}/{converted}'
                        ], capture_output=True, input='y\n')
                    subprocess.run(
                        ['sudo', 'vgreduce', vg_name, dest], capture_output=True
                    )
                    subprocess.run(
                        ['sudo', 'pvremove', dest], capture_output=True
                    )
                    return False

                converted_lvs.append(lv_name)
                print(f"[SUCCESS] {lv_name} converted to RAID1")

            except Exception as e:
                print(f"[!] Unexpected error converting {lv_name}: {e}")
                return False

        print(f"\n[*] RAID1 synchronization started for all LVs")
        print(f"[*] Checking sync status...")
        try:
            result = subprocess.run(
                ['sudo', 'lvs', '-a', '-o', 'lv_name,copy_percent', vg_name],
                capture_output=True, text=True
            )
            print(result.stdout)
        except Exception:
            pass

        try:
            current_fstype, current_fsflags = self._detect_fsinfo()
        except Exception:
            current_fstype = 'xfs'
            current_fsflags = 'defaults'

        print(f"\n[*] Updating migration database...")
        p_orig = utils.get_persistent_path(origin)
        p_dest = utils.get_persistent_path(dest)

        database.record_migration(
            name=name, orig=p_orig, dest=p_dest,
            meta_orig='', meta_dest='',
            throttle=0, fstype=current_fstype, fsflags=current_fsflags,
            migration_type='lvm'
        )

        print(f"\n{'='*60}")
        print(f"[SUCCESS] LVM RAID1 migration complete!")
        print(f"{'='*60}")
        print(f"[*] Volume Group: {vg_name}")
        print(f"[*] Converted LVs: {', '.join(converted_lvs)}")
        print(f"[*] Origin PV: {origin}")
        print(f"[*] Mirror PV: {dest}")
        print(f"")
        print(f"[*] Current status:")
        print(f"    - All LVs are now RAID1 mirrored")
        print(f"    - System is RUNNING on mirrored volumes")
        print(f"    - Sync in progress (check with: lvs -a -o +devices,copy_percent)")
        print(f"    - NO REBOOT REQUIRED")
        print(f"    - NO INITRAMFS HOOKS NEEDED (LVM handles boot automatically)")
        print(f"")
        print(f"[*] Monitor sync progress:")
        print(f"    sudo lvs -a -o lv_name,copy_percent {vg_name}")
        print(f"")
        print(f"[*] Next steps:")
        print(f"    1. Wait for sync to complete (100.00%)")
        print(f"    2. Optionally test reboot (mirror boots automatically)")
        print(f"    3. When ready, finalize: ./las.py break --name {name}")
        print(f"       (This removes origin PV, keeping only mirror)")
        print(f"{'='*60}")

        return True

    def _resolve_origin(self, record):
        origin_dev = record['orig']
        if origin_dev.startswith('/dev/disk/by-id/'):
            try:
                origin_dev = os.path.realpath(origin_dev)
            except Exception as e:
                print(f"[!] Warning: Could not resolve device path: {e}")
        return origin_dev

    def check_sync(self, name, record):
        origin_dev = self._resolve_origin(record)
        lvm_info = utils.detect_lvm_info(origin_dev)
        if not lvm_info:
            return "Unknown", "0%", False

        vg_name = lvm_info['vg_name']
        try:
            result = subprocess.run(
                ['sudo', 'lvs', '-o', 'lv_name,copy_percent',
                 '--noheadings', vg_name],
                capture_output=True, text=True
            )
            raw = result.stdout.strip()

            incomplete = False
            for line in raw.split('\n'):
                if line.strip() and '100.00' not in line:
                    incomplete = True
                    break

            if incomplete:
                return raw, "syncing", False
            return raw, "100.00%", True
        except Exception:
            return "Unknown", "0%", False

    def break_mirror(self, name, record, commit=False):
        print(f"[*] Detected LVM RAID1 migration")
        origin_dev = self._resolve_origin(record)

        lvm_info = utils.detect_lvm_info(origin_dev)
        if not lvm_info:
            print(f"[!] Could not detect LVM info for {origin_dev}")
            sys.exit(1)

        vg_name = lvm_info['vg_name']
        lvs = lvm_info['lvs']

        print(f"[*] Checking RAID sync status...")
        try:
            result = subprocess.run(
                ['sudo', 'lvs', '-o', 'lv_name,copy_percent',
                 '--noheadings', vg_name],
                capture_output=True, text=True
            )
            print(result.stdout)

            incomplete = False
            for line in result.stdout.strip().split('\n'):
                if line.strip() and '100.00' not in line:
                    incomplete = True
                    break

            if incomplete:
                resp = input("[!] Sync incomplete. Finalize anyway? (y/N): ")
                if resp.lower() != 'y':
                    sys.exit(0)
        except Exception:
            print(f"[!] Could not check sync status")
            resp = input("[!] Continue anyway? (y/N): ")
            if resp.lower() != 'y':
                sys.exit(0)

        print(f"\n[*] Removing origin PV from RAID1 mirrors...")
        failed_lvs = []

        for lv in lvs:
            lv_name = lv['lv_name']
            print(f"[*] Converting {lv_name} to linear (removing origin leg)...")
            try:
                result = subprocess.run([
                    'sudo', 'lvconvert', '-m', '0',
                    f'/dev/{vg_name}/{lv_name}', origin_dev
                ], capture_output=True, text=True, input='y\n')

                if result.returncode != 0:
                    print(f"[!] Failed to convert {lv_name}: {result.stderr}")
                    failed_lvs.append(lv_name)
                else:
                    print(f"[SUCCESS] {lv_name} converted to linear")
            except Exception as e:
                print(f"[!] Error converting {lv_name}: {e}")
                failed_lvs.append(lv_name)

        if failed_lvs:
            print(f"\n[!] WARNING: Failed to convert some LVs: "
                  f"{', '.join(failed_lvs)}")
            print(f"[!] Manual intervention may be required")

        print(f"\n[*] Removing origin PV {origin_dev} from VG {vg_name}...")
        try:
            subprocess.run(
                ['sudo', 'vgreduce', vg_name, origin_dev],
                check=True, capture_output=True
            )
            print(f"[SUCCESS] Origin PV removed from VG")

            if commit:
                print(f"[*] Removing PV metadata from {origin_dev}...")
                subprocess.run(
                    ['sudo', 'pvremove', origin_dev],
                    check=True, capture_output=True
                )
                print(f"[SUCCESS] PV metadata removed - disk can be reused")
        except subprocess.CalledProcessError as e:
            print(f"[!] Failed to remove origin PV: {e}")
            print(f"[!] Manual cleanup may be needed: "
                  f"vgreduce {vg_name} {origin_dev}")

        database.delete_migration(name)
        print(f"\n[SUCCESS] LVM RAID1 migration finalized")
        print(f"[*] All LVs now run on destination only")
        print(f"[*] Origin disk {origin_dev} has been removed from VG")
        return True

    def revert(self, name, record):
        origin_dev = self._resolve_origin(record)
        dest_dev = record['dest']
        if dest_dev.startswith('/dev/disk/by-id/'):
            try:
                dest_dev = os.path.realpath(dest_dev)
            except Exception:
                pass

        lvm_info = utils.detect_lvm_info(origin_dev)
        if lvm_info:
            vg_name = lvm_info['vg_name']
            for lv in lvm_info['lvs']:
                lv_name = lv['lv_name']
                try:
                    subprocess.run([
                        'sudo', 'lvconvert', '-m', '0',
                        f'/dev/{vg_name}/{lv_name}', dest_dev
                    ], capture_output=True, text=True, input='y\n')
                except Exception:
                    pass

            try:
                subprocess.run(
                    ['sudo', 'vgreduce', vg_name, dest_dev],
                    capture_output=True
                )
                subprocess.run(
                    ['sudo', 'pvremove', dest_dev],
                    capture_output=True
                )
            except Exception:
                pass

        database.delete_migration(name)
        print(f"[SUCCESS] LVM migration reverted for {name}")
        return True
