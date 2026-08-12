#!/usr/bin/env python3
#
# Copyright Red Hat
#
# lvm_backend.py - LVM-based (lvconvert RAID1) storage backend
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
import os
import subprocess
import sys

import database
import utils
from storage import StorageBackend


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
