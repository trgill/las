#!/usr/bin/env python3
#
# Copyright Red Hat
#
# dm.py - Lift and Shift dm interface
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
"""
migration interactions.
"""
import os
import subprocess
import re
import time
import utils

# Boom Python API
import boom
from boom.bootloader import (
    BootEntry,
    BootParams,
    find_entries,
    drop_entries,
)
from boom.osprofile import find_profiles


class RAIDEngine:

    def __init__(self, name):
        self.name = name
        self.sectors = None

    def init_raid_metadata(self, orig, dest, meta_orig, meta_dest):
        """
        Initializes RAID1 metadata using the 'Missing Leg' strategy.
        Initializes the destination as Leg 1 of a degraded array, then
        clones that valid metadata to the origin meta disk.
        """
        # 1. Calculate Sectors (Aligned to 1024)
        res = subprocess.run(['blockdev', '--getsz', orig], capture_output=True, text=True)
        if res.returncode != 0 or not res.stdout.strip():
            print(f"[!] Failed to get block size for {orig}: {res.stderr.strip()}")
            return False
        self.sectors = (int(res.stdout.strip()) // 1024) * 1024

        print(f"[*] Targeting {self.sectors} sectors (1024 sector alignment)")

        # 2. Wipe existing signatures on destination and metadata disks
        for dev in [dest, meta_orig, meta_dest]:
            subprocess.run(['sudo', 'wipefs', '-a', dev], check=True)

        # 3. Create 'Degraded' RAID (Leg 0 is missing)
        # Table format: 0 <len> raid raid1 <#opts> <region> <nosync> <#legs> <m0> <d0> <m1> <d1>
        prime_name = f"las_prime_{self.name}"
        prime_table = (
            f"0 {self.sectors} raid raid1 2 1024 nosync 2 "
            f"- - {meta_dest} {dest}"
        )

        try:
            print("[*] Priming Leg 1 metadata (Leg 0 marked as missing)...")
            subprocess.run(['sudo', 'dmsetup', 'create', prime_name, '--table', prime_table], check=True)

            # Allow kernel to settle the bits
            time.sleep(2)

            # Remove the device to flush and close
            subprocess.run(['sudo', 'dmsetup', 'remove', prime_name], check=True)

            # 4. Clone metadata from Leg 1 to Leg 0
            # Since Leg 1 is now a valid RAID leg, its superblock is perfect.
            print(f"[*] Cloning valid metadata: {meta_dest} -> {meta_orig}")
            subprocess.run([
                'sudo', 'dd',
                f'if={meta_dest}',
                f'of={meta_orig}',
                'bs=1M', 'count=10', 'status=none'
            ], check=True)

            print("[SUCCESS] Metadata initialized via Missing Leg Strategy.")
            return True

        except subprocess.CalledProcessError as e:
            print(f"[!] Metadata priming failed: {e}")
            return False

    def setup_boom_entry(self, img_path, fstype, fsflags):
        """
        Creates a Boom entry for live migration using the boom Python API.
        Consolidates root arguments and ensures mapper devices are
        correctly announced for /boot and /home.
        """
        clean_name = self.name.strip().replace(']', '').replace('[', '')

        # 1. Identify current root and partition index
        try:
            root_src = subprocess.check_output(['findmnt', '-n', '-o', 'SOURCE', '/'], text=True).strip()
            root_src = root_src.replace(']', '').replace('[', '').strip()
            root_idx_match = re.search(r'(\d+)(?:/.*)?$', root_src)
            if not root_idx_match:
                print(f"[!] Could not find partition index for {root_src}")
                return False
            root_idx = root_idx_match.group(1)

        except Exception as e:
            print(f"[!] Detection error: {e}")
            return False

        # 2. Define the target mapper for Root
        separator = 'p' if clean_name[-1].isdigit() else ''
        root_mapper = f"/dev/mapper/{clean_name}{separator}{root_idx}"

        # 3. Process Mounts
        migration_opts = "x-systemd.device-timeout=60s,nofail"

        try:
            mount_data = subprocess.check_output([
                'findmnt', '-l', '-n', '--real',
                '-o', 'TARGET,SOURCE,FSTYPE,OPTIONS'
            ], text=True).splitlines()
        except Exception as e:
            print(f"[!] Mount scan error: {e}")
            return False

        mount_args = []
        boot_dev_display = "Unknown"

        for line in mount_data:
            parts = line.split(None, 3)
            if len(parts) < 4:
                continue
            target, source, mnt_fstype, mnt_opts = parts

            # SKIP ROOT: Boom handles this via root_device
            if target == "/" or target == "/root":
                continue

            source = source.replace(']', '').replace('[', '').strip()
            if not source.startswith('/dev/'):
                continue

            dev_str = source

            if target == "/boot":
                part_match = re.search(r'(\d+)', source)
                idx = part_match.group(1) if part_match else "2"
                dev_str = f"/dev/mapper/{clean_name}{separator}{idx}"
                boot_dev_display = dev_str

            elif target == "/home":
                part_match = re.search(r'(\d+)', source)
                idx = part_match.group(1) if part_match else root_idx
                dev_str = f"/dev/mapper/{clean_name}{separator}{idx}"

                # Update options to include explicit device pointer for Btrfs
                if "subvol=" not in mnt_opts:
                    clean_opts = re.sub(r'subvolid=\d+', '', mnt_opts)
                    mnt_opts = f"{clean_opts.strip(',')},subvol=/home"
                mnt_opts = f"{mnt_opts},device={dev_str}"

            # Build the mount-extra string
            clean_opts = mnt_opts.replace(",seclabel", "").replace("zstd:1", "zstd")
            final_opts = f"{clean_opts.replace(',,', ',').strip(',')},{migration_opts}"
            mount_args.append(f"systemd.mount-extra={dev_str}:{target}:{mnt_fstype}:{final_opts}")

        # 4. Finalize Kernel Options
        kver = subprocess.check_output(['uname', '-r'], text=True).strip()
        clean_fsflags = fsflags.replace(",seclabel", "").replace("zstd:1", "zstd")

        core_args = [
            "ro rd.fstab=0 rd.retry=60 rootdelay=5",
            "rd.driver.pre=dm-raid rd.timeout=60 rd.dm=1",
            "selinux=0",
            "3",
            f"rootfstype={fstype if fstype else 'auto'}",
            f"rootflags={clean_fsflags},device={root_mapper}",
            "SYSTEMD_SULOGIN_FORCE=1 rd.shell loglevel=7"
        ]

        all_args_list = core_args + mount_args
        all_args_list += ["console=tty0", "console=ttyS0,115200"]
        all_opts = " ".join(all_args_list)

        rel_img_path = img_path.replace("/boot", "", 1) if img_path.startswith("/boot") else img_path
        rel_img_path = rel_img_path.replace("//", "/")

        # 5. Create Boom Entry via CLI (subprocess)
        print(f"[*] LAS Entry: Root on {root_mapper}, Boot on {boot_dev_display}")

        # Universal path resolution for BIOS/UEFI compatibility
        # Check if /boot is a separate partition (standard on UEFI/XFS installs)
        is_boot_separate = os.path.ismount('/boot')

        if is_boot_separate:
            # If /boot is a separate partition, GRUB treats its root as '/'
            # We strip '/boot' from the start: /boot/initrd.img -> /initrd.img
            final_img_path = img_path.replace("/boot", "", 1) if img_path.startswith("/boot") else img_path
            final_kern_path = f"/vmlinuz-{kver}"
        else:
            # If /boot is just a folder on '/', we need the full absolute path
            final_img_path = img_path
            final_kern_path = f"/boot/vmlinuz-{kver}"

        # Clean up any potential double slashes (e.g. //initramfs)
        final_img_path = final_img_path.replace("//", "/")
        final_kern_path = final_kern_path.replace("//", "/")

        try:
            # We add --boot-dir to force boom to write to the physical mount
            # We add --no-dev because /dev/mapper/migration1 isn't active yet
            boom_cmd = [
                "boom", "create",
                "--title", f"LAS-{clean_name}",
                "--root-device", root_mapper,
                "--boot-dir", "/boot",   # <--- THE CRITICAL ADDITION
                "-i", final_img_path, 
                "-l", final_kern_path,
                "--options", all_opts,
                "--no-dev"
            ]

            result = subprocess.run(boom_cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"[SUCCESS] Boom entry 'LAS-{clean_name}' created.")
                return True
            else:
                # This will tell us WHY it's failing to write the file
                print(f"[!] Boom CLI error: {result.stderr.strip()}")
                return False

        except Exception as e:
            print(f"[!] Failed to invoke Boom CLI: {e}")
            return False


    def activate_passive(self, orig, dest, m_orig, m_dest):
        """Creates a RAID1 target in 'nosync' mode for safe LUN adoption."""
        size = utils.get_block_size(orig)
        region_size = "1024"  # 512KB chunks
        table = f"0 {size} raid raid1 2 {region_size} nosync 2 {m_orig} {orig} {m_dest} {dest}"

        # Cleanup any stale mappings to prevent -EBUSY
        subprocess.run(["sudo", "dmsetup", "remove", self.name], capture_output=True)

        p = subprocess.Popen(['sudo', 'dmsetup', 'create', self.name], stdin=subprocess.PIPE, text=True)
        p.communicate(input=table)
        return p.returncode == 0

    def get_status(self):
        """Parses dmsetup status to extract sync percentage."""
        res = subprocess.run(['sudo', 'dmsetup', 'status', self.name], capture_output=True, text=True)
        if res.returncode != 0:
            return "Offline", "0%"
        raw = res.stdout.strip()
        match = re.search(r'(\d+)/(\d+)', raw)
        if match:
            synced, total = map(int, match.groups())
            pct = (synced / total * 100) if total > 0 else 0
            return raw, f"{pct:.2f}%"
        return raw, "Checking..."

    def start_sync(self, name, new_throttle):
        """
        Safely updates the throttle on an active migration mapper.
        Uses the staged 'load' method to avoid 'Invalid Argument' errors.

        Args:
            name (str): Migration name
            new_throttle (int or None): New throttle in KiB/s, or None to use fast default

        Returns:
            tuple: (success: bool, actual_throttle: int or None)
        """
        try:
            # 1. Get the EXACT table currently running in the kernel
            current_table = subprocess.check_output(
                ['sudo', 'dmsetup', 'table', name], text=True
            ).strip()

            # 2. If no throttle specified, use a fast default to "unleash" sync
            if new_throttle is None:
                new_throttle = 500000  # 500 MB/s - fast unrestricted sync
                print(f"[*] No throttle specified, using fast default: {new_throttle} KiB/s")

            # 3. Update the throttle values in the string
            new_max = new_throttle * 2
            updated_table = re.sub(r'min_recovery_rate \d+', f'min_recovery_rate {new_throttle}', current_table)
            updated_table = re.sub(r'max_recovery_rate \d+', f'max_recovery_rate {new_max}', updated_table)

            # 4. Stage the table (Load into inactive slot)
            load_proc = subprocess.Popen(['sudo', 'dmsetup', 'load', name], stdin=subprocess.PIPE)
            load_proc.communicate(input=updated_table.encode())
            if load_proc.returncode != 0:
                print(f"[!] dmsetup load failed with return code {load_proc.returncode}, aborting.")
                return False, None

            # 5. Atomic Switch
            subprocess.run(['sudo', 'dmsetup', 'suspend', name], check=True)
            subprocess.run(['sudo', 'dmsetup', 'resume', name], check=True)

            return True, new_throttle

        except Exception as e:
            print(f"[!] Engine Sync Error: {e}")
            # Emergency resume just in case it got stuck in suspended state
            subprocess.run(['sudo', 'dmsetup', 'resume', name], stderr=subprocess.DEVNULL)
            return False, None

    def remount_to_mapper(self, orig_dev, hook_script=None):
        """Swaps physical mount for mapper mount live."""
        mount_point = utils.get_mount_point(orig_dev)
        if not mount_point:
            return None

        utils.run_hook(hook_script, "suspend")

        if subprocess.run(["sudo", "umount", mount_point]).returncode == 0:
            if (
                subprocess.run(
                    ["sudo", "mount", f"/dev/mapper/{self.name}", mount_point]
                ).returncode == 0
            ):
                utils.run_hook(hook_script, "resume")
                return mount_point

            # Rollback
            subprocess.run(["sudo", "mount", orig_dev, mount_point])
            utils.run_hook(hook_script, "resume")
            return None
        else:
            utils.list_blocking_pids(mount_point)

        utils.run_hook(hook_script, "resume")
        return None

    def cleanup_boom_entry(self):
        clean_name = self.name.strip().replace(']', '').replace('[', '')
        # Clean up Boom boot entry for 'migration'
        print(f"[*] Cleaning up Boom boot entry for '{clean_name}'...")
        
        # Try 'delete' first since that's what your current VM uses
        # then fallback to 'drop' for older environments
        success = False
        for cmd_verb in ["delete", "drop"]:
            try:
                # We target by title and pipe "y" to handle confirmation prompts
                drop_cmd = ["boom", cmd_verb, "--title", f"LAS-{clean_name}"]
                result = subprocess.run(drop_cmd, input="y\n", capture_output=True, text=True)
                
                if result.returncode == 0:
                    print(f"[SUCCESS] Boom entry removed via '{cmd_verb}'.")
                    success = True
                    break
            except Exception:
                continue

        if not success:
            print(f"[!] Could not find or remove Boom entry 'LAS-{clean_name}'.")

    def stop(self):
        """Removes the device mapper device."""
        return (
            subprocess.run(
                ["sudo", "dmsetup", "remove", self.name], capture_output=True
            ).returncode == 0
        )

