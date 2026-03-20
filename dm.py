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
migration interactions.
"""
import subprocess
import re
import utils

class RAIDEngine:
    def __init__(self, name):
        self.name = name
    
    def init_raid_metadata(self, orig, dest, meta_orig, meta_dest):
        """
        Initializes RAID1 metadata for both legs using a 1024 region size.
        Uses a loopback alias on 'dest' (/dev/sdd) to proxy for the busy 'orig' (/dev/sda).
        """
        import subprocess
        import time

        # 1. Calculate Sectors (Aligned to 1024 / 512KiB)
        # This ensures the RAID table ends exactly on a region boundary.
        res = subprocess.run(['blockdev', '--getsz', orig], capture_output=True, text=True)
        raw_sectors = int(res.stdout.strip())
        self.sectors = (raw_sectors // 1024) * 1024

        print(f"[*] Target Geometry: {self.sectors} sectors (Region Size: 1024)")

        # 2. Wipe existing signatures to ensure a clean 'dmsetup'
        # This prevents 'Device or resource busy' if old partitions exist on sdd.
        print(f"[*] Wiping signatures on {dest}, {meta_orig}, {meta_dest}...")
        for dev in [dest, meta_orig, meta_dest]:
            subprocess.run(['sudo', 'wipefs', '-a', dev], check=True, capture_output=True)
        
        # 3. Create Loopback Alias for Destination (/dev/sdd)
        # This lets us 'open' the same physical disk twice (once as sdd, once as loopX).
        loop_res = subprocess.run(['sudo', 'losetup', '--find', '--show', dest], 
                                  capture_output=True, text=True)
        loop_dev = loop_res.stdout.strip()
        
        if not loop_dev.startswith('/dev/loop'):
            print("[!] Error: Could not allocate a loopback device.")
            return False

        try:
            print(f"[*] Using {loop_dev} as Proxy-Leg-0 for the busy origin...")
            
            # 4. The 'Prime' Table
            # 2: Optional parameter count (region_size + nosync)
            # 1024: The granular region size
            # nosync: Skip initial mirror sync
            # 2: Total RAID legs
            table = (
                f"0 {self.sectors} raid raid1 2 1024 nosync 2 "
                f"{meta_orig} {loop_dev} {meta_dest} {dest}"
            )

            # 5. Create the temporary RAID device to write the clean superblocks
            temp_name = f"las_prime_{self.name}"
            subprocess.run(['sudo', 'dmsetup', 'create', temp_name, '--table', table], 
                           check=True, capture_output=True, text=True)
            
            # Allow the kernel a moment to commit the headers to sdc and sdb
            time.sleep(1)
            
            # 6. Remove the temporary device to flush buffers and close the disks
            subprocess.run(['sudo', 'dmsetup', 'remove', temp_name], check=True)
            print("[SUCCESS] Leg 0 (Origin) and Leg 1 (Dest) metadata successfully initialized.")
            return True

        except subprocess.CalledProcessError as e:
            print(f"[!] Metadata initialization failed: {e.stderr}")
            return False
        finally:
            # 7. Cleanup: Always detach the loopback to free the disk
            print(f"[*] Detaching loopback proxy {loop_dev}...")
            subprocess.run(['sudo', 'losetup', '-d', loop_dev])
        
    def get_dm_mod_string(self, orig, dest, meta_orig, meta_dest):
        """
        Generates the dm-mod.create string using persistent IDs and correct 
        RAID1 pairings: 2 <Meta_A> <Data_A> <Meta_B> <Data_B>
        """
        # Resolve volatile names (/dev/sda) to persistent IDs
        p_orig = utils.get_persistent_path(orig)
        p_dest = utils.get_persistent_path(dest)
        p_m_orig = utils.get_persistent_path(meta_orig)
        p_m_dest = utils.get_persistent_path(meta_dest)

        # The Magic Formula: 
        # 1. Start with 0 <sectors>
        # 2. Target type 'raid', sub-type 'raid1'
        # 3. '1 nosync' (parameter count + parameter)
        # 4. '8192' (region size)
        # 5. '2' (number of raid copies)
        # 6. Pairs of (Metadata, Data)
        table = (
            f"0 {self.sectors} raid raid1 2 nosync 8192 2 "
            f"{p_m_orig} {p_orig} {p_m_dest} {p_dest}"
        )
        
        # Format: <name>,<uuid>,<major>,<flags>,<table>
        return f"{self.name},,0,rw,{table}"

    def setup_boom_entry(self, orig, dest, m_orig, m_dest):
        dm_string = self.get_dm_mod_string(orig, dest, m_orig, m_dest)
        kver = subprocess.run(['uname', '-r'], capture_output=True, text=True).stdout.strip()
        
        # FIX 1: Load the raid module BEFORE dm-init runs
        # FIX 2: Swap console order so ttyS0 is the interactive one (last)
        # FIX 3: Force emergency shell to not ask for password
        debug_opts = (
            "rd.driver.pre=dm-raid "
            "SYSTEMD_SULOGIN_FORCE=1 "
            "console=tty0 "
            "console=ttyS0,115200n8 "
            "rd.debug loglevel=7"
        )
        
        try:
            # Ensure the LAS profile exists for Boom
            subprocess.run(['boom', 'profile', 'create', '--from-host', '--name', 'las'], capture_output=True)
            
            cmd = [
                'boom', 'entry', 'create', 
                '--title', f'LAS-{self.name}',
                '--root-device', f'/dev/mapper/{self.name}',
                '--version', kver,
                '--no-dev',
                '--add-opts', f'{debug_opts} dm-mod.create="{dm_string}"'
            ]
            
            res = subprocess.run(cmd, capture_output=True, text=True)
            if res.returncode == 0:
                print(f"[SUCCESS] Boom entry 'LAS-{self.name}' created.")
                return True
            else:
                print(f"[!] Boom failed: {res.stderr}")
                return False
        except Exception as e:
            print(f"[!] Boom interface error: {e}")
            return False

    def activate_passive(self, orig, dest, m_orig, m_dest):
        """Creates a RAID1 target in 'nosync' mode for safe LUN adoption."""
        size = utils.get_block_size(orig)
        region_size = "1024"  # 512KB chunks
        # '1 nosync' ensures the destination isn't overwritten immediately
        table = f"0 {size} raid raid1 2 {region_size} nosync 2 {m_orig} {orig} {m_dest} {dest}"

        # Cleanup any stale mappings to prevent -EBUSY
        subprocess.run(["dmsetup", "remove", self.name], capture_output=True)
        
        p = subprocess.Popen(['dmsetup', 'create', self.name], stdin=subprocess.PIPE, text=True)
        p.communicate(input=table)
        return p.returncode == 0
    
    def get_status(self):
        """Parses dmsetup status to extract sync percentage."""
        res = subprocess.run(['dmsetup', 'status', self.name], capture_output=True, text=True)
        if res.returncode != 0:
            return "Offline", "0%"

        raw = res.stdout.strip()
        # dmsetup status for raid typically looks like:
        # 0 19529728 raid 2 AA 1856/19529728
        match = re.search(r'(\d+)/(\d+)', raw)
        if match:
            synced, total = map(int, match.groups())
            pct = (synced / total * 100) if total > 0 else 0
            return raw, f"{pct:.2f}%"
        
        return raw, "Checking..."

    def start_sync(self, orig, dest, m_orig, m_dest, throttle=None):
        size = utils.get_block_size(orig)
        feat = f"2 max_recovery_rate {throttle}" if throttle else "0"
        table = f"0 {size} raid raid1 {feat} 1024 2 {m_orig} {orig} {m_dest} {dest}"

        subprocess.run(["dmsetup", "suspend", self.name])
        p = subprocess.Popen(
            ["dmsetup", "load", self.name], stdin=subprocess.PIPE, text=True
        )
        p.communicate(input=table)
        return subprocess.run(["dmsetup", "resume", self.name]).returncode == 0

    def remount_to_mapper(self, orig_dev, hook_script=None):
        mount_point = utils.get_mount_point(orig_dev)
        if not mount_point:
            return None

        utils.run_hook(hook_script, "suspend")
        if subprocess.run(["umount", mount_point]).returncode == 0:
            if (
                subprocess.run(
                    ["mount", f"/dev/mapper/{self.name}", mount_point]
                ).returncode
                == 0
            ):
                utils.run_hook(hook_script, "resume")
                return mount_point
            # Rollback
            subprocess.run(["mount", orig_dev, mount_point])
        else:
            utils.list_blocking_pids(mount_point)

        utils.run_hook(hook_script, "resume")
        return None
    
    def cleanup_boom_entry(self):
        """
        Removes the BLS boot entry created for the migration.
        This should be called during 'las break' or if a migration is aborted.
        """
        print(f"[*] Cleaning up Boom boot entry for '{self.name}'...")
        
        # We target the entry by the title we assigned during prepare-root
        # The title format used was 'LAS-{self.name}'
        title_to_delete = f"LAS-{self.name}"
        
        try:
            # We use --title to find the specific entry.
            # subprocess.run is used with capture_output to keep the CLI clean 
            # unless an actual error occurs.
            cmd = ['boom', 'entry', 'delete', '--title', title_to_delete]
            res = subprocess.run(cmd, capture_output=True, text=True)
            
            if res.returncode == 0:
                print(f"[SUCCESS] Boot entry '{title_to_delete}' removed.")
                return True
            else:
                # If boom returns a non-zero exit code, check if it's just 'not found'
                if "no matching entries" in res.stderr.lower():
                    print(f"[*] Note: No Boom entry found for '{title_to_delete}' (already clean).")
                    return True
                else:
                    print(f"[!] Warning: Boom cleanup failed: {res.stderr.strip()}")
                    return False
                    
        except FileNotFoundError:
            print("[!] Error: 'boom' command not found. Is boom-boot installed?")
            return False
        except Exception as e:
            print(f"[!] Unexpected error during Boom cleanup: {e}")
            return False
    def stop(self):
        return (
            subprocess.run(
                ["dmsetup", "remove", self.name], capture_output=True
            ).returncode
            == 0
        )
