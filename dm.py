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
        Initializes RAID1 metadata using the 'Missing Leg' strategy.
        Initializes the destination as Leg 1 of a degraded array, then
        clones that valid metadata to the origin meta disk.
        """
        import subprocess
        import time

        # 1. Calculate Sectors (Aligned to 1024)
        res = subprocess.run(['blockdev', '--getsz', orig], capture_output=True, text=True)
        self.sectors = (int(res.stdout.strip()) // 1024) * 1024

        print(f"[*] Targeting {self.sectors} sectors (1024 sector alignment)")

        # 2. Wipe existing signatures on destination and metadata disks
        for dev in [dest, meta_orig, meta_dest]:
            subprocess.run(['sudo', 'wipefs', '-a', dev], check=True)
        
        # 3. Create 'Degraded' RAID (Leg 0 is missing)
        # Table format: 0 <len> raid raid1 <#opts> <region> <nosync> <#legs> <m0> <d0> <m1> <d1>
        # We use '-' for Leg 0's meta and data.
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
            # Copying it to meta_orig gives Leg 0 a starting point.
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
        
    def get_dm_mod_string(self, orig, dest, meta_orig, meta_dest):
        # Resolve to persistent IDs
        p_orig = utils.get_persistent_path(orig)
        p_dest = utils.get_persistent_path(dest)
        p_m_orig = utils.get_persistent_path(meta_orig)
        p_m_dest = utils.get_persistent_path(meta_dest)

        # The kernel 'raid' target expects:
        # raid <subtype> <#opt_params> <opt_params...> <#legs> [<meta_dev> <data_dev>...]
        # Parameters: 2 (count), nosync, region_size 1024
        table = f"0 {self.sectors} raid raid1 1 nosync 2 {p_m_orig} {p_orig} {p_m_dest} {p_dest}"
        
        return f"{self.name},,0,rw,{table}"

    def setup_boom_entry(self, orig, dest, m_orig, m_dest):
        dm_string = self.get_dm_mod_string(orig, dest, m_orig, m_dest)
        kver = subprocess.run(['uname', '-r'], capture_output=True, text=True).stdout.strip()
        
        # FIX 1: Load the raid module BEFORE dm-init runs
        # FIX 2: Swap console order so ttyS0 is the interactive one (last)
        # FIX 3: Force emergency shell to not ask for password
        # Define arguments as a clean list
        args = [
            "rd.driver.pre=dm-raid",
            "rd.timeout=30",
            "rd.shell",
            f"root=/dev/mapper/{self.name}",
            "SYSTEMD_SULOGIN_FORCE=1",
            "console=tty0",
            "console=ttyS0,115200n8",
            "rd.debug",
            "loglevel=7",
            f'dm-mod.create={dm_string}'
        ]
        
        # Join them with a single space safely
        opts = " ".join(args)
        
        try:
            # Ensure the LAS profile exists for Boom
            subprocess.run(['boom', 'profile', 'create', '--from-host', '--name', 'las'], capture_output=True)
            
            cmd = [
                'boom', 'entry', 'create', 
                '--title', f'LAS-{self.name}',
                '--root-device', f'/dev/mapper/{self.name}',
                '--version', kver,
                '--no-dev',
                '--add-opts', f'{opts}'
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
