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

    def get_dm_mod_string(self, orig, dest, m_orig, m_dest):
        size = utils.get_block_size(orig)
        table = f"0 {size} raid raid1 0 1024 2 {m_orig} {orig} {m_dest} {dest}"
        return f"{self.name},,0,0,{table}"

    def setup_boom_entry(self, orig, dest, m_orig, m_dest):
        dm_string = self.get_dm_mod_string(orig, dest, m_orig, m_dest)
        try:
            # Check if profile exists first
            subprocess.run(['boom', 'profile', 'create', '--from-host', '--name', 'las'], capture_output=True)
            
            cmd = [
                'boom', 'entry', 'create', '--title', f'LAS-{self.name}',
                '--root-device', f'/dev/mapper/{self.name}',
                '--no-dev',  # root device is constructed at boot
                '--add-opts', f'dm-mod.create="{dm_string}"'
            ]
            res = subprocess.run(cmd, capture_output=True, text=True)
            
            if res.returncode != 0:
                print(f"[!] Boom Error: {res.stderr}") # <--- Critical for debugging
                return False
            return True
        except Exception as e:
            print(f"[!] Python Boom Interface Exception: {e}")
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

    def stop(self):
        return (
            subprocess.run(
                ["dmsetup", "remove", self.name], capture_output=True
            ).returncode
            == 0
        )
