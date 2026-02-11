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
import sys
import logging
import re

logger = logging.getLogger("las")


class RAIDEngine:
    def __init__(self, name):
        self.name = name

    def _flush(self):
        """Perform a multi-stage flush: sync, blockdev flush, and optional fsfreeze."""
        device_path = f"/dev/mapper/{self.name}"
        print(f"[*] Performing deep flush on {device_path}...")

        # 1. Standard sync to flush page cache
        subprocess.run(["sync"], check=True)

        # 2. Try to find the mount point to freeze/thaw (forces XFS log commit)
        mount_point = None
        with open("/proc/mounts", "r") as f:
            for line in f:
                if device_path in line:
                    mount_point = line.split()[1]
                    break

        if mount_point:
            try:
                print(f"[*] Freezing {mount_point} to commit XFS logs...")
                subprocess.run(["fsfreeze", "-f", mount_point], check=True)
                subprocess.run(["fsfreeze", "-u", mount_point], check=True)
            except subprocess.CalledProcessError:
                print(
                    "[!] Warning: Could not freeze filesystem. Proceeding with block flush."
                )

        # 3. Final block-level flush
        res = subprocess.run(
            ["blockdev", "--flushbufs", device_path], capture_output=True
        )
        return res.returncode == 0

    def _get_size(self, dev):
        """Returns size in 512-byte sectors. Handles device:offset syntax."""
        clean_dev = dev.split(":")[0]
        res = subprocess.run(
            ["blockdev", "--getsz", clean_dev], capture_output=True, text=True
        )
        if res.returncode != 0:
            logger.error(f"Failed to access device {clean_dev}")
            sys.exit(1)
        return res.stdout.strip()

    def _run_dm(self, action, table=None):
        """Core wrapper for dmsetup ioctls."""
        cmd = ["dmsetup", action, self.name]
        if table:
            # Print the manual command equivalent for debugging
            print(f'[*] EXEC: echo "{table}" | dmsetup {action} {self.name}')
        else:
            print(f"[*] EXEC: dmsetup {action} {self.name}")
        try:
            if table:
                p = subprocess.Popen(cmd, stdin=subprocess.PIPE, text=True)
                p.communicate(input=table)
                return p.returncode == 0
            else:
                res = subprocess.run(cmd, capture_output=True)
                return res.returncode == 0
        except Exception as e:
            logger.error(f"dmsetup {action} failed: {e}")
            return False

    def activate_passive(self, orig, dest, m_orig, m_dest):
        """Adopts devices into a RAID1 set with 'nosync' protection."""
        size = self._get_size(orig)
        region_size = "1024"  # 512KB
        # '1 nosync' means exactly 1 feature argument ('nosync') follows.
        table = f"0 {size} raid raid1 2 {region_size} nosync 2 {m_orig} {orig} {m_dest} {dest}"

        # Cleanup previous failed attempts to avoid "Device or resource busy"
        subprocess.run(["dmsetup", "remove", self.name], capture_output=True)

        # Print the manual command equivalent for debugging
        print(f'[*] EXEC: echo "{table}" | dmsetup create {self.name}')

        return self._run_dm("create", table)

    def start_sync(self, orig, dest, m_orig, m_dest, throttle=None):
        """Reloads the table to remove nosync and begin synchronization."""
        size = self._get_size(orig)
        region_size = "1024"

        # '2 max_recovery_rate <val>' consists of 2 arguments.
        if throttle:
            feat_args = f"2 max_recovery_rate {throttle}"
        else:
            feat_args = "1"

        table = f"0 {size} raid raid1 {feat_args} {region_size} 2 {m_orig} {orig} {m_dest} {dest}"

        if self._run_dm("suspend"):
            if self._run_dm("load", table):
                return self._run_dm("resume")
        self._run_dm("resume")
        return False

    def stop(self):
        """Flushes buffers and then removes the mapper device."""
        if self._flush():
            return self._run_dm("remove")
        else:
            print("[ERROR] Failed to flush buffers. Aborting stop for safety.")
            return False

    def get_status(self):
        """Returns a tuple of (raw_status, percent_complete)."""
        res = subprocess.run(
            ["dmsetup", "status", self.name], capture_output=True, text=True
        )
        if res.returncode != 0:
            return None, "0"

        raw = res.stdout.strip()

        # Typical raid status: 0 19529728 raid raid1 2 AA 19529728/19529728 1024 ...
        # We look for the pattern 'synced/total'
        match = re.search(r"(\d+)/(\d+)", raw)
        if match:
            synced = int(match.group(1))
            total = int(match.group(2))
            if total > 0:
                percent = (synced / total) * 100
                return raw, f"{percent:.2f}%"

        return raw, "0.00%"
