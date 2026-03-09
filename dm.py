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
import os
import re
import time
import hashlib
import logging

logger = logging.getLogger("las")

class RAIDEngine:
    def __init__(self, name):
        self.name = name

    def list_blocking_pids(self, mount_point):
        """Lists processes currently using the mount point."""
        print(f"[!] Mount point {mount_point} is busy. Blocking processes:")
        try:
            # -m: Name of the mount point
            # -v: Verbose (shows USER, PID, ACCESS, COMMAND)
            res = subprocess.run(
                ["fuser", "-m", "-v", mount_point], capture_output=True, text=True
            )
            print(res.stdout)
        except Exception as e:
            print(f"[!] Could not run fuser: {e}")

    def remount_to_mapper(self, orig_dev, mapper_name):
        """Attempts to swap mount from physical device to virtual mapper."""
        mount_point = self.get_mount_point(orig_dev)
        mapper_dev = f"/dev/mapper/{mapper_name}"

        if not mount_point:
            print(f"[*] {orig_dev} is not mounted. No swap needed.")
            return None

        print(f"[*] Origin {orig_dev} is mounted at {mount_point}. Attempting swap...")

        # 1. First attempt at a clean unmount
        res = subprocess.run(["umount", mount_point], capture_output=True, text=True)

        if res.returncode != 0:
            # 2. If it fails, list why
            self.list_blocking_pids(mount_point)
            print(
                f"[!] ERROR: Cannot swap mount. Please close the above processes and try again."
            )
            return None

        # 3. If unmount succeeded, mount the mapper
        try:
            subprocess.run(["mount", mapper_dev, mount_point], check=True)
            print(f"[SUCCESS] {mapper_dev} is now live at {mount_point}")
            return mount_point
        except subprocess.CalledProcessError as e:
            print(
                f"[CRITICAL] Unmounted {orig_dev} but failed to mount {mapper_dev}: {e}"
            )
            return None

    def clone_header(self, orig, dest, size_mb=1):
        """Manually copies the first 1MB to sync disk labels and XFS superblocks."""
        print(
            f"[*] Manually cloning disk label and XFS header from {orig} to {dest}..."
        )
        try:
            # conv=fsync ensures the write hits the platter before we continue
            cmd = [
                "dd",
                f"if={orig}",
                f"of={dest}",
                f"bs={size_mb}M",
                "count=1",
                "conv=notrunc,fsync",
            ]
            res = subprocess.run(cmd, capture_output=True)
            return res.returncode == 0
        except Exception as e:
            print(f"[!] Header clone failed: {e}")
            return False

    def verify_xfs_magic(self, dev):
        """Checks if the first 4 bytes of a device match the XFS magic string."""
        try:
            with open(dev, "rb") as f:
                magic = f.read(4)
                # 0x58 46 53 42 is "XFSB" in ASCII
                return magic == b"XFSB"
        except Exception as e:
            print(f"[!] Could not read magic number from {dev}: {e}")
            return False

    def _get_size(self, dev):
        """Returns size in 512-byte sectors. Handles device:offset syntax."""
        clean_dev = dev.split(":")[0]

        if not os.path.exists(clean_dev):
            print(f"[!] Error: Device {clean_dev} not found.")
            sys.exit(1)

        res = subprocess.run(
            ["blockdev", "--getsz", clean_dev], capture_output=True, text=True
        )
        if res.returncode != 0:
            print(f"[!] Error: Could not get size for {clean_dev}")
            sys.exit(1)

        return int(res.stdout.strip())

    def validate_sizes(self, orig, dest):
        """Compares sectors of orig and dest. Returns True if dest is large enough."""
        orig_size = self._get_size(orig)
        dest_size = self._get_size(dest)

        print(
            f"[*] Size Check: Source ({orig_size} sectors) | Destination ({dest_size} sectors)"
        )

        if dest_size < orig_size:
            print(
                f"[!] ERROR: Destination device is smaller than the source by {orig_size - dest_size} sectors."
            )
            return False

        if dest_size > orig_size:
            print(
                f"[*] WARNING: Destination is larger than source. Excess space will be unusable."
            )

        return True

    def _hash_chunk(self, dev, offset_bytes, size_bytes):
        """Hashes a specific slice of a block device for integrity checks."""
        try:
            with open(dev, "rb") as f:
                f.seek(offset_bytes)
                chunk = f.read(size_bytes)
                return hashlib.sha256(chunk).hexdigest()
        except Exception as e:
            return str(e)

    def _run_dm(self, action, table=None):
        """Standard wrapper for dmsetup with command printing for transparency."""
        if table:
            print(f"[*] EXEC: echo '{table}' | dmsetup {action} {self.name}")
        else:
            print(f"[*] EXEC: dmsetup {action} {self.name}")

        cmd = ["dmsetup", action, self.name]
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

    def update_xfs_uuid(self, dev):
        """Generates a new, unique UUID for an XFS filesystem on the given device."""
        if not self.verify_xfs_magic(dev):
            return True  # Not XFS, skip

        print(
            f"[*] XFS detected on {dev}. Generating new UUID to prevent collisions..."
        )
        try:
            # -U generate creates a new random UUID
            res = subprocess.run(
                ["xfs_admin", "-U", "generate", dev], capture_output=True, text=True
            )
            if res.returncode == 0:
                print(f"[SUCCESS] New UUID generated for {dev}")
                return True
            else:
                print(f"[!] xfs_admin failed: {res.stderr}")
                return False
        except Exception as e:
            print(f"[!] Error updating UUID: {e}")
            return False

    def activate_passive(self, orig, dest, m_orig, m_dest):
        """Creates a RAID1 target in 'nosync' mode for safe LUN adoption."""
        size = self._get_size(orig)
        region_size = "1024"  # 512KB chunks
        # '1 nosync' ensures the destination isn't overwritten immediately
        table = f"0 {size} raid raid1 2 {region_size} nosync 2 {m_orig} {orig} {m_dest} {dest}"

        # Cleanup any stale mappings to prevent -EBUSY
        subprocess.run(["dmsetup", "remove", self.name], capture_output=True)
        return self._run_dm("create", table)

    def start_sync(self, orig, dest, m_orig, m_dest, throttle=None):
        """Reloads the table with max_recovery_rate to start sync."""
        size = self._get_size(orig)
        region_size = "1024"
        # TODO: add throttle support - not sure it is a good idea.
        # feat_args = f"2 max_recovery_rate {throttle}" if throttle else "1"

        table = f"0 {size} raid raid1 2 {region_size} sync 2 {m_orig} {orig} {m_dest} {dest}"

        if self._run_dm("suspend"):
            if self._run_dm("load", table):
                return self._run_dm("resume")
        self._run_dm("resume")
        return False

    def verify_integrity(self, orig, dest, samples=5, chunk_size=1024 * 1024):
        """Compares multiple 1MB chunks to ensure orig/dest are identical."""
        total_bytes = int(self._get_size(orig)) * 512
        offsets = [
            0,
            total_bytes // 4,
            total_bytes // 2,
            total_bytes * 3 // 4,
            total_bytes - chunk_size,
        ]

        print(f"[*] Verifying block integrity ({samples} samples)...")
        for i, offset in enumerate(offsets):
            h_orig = self._hash_chunk(orig, offset, chunk_size)
            h_dest = self._hash_chunk(dest, offset, chunk_size)

            if h_orig != h_dest:
                print(f"[!] INTEGRITY FAILURE at byte {offset}")
                return False
            print(f"    Sample {i+1}/{samples} match.")
        return True

    def stop(self):
        """Flushes buffers, settling XFS logs, then removes the device."""
        device_path = f"/dev/mapper/{self.name}"
        if os.path.exists(device_path):
            print(f"[*] Flushing buffers for {self.name}...")
            subprocess.run(["sync"], check=True)

            # Attempt fsfreeze to commit XFS log tail if still mounted
            with open("/proc/mounts", "r") as f:
                for line in f:
                    if device_path in line:
                        mnt = line.split()[1]
                        subprocess.run(["fsfreeze", "-f", mnt], capture_output=True)
                        subprocess.run(["fsfreeze", "-u", mnt], capture_output=True)

            subprocess.run(
                ["blockdev", "--flushbufs", device_path], capture_output=True
            )
            time.sleep(1)  # Final settle time

        return self._run_dm("remove")

    def get_status(self):
        """Parses dmsetup status for raw output and % completion."""
        res = subprocess.run(
            ["dmsetup", "status", self.name], capture_output=True, text=True
        )
        if res.returncode != 0:
            return None, "0.00%"
        raw = res.stdout.strip()
        match = re.search(r"(\d+)/(\d+)", raw)
        if match:
            synced, total = map(int, match.groups())
            pct = (synced / total * 100) if total > 0 else 0
            return raw, f"{pct:.2f}%"
        return raw, "0.00%"
