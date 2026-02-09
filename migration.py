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
import shutil
import os


def check_dependencies(is_boot):
    """Verifies required system utilities are installed."""
    required = ["blockdev", "wipefs", "fsfreeze"]
    if is_boot:
        required.append("mdadm")
    else:
        required.append("dmsetup")

    missing = [tool for tool in required if shutil.which(tool) is None]
    if missing:
        print(f"Error: Missing required system tools: {', '.join(missing)}")
        return False
    return True


class DMEngine:
    def pair(self, name, origin, destination):
        subprocess.run(["modprobe", "dm-raid"], check=False)
        size = subprocess.check_output(["blockdev", "--getsz", origin]).decode().strip()
        table = f"0 {size} raid raid1 3 0 region_size 64 2 - {origin} - {destination}"
        proc = subprocess.Popen(
            ["dmsetup", "create", name],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        _, stderr = proc.communicate(input=table.encode())
        return proc.returncode == 0

    def takeover(self, name, origin):
        """Safely redirects a live mount to the DM device using fsfreeze."""
        mount_pt, fs_type, options = self._get_mount_info(origin)
        if not mount_pt:
            return False, "Origin is not currently mounted."

        # Pre-check: Is filesystem Read-Only (likely due to error)?
        if "ro" in options.split(","):
            return False, "Filesystem is Read-Only. Resolve errors before takeover."

        mapper_path = f"/dev/mapper/{name}"
        try:
            # 1. Freeze I/O
            subprocess.run(["fsfreeze", "--freeze", mount_pt], check=True)

            # 2. Remount to new device path
            cmd = ["mount", "-o", f"remount,{options}", mapper_path, mount_pt]
            res = subprocess.run(cmd, capture_output=True, text=True)

            # 3. Always Thaw
            subprocess.run(["fsfreeze", "--unfreeze", mount_pt], check=True)

            if res.returncode != 0:
                return False, f"Remount failed: {res.stderr.strip()}"
            return True, mount_pt
        except Exception as e:
            subprocess.run(["fsfreeze", "--unfreeze", mount_pt], check=False)
            return False, str(e)

    def _get_mount_info(self, device):
        real_dev = os.path.realpath(device)
        with open("/proc/mounts", "r") as f:
            for line in f:
                p = line.split()
                if os.path.realpath(p[0]) == real_dev:
                    return p[1], p[2], p[3]
        return None, None, None

    def get_status(self, name):
        try:
            res = subprocess.run(
                ["dmsetup", "status", name], capture_output=True, text=True
            )
            parts = res.stdout.split()
            sync_info = parts[6]
            done, total = map(int, sync_info.split("/"))
            return f"{(done/total)*100:.2f}%"
        except:
            return "0.00%"


class MDEngine:
    def pair(self, name, origin, destination):
        cmd = [
            "mdadm",
            "--create",
            f"/dev/md/{name}",
            "--run",
            "--level=1",
            "--raid-devices=2",
            "--metadata=1.0",
            "--homehost=any",
            origin,
            destination,
        ]
        res = subprocess.run(cmd, capture_output=True)
        return res.returncode == 0

    def get_status(self, name):
        try:
            res = subprocess.run(
                ["mdadm", "--detail", f"/dev/md/{name}"], capture_output=True, text=True
            )
            for line in res.stdout.split("\n"):
                if "Rebuild Status" in line:
                    return line.split(":")[-1].strip()
            return "100.00%"
        except:
            return "0.00%"


def get_engine(is_boot):
    return MDEngine() if is_boot else DMEngine()
