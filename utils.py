import subprocess
import os
import hashlib
import sys

def get_block_size(dev):
    """Returns size in 512-byte sectors."""
    clean_dev = dev.split(':')[0]
    if not os.path.exists(clean_dev):
        print(f"[!] Error: Device {clean_dev} not found.")
        sys.exit(1)
    res = subprocess.run(['blockdev', '--getsz', clean_dev], capture_output=True, text=True)
    if res.returncode != 0:
        print(f"[!] Error: Could not get size for {clean_dev}")
        sys.exit(1)
    return int(res.stdout.strip())

def verify_xfs_magic(dev):
    """Checks for XFSB signature."""
    try:
        with open(dev, 'rb') as f:
            return f.read(4) == b'XFSB'
    except Exception:
        return False

def clone_header(orig, dest, size_mb=1):
    """Bit-copy the start of the disk for labels/superblocks."""
    print(f"[*] Cloning first {size_mb}MB from {orig} to {dest}...")
    cmd = ['dd', f'if={orig}', f'of={dest}', f'bs={size_mb}M', 'count=1', 'conv=notrunc,fsync']
    return subprocess.run(cmd, capture_output=True).returncode == 0

def update_xfs_uuid(dev):
    """Generates a new UUID to prevent kernel collisions."""
    if not verify_xfs_magic(dev):
        return True
    print(f"[*] Generating new XFS UUID for {dev}...")
    return subprocess.run(['xfs_admin', '-U', 'generate', dev], capture_output=True).returncode == 0

def get_mount_point(dev):
    """Finds where a device is mounted."""
    with open('/proc/mounts', 'r') as f:
        for line in f:
            parts = line.split()
            if parts[0] == dev:
                return parts[1]
    return None

def list_blocking_pids(mount_point):
    """Runs fuser to identify blocking processes."""
    print(f"[!] Mount point {mount_point} is busy. Blocking processes:")
    try:
        res = subprocess.run(['fuser', '-m', '-v', mount_point], capture_output=True, text=True)
        print(res.stdout)
    except Exception as e:
        print(f"[!] Could not run fuser: {e}")

def run_hook(script_path, action):
    """Executes user-defined quiesce scripts."""
    if not script_path or not os.path.exists(script_path):
        return True
    print(f"[*] Invoking user hook ({action}): {script_path}...")
    res = subprocess.run([script_path, action], capture_output=True, text=True)
    return res.returncode == 0
