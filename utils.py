#!/usr/bin/env python3
#
# Copyright Red Hat
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
import subprocess
import os
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

def get_mount_point(dev):
    """Finds where a device is mounted."""
    with open('/proc/mounts', 'r') as f:
        for line in f:
            parts = line.split()
            if parts and parts[0] == dev:
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

def get_persistent_path(dev_path):
    """
    Returns the /dev/disk/by-id/ path for a disk or a partition.
    Correctly handles suffixes like -part3 for RAID members.
    """
    import os
    if not dev_path.startswith('/dev/'):
        return dev_path
    
    dev_name = os.path.basename(dev_path)
    by_id_dir = '/dev/disk/by-id'
    
    if os.path.exists(by_id_dir):
        for link in os.listdir(by_id_dir):
            full_link = os.path.join(by_id_dir, link)
            if os.path.realpath(full_link).endswith(dev_name):
                # Prefer scsi- or nvme- IDs over wwn- or ata-
                if link.startswith(('scsi-', 'nvme-')):
                    return full_link
    return dev_path

def inject_las_assembly_hook(name, p_orig, p_dest, p_m_orig, p_m_dest, throttle_kibs=51200):
    rate = throttle_kibs if throttle_kibs and throttle_kibs > 0 else 51200
    max_rate = rate * 2
    
    hook_content = f"""#!/bin/sh
# LAS Dynamic Assembly Hook (Lift and Shift)
echo "LAS: Starting hardware discovery..."
udevadm settle --timeout=30

i=0
while [ $i -lt 15 ]; do
    [ -e "{p_orig}" ] && break
    echo "LAS: Waiting for {p_orig}..."
    sleep 1
    i=$((i+1))
done

if [ -e "{p_orig}" ]; then
    SIZE=$(blockdev --getsz {p_orig})
    echo "LAS: Source {p_orig} found. Size: $SIZE sectors."
    
    # Corrected Parameter Count: 7
    TABLE="0 $SIZE raid raid1 7 1024 rebuild 1 min_recovery_rate {rate} max_recovery_rate {max_rate} 2 {p_m_orig} {p_orig} {p_m_dest} {p_dest}"
    
    echo "LAS: Assembling /dev/mapper/{name}..."
    echo "$TABLE" | dmsetup create {name}
    
    udevadm settle
    if [ -e "/dev/mapper/{name}" ]; then
        echo "LAS: Scanning for partitions..."
        # Try full path, fallback to simple command
        /usr/sbin/partprobe "/dev/mapper/{name}" 2>/dev/null || partprobe "/dev/mapper/{name}"
        udevadm settle
    fi
else
    echo "LAS: CRITICAL ERROR - Source disk not found!"
    exit 1
fi
"""

    hook_filename = f"99-las-assemble-{name}.sh"
    tmp_hook_path = os.path.join("/tmp", hook_filename)
    
    try:
        with open(tmp_hook_path, "w") as f:
            f.write(hook_content)
        os.chmod(tmp_hook_path, 0o755)

        kver = subprocess.check_output(['uname', '-r'], text=True).strip()
        migration_img = f"/boot/initramfs-las-{name}.img"
        
        print(f"[*] Generating LAS Initramfs: {migration_img}")
        
        # Build command
        cmd = [
            'sudo', 'dracut', '--force',
            '--add', 'dm',
            '--add-drivers', 'dm-raid raid1',
            '--install', 'dmsetup partprobe blockdev',
            '--include', tmp_hook_path, f'/usr/lib/dracut/hooks/pre-mount/{hook_filename}',
            migration_img, kver
        ]
        
        subprocess.run(cmd, check=True, capture_output=True)
        
        # Ensure bits are on disk
        subprocess.run(['sync'], check=True)
        
        if os.path.exists(tmp_hook_path):
            os.remove(tmp_hook_path)
            
        return migration_img

    except subprocess.CalledProcessError as e:
        print(f"[!] Dracut failed: {e.stderr.decode()}")
        return None

def remove_las_assembly_hook(name):
    """
    Removes the LAS hook and rebuilds the Initramfs to a standard state.
    """
    try:
        kver = subprocess.check_output(['uname', '-r'], text=True).strip()
        initrd_path = f"/boot/initramfs-{kver}.img"
        
        print(f"[*] Removing LAS hook for '{name}' and restoring Initramfs...")
        
        # We run dracut without the --include flag. 
        # This effectively rebuilds the image based on the system's standard 
        # configuration, dropping our custom script.
        subprocess.run(['sudo', 'dracut', '--force', initrd_path, kver], check=True)
        
        print(f"[SUCCESS] Initramfs restored. LAS hook removed.")
        return True
    except Exception as e:
        print(f"[!] Failed to clean Initramfs: {e}")
        return False
    

def get_root_filesystem_info(dev):
    """
    Detects the filesystem type and necessary mount flags (like subvolumes).
    """
    try:
        # Get the Type (xfs, btrfs, ext4)
        fstype_res = subprocess.run(
            ['blkid', '-o', 'value', '-s', 'TYPE', dev],
            capture_output=True, text=True, check=True
        )
        fstype = fstype_res.stdout.strip()

        # Default flags
        flags = "rw,relatime"

        # Special handling for Btrfs (Fedora/OpenSUSE)
        if fstype == "btrfs":
            # We look for the 'root' subvolume which is standard on Fedora
            flags += ",subvol=root"

        return fstype, flags
    except Exception as e:
        print(f"[!] Warning: Could not detect filesystem for {dev}: {e}")
        return "auto", "rw"
    
def get_root_device():
    """
    Finds the underlying partition device for the current root (/).
    Returns a path like /dev/sda3 or /dev/nvme0n1p3.
    """
    try:
        with open("/proc/mounts", "r") as f:
            for line in f:
                parts = line.split()
                # Find the entry where the mount point is exactly '/'
                if len(parts) > 1 and parts[1] == "/":
                    return parts[0]
    except Exception as e:
        print(f"[!] Error detecting root device: {e}")
    return None

def get_root_partition_info():
    """
    Uses findmnt to identify the root device and extract the partition index.
    Returns a tuple: (full_path, index) e.g. ("/dev/sda3", "3")
    """
    import subprocess
    import re

    try:
        # Get the source device for the root mount
        # -n (no headings), -o SOURCE (only the device path)
        root_dev = subprocess.check_output(
            ['findmnt', '-n', '-o', 'SOURCE', '/'], 
            text=True
        ).strip()

        # Extract the trailing digit (partition index)
        # Works for /dev/sda3 -> 3, or /dev/nvme0n1p3 -> 3
        match = re.search(r'(\d+)$', root_dev)
        part_index = match.group(1) if match else "3"
        
        return root_dev, part_index
    except Exception as e:
        print(f"[!] Error detecting root with findmnt: {e}")
        return "/dev/sda3", "3" # Fallback for standard Fedora
    
def prime_source_metadata(engine, origin, meta_orig):
    """
    Writes a minimal DM-RAID superblock to the source metadata device.
    This ensures the boot hook has a 'valid' starting point.
    """
    # 1. Zero out the start of the source metadata to clear old junk
    engine.wipe_metadata(meta_orig)
    
    # 2. Write the RAID superblock to meta_orig
    # This identifies 'origin' as the valid data source.
    return engine.write_dm_raid_superblock(meta_orig, origin_uuid=engine.get_uuid(origin))

def validate_migration_geometry(source_dev, dest_dev, meta_orig, meta_dest):
    """
    Checks all involved disks to ensure the migration will physically fit.
    """
    print(f"[*] Validating disk geometry for RAID assembly...")

    # Get sector counts using your existing function
    src_sectors = get_block_size(source_dev)
    dest_sectors = get_block_size(dest_dev)
    
    # Metadata devices also need a minimum size (usually ~4096 sectors for RAID1 metadata)
    meta_orig_sectors = get_block_size(meta_orig)
    meta_dest_sectors = get_block_size(meta_dest)

    print(f"    Source ({source_dev}): {src_sectors} sectors")
    print(f"    Destination ({dest_dev}): {dest_sectors} sectors")

    # 1. Check Primary Data Disk Size
    if dest_sectors < src_sectors:
        diff = src_sectors - dest_sectors
        print(f"\n[!] ERROR: Destination disk {dest_dev} is TOO SMALL.")
        print(f"    Missing {diff} sectors. Expand the disk in Virt-Manager.")
        return False

    # 2. Check Metadata Device Size (Safety check)
    # RAID1 metadata usually needs at least 8 sectors, but we'll check for 1MB (2048 sectors)
    if meta_orig_sectors < 2048 or meta_dest_sectors < 2048:
        print(f"\n[!] ERROR: Metadata disks ({meta_orig}/{meta_dest}) are too small.")
        return False

    print("[OK] Geometry validation passed.")
    return True