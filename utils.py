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

def inject_las_assembly_hook(name, dm_table_string, wait_disks):
    """
    Creates a specialized, isolated Initramfs for LAS migration.
    Dynamically waits for specific disks to prevent -ENOENT errors.
    """
    # Create the shell-script snippet that waits for each required disk
    wait_logic = ""
    for disk in wait_disks:
        wait_logic += f"""
    echo "LAS: Waiting for {disk}..."
    i=0
    while [ $i -lt 15 ]; do
        [ -e "{disk}" ] && break
        sleep 1
        i=$((i+1))
    done
"""

    # The actual hook script content
    hook_content = f"""#!/bin/sh
# LAS Auto-Assembly Hook for {name}
echo "LAS: Settling storage hardware..."
udevadm settle --timeout=10

{wait_logic}

if [ ! -e /dev/mapper/{name} ]; then
    echo "LAS: Assembling RAID device '{name}'..."
    echo "{dm_table_string}" | dmsetup create {name}
fi
"""
    
    hook_path = f"/tmp/99-las-assemble-{name}.sh"
    
    try:
        # 1. Write the temporary hook script
        with open(hook_path, "w") as f:
            f.write(hook_content)
        os.chmod(hook_path, 0o755)

        # 2. Define the UNIQUE Initramfs path
        kver = subprocess.check_output(['uname', '-r'], text=True).strip()
        migration_img = f"/boot/initramfs-las-{name}.img"
        
        print(f"[*] Building isolated Initramfs: {migration_img}")
        
        # 3. Use Dracut to build the isolated image
        # --include: puts our hook into the pre-mount directory
        # --add-drivers: ensures dm-raid and raid1 are physically present
        # --install: ensures dmsetup is available in the shell
        subprocess.run([
            'sudo', 'dracut', '--force',
            '--add', 'dm',
            '--add-drivers', 'dm-raid raid1',
            '--install', 'dmsetup',
            '--include', hook_path, f'/usr/lib/dracut/hooks/pre-mount/99-las-assemble-{name}.sh',
            migration_img, kver
        ], check=True, capture_output=True)
        
        # Clean up the temp file
        if os.path.exists(hook_path):
            os.remove(hook_path)
            
        return migration_img

    except subprocess.CalledProcessError as e:
        print(f"[!] Dracut failed: {e.stderr.decode()}")
        return None
    except Exception as e:
        print(f"[!] Error creating migration image: {e}")
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