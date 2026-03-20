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

def check_initramfs_capabilities():
    """Checks if the current boot image can handle DM-RAID."""
    kver = subprocess.run(['uname', '-r'], capture_output=True, text=True).stdout.strip()
    img = f"/boot/initramfs-{kver}.img"
    
    res = subprocess.run(['lsinitrd', img], capture_output=True, text=True)
    if "dm-raid" not in res.stdout:
        print(f"[!] CRITICAL: {img} is missing 'dm-raid' modules.")
        print("[*] Fix with: sudo dracut --add 'raid dm' --force")
        return False
    return True

def verify_initramfs_dm_support():
    """
    Checks if the current Initramfs has the drivers required 
    to process dm-mod.create at boot time.
    """
    kver = subprocess.run(['uname', '-r'], capture_output=True, text=True).stdout.strip()
    img = f"/boot/initramfs-{kver}.img"
    
    if not os.path.exists(img):
        return False, f"Initramfs not found at {img}"

    # lsinitrd lists all files in the initramfs
    res = subprocess.run(['lsinitrd', img], capture_output=True, text=True)
    contents = res.stdout
    
    # Check for the actual kernel modules
    has_dm_raid = "dm-raid.ko" in contents
    has_raid1 = "raid1.ko" in contents
    
    # Check for the dracut module that handles RAID assembly
    has_dracut_module = "modules.d/90dmraid" in contents or "modules.d/90mdraid" in contents

    if not (has_dm_raid and has_raid1):
        return False, (
            f"MISSING DRIVERS: {img} does not contain dm-raid or raid1.\n"
            f"Fix: sudo dracut --add-drivers 'dm-raid raid1' --force --no-hostonly"
        )
    
    return True, "Initramfs verified for DM-RAID support."

def rebuild_initramfs():
    """Forces a rebuild of the Initramfs with required RAID drivers."""
    kver = os.uname().release
    img = f"/boot/initramfs-{kver}.img"
    
    print(f"[*] Rebuilding {img} with dm-raid support...")
    
    # We use --add-drivers to bypass the missing 'raid' dracut module
    # and --no-hostonly to ensure it works even if RAID isn't active now.
    cmd = [
        'sudo', 'dracut', 
        '--add-drivers', 'dm-raid raid1', 
        '--force', 
        '--no-hostonly', 
        img
    ]
    
    try:
        res = subprocess.run(cmd, capture_output=True, text=True)
        if res.returncode == 0:
            print("[SUCCESS] Initramfs updated with dm-raid and raid1 drivers.")
            return True
        else:
            print(f"[!] Dracut Error: {res.stderr}")
            return False
    except Exception as e:
        print(f"[!] Failed to execute dracut: {e}")
        return False
    
def get_persistent_path(dev_path):
    """
    Translates a volatile /dev/sdX path into a persistent /dev/disk/by-id path.
    """
    if not dev_path.startswith('/dev/sd'):
        return dev_path # Already persistent or not a standard disk
        
    dev_name = os.path.basename(dev_path)
    base_id_path = '/dev/disk/by-id'
    
    if not os.path.exists(base_id_path):
        return dev_path

    for link in os.listdir(base_id_path):
        full_link = os.path.join(base_id_path, link)
        if os.path.realpath(full_link) == os.path.realpath(dev_path):
            # We prefer 'virtio-' or 'ata-' IDs over 'dm-uuid'
            return full_link
            
    return dev_path