#!/usr/bin/env python3
#
# Copyright Red Hat
#
# utils.py - Lift and Shift helpers
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

def inject_las_assembly_hook(name, p_orig, p_dest, p_m_orig, p_m_dest, partitions=None, throttle_kibs=3072):
    # DEFAULT RATE: 3072 KiB/s (~3 MB/s)
    # This balances boot responsiveness with reasonable sync speed.
    rate = throttle_kibs if throttle_kibs and throttle_kibs > 0 else 3072
    max_rate = rate * 10

    # Helper function to generate dynamic partition mappings
    def generate_partition_mappings(name, partitions):
        """Generate dmsetup linear mapping commands for all partitions."""
        mapping_lines = []
        for part in partitions:
            num = part['num']
            start = part['start']
            size = part['size']

            # dmsetup create requires: "0 <size> linear <device> <offset>"
            mapping_lines.append(
                f'if echo "0 {size} linear /dev/mapper/{name} {start}" | dmsetup create {name}{num}; then\n'
                f'    MAPPED=$((MAPPED + 1))\n'
                f'else\n'
                f'    echo "LAS: Warning - failed to map partition {num}"\n'
                f'    FAILED=$((FAILED + 1))\n'
                f'fi'
            )

        return '\n\n'.join(mapping_lines)

    # If partitions weren't provided, parse them from the origin device
    if not partitions:
        print(f"[*] Partition data not provided, parsing from {p_orig}")
        # Resolve persistent path to actual device
        import subprocess as sp
        actual_dev = sp.check_output(['readlink', '-f', p_orig], text=True).strip()
        # Remove partition number if present (e.g., /dev/sda1 -> /dev/sda)
        import re
        base_dev = re.sub(r'[0-9]+$', '', actual_dev)
        base_dev = re.sub(r'p[0-9]+$', '', base_dev)  # Handle nvme0n1p1 -> nvme0n1

        partitions = parse_partition_table(base_dev)
        if not partitions:
            print(f"[!] ERROR: Could not parse partition table from {base_dev}")
            print(f"[!] Cannot proceed without partition information")
            return None

    # Generate dynamic partition mappings
    partition_map_commands = generate_partition_mappings(name, partitions)
    print(f"[*] Using dynamic partition mapping for {len(partitions)} partitions")

    hook_content = f"""#!/bin/sh
# LAS Dynamic Assembly Hook
# Auto-generated for migration: {name}

echo "LAS: Starting hardware discovery..."
udevadm settle --timeout=30

# Force kernel to forget physical Btrfs signatures to avoid conflicts
/usr/sbin/btrfs device scan --forget 2>/dev/null || true

# Wait for physical source
i=0
while [ $i -lt 15 ]; do
    [ -e "{p_orig}" ] && break
    sleep 1
    i=$((i+1))
done

if [ ! -e "{p_orig}" ]; then
    echo "LAS: ERROR - Source disk {p_orig} not found!"
    exit 1
fi

echo "LAS: Source device {p_orig} ready"

# Prevent partition scanner conflicts
echo "LAS: Hiding physical partitions..."
partx -d {p_orig} 2>/dev/null || echo "LAS: partx not available, skipping partition hide"

# Clean up any stale mapper devices
if dmsetup info {name} >/dev/null 2>&1; then
    echo "LAS: Removing stale {name}..."
    dmsetup remove {name} 2>/dev/null || true
fi

# Flush buffers
blockdev --flushbufs {p_orig} 2>/dev/null || true

# Get disk size
SIZE=$(blockdev --getsz {p_orig})
echo "LAS: Disk size: $SIZE sectors"

# 1. Assemble the RAID Mirror
# rebuild 1 triggers the sync, but our throttled 'rate' keeps it from hanging the boot.
TABLE="0 $SIZE raid raid1 7 1024 rebuild 1 min_recovery_rate {rate} max_recovery_rate {max_rate} 2 {p_m_orig} {p_orig} {p_m_dest} {p_dest}"
echo "LAS: Assembling /dev/mapper/{name}..."
if ! echo "$TABLE" | dmsetup create {name}; then
    echo "LAS: ERROR - Failed to create RAID mirror"
    echo "LAS: Diagnostic information:"
    dmsetup ls || true
    lsblk -o NAME,MAJ:MIN,SIZE,TYPE,MOUNTPOINT || true
    exit 1
fi

# Verify RAID device appeared and wait for it to be ready
echo "LAS: Waiting for /dev/mapper/{name} to become ready..."
i=0
while [ $i -lt 30 ]; do
    if [ -b "/dev/mapper/{name}" ]; then
        echo "LAS: RAID device /dev/mapper/{name} is ready"
        break
    fi
    sleep 1
    i=$((i+1))
done

if [ ! -b "/dev/mapper/{name}" ]; then
    echo "LAS: ERROR - RAID device did not appear as a block device"
    dmsetup ls || true
    ls -la /dev/mapper/ || true
    exit 1
fi

udevadm settle --timeout=10

# 2. Manually Map the Partitions
# This bypasses partprobe issues by creating explicit linear targets
# that match exactly what your 'las.py' expects.
echo "LAS: Creating linear partition mappings..."

MAPPED=0
FAILED=0

{partition_map_commands}

echo "LAS: Mapped $MAPPED partitions ($FAILED failed)"

if [ $MAPPED -eq 0 ]; then
    echo "LAS: ERROR - No partitions were mapped successfully"
    exit 1
fi

# 3. Update /etc/fstab on the root filesystem to use mapper devices
# This prevents systemd from trying to mount old device partitions that are now RAID members
echo "LAS: Updating /etc/fstab to use mapper devices..."

# Resolve the persistent path to actual device name (e.g., /dev/disk/by-path/... -> /dev/sda)
ORIGIN_DEV=$(readlink -f {p_orig} | sed 's/[0-9]*$//')
echo "LAS: Origin disk: $ORIGIN_DEV"

# Mount root temporarily to update fstab
TEMP_ROOT="/tmp/las_root_$$"
mkdir -p "$TEMP_ROOT"

if mount -o ro /dev/mapper/{name}3 "$TEMP_ROOT" 2>/dev/null; then
    FSTAB="$TEMP_ROOT/etc/fstab"

    if [ -f "$FSTAB" ]; then
        # Remount read-write
        mount -o remount,rw "$TEMP_ROOT"

        # Backup original fstab
        cp "$FSTAB" "${{FSTAB}}.pre-las-$(date +%Y%m%d)"

        # Replace all partition references on the origin disk with mapper devices
        # Works for /dev/sda1, /dev/vda1, /dev/nvme0n1p1, etc.
        # Match the origin device followed by partition number, replace with mapper equivalent
        ORIGIN_BASE=$(basename "$ORIGIN_DEV")

        # Handle standard naming (sda, vda, hda, xvda)
        sed -i "s|${{ORIGIN_DEV}}1|/dev/mapper/{name}1|g" "$FSTAB"
        sed -i "s|${{ORIGIN_DEV}}2|/dev/mapper/{name}2|g" "$FSTAB"
        sed -i "s|${{ORIGIN_DEV}}3|/dev/mapper/{name}3|g" "$FSTAB"

        # Handle NVMe naming (nvme0n1p1)
        sed -i "s|${{ORIGIN_DEV}}p1|/dev/mapper/{name}1|g" "$FSTAB"
        sed -i "s|${{ORIGIN_DEV}}p2|/dev/mapper/{name}2|g" "$FSTAB"
        sed -i "s|${{ORIGIN_DEV}}p3|/dev/mapper/{name}3|g" "$FSTAB"

        echo "LAS: Updated /etc/fstab:"
        grep -v '^#' "$FSTAB" | grep -v '^$' || true

        # Sync and unmount
        sync
        umount "$TEMP_ROOT"
    else
        echo "LAS: WARNING - /etc/fstab not found in root filesystem"
        umount "$TEMP_ROOT"
    fi
else
    echo "LAS: WARNING - Could not mount root to update fstab"
fi

rm -rf "$TEMP_ROOT"

# 4. Final Announcement
udevadm trigger --action=add /dev/mapper/{name}* || true
udevadm settle --timeout=10
echo "LAS: Mapper hierarchy ready. Recovery running in background at {rate} KiB/s."
echo "LAS: RAID device: /dev/mapper/{name} with $MAPPED partition(s)"
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
        
        # Build command: Install tools needed for device conflict prevention
        cmd = [
            'sudo', 'dracut', '--force',
            '--add', 'dm',
            '--add-drivers', 'dm-raid raid1',
            '--install', 'dmsetup blockdev udevadm btrfs partx lsblk',
            '--include', tmp_hook_path, f'/usr/lib/dracut/hooks/pre-mount/{hook_filename}',
            migration_img, kver
        ]
        
        # Removed capture_output so you can see dracut progress and avoid 'tofu' hangs
        subprocess.run(cmd, check=True)
        
        subprocess.run(['sync'], check=True)
        return migration_img

    except subprocess.CalledProcessError as e:
        print(f"[!] Dracut failed.")
        return None
    
def inject_lvm_assembly_hook(name, p_orig, p_dest, p_m_orig, p_m_dest, vg_name, throttle_kibs=3072):
    """
    Generates an initramfs hook for LVM-based migrations.

    Differences from partition-based hook:
    - No partition table parsing or dm-linear mappings
    - Activates VG after RAID assembly via vgchange -ay
    - Root device is /dev/mapper/vg_name-lv_root

    Args:
        name (str): Migration name (e.g., "migration")
        p_orig, p_dest, p_m_orig, p_m_dest (str): Persistent device paths
        vg_name (str): LVM Volume Group name
        throttle_kibs (int): Initial sync throttle in KiB/s

    Returns:
        str: Path to generated initramfs, or None on failure
    """
    rate = throttle_kibs
    max_rate = rate * 10  # 10x throttle for max recovery rate

    hook_content = f"""#!/bin/sh
# LAS LVM Assembly Hook
# Auto-generated for migration: {name}
# Volume Group: {vg_name}

set -e  # Exit on error

echo "LAS-LVM: Starting hardware discovery..."
udevadm settle --timeout=30

# Wait for physical source PV
i=0
while [ $i -lt 15 ]; do
    [ -e "{p_orig}" ] && break
    sleep 1
    i=$((i+1))
done

if [ ! -e "{p_orig}" ]; then
    echo "LAS-LVM: ERROR - Source PV {p_orig} not found!"
    exit 1
fi

echo "LAS-LVM: Source PV {p_orig} ready"

# Deactivate origin VG to prevent conflicts
echo "LAS-LVM: Deactivating origin VG {vg_name}..."
vgchange -an {vg_name} 2>/dev/null || true

# Clean up any stale mapper devices
if dmsetup info {name} >/dev/null 2>&1; then
    echo "LAS-LVM: Removing stale {name}..."
    dmsetup remove {name} 2>/dev/null || true
fi

# Flush buffers
blockdev --flushbufs {p_orig} 2>/dev/null || true

# Get PV size
SIZE=$(blockdev --getsz {p_orig})
echo "LAS-LVM: PV size: $SIZE sectors"

# Assemble the RAID Mirror
TABLE="0 $SIZE raid raid1 7 1024 rebuild 1 min_recovery_rate {rate} max_recovery_rate {max_rate} 2 {p_m_orig} {p_orig} {p_m_dest} {p_dest}"
echo "LAS-LVM: Assembling /dev/mapper/{name}..."
if ! echo "$TABLE" | dmsetup create {name}; then
    echo "LAS-LVM: ERROR - Failed to create RAID mirror"
    echo "LAS-LVM: Diagnostic information:"
    dmsetup ls || true
    lsblk -o NAME,MAJ:MIN,SIZE,TYPE,MOUNTPOINT || true
    exit 1
fi

# Verify RAID device appeared
if [ ! -e "/dev/mapper/{name}" ]; then
    echo "LAS-LVM: ERROR - RAID device did not appear"
    exit 1
fi

udevadm settle --timeout=10

# Scan for LVM on the RAID mirror
echo "LAS-LVM: Scanning for Volume Groups..."
pvscan --cache /dev/mapper/{name} || true
vgscan --cache || true

# Activate the VG on the mirror
echo "LAS-LVM: Activating VG {vg_name} on /dev/mapper/{name}..."
if ! vgchange -ay {vg_name}; then
    echo "LAS-LVM: ERROR - Failed to activate VG {vg_name}"
    echo "LAS-LVM: VG status:"
    vgdisplay {vg_name} || true
    exit 1
fi

# Verify LVs are available
echo "LAS-LVM: Verifying Logical Volumes..."
lvscan | grep {vg_name} || true

udevadm settle --timeout=10
echo "LAS-LVM: Volume Group {vg_name} ready. Recovery running at {rate} KiB/s."
"""

    hook_name = f"99-las-lvm-assemble-{name}.sh"
    hook_path = f"/tmp/{hook_name}"

    try:
        with open(hook_path, 'w') as f:
            f.write(hook_content)
        os.chmod(hook_path, 0o755)

        print(f"[*] LVM assembly hook created: {hook_path}")
    except Exception as e:
        print(f"[!] Failed to write LVM hook: {e}")
        return None

    # Build initramfs with the hook
    kver = subprocess.check_output(['uname', '-r'], text=True).strip()
    img_name = f"initramfs-las-{name}.img"
    img_path = f"/boot/{img_name}"

    print(f"[*] Building initramfs with LVM support...")

    # dracut needs LVM modules and tools
    dracut_cmd = [
        "dracut",
        "--force",
        "--add", "lvm",  # Include LVM dracut module
        "--add", "dm",   # Include device-mapper module
        "--install", "lsblk dmsetup",  # Include diagnostic and dm tools
        "--include", hook_path, f"/usr/lib/dracut/hooks/pre-mount/{hook_name}",
        img_path,
        kver
    ]

    result = subprocess.run(dracut_cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"[!] Dracut failed: {result.stderr}")
        return None

    print(f"[SUCCESS] Initramfs created: {img_path}")
    return img_path


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

def regenerate_filesystem_uuid(device, fstype):
    """
    Generates a new UUID for a filesystem to prevent conflicts.

    Args:
        device (str): Block device path (e.g., /dev/sda1)
        fstype (str): Filesystem type (xfs, ext4, btrfs, etc.)

    Returns:
        bool: True if successful, False otherwise
    """
    import subprocess

    print(f"[*] Regenerating UUID for {device} ({fstype})...")

    try:
        if fstype == 'xfs':
            # XFS: xfs_admin -U generate
            subprocess.run(
                ['xfs_admin', '-U', 'generate', device],
                check=True,
                capture_output=True,
                text=True
            )
            print(f"[SUCCESS] XFS UUID regenerated on {device}")

        elif fstype in ['ext2', 'ext3', 'ext4']:
            # ext* family: tune2fs -U random
            subprocess.run(
                ['tune2fs', '-U', 'random', device],
                check=True,
                capture_output=True,
                text=True
            )
            print(f"[SUCCESS] ext{fstype[-1]} UUID regenerated on {device}")

        elif fstype == 'btrfs':
            # Btrfs: btrfstune -u (generates random UUID)
            subprocess.run(
                ['btrfstune', '-u', device],
                check=True,
                capture_output=True,
                text=True
            )
            print(f"[SUCCESS] Btrfs UUID regenerated on {device}")

        else:
            print(f"[!] Unsupported filesystem type for UUID regeneration: {fstype}")
            return False

        # Display new UUID
        result = subprocess.run(
            ['blkid', '-s', 'UUID', '-o', 'value', device],
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode == 0:
            new_uuid = result.stdout.strip()
            print(f"[*] New UUID: {new_uuid}")

        return True

    except subprocess.CalledProcessError as e:
        print(f"[!] Failed to regenerate UUID: {e}")
        if e.stderr:
            print(f"[!] Error: {e.stderr}")
        return False
    except Exception as e:
        print(f"[!] Unexpected error: {e}")
        return False


def detect_lvm_info(device):
    """
    Detects if a device is an LVM Physical Volume and extracts VG/LV information.

    Args:
        device (str): Device path (e.g., /dev/sda2, /dev/nvme0n1p2)

    Returns:
        dict or None: LVM info if PV detected, None otherwise

    Example return value:
    {
        'is_pv': True,
        'pv_name': '/dev/sda2',
        'pv_uuid': 'abc123...',
        'vg_name': 'fedora',
        'vg_uuid': 'def456...',
        'lvs': [
            {'lv_name': 'root', 'lv_path': '/dev/mapper/fedora-root', 'size_sectors': 123456},
            {'lv_name': 'home', 'lv_path': '/dev/mapper/fedora-home', 'size_sectors': 234567},
        ]
    }
    """
    import re
    import subprocess

    try:
        # Check if device is a Physical Volume
        # pvdisplay -c outputs colon-separated format:
        # /dev/sda2:fedora:251658240:-1:8:8:-1:4096:30719:0:30719:abc-123-def...
        pv_output = subprocess.check_output(
            ['pvdisplay', '-c', device],
            text=True,
            stderr=subprocess.DEVNULL
        ).strip()

        if not pv_output:
            return None  # Not a PV

        # Parse PV info
        fields = pv_output.split(':')
        pv_name = fields[0]
        vg_name = fields[1]
        pv_uuid = fields[11] if len(fields) > 11 else None

        if not vg_name or vg_name == '-':
            print(f"[!] {device} is a PV but not assigned to any VG")
            return None

        # Get VG UUID
        vg_uuid_output = subprocess.check_output(
            ['vgdisplay', '-c', vg_name],
            text=True,
            stderr=subprocess.DEVNULL
        ).strip()
        vg_uuid = vg_uuid_output.split(':')[11] if vg_uuid_output else None

        # Get all LVs in the VG
        # lvdisplay -c outputs: /dev/fedora/root:fedora:3:1:-1:1:...
        lv_output = subprocess.check_output(
            ['lvdisplay', '-c'],
            text=True,
            stderr=subprocess.DEVNULL
        ).strip().splitlines()

        lvs = []
        for line in lv_output:
            fields = line.split(':')
            lv_path = fields[0]
            lv_vg = fields[1]

            if lv_vg == vg_name:
                lv_name = lv_path.split('/')[-1]
                # Get size in sectors (512-byte)
                lv_size_output = subprocess.check_output(
                    ['blockdev', '--getsz', lv_path],
                    text=True
                ).strip()

                lvs.append({
                    'lv_name': lv_name,
                    'lv_path': lv_path,
                    'size_sectors': int(lv_size_output)
                })

        return {
            'is_pv': True,
            'pv_name': pv_name,
            'pv_uuid': pv_uuid,
            'vg_name': vg_name,
            'vg_uuid': vg_uuid,
            'lvs': lvs
        }

    except subprocess.CalledProcessError:
        # Not an LVM PV
        return None
    except Exception as e:
        print(f"[!] Error detecting LVM info for {device}: {e}")
        return None


def validate_lvm_migration(origin_lvm_info, dest):
    """
    Validates LVM migration readiness.

    Args:
        origin_lvm_info (dict): LVM info from detect_lvm_info()
        dest (str): Destination device path

    Returns:
        bool: True if valid, False otherwise
    """
    # Check destination is not also a PV
    dest_lvm = detect_lvm_info(dest)
    if dest_lvm:
        print(f"[!] Destination {dest} is already an LVM PV")
        print(f"[!] VG: {dest_lvm['vg_name']}")
        print(f"[!] Please use a clean device or wipe with: vgremove {dest_lvm['vg_name']} && pvremove {dest}")
        return False

    # Check we have at least one LV
    if not origin_lvm_info['lvs']:
        print(f"[!] VG {origin_lvm_info['vg_name']} has no Logical Volumes")
        return False

    # Check if multiple PVs in VG (not supported yet)
    try:
        pv_count_output = subprocess.check_output(
            ['vgdisplay', '-c', origin_lvm_info['vg_name']],
            text=True
        ).strip()
        # Field 9 is PV count
        pv_count = int(pv_count_output.split(':')[9])

        if pv_count > 1:
            print(f"[!] VG {origin_lvm_info['vg_name']} spans {pv_count} Physical Volumes")
            print(f"[!] Multi-PV migration is not supported yet")
            return False
    except Exception as e:
        print(f"[!] Could not verify PV count: {e}")
        return False

    print(f"[*] LVM validation passed:")
    print(f"    VG: {origin_lvm_info['vg_name']}")
    print(f"    PV: {origin_lvm_info['pv_name']}")
    print(f"    LVs: {', '.join([lv['lv_name'] for lv in origin_lvm_info['lvs']])}")

    return True


def regenerate_pv_uuid(device):
    """
    Regenerates LVM Physical Volume UUID to prevent conflicts.

    Used during break --commit to give origin PV a new UUID
    so it doesn't conflict with the migrated destination.

    Args:
        device (str): PV device path (e.g., /dev/sda2)

    Returns:
        bool: True if successful, False otherwise
    """
    import subprocess

    try:
        # Verify it's actually a PV
        lvm_info = detect_lvm_info(device)
        if not lvm_info or not lvm_info['is_pv']:
            print(f"[!] {device} is not an LVM Physical Volume")
            return False

        vg_name = lvm_info['vg_name']
        old_uuid = lvm_info['pv_uuid']

        print(f"[*] Current PV UUID: {old_uuid}")
        print(f"[*] Generating new UUID for {device}...")

        # pvchange -u generates a new random UUID
        result = subprocess.run(
            ['sudo', 'pvchange', '-u', device],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"[!] pvchange failed: {result.stderr}")
            return False

        # Verify new UUID
        new_lvm_info = detect_lvm_info(device)
        new_uuid = new_lvm_info['pv_uuid'] if new_lvm_info else None

        if new_uuid and new_uuid != old_uuid:
            print(f"[SUCCESS] PV UUID regenerated: {new_uuid}")
            print(f"[*] VG {vg_name} will need to rescan PVs on next boot")
            return True
        else:
            print(f"[!] UUID did not change")
            return False

    except Exception as e:
        print(f"[!] PV UUID regeneration failed: {e}")
        return False


def check_migration_readiness(source_dev, dest_dev, meta_orig, meta_dest, verbose=True):
    """
    Comprehensive pre-flight check for migration readiness.

    Args:
        source_dev (str): Source data partition
        dest_dev (str): Destination data partition
        meta_orig (str): Source metadata device
        meta_dest (str): Destination metadata device
        verbose (bool): Print detailed information

    Returns:
        bool: True if all checks pass, False otherwise
    """
    # Minimum sizes in sectors
    MIN_META_SECTORS = 2048  # 1 MB minimum for RAID metadata

    # Helper function to format sizes with appropriate units
    def format_size(sectors):
        """Format sector count with appropriate unit (KB, MB, GB)."""
        bytes_size = sectors * 512
        kb = bytes_size / 1024
        mb = kb / 1024
        gb = mb / 1024

        if gb >= 1.0:
            return f"{gb:.2f} GB"
        elif mb >= 1.0:
            return f"{mb:.2f} MB"
        else:
            return f"{kb:.2f} KB"

    if verbose:
        print("\n" + "="*60)
        print("LAS Migration Pre-Flight Check")
        print("="*60)

    try:
        # Get all device sizes
        src_sectors = get_block_size(source_dev)
        dest_sectors = get_block_size(dest_dev)
        meta_orig_sectors = get_block_size(meta_orig)
        meta_dest_sectors = get_block_size(meta_dest)

        if verbose:
            print(f"\n[*] Device Size Report:")
            print(f"    Source Data:      {source_dev}")
            print(f"      Size:           {src_sectors:,} sectors ({format_size(src_sectors)})")
            print(f"\n    Destination Data: {dest_dev}")
            print(f"      Size:           {dest_sectors:,} sectors ({format_size(dest_sectors)})")

            if dest_sectors >= src_sectors:
                extra = dest_sectors - src_sectors
                if extra > 0:
                    print(f"      Surplus:        {extra:,} sectors ({format_size(extra)}) ✓")
                else:
                    print(f"      Match:          Exact size match ✓")
            else:
                shortage = src_sectors - dest_sectors
                print(f"      SHORTAGE:       {shortage:,} sectors ({format_size(shortage)}) ✗")

            print(f"\n    Source Metadata:  {meta_orig}")
            print(f"      Size:           {meta_orig_sectors:,} sectors ({format_size(meta_orig_sectors)})")
            print(f"      Required:       {MIN_META_SECTORS:,} sectors ({format_size(MIN_META_SECTORS)})")

            if meta_orig_sectors >= MIN_META_SECTORS:
                print(f"      Status:         ✓ OK")
            else:
                print(f"      Status:         ✗ TOO SMALL")

            print(f"\n    Dest Metadata:    {meta_dest}")
            print(f"      Size:           {meta_dest_sectors:,} sectors ({format_size(meta_dest_sectors)})")
            print(f"      Required:       {MIN_META_SECTORS:,} sectors ({format_size(MIN_META_SECTORS)})")

            if meta_dest_sectors >= MIN_META_SECTORS:
                print(f"      Status:         ✓ OK")
            else:
                print(f"      Status:         ✗ TOO SMALL")

        # Validation checks
        checks_passed = True
        errors = []

        # Check 1: Destination must be >= source size
        if dest_sectors < src_sectors:
            shortage = src_sectors - dest_sectors
            errors.append(f"Destination is {shortage:,} sectors ({format_size(shortage)}) too small")
            checks_passed = False

        # Check 2: Metadata devices must meet minimum size
        if meta_orig_sectors < MIN_META_SECTORS:
            shortage = MIN_META_SECTORS - meta_orig_sectors
            errors.append(f"Source metadata device is {shortage:,} sectors ({format_size(shortage)}) too small (minimum {format_size(MIN_META_SECTORS)})")
            checks_passed = False

        if meta_dest_sectors < MIN_META_SECTORS:
            shortage = MIN_META_SECTORS - meta_dest_sectors
            errors.append(f"Destination metadata device is {shortage:,} sectors ({format_size(shortage)}) too small (minimum {format_size(MIN_META_SECTORS)})")
            checks_passed = False

        if verbose:
            print(f"\n{'='*60}")
            print(f"[*] Validation Results:")
            print(f"{'='*60}")

            if checks_passed:
                print(f"✓ All checks passed - migration is ready to proceed")
            else:
                print(f"✗ {len(errors)} error(s) found:")
                for i, err in enumerate(errors, 1):
                    print(f"  {i}. {err}")

            print(f"{'='*60}\n")

        return checks_passed

    except Exception as e:
        if verbose:
            print(f"\n[!] Error during pre-flight check: {e}")
        return False


def validate_migration_geometry(source_dev, dest_dev, meta_orig, meta_dest):
    """
    Checks all involved disks to ensure the migration will physically fit.
    (Legacy function - kept for backward compatibility)
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

def sync_partition_table(src, dest):
    """Copies the partition table from src to dest and fixes up GPT headers."""
    print(f"[*] Syncing partition table from {src} to {dest}...")
    try:
        dump = subprocess.check_output(['sfdisk', '-d', src])
        process = subprocess.Popen(['sfdisk', dest], stdin=subprocess.PIPE)
        process.communicate(input=dump)

        subprocess.run(['sgdisk', '-e', dest], check=True)
        subprocess.run(['partprobe', dest], check=True)
    except Exception as e:
        print(f"[!] Failed to sync geometry: {e}")
        return False
    subprocess.run(['udevadm', 'settle'], check=False)
    return True


def parse_partition_table(device):
    """
    Parses partition table from device and returns partition geometry.

    Args:
        device (str): Block device path (e.g., /dev/sda)

    Returns:
        List of dicts with partition info, e.g.:
        [
            {'num': 1, 'start': 2048, 'size': 1024000, 'type': 'EFI System'},
            {'num': 2, 'start': 1026048, 'size': 2097152, 'type': 'Linux filesystem'},
            {'num': 3, 'start': 3123200, 'size': 83886080, 'type': 'Linux filesystem'}
        ]
        Returns None on error.
    """
    import re
    import subprocess

    try:
        # Use sfdisk --dump (same as -d but more explicit)
        output = subprocess.check_output(
            ['sfdisk', '--dump', device],
            text=True,
            stderr=subprocess.DEVNULL
        )

        partitions = []
        for line in output.splitlines():
            # Match lines like: /dev/sda1 : start=2048, size=1024000, type=C12A7328-...
            match = re.match(r'^\s*\S+(\d+)\s*:\s*start=\s*(\d+),\s*size=\s*(\d+)', line)
            if match:
                part_num = int(match.group(1))
                start = int(match.group(2))
                size = int(match.group(3))

                # Extract type if present (optional, for debugging)
                type_match = re.search(r'type=([^,]+)', line)
                part_type = type_match.group(1) if type_match else 'unknown'

                partitions.append({
                    'num': part_num,
                    'start': start,
                    'size': size,
                    'type': part_type
                })

        if not partitions:
            print(f"[!] Warning: No partitions detected on {device}")
            return None

        return partitions

    except Exception as e:
        print(f"[!] Failed to parse partition table from {device}: {e}")
        return None