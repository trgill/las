# Copyright Red Hat
#
# raid.py - Lift and Shift RAID interface
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
import struct
import uuid
import zlib
import os
import subprocess

# DM-RAID Constants
DM_RAID_MAGIC = 0x72616964  # 'raid' in ASCII (little-endian)
DM_RAID_VERSION = 1         # v1.1.0

def check_dm_raid_version():
    """
    Check if dm-raid kernel module is available and log version info.

    Returns:
        bool: True if dm-raid appears available, False otherwise
    """
    try:
        # Check if dm-raid module is loaded or available
        modinfo = subprocess.check_output(
            ['modinfo', 'dm-raid'],
            text=True,
            stderr=subprocess.DEVNULL
        )

        # Extract version if present
        for line in modinfo.splitlines():
            if line.startswith('version:'):
                version = line.split(':', 1)[1].strip()
                print(f"[*] dm-raid module version: {version}")
                return True
            elif line.startswith('vermagic:'):
                # Fallback: kernel version info
                vermagic = line.split(':', 1)[1].strip()
                print(f"[*] dm-raid module vermagic: {vermagic}")
                return True

        print("[*] dm-raid module found")
        return True

    except subprocess.CalledProcessError:
        print("[!] WARNING: dm-raid kernel module not found")
        print("[!] RAID assembly may fail at boot")
        return False
    except Exception as e:
        print(f"[!] WARNING: Could not verify dm-raid version: {e}")
        return True  # Proceed anyway, but warn


def dump_raid_metadata(meta_dev):
    """
    Parse and display RAID metadata for debugging.

    Args:
        meta_dev (str): Path to metadata device (e.g., /dev/sdc)

    Returns:
        bool: True if metadata is valid, False otherwise
    """
    try:
        with open(meta_dev, "rb") as f:
            data = f.read(64)  # Read header + CRC

        if len(data) < 60:
            print(f"[!] Metadata too short: {len(data)} bytes")
            return False

        # Unpack the binary structure
        magic, features, num_dev, array_pos, events, failed, uuid_bytes, dev_sz, crc = struct.unpack(
            "<IIIIQQ16sQI", data[:60]
        )

        # Display parsed metadata
        print(f"\n[*] RAID Metadata on {meta_dev}:")
        print(f"    Magic:          {hex(magic)} {'✓ VALID' if magic == DM_RAID_MAGIC else '✗ INVALID'}")
        print(f"    Features:       {features}")
        print(f"    Num Devices:    {num_dev}")
        print(f"    Array Position: {array_pos} {'(Primary)' if array_pos == 0 else '(Secondary)'}")
        print(f"    Events:         {events}")
        print(f"    Failed Devices: {failed}")
        print(f"    UUID:           {uuid_bytes.hex()[:16]}...")
        print(f"    Device Size:    {dev_sz} sectors ({(dev_sz * 512) / (1024**3):.2f} GB)")
        print(f"    CRC32:          {hex(crc)}")

        # Verify CRC
        header_data = struct.pack(
            "<IIIIQQ16sQ",
            magic, features, num_dev, array_pos, events, failed, uuid_bytes, dev_sz
        )
        calc_crc = zlib.crc32(header_data) & 0xffffffff

        if crc == calc_crc:
            print(f"    CRC Check:      ✓ PASS")
        else:
            print(f"    CRC Check:      ✗ FAIL (calculated: {hex(calc_crc)})")
            return False

        print()
        return magic == DM_RAID_MAGIC

    except FileNotFoundError:
        print(f"[!] Metadata device not found: {meta_dev}")
        return False
    except Exception as e:
        print(f"[!] Could not read metadata: {e}")
        return False


def write_dm_raid_superblock(meta_dev, origin_dev_sz):
    """
    Writes a spec-compliant DM-RAID v1.1.0 superblock to Leg 0.

    This function exists because dm-raid requires pre-existing metadata
    to determine which RAID leg is authoritative during migration.

    Binary Format (Little Endian):
    ┌────────┬──────┬────────────────┬────────────┬───────────────────────────┐
    │ Offset │ Size │ Field          │ Value      │ Purpose                   │
    ├────────┼──────┼────────────────┼────────────┼───────────────────────────┤
    │ 0x00   │ 4    │ magic          │ 0x72616964 │ 'raid' in ASCII           │
    │ 0x04   │ 4    │ features       │ 0          │ No special features       │
    │ 0x08   │ 4    │ num_devices    │ 2          │ RAID1 = 2 legs            │
    │ 0x0C   │ 4    │ array_position │ 0          │ This is Leg 0 (primary)   │
    │ 0x10   │ 8    │ events         │ 1          │ Event counter             │
    │ 0x18   │ 8    │ failed_devices │ 0          │ No failed devices         │
    │ 0x20   │ 16   │ uuid           │ random     │ Unique array ID           │
    │ 0x30   │ 8    │ dev_size       │ sectors    │ Device size (512b blocks) │
    │ 0x38   │ 4    │ crc32          │ calculated │ CRC32 of preceding fields │
    └────────┴──────┴────────────────┴────────────┴───────────────────────────┘

    Why Manual Writing:
    - Origin disk already contains live data that must be preserved
    - Destination disk is uninitialized and must sync FROM origin
    - We need kernel to treat origin as authoritative (Leg 0)
    - initramfs environment is too minimal for complex initialization
    - Boot hook uses 'rebuild 1' which means "sync Leg 1 from Leg 0"

    Reference: Linux kernel source drivers/md/dm-raid.c
    Version: Targets dm-raid v1.1.0 (stable since kernel 3.8+)

    Args:
        meta_dev (str): Path to the metadata device (e.g., /dev/sdc)
        origin_dev_sz (int): Size of the source data device in sectors

    Returns:
        bool: True if successful, False otherwise
    """
    # Check dm-raid availability before writing
    check_dm_raid_version()
    # 1. Generate a unique UUID for the RAID set
    raid_uuid = uuid.uuid4().bytes

    # 2. Define the binary structure (Internal Metadata Layout)
    # Format: < (Little Endian)
    # I: magic (4), I: features (4), I: num_devices (4), I: array_position (4)
    # Q: events (8), Q: failed_devices (8), 16s: uuid (16), Q: dev_size (8)
    # I: crc (4)
    
    # We pack everything EXCEPT the CRC first to calculate the checksum
    pre_crc_format = "<IIIIQQ16sQ"
    header_data = struct.pack(pre_crc_format,
        DM_RAID_MAGIC,    # Magic: 'raid'
        0,                # Features (none)
        2,                # Total devices in RAID
        0,                # This is Leg 0 (Source)
        1,                # Event counter (start at 1)
        0,                # Failed devices
        raid_uuid,        # 16-byte UUID
        origin_dev_sz     # Size in 512b sectors
    )

    # 3. Calculate CRC32
    # The kernel expects a CRC32 of the header fields
    header_crc = zlib.crc32(header_data) & 0xffffffff

    # 4. Final binary assembly (Header + CRC)
    final_header = header_data + struct.pack("<I", header_crc)

    try:
        # Prepare a 4KB aligned buffer (required for O_DIRECT)
        buffer = bytearray(4096)
        buffer[:len(final_header)] = final_header
        # Rest of buffer is already zeros from bytearray initialization

        # Open with O_DIRECT for unbuffered I/O
        fd = os.open(meta_dev, os.O_WRONLY | os.O_DIRECT | os.O_SYNC)
        try:
            # Write the 4KB-aligned buffer directly
            bytes_written = os.write(fd, bytes(buffer))
            if bytes_written != 4096:
                raise IOError(f"Incomplete write: {bytes_written}/4096 bytes")
            os.fsync(fd)
        finally:
            os.close(fd)

        print(f"[SUCCESS] RAID Superblock written to {meta_dev} (Leg 0)")

        # Verify what we just wrote
        print(f"[*] Verifying metadata...")
        if not dump_raid_metadata(meta_dev):
            print(f"[!] WARNING: Metadata verification failed")
            return False

        return True
    except Exception as e:
        print(f"[!] Error writing RAID metadata: {e}")
        return False

def wipe_metadata(meta_dev):
    """
    Clears the first 1MB of a metadata device to ensure no ghost RAIDs exist.
    """
    try:
        with open(meta_dev, "wb") as f:
            f.write(b'\x00' * (1024 * 1024))
            f.flush()
        return True
    except Exception as e:
        print(f"[!] Could not wipe {meta_dev}: {e}")
        return False