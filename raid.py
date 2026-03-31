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

# DM-RAID Constants
DM_RAID_MAGIC = 0x72616964  # 'raid' in ASCII
DM_RAID_VERSION = 1         # v1.1.0

def write_dm_raid_superblock(meta_dev, origin_dev_sz):
    """
    Writes a spec-compliant DM-RAID v1.1.0 superblock to Leg 0.
    
    Args:
        meta_dev (str): Path to the metadata device (e.g., /dev/sdc)
        origin_dev_sz (int): Size of the source data device in sectors
    """
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
        # Open in binary mode with direct synchronization
        fd = os.open(meta_dev, os.O_WRONLY | os.O_DIRECT | os.O_SYNC)
        with os.fdopen(fd, "wb") as f:
            # Write the header at the very beginning of the device
            f.write(final_header)
            
            # Pad the rest of the first 4KB sector with zeros
            padding = 4096 - len(final_header)
            f.write(b'\x00' * padding)
            
            f.flush()
            os.fsync(f.fileno())
            
        print(f"[SUCCESS] RAID Superblock written to {meta_dev} (Leg 0)")
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