# LAS: Lift and Shift - TESTING ONLY

## ⚠️ WARNING: EXPERIMENTAL SOFTWARE

**LAS (Lift and Shift)** is currently in **Alpha** status. This tool performs low-level manipulation of block devices, kernel boot parameters, and Initramfs structures. 

* **DATA LOSS RISK:** Incorrect usage can result in an unbootable system or permanent data corruption.
* **NO WARRANTY:** This software is provided "as is," without warranty of any kind. 
* **REQUIREMENTS:**
    * Always perform a full backup of your data before attempting a migration.
    * Testing in a Virtual Machine (KVM/QEMU) is **strongly recommended** before running on bare metal.
    * Familiarity with the Linux Emergency Shell and `dmsetup` is required for recovery in case of failure.

---

## Overview
**LAS** is a specialized utility designed to migrate a running Linux root filesystem onto a RAID-1 mirror with minimal downtime. It achieves this by intercepting the boot process via a custom Initramfs hook, assembling a degraded DM-RAID device, and pivoting the root mount to the new mirrored structure.

## Status Report: March 19–20 Milestones
During this period, the project shifted from static configuration to a dynamic, self-assembling architecture:

* **Dynamic Initramfs Generation:** Switched to a custom Dracut-based image generation strategy. The system now injects a specialized hook (`99-las-assemble-migration.sh`) into the pre-mount phase.
* **Tooling Injection:** Automated the inclusion of `dmsetup`, `partprobe`, and `blockdev` into the bootloader environment.
* **Read Stability (The `rebuild 1` Fix):** Implemented the `rebuild 1` flag in the DM-RAID table to force the kernel to read exclusively from the original source disk, successfully resolving Btrfs checksum (`csum`) errors.
* **Partition Discovery:** Resolved "Missing Device" timeouts by integrating `udevadm settle` and `partprobe` within the boot hook, ensuring `/dev/mapper/migrationX` nodes are populated before the real root is mounted.

## Usage
The primary entry point is `las.py`. 

```bash
./las.py prepare-root --name migration --orig /dev/sda --dest /dev/sdd --meta-orig /dev/sdc --meta-dest /dev/sdb
```

### Core Components
* **`las.py`**: Main orchestrator for the Lift and Shift workflow.
* **`utils.py`**: Handles Dracut hook injection and persistent device path resolution.
* **`dm.py`**: Manages Device Mapper table generation and Boom/Bootloader configurations.

---

**Would you like me to add a "Troubleshooting" section specifically for the Btrfs metadata errors we solved last week?**
