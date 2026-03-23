# LAS (Live Array Storage) Migration Tool

**LAS** is a specialized utility designed to migrate a running Linux system from a single block device to a mirrored **DM-RAID1** array without downtime. It uses an isolated "Pre-flight" boot strategy to ensure the migration is successful before any permanent changes are made to the system's primary boot configuration.

## 🚀 Key Features

* **Missing Leg Strategy:** Initializes a RAID1 mirror using the existing data as the "source of truth," cloning metadata to a new drive while keeping the original data intact.
* **Isolated Initramfs:** Generates a standalone `initramfs` containing a custom Dracut hook. This hook handles the RAID assembly and device-mapper discovery before the root filesystem is mounted.
* **Boom Integration:** Uses the `boom` boot manager to create a temporary, safe GRUB entry. If the RAID fails to assemble, the system remains bootable via the original standard kernel entries.
* **Persistent Pathing:** Automatically resolves `by-id` paths for all RAID members to ensure boot-time stability, even if drive letters (`/dev/sdX`) change.
* **Btrfs/XFS Awareness:** Automatically detects filesystem types and subvolume flags (e.g., `subvol=root`) to ensure the kernel can pivot to the new RAID device.

---

## 🏗 Architecture

The migration happens in three distinct phases:

1.  **Preparation (`prepare-root`):**
    * Detects the active root partition and filesystem parameters.
    * Initializes RAID metadata on the destination and metadata drives.
    * Injects a custom assembly script into a new, isolated `initramfs`.
    * Creates a `boom` boot entry pointing to the new RAID mapper.

2.  **The Pivot (Reboot):**
    * The user selects the **LAS-migration** entry in GRUB.
    * The custom hook waits for the physical disks to appear, assembles `/dev/mapper/migration`, and pivots the root to the array.

3.  **Synchronization (Post-Boot):**
    * Once booted into the RAID, the user can begin the background rebuild to synchronize the mirrors.

---

## 🛠 Usage

### 1. Prepare the Migration
Replace the device paths with your actual hardware IDs.

```bash
sudo ./las.py prepare-root \
  --name migration \
  --orig /dev/sda \
  --dest /dev/sdd \
  --meta-orig /dev/sdc \
  --meta-dest /dev/sdb
