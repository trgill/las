LAS: Lift and Shift (RAID 1 Migration Tool) — TESTING ONLY

⚠️ WARNING: EXPERIMENTAL SOFTWARE
LAS (Lift and Shift) is currently in proof-of-concept only. This tool performs low-level manipulation of block devices, kernel boot parameters, and Initramfs structures.

    DATA LOSS RISK: Usage can result in an unbootable system or permanent data corruption.

    NO WARRANTY: This software is provided "as is," without warranty of any kind.

    REQUIREMENTS:

        DO NOT USE ON PRODUCTION SYSTEMS: This software is not ready for general use and should be considered a proof-of-concept only.


🚀 Features

    Live Abstraction: Moves the root (/), /boot, and /home partitions onto a dm-raid layer.

    Safe Boot Throttling: Automatically limits RAID resync speed during the initial migration boot to prevent I/O starvation and "Protocol Errors."

    Atomic Sync Control: Provides a robust way to unleash full disk performance once the system is stable using a safety-first dmsetup message approach.

    Boom Integration: Leverages boom and blscfg for safe, side-by-side boot entries.

    SELinux Aware: Gracefully handles the transition from unconfined migration states to fully labeled, enforcing environments.

🛠️ The Workflow
1. Preparation

Run the tool from your live system to identify disks and prepare the migration environment.
Bash

# Prepare the mirror on /dev/sdd using /dev/sdb and /dev/sdc for metadata
./las.py prepare-root --dest /dev/sdd --meta-orig /dev/sdb --meta-dest /dev/sdc --boot-throttle 5000

    Note: The --boot-throttle keeps the initial sync slow (e.g., 2.5MB/s) so the first boot remains responsive and avoids systemd timeouts.

2. The Migration Boot

Reboot the system and select the LAS-migration entry from the GRUB menu.

    The system will boot into Text Mode (Runlevel 3) with SELinux disabled (selinux=0).

    This is necessary because the new block device stack and uninitialized file labels would otherwise trigger "Access Denied" errors during the boot sequence.

    The initramfs hook assembles the RAID mirror and maps linear partitions (migration1, 2, 3) before mounting the root filesystem.

3. Unleash Performance

Once you have reached the login prompt and verified the mirror is healthy, increase the sync speed.
Bash

# Boost sync speed to 250MB/s (500,000 sectors/s)
./las.py sync --name migration --throttle 500000

    Mechanism: This command primarily uses dmsetup message, which updates the kernel's throttle live without suspending I/O, making it safe to use even under heavy system load.

4. Finalize Security

After the RAID status reaches [UU] in /proc/mdstat, trigger a relabel to re-enable SELinux and return to a standard boot profile.
Bash

touch /.autorelabel
reboot

🔍 Technical Architecture

LAS utilizes the Device Mapper framework to create a multi-layered block device stack:

    RAID Layer (dm-raid): Combines the origin and destination disks into a synchronized mirror.

    Linear Layer (dm-linear): Slices the RAID device into virtual partitions that mirror the original disk geometry.

    Filesystem Layer: The filesystem (Btrfs/EXT4) resides on top of the linear mapper devices, abstracted from the physical hardware swap.

⚠️ Troubleshooting
Frozen Shell / Hanging Commands

If the system hangs during a sync command (usually only if falling back to a table reload), the device may be suspended.

    Fix: Run sudo dmsetup resume migration from a separate TTY or SSH session.

Protocol Errors / Tofu Boxes

If you see character corruption or receive D-Bus protocol errors:

    This is a symptom of I/O starvation or SELinux interference. Ensure you are booting with selinux=0 and a low initial boot-throttle.

📜 License

Copyright 2026

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.