#!/usr/bin/env python3
#
# Copyright Red Hat
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
#!/usr/bin/env python3
#
# Copyright Red Hat
#
# snapm/_snapm.py - Snapshot Manager global definitions
#
# This file is part of the snapm project.
#
# SPDX-License-Identifier: Apache-2.0
"""
migration interactions.
"""
import subprocess
import re
import time
import utils

class RAIDEngine:
    def __init__(self, name):
        self.name = name
        self.sectors = None

    def init_raid_metadata(self, orig, dest, meta_orig, meta_dest):
        """
        Initializes RAID1 metadata using the 'Missing Leg' strategy.
        Initializes the destination as Leg 1 of a degraded array, then
        clones that valid metadata to the origin meta disk.
        """
        # 1. Calculate Sectors (Aligned to 1024)
        res = subprocess.run(['blockdev', '--getsz', orig], capture_output=True, text=True)
        self.sectors = (int(res.stdout.strip()) // 1024) * 1024

        print(f"[*] Targeting {self.sectors} sectors (1024 sector alignment)")

        # 2. Wipe existing signatures on destination and metadata disks
        for dev in [dest, meta_orig, meta_dest]:
            subprocess.run(['sudo', 'wipefs', '-a', dev], check=True)
        
        # 3. Create 'Degraded' RAID (Leg 0 is missing)
        # Table format: 0 <len> raid raid1 <#opts> <region> <nosync> <#legs> <m0> <d0> <m1> <d1>
        prime_name = f"las_prime_{self.name}"
        prime_table = (
            f"0 {self.sectors} raid raid1 2 1024 nosync 2 "
            f"- - {meta_dest} {dest}"
        )

        try:
            print("[*] Priming Leg 1 metadata (Leg 0 marked as missing)...")
            subprocess.run(['sudo', 'dmsetup', 'create', prime_name, '--table', prime_table], check=True)
            
            # Allow kernel to settle the bits
            time.sleep(2)
            
            # Remove the device to flush and close
            subprocess.run(['sudo', 'dmsetup', 'remove', prime_name], check=True)

            # 4. Clone metadata from Leg 1 to Leg 0
            # Since Leg 1 is now a valid RAID leg, its superblock is perfect.
            print(f"[*] Cloning valid metadata: {meta_dest} -> {meta_orig}")
            subprocess.run([
                'sudo', 'dd', 
                f'if={meta_dest}', 
                f'of={meta_orig}', 
                'bs=1M', 'count=10', 'status=none'
            ], check=True)

            print("[SUCCESS] Metadata initialized via Missing Leg Strategy.")
            return True

        except subprocess.CalledProcessError as e:
            print(f"[!] Metadata priming failed: {e}")
            return False

    def setup_boom_entry(self, img_path, fstype, fsflags):
        """
        Creates a Boom entry for live migration.
        Forces /boot to remain on the physical origin device while 
        moving other partitions to the migration mapper.
        """
        import subprocess
        import os
        import re

        clean_name = self.name.strip().replace(']', '').replace('[', '')

        # 1. Identify current root and partition index
        try:
            root_src = subprocess.check_output(['findmnt', '-n', '-o', 'SOURCE', '/'], text=True).strip()
            root_src = root_src.replace(']', '').replace('[', '').strip()
            
            root_idx_match = re.search(r'(\d+)(?:/.*)?$', root_src)
            if not root_idx_match:
                print(f"[!] Could not find partition index for {root_src}")
                return False
            root_idx = root_idx_match.group(1)

            # Detect the physical parent disk (e.g., sda)
            disk_match = re.match(r'/dev/([a-z]+|nvme\d+n\d+)', root_src)
            source_disk = disk_match.group(1) if disk_match else \
                subprocess.check_output(f"lsblk -no PKNAME {root_src.split('/')[2]} | head -n1", shell=True, text=True).strip()
        except Exception as e:
            print(f"[!] Detection error: {e}")
            return False

        # 2. Define the target mapper for Root
        separator = 'p' if clean_name[-1].isdigit() else ''
        root_mapper = f"/dev/mapper/{clean_name}{separator}{root_idx}"

        # 3. Process Mounts
        migration_opts = "x-systemd.device-timeout=60s,nofail"
        try:
            mount_data = subprocess.check_output([
                        'findmnt', '-l', '-n', '--real',
                        '-o', 'TARGET,SOURCE,FSTYPE,OPTIONS'
                    ], text=True).splitlines()   
        except Exception as e:
            print(f"[!] Mount scan error: {e}")
            return False


        mount_args = []
        boot_dev_display = "Unknown" # For the final print

        for line in mount_data:
            parts = line.split(None, 3)
            if len(parts) < 4: continue
            target, source, mnt_fstype, mnt_opts = parts
            # Root is handled by the primary 'root=' kernel parameter.
            # Adding it here via 'mount-extra' causes the generator crash.
            if target == "/" or target == "/root": 
                continue

            source = source.replace(']', '').replace('[', '').strip()
            
            # STICKY RULE: Only process things that look like real devices
            if not source.startswith('/dev/'): 
                continue

            dev_str = source

            if target == "/boot":
                dev_str = source # /dev/sda2
                boot_dev_display = source
            
            elif target == "/home" and mnt_fstype == "btrfs":
                if "subvol=" not in mnt_opts:
                    mnt_opts += ",subvol=home"
                dev_str = f"/dev/mapper/{clean_name}{separator}{root_idx}"
            
            elif source_disk in source:
                part_match = re.search(r'(\d+)(?:/.*)?$', source)
                idx = part_match.group(1) if part_match else os.path.basename(source)
                dev_str = f"/dev/mapper/{clean_name}{separator}{idx}"

            # 3. Create the pair: Mask the old, Add the new
            unit_name = target.strip('/').replace('/', '-')
            if unit_name:
                mount_args.append(f"systemd.mask={unit_name}.mount")
                
                clean_opts = mnt_opts.replace(",seclabel", "").replace("zstd:1", "zstd")
                final_opts = f"{clean_opts.strip(',')},{migration_opts}"
                mount_args.append(f"systemd.mount-extra={dev_str}:{target}:{mnt_fstype}:{final_opts}")

        # 4. Finalize Kernel Options
        kver = subprocess.check_output(['uname', '-r'], text=True).strip()
        clean_fsflags = fsflags.replace(",seclabel", "").replace("zstd:1", "zstd")
        
        # Keep rd.fstab=0 to avoid duplicate mount attempts from the on-disk fstab
        core_args = [
            f"root={root_mapper} ro",
            "rd.fstab=0 rd.retry=60 rootdelay=5",
            "rd.driver.pre=dm-raid rd.timeout=60 rd.dm=1",
            f"rootfstype={fstype if fstype else 'auto'}",
            f"rootflags={clean_fsflags},device={root_mapper}",
            "SYSTEMD_SULOGIN_FORCE=1 rd.shell loglevel=7"
        ]

        # Combine core args with our mount-extras and masksk
        all_args_list = core_args + mount_args

        # Ensure Console settings are last
        console_args = ["console=tty0", "console=ttyS0,115200"]
        all_args_list += console_args

        all_opts = " ".join(all_args_list)
        rel_img_path = img_path.replace("/boot", "", 1) if img_path.startswith("/boot") else img_path

        # 5. Create Boom Entry
        cmd = [
            'sudo', 'boom', 'entry', 'create', 
            '--title', f'LAS-{clean_name}',
            '--root-device', root_mapper,
            '--no-fstab',
            '-v', kver,
            '-i', rel_img_path.replace("//", "/"),
            '--add-opts', all_opts,
            '--no-dev'
        ]

        print(f"[*] LAS Entry: Root on Mapper, Boot on Physical {source}")
        subprocess.run(cmd, check=True)
        return True

    def activate_passive(self, orig, dest, m_orig, m_dest):
        """Creates a RAID1 target in 'nosync' mode for safe LUN adoption."""
        size = utils.get_block_size(orig)
        region_size = "1024"  # 512KB chunks
        table = f"0 {size} raid raid1 2 {region_size} nosync 2 {m_orig} {orig} {m_dest} {dest}"

        # Cleanup any stale mappings to prevent -EBUSY
        subprocess.run(["sudo", "dmsetup", "remove", self.name], capture_output=True)
        
        p = subprocess.Popen(['sudo', 'dmsetup', 'create', self.name], stdin=subprocess.PIPE, text=True)
        p.communicate(input=table)
        return p.returncode == 0
    
    def get_status(self):
        """Parses dmsetup status to extract sync percentage."""
        res = subprocess.run(['sudo', 'dmsetup', 'status', self.name], capture_output=True, text=True)
        if res.returncode != 0:
            return "Offline", "0%"

        raw = res.stdout.strip()
        match = re.search(r'(\d+)/(\d+)', raw)
        if match:
            synced, total = map(int, match.groups())
            pct = (synced / total * 100) if total > 0 else 0
            return raw, f"{pct:.2f}%"
        
        return raw, "Checking..."

    def start_sync(self, orig, dest, m_orig, m_dest, throttle=None):
        """Suspends device and reloads table to start background synchronization."""
        size = utils.get_block_size(orig)
        feat = f"2 max_recovery_rate {throttle}" if throttle else "0"
        table = f"0 {size} raid raid1 {feat} 1024 2 {m_orig} {orig} {m_dest} {dest}"

        subprocess.run(["sudo", "dmsetup", "suspend", self.name])
        p = subprocess.Popen(
            ["sudo", "dmsetup", "load", self.name], stdin=subprocess.PIPE, text=True
        )
        p.communicate(input=table)
        return subprocess.run(["sudo", "dmsetup", "resume", self.name]).returncode == 0

    def remount_to_mapper(self, orig_dev, hook_script=None):
        """Swaps physical mount for mapper mount live."""
        mount_point = utils.get_mount_point(orig_dev)
        if not mount_point:
            return None

        utils.run_hook(hook_script, "suspend")
        if subprocess.run(["sudo", "umount", mount_point]).returncode == 0:
            if (
                subprocess.run(
                    ["sudo", "mount", f"/dev/mapper/{self.name}", mount_point]
                ).returncode == 0
            ):
                utils.run_hook(hook_script, "resume")
                return mount_point
            # Rollback
            subprocess.run(["sudo", "mount", orig_dev, mount_point])
        else:
            utils.list_blocking_pids(mount_point)

        utils.run_hook(hook_script, "resume")
        return None
    
    def cleanup_boom_entry(self):
        """Removes the BLS boot entry created for the migration."""
        title_to_delete = f"LAS-{self.name}"
        print(f"[*] Cleaning up Boom boot entry for '{self.name}'...")
        
        try:
            cmd = ['sudo', 'boom', 'entry', 'delete', '--title', title_to_delete]
            res = subprocess.run(cmd, capture_output=True, text=True)
            
            if res.returncode == 0:
                print(f"[SUCCESS] Boot entry '{title_to_delete}' removed.")
                return True
            else:
                if "no matching entries" in res.stderr.lower():
                    print(f"[*] Note: No Boom entry found for '{title_to_delete}'.")
                    return True
                print(f"[!] Warning: Boom cleanup failed: {res.stderr.strip()}")
                return False
        except Exception as e:
            print(f"[!] Unexpected error during Boom cleanup: {e}")
            return False

    def stop(self):
        """Removes the device mapper device."""
        return (
            subprocess.run(
                ["sudo", "dmsetup", "remove", self.name], capture_output=True
            ).returncode == 0
        )
    
