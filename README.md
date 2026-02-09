# ⚠️ WARNING: EXPERIMENTAL CODE
**DO NOT USE ON PRODUCTION SYSTEMS.**

## Known Issues & Technical Caveats
1. **Metadata Collisions:** Uses `dm` placeholders or `mdadm v1.0` to avoid overwriting sector 0 (XFS superblocks).
2. **Split-Brain:** Direct writes to origin during a live pair will bypass the mirror.
3. **Initramfs:** Boot migrations require rebuilding the initrd to include RAID modules.

## Usage
- **Data LUN:** `sudo ./las pair --name data --origin /dev/xxx --dest /dev/xxx`
- **Boot LUN:** `sudo ./las pair --name sys --origin /dev/xxx --dest /dev/xxx --boot`