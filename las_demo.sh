#!/bin/bash
#

umount /dev/sda5 
./las
./las create --origin /dev/sda5 --dest /dev/sda6 --name migration --force
mount /dev/mapper/migration /mnt/migration

dd if=/dev/urandom of=/mnt/mp1/origin_test.img bs=1M count=3072 status=progress

sha256sum /mnt/migration/origin_test.img | tee /mnt/mp1/origin_test.sha256
./las list
./las status --name migration

ls -la /dev/mapper/migration
 
umount /dev/mapper/migration /mnt/migration

./las finish --name migration

mount /dev/sda5 /mnt/source
mount -o nouuid /dev/sda6 /mnt/target

diff /mnt/source/origin_test.sha256 /mnt/target/origin_test.sha256
