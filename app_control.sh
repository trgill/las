#!/bin/bash
# Example script to quiesce applications
ACTION=$1

case $ACTION in
    suspend)
        echo "Locking database and freezing XFS..."
        # mysql -e "FLUSH TABLES WITH READ LOCK;" 
        # xfs_freeze -f /mnt/migration
        exit 0
        ;;
    resume)
        echo "Unlocking database and thawing XFS..."
        # xfs_freeze -u /mnt/migration
        # mysql -e "UNLOCK TABLES;"
        exit 0
        ;;
    *)
        exit 1
        ;;
esac
