#!/usr/bin/env python3
#
# Copyright Red Hat
#
# storage.py - Unified storage backend abstractions for LAS
#
# This file is part of the las project.
#
# SPDX-License-Identifier: Apache-2.0
"""
Storage backend abstraction layer.

Provides a common interface for partition-based (dm-raid) and LVM-based
(lvconvert RAID1) migrations. Concrete implementations live in
partition_backend.py and lvm_backend.py.
"""
from abc import ABC, abstractmethod
import subprocess

import utils


class StorageBackend(ABC):
    backend_type = None

    @abstractmethod
    def validate(self, origin, dest, **kwargs):
        """Pre-flight validation. Returns True if ready to proceed."""

    @abstractmethod
    def prepare(self, name, origin, dest, **kwargs):
        """Execute full preparation workflow. Returns True on success."""

    @abstractmethod
    def check_sync(self, name, record):
        """Returns (raw_status, percent_string, is_synced)."""

    @abstractmethod
    def break_mirror(self, name, record, commit=False):
        """Finalize migration by removing origin. Returns True on success."""

    @abstractmethod
    def revert(self, name, record):
        """Revert migration and clean up. Returns True on success."""

    @staticmethod
    def detect(origin):
        """Auto-detect backend from origin device."""
        from partition_backend import PartitionBackend
        from lvm_backend import LVMBackend

        lvm_info = utils.detect_lvm_info(origin)
        if lvm_info and lvm_info['is_pv']:
            return LVMBackend()
        return PartitionBackend()

    @staticmethod
    def from_record(record):
        """Reconstruct backend from a database migration record."""
        from partition_backend import PartitionBackend
        from lvm_backend import LVMBackend

        mtype = record.get('migration_type', '')
        if mtype == 'lvm':
            return LVMBackend()
        if mtype == 'partition':
            return PartitionBackend()
        # Backward compat for records created before migration_type column
        if not record.get('meta_orig') or record['meta_orig'] == '':
            return LVMBackend()
        return PartitionBackend()

    def _detect_fsinfo(self):
        cmd = ["findmnt", "-n", "-o", "FSTYPE,OPTIONS", "/"]
        fs_info = subprocess.check_output(cmd, text=True).strip().split()
        return fs_info[0], fs_info[1]
