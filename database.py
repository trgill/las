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
database interactions.
"""
import sqlite3
import os

DB_PATH = "/var/lib/las/migrations.db"

def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS migrations (
                name TEXT PRIMARY KEY, 
                origin TEXT NOT NULL, 
                target TEXT NOT NULL,
                meta_orig TEXT NOT NULL, 
                meta_dest TEXT NOT NULL,
                throttle INTEGER, 
                status TEXT DEFAULT 'nosync'
            )
        """
        )
        conn.commit()


def record_migration(name, orig, dest, m_orig, m_dest, throttle, active=True):
    status = "active" if active else "nosync"
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            REPLACE INTO migrations (name, origin, target, meta_orig, meta_dest, throttle, status) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
            (name, orig, dest, m_orig, m_dest, throttle, status),
        )
        conn.commit()


def get_migration(name):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT origin, target, meta_orig, meta_dest, throttle FROM migrations WHERE name=?",
            (name,),
        )
        return cursor.fetchone()


def mark_complete(name):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("UPDATE migrations SET status='completed' WHERE name=?", (name,))
        conn.commit()


def delete_migration(name):
    """Removes the migration record entirely from the database."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM migrations WHERE name=?", (name,))
        conn.commit()
