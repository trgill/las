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

# The database file will be created in the same directory as the script
DB_PATH = "las_migration.db"

def _get_conn():
    """Helper to establish a connection with row access enabled."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Creates the migration table if it doesn't already exist."""
    with _get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                name TEXT PRIMARY KEY,
                orig TEXT NOT NULL,
                dest TEXT NOT NULL,
                meta_orig TEXT NOT NULL,
                meta_dest TEXT NOT NULL,
                throttle INTEGER,
                active INTEGER DEFAULT 0
            )
        """)

def record_migration(name, orig, dest, meta_orig, meta_dest, throttle):
    """Saves or updates a migration record."""
    init_db()
    with _get_conn() as conn:
        conn.execute("""
            INSERT OR REPLACE INTO migrations 
            (name, orig, dest, meta_orig, meta_dest, throttle, active)
            VALUES (?, ?, ?, ?, ?, ?, 1)
        """, (name, orig, dest, meta_orig, meta_dest, throttle))
        conn.commit()

def get_migration(name):
    """Retrieves a single migration record by name."""
    init_db()
    with _get_conn() as conn:
        res = conn.execute("SELECT * FROM migrations WHERE name = ?", (name,)).fetchone()
        return dict(res) if res else None

def list_all_migrations():
    """Returns a list of all migration records for the 'list' command."""
    init_db()
    with _get_conn() as conn:
        rows = conn.execute("SELECT * FROM migrations").fetchall()
        return [dict(row) for row in rows]

def update_throttle(name, throttle):
    """Updates the sync throttle value for an existing migration."""
    init_db()
    with _get_conn() as conn:
        conn.execute("UPDATE migrations SET throttle = ? WHERE name = ?", (throttle, name))
        conn.commit()

def delete_migration(name):
    """Removes a migration record upon completion ('break')."""
    init_db()
    with _get_conn() as conn:
        conn.execute("DELETE FROM migrations WHERE name = ?", (name,))
        conn.commit()