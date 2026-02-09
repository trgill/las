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
import pickle
import os

DB_PATH = "/etc/las.db"


class LunPair:
    def __init__(self, name, origin, destination):
        self.name = name
        self.origin = origin
        self.destination = destination
        self.is_boot = False


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS pairs (name TEXT PRIMARY KEY, data BLOB)")
    conn.commit()
    conn.close()


def save_pair(pair):
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO pairs VALUES (?, ?)", (pair.name, pickle.dumps(pair))
    )
    conn.commit()
    conn.close()


def get_pair(name):
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute("SELECT data FROM pairs WHERE name = ?", (name,)).fetchone()
    conn.close()
    return pickle.loads(row[0]) if row else None


def list_all_pairs():
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("SELECT data FROM pairs").fetchall()
    conn.close()
    return [pickle.loads(r[0]) for r in rows]


def delete_pair_from_db(name):
    conn = sqlite3.connect(DB_PATH)
    conn.execute("DELETE FROM pairs WHERE name = ?", (name,))
    conn.commit()
    conn.close()
