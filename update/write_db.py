"""Writes organized cable data to an existing database or a new database.
"""
import sqlite3
from pathlib import Path
from json import load
from clean_data import parse_data


def write_db(
    cleaned_data=None,
    data_file="./update/data/current_data",
    db_dir="./update/db/",
    db_name ="scn.db"
    ):
    """
    Invariant: If given data directly (and not given a file), 
    the data must already be cleaned and properly formatted.

    THIS FUNCTION ASSUMES IT'S OKAY TO DELETE THE DATABASE AT THE PATH 
    (db_dir/db_name).absolute().resolve()
    """
    data_file = Path(data_file).absolute()
    db_dir = Path(db_dir).absolute()

    # Load the data
    if (not cleaned_data) and data_file.resolve().exists():
        with open(data_file.resolve(), "r") as f:
            data = load(f)
        cleaned_data = parse_data(data)
    else:
        # TODO: Return something to indicate a problem!
        return

    # Build path for database file
    db_path = (db_dir / db_name).absolute()

    # Delete existing database if it exists
    # THIS FUNCTION ASSUMES IT'S OKAY TO DELETE THE PROVIDED DATABSE 
    # (db_dir/db_name).absolute().resolve()!
    if db_path.resolve().exists():
        db_path.resolve().unlink(missing_ok=True)

    # Create database directory
    db_path.mkdir(parents=True, exist_ok=True)

    # Connect to database (sqlite3 creates the database file if not exists)
    db = sqlite3.connect(
        db_path.resolve(),
        detect_types=sqlite3.PARSE_DECLTYPES
        )

    # Handling storage of boolean types from https://stackoverflow.com/a/16936992.
    sqlite3.register_adapter(bool, int)
    sqlite3.register_converter("BOOLEAN", lambda v: v != '0')
    cur = db.cursor()

    ## Create the tables (if they don't exist)
    # Primary tables
    cur.execute("""CREATE TABLE IF NOT EXISTS cable(
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL,
                code TEXT NOT NULL,
                url TEXT,
                length INTEGER,
                rfs_year INTEGER,
                rfs_text TEXT,
                planned BOOLEAN,
                notes TEXT
                )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS country(
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
                )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS point(
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL,
                code TEXT NOT NULL, 
                country_id INTEGER NOT NULL,
                FOREIGN KEY(country_id) REFERENCES country(id)
                )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS supplier(
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
                )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS owner(
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
                )""")
    db.commit()

    # Intersection tables
    cur.execute("""CREATE TABLE IF NOT EXISTS cable_point(
                point_cable_id INTEGER NOT NULL PRIMARY KEY,
                cable_id INTEGER NOT NULL,
                point_id INTEGER NOT NULL,
                FOREIGN KEY(cable_id) REFERENCES cable(id),
                FOREIGN KEY(point_id) REFERENCES point(id)
                )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS cable_owner(
                owner_cable_id INTEGER NOT NULL PRIMARY KEY,
                cable_id INTEGER NOT NULL,
                owner_id INTEGER NOT NULL,
                FOREIGN KEY(cable_id) REFERENCES cable(id),
                FOREIGN KEY(owner_id) REFERENCES owner(id)
                )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS cable_supplier(
                supplier_cable_id INTEGER NOT NULL PRIMARY KEY,
                cable_id INTEGER NOT NULL,
                supplier_id INTEGER NOT NULL,
                FOREIGN KEY(cable_id) REFERENCES cable(id),
                FOREIGN KEY(supplier_id) REFERENCES supplier(id)
                )""")
    db.commit()

    # Table insertions
    for cable_vals in cleaned_data["cable"]:
        cur.execute("""INSERT INTO cable (
                    id, name, code, url, length, rfs_year, rfs_text, planned, notes)
                    VALUES (?,?,?,?,?,?,?,?,?)""", cable_vals)
    db.commit()

    for country in cleaned_data["country"]:
        country_id = cleaned_data["country"][country]
        cur.execute("""INSERT INTO country (id, name) VALUES (?,?)""",
                    [country_id, country])
    db.commit()

    for p in cleaned_data["point"]:
        point = cleaned_data["point"][p]
        cur.execute("""INSERT INTO point (id, code, name, country_id) 
                    VALUES (?,?,?,?)""",
                    [point.id, point.code, point.name, point.country_id])
        # Intersection table
        for cable_id in point.cables:
            cur.execute("""INSERT INTO cable_point (point_id, cable_id)
                        VALUES (?,?)""", [point.id, cable_id])
    db.commit()

    for o in cleaned_data["owner"]:
        o_id = cleaned_data["owner"][o][0]
        cur.execute("""INSERT INTO owner (id, name) VALUES (?,?)""",
                    [o_id, o])
        for cable_id in cleaned_data["owner"][o][1]:
            cur.execute("""INSERT INTO cable_owner (owner_id, cable_id) VALUES (?,?)""",
                        [o_id, cable_id])
    db.commit()

    for s in cleaned_data["supplier"]:
        s_id = cleaned_data["supplier"][s][0]
        cur.execute("""INSERT INTO supplier (id, name) VALUES (?,?)""",
                    [s_id, s])
        for cable_id in cleaned_data["supplier"][s][1]:
            cur.execute("""INSERT INTO cable_supplier (supplier_id, cable_id) VALUES (?,?)""",
                        [s_id, cable_id])
    db.commit()
