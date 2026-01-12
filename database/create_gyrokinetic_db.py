#!/usr/bin/env python3
import argparse
import os
import sqlite3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a SQLite DB with a pfile/gfile table.",
    )
    parser.add_argument(
        "--db",
        default="gyrokinetic_simulations.db",
        help="Path to the SQLite database file.",
    )
    return parser.parse_args()


def create_schema(conn: sqlite3.Connection) -> None:
    # Origin of the Equil (pfile,gfile)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_origin (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            origin TEXT NOT NULL,
            copy TEXT NOT NULL,
            creation_date TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    # Database of equilibrium based on pfile and gfile
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS data_equil (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_origin_id INTEGER NOT NULL,
            folder_path TEXT NOT NULL,
            pfile TEXT,
            pfile_content TEXT,
            gfile TEXT,
            gfile_content TEXT,
            transpfile TEXT,
            shot_number TEXT,
            time REAL,
            active INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0, 1)),
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (data_origin_id) REFERENCES data_origin(id)
        )
        """
    )
    # Gyrokinetic codes used to compute the QL or turbulent transport
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_code (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL CHECK (name IN ('GX', 'CGYRO')),
            version TEXT NOT NULL,
            creation_date TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    # This refers to a study of an equilrium with a given gyrokinetic code
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_study (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data_equil_id INTEGER NOT NULL,
            gk_code_id INTEGER NOT NULL,
            COMMENT  TEXT NOT NULL,
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (data_equil_id) REFERENCES data_equil(id),
            FOREIGN KEY (gk_code_id) REFERENCES gk_code(id)
        )
        """
    )
    # Input files for each run (radial location, linear and nonlinear, kinetic electrons or not, collisions or not,...) many inputs points toward one simu
    # This could contains description of MHD, plasma profiles, and
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_input (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gk_study_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            content TEXT NOT NULL,
            psin REAL NOT NULL,
            is_linear INTEGER NOT NULL DEFAULT 0 CHECK (is_linear IN (0, 1)),
            is_adiabatic_electron INTEGER NOT NULL DEFAULT 0 CHECK (is_adiabatic_electron IN (0, 1)),
            status TEXT NOT NULL CHECK (status IN ('WAIT', 'TORUN', 'BATCH', 'CRASHED', 'SUCCESS')),
            comment TEXT NOT NULL DEFAULT '',
            geo_option TEXT,
            rhoc REAL,
            Rmaj REAL,
            R_geo REAL,
            qinp REAL,
            shat REAL,
            shift REAL,
            akappa REAL,
            akappri REAL,
            tri REAL,
            tripri REAL,
            betaprim REAL,
            electron_z REAL,
            electron_mass REAL,
            electron_dens REAL,
            electron_temp REAL,
            electron_temp_ev REAL,
            electron_tprim REAL,
            electron_fprim REAL,
            electron_vnewk REAL,
            ion_z REAL,
            ion_mass REAL,
            ion_dens REAL,
            ion_temp REAL,
            ion_temp_ev REAL,
            ion_tprim REAL,
            ion_fprim REAL,
            ion_vnewk REAL,
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (gk_study_id) REFERENCES gk_study(id) ON DELETE CASCADE
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_batch (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_database_name TEXT NOT NULL,
            remote_folder TEXT NOT NULL,
            status TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_linear_run (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gk_study_id INTEGER NOT NULL,
            ky REAL NOT NULL,
            gamma REAL NOT NULL,
            omega REAL NOT NULL,
            units TEXT DEFAULT 'norm',
            mode INTEGER DEFAULT 1,
            remote_host TEXT,
            remote_path TEXT,
            status TEXT NOT NULL CHECK (status IN ('WAIT', 'TORUN', 'BATCH', 'CRASHED', 'SUCCESS')),
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (gk_study_id) REFERENCES gk_study(id) ON DELETE CASCADE,
            UNIQUE (gk_study_id, ky, mode)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_nonlinear_run (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gk_study_id INTEGER NOT NULL,
            t_min REAL NOT NULL,
            t_max REAL NOT NULL,
            q_i_avg REAL,
            q_e_avg REAL,
            pflux_i_avg REAL,
            pflux_e_avg REAL,
            phi2_avg REAL,
            remote_host TEXT,
            remote_path TEXT,
            status TEXT NOT NULL CHECK (status IN ('WAIT', 'TORUN', 'BATCH', 'CRASHED', 'SUCCESS')),
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (gk_study_id) REFERENCES gk_study(id) ON DELETE CASCADE,
            UNIQUE (gk_study_id, t_min, t_max)
        )
        """
    )

    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gk_study_data_equil_id
        ON gk_study (data_equil_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gk_study_gk_code_id
        ON gk_study (gk_code_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_data_equil_data_origin_id
        ON data_equil (data_origin_id)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_data_equil_pfile_gfile
        ON data_equil (data_origin_id, pfile, gfile)
        WHERE pfile IS NOT NULL AND gfile IS NOT NULL
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_data_equil_transpfile_time
        ON data_equil (data_origin_id, transpfile, time)
        WHERE transpfile IS NOT NULL
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gk_input_gk_study_id
        ON gk_input (gk_study_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gk_linear_run_gk_study_id
        ON gk_linear_run (gk_study_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gk_nonlinear_run_gk_study_id
        ON gk_nonlinear_run (gk_study_id)
        """
    )
    conn.commit()


def seed_gk_code(conn: sqlite3.Connection) -> None:
    sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "insert_gk_code_GX_CGYRO.sql")
    with open(sql_path, "r", encoding="utf-8") as handle:
        conn.executescript(handle.read())
    conn.commit()


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        create_schema(conn)
        seed_gk_code(conn)
    finally:
        conn.close()
    print(
        f"Created {args.db} with tables data_origin, data_equil, gk_code, and gk_study."
    )


if __name__ == "__main__":
    main()
