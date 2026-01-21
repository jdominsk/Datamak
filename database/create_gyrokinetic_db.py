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
            tokamak TEXT NOT NULL DEFAULT 'NSTX',
            creation_date TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(data_origin)").fetchall()}
    if "tokamak" not in columns:
        conn.execute("ALTER TABLE data_origin ADD COLUMN tokamak TEXT NOT NULL DEFAULT 'NSTX'")
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
            shot_time REAL,
            shot_variant TEXT,
            active INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0, 1)),
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            comment TEXT,
            FOREIGN KEY (data_origin_id) REFERENCES data_origin(id)
        )
        """
    )
    equil_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(data_equil)").fetchall()
    }
    if "shot_time" not in equil_columns and "time" in equil_columns:
        conn.execute("ALTER TABLE data_equil RENAME COLUMN time TO shot_time")
        equil_columns = {
            row[1] for row in conn.execute("PRAGMA table_info(data_equil)").fetchall()
        }
    if "shot_variant" not in equil_columns:
        conn.execute("ALTER TABLE data_equil ADD COLUMN shot_variant TEXT")
        equil_columns.add("shot_variant")
    if "comment" not in equil_columns:
        conn.execute("ALTER TABLE data_equil ADD COLUMN comment TEXT")
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_data_equil_shot_variant_time
        ON data_equil (data_origin_id, shot_number, shot_variant, shot_time)
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
    # Model configuration templates for GK inputs
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_model (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            active INTEGER NOT NULL DEFAULT 0 CHECK (active IN (0, 1)),
            gk_code_id INTEGER NOT NULL,
            is_linear INTEGER NOT NULL DEFAULT 0 CHECK (is_linear IN (0, 1)),
            is_adiabatic INTEGER NOT NULL DEFAULT 0 CHECK (is_adiabatic IN (0, 1, 2)),
            is_electrostatic INTEGER NOT NULL DEFAULT 1 CHECK (is_electrostatic IN (0, 1)),
            input_template TEXT NOT NULL,
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
            gk_model_id INTEGER NOT NULL,
            file_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            content TEXT NOT NULL,
            psin REAL NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('NEW', 'WAIT', 'TORUN', 'BATCH', 'CRASHED', 'SUCCESS')),
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
            beta REAL,
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
            FOREIGN KEY (gk_study_id) REFERENCES gk_study(id) ON DELETE CASCADE,
            FOREIGN KEY (gk_model_id) REFERENCES gk_model(id)
        )
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_input)").fetchall()}
    if "gk_model_id" not in columns:
        conn.execute("ALTER TABLE gk_input ADD COLUMN gk_model_id INTEGER")
    if "beta" not in columns:
        conn.execute("ALTER TABLE gk_input ADD COLUMN beta REAL")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_batch (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_database_name TEXT NOT NULL,
            remote_folder TEXT NOT NULL,
            remote_host TEXT,
            status TEXT NOT NULL
        )
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS gk_run (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            remote_id INTEGER NOT NULL,
            gk_input_id INTEGER,
            gk_batch_id INTEGER,
            input_folder TEXT,
            job_folder TEXT,
            archive_folder TEXT,
            input_name TEXT,
            nb_nodes INTEGER,
            job_id TEXT,
            status TEXT,
            input_content TEXT,
            remote_host TEXT,
            remote_folder TEXT,
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            t_max REAL,
            ky_abs_mean REAL,
            gamma_max REAL,
            diffusion REAL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_gk_run_remote_input_name
        ON gk_run (remote_host, remote_folder, input_name)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_gk_run_remote_id_batch
        ON gk_run (remote_id, gk_batch_id)
        """
    )
    columns = {row[1] for row in conn.execute("PRAGMA table_info(gk_run)")}
    if "remote_id" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN remote_id INTEGER NOT NULL DEFAULT 0")
    if "ky_abs_mean" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN ky_abs_mean REAL")
    if "gamma_max" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN gamma_max REAL")
    if "diffusion" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN diffusion REAL")
    if "t_max" not in columns:
        conn.execute("ALTER TABLE gk_run ADD COLUMN t_max REAL")

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
        CREATE TABLE IF NOT EXISTS gk_convergence_timeseries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            gk_run_id INTEGER NOT NULL,
            gk_input_id INTEGER NOT NULL,
            phi2_tot_f32 BLOB NOT NULL,
            n_points INTEGER NOT NULL DEFAULT 100 CHECK (n_points = 100),
            window_t_min REAL,
            window_t_max REAL,
            gamma_mean REAL,
            relstd REAL,
            slope_norm REAL,
            method TEXT NOT NULL DEFAULT 'A',
            r2 REAL,
            ky_abs_mean REAL,
            gamma_max REAL,
            diffusion REAL,
            is_converged INTEGER NOT NULL DEFAULT 0 CHECK (is_converged IN (0, 1)),
            creation_date TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (gk_run_id) REFERENCES gk_run(id) ON DELETE CASCADE,
            FOREIGN KEY (gk_input_id) REFERENCES gk_input(id) ON DELETE CASCADE
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
        CREATE UNIQUE INDEX IF NOT EXISTS uq_gk_study_key
        ON gk_study (data_equil_id, gk_code_id)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_gk_input_key
        ON gk_input (gk_study_id, gk_model_id, psin)
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
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gk_convergence_timeseries_run_id
        ON gk_convergence_timeseries (gk_run_id)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_gk_convergence_timeseries_input_id
        ON gk_convergence_timeseries (gk_input_id)
        """
    )
    conn.commit()


def seed_gk_code(conn: sqlite3.Connection) -> None:
    sql_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "insert_gk_code_GX_CGYRO.sql")
    with open(sql_path, "r", encoding="utf-8") as handle:
        conn.executescript(handle.read())
    conn.commit()


def seed_gk_model(conn: sqlite3.Connection) -> None:
    sql_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "insert_gk_model_templates.sql",
    )
    if not os.path.exists(sql_path):
        return
    with open(sql_path, "r", encoding="utf-8") as handle:
        conn.executescript(handle.read())
    conn.commit()


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(args.db)
    try:
        create_schema(conn)
        seed_gk_code(conn)
        seed_gk_model(conn)
    finally:
        conn.close()
    print(
        "Created "
        f"{args.db} with tables data_origin, data_equil, gk_code, gk_model, and gk_study."
    )


if __name__ == "__main__":
    main()
