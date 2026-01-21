#!/bin/bash
set -euo pipefail

python3 - <<'PY'
import os, sqlite3

src = "/u/jdominsk/DTwin/remote_gk_inputs/remote_gk_inputs_20260116_160506.work.db"
out_dir = "/u/jdominsk/DTwin/remote_gk_inputs/shards"
shards = 8
os.makedirs(out_dir, exist_ok=True)

src_conn = sqlite3.connect(src)
schema = "\n".join(r[0] for r in src_conn.execute(
    "SELECT sql FROM sqlite_master WHERE type='table' AND name='remote_gk_input'"
) if r[0])
if not schema:
    raise SystemExit("remote_gk_input table not found")

conns = []
for i in range(shards):
    path = os.path.join(out_dir, f"remote_gk_inputs_shard_{i}.db")
    if os.path.exists(path):
        os.remove(path)
    conn = sqlite3.connect(path)
    conn.execute(schema)
    conn.commit()
    conns.append(conn)

rows = src_conn.execute(
    "SELECT * FROM remote_gk_input WHERE status='PENDING'"
).fetchall()
cols = [d[1] for d in src_conn.execute("PRAGMA table_info(remote_gk_input)").fetchall()]
placeholders = ",".join("?" for _ in cols)
insert_sql = f"INSERT INTO remote_gk_input VALUES ({placeholders})"
for row in rows:
    shard = row[0] % shards
    conns[shard].execute(insert_sql, row)

for conn in conns:
    conn.commit()
    conn.close()
src_conn.close()
print("Shards created in", out_dir)
PY