import os, sys
import psycopg2
import psycopg2.extras
from datetime import date

STAGES = ['FIT UP', 'WELDING', 'BLASTING & PAINTING', 'SEND TO SITE']

WORKER_TYPES = [
    'Cutting Man', 'Supervisor', 'Foremen', 'Fitter', 'Welder',
    'Helper', 'Semi Skill', 'Material Coordinator', 'Material Handler',
]
SHIFT_LABELS = ['Regular', 'OT→6:30', 'OT→7:30', 'OT→10:00', 'Sun/PH']
SHIFT_KEYS   = ['regular', 'ot1', 'ot2', 'ot3', 'sun_ph']
SHIFT_HOURS  = {'regular': 7.5, 'ot1': 8.5, 'ot2': 9.5, 'ot3': 11.5, 'sun_ph': 7.5}


# ── PostgreSQL connection wrapper ──────────────────────────────────────────────

class _CurWrap:
    """Thin cursor wrapper: provides fetchone/fetchall/lastrowid over psycopg2 cursor."""
    def __init__(self, cur):
        self._cur = cur

    def fetchone(self):
        return self._cur.fetchone()  # RealDictRow (dict subclass) or None

    def fetchall(self):
        return self._cur.fetchall() or []  # list of RealDictRow

    @property
    def lastrowid(self):
        """Fetch the id returned by a RETURNING id clause."""
        row = self._cur.fetchone()
        return row['id'] if row else None


class _DBConn:
    """psycopg2 connection wrapper that mimics sqlite3 usage patterns.
    Converts ? placeholders to %s and uses RealDictCursor for dict-like row access."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        sql = sql.replace('?', '%s')
        cur = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params or [])
        return _CurWrap(cur)

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


def _conn():
    import streamlit as st
    import socket
    from urllib.parse import urlparse, unquote

    url = st.secrets['database_url']
    p = urlparse(url)

    # Resolve hostname to IPv4 — Streamlit Cloud cannot reach Supabase over IPv6
    host = p.hostname
    try:
        host = socket.getaddrinfo(host, None, socket.AF_INET)[0][4][0]
    except Exception:
        pass  # fall back to hostname if IPv4 lookup fails

    conn = psycopg2.connect(
        host=host,
        port=p.port or 5432,
        dbname=p.path.lstrip('/'),
        user=p.username,
        password=unquote(p.password or ''),
        sslmode='require',
    )
    return _DBConn(conn)


# ── Schema initialisation ──────────────────────────────────────────────────────

def init():
    db = _conn()

    # Core tables
    db.execute("""
        CREATE TABLE IF NOT EXISTS assemblies (
            assembly_mark   TEXT PRIMARY KEY,
            total_weight_kg DOUBLE PRECISION DEFAULT 0,
            description     TEXT DEFAULT ''
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS parts (
            id                SERIAL PRIMARY KEY,
            assembly_mark     TEXT NOT NULL,
            sub_assembly_mark TEXT DEFAULT '',
            part_mark         TEXT DEFAULT '',
            no                INTEGER DEFAULT 1,
            name              TEXT DEFAULT '',
            profile           TEXT DEFAULT '',
            kg_per_m          DOUBLE PRECISION DEFAULT 0,
            length_mm         DOUBLE PRECISION DEFAULT 0,
            total_weight_kg   DOUBLE PRECISION DEFAULT 0,
            profile2          TEXT DEFAULT '',
            grade             TEXT DEFAULT '',
            remark            TEXT DEFAULT '',
            FOREIGN KEY (assembly_mark) REFERENCES assemblies(assembly_mark)
        )
    """)
    db.execute("""
        CREATE TABLE IF NOT EXISTS progress (
            id                SERIAL PRIMARY KEY,
            entry_date        TEXT NOT NULL,
            assembly_mark     TEXT NOT NULL,
            stage             TEXT NOT NULL,
            weight_kg         DOUBLE PRECISION DEFAULT 0,
            qty               INTEGER DEFAULT 0,
            inspector         TEXT DEFAULT '',
            remarks           TEXT DEFAULT '',
            created_at        TIMESTAMPTZ DEFAULT NOW(),
            sub_assembly_mark TEXT DEFAULT '',
            delivery_order_no TEXT DEFAULT ''
        )
    """)
    db.commit()

    # Idempotent column migrations
    for col_sql in [
        "ALTER TABLE progress ADD COLUMN IF NOT EXISTS sub_assembly_mark TEXT DEFAULT ''",
        "ALTER TABLE progress ADD COLUMN IF NOT EXISTS delivery_order_no TEXT DEFAULT ''",
        "ALTER TABLE parts    ADD COLUMN IF NOT EXISTS remark TEXT DEFAULT ''",
    ]:
        db.execute(col_sql)
    db.commit()

    # Data migration: rename old DELIVERY stage
    db.execute("UPDATE progress SET stage='SEND TO SITE' WHERE stage='DELIVERY'")
    db.commit()

    # Sync assembly totals from parts (fixes stale values on startup)
    db.execute("""
        UPDATE assemblies
        SET total_weight_kg = (
            SELECT COALESCE(SUM(total_weight_kg), 0)
            FROM parts WHERE assembly_mark = assemblies.assembly_mark
        )
    """)
    db.commit()

    # Users table
    db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            SERIAL PRIMARY KEY,
            username      TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role          TEXT DEFAULT 'user',
            active        INTEGER DEFAULT 1
        )
    """)
    db.commit()

    if db.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()['cnt'] == 0:
        db.execute(
            "INSERT INTO users (username, password_hash, role) VALUES (?,?,?)",
            ('admin', _hash('admin123'), 'admin')
        )
        db.commit()

    # Manpower tables
    db.execute("""
        CREATE TABLE IF NOT EXISTS manpower (
            id          SERIAL PRIMARY KEY,
            entry_date  TEXT NOT NULL UNIQUE,
            regular     INTEGER DEFAULT 0,
            ot1         INTEGER DEFAULT 0,
            ot2         INTEGER DEFAULT 0,
            ot3         INTEGER DEFAULT 0,
            sun_ph      INTEGER DEFAULT 0,
            created_at  TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    db.commit()

    db.execute("""
        CREATE TABLE IF NOT EXISTS manpower_detail (
            id          SERIAL PRIMARY KEY,
            entry_date  TEXT NOT NULL,
            worker_type TEXT NOT NULL,
            shift       TEXT NOT NULL,
            count       INTEGER DEFAULT 0,
            UNIQUE(entry_date, worker_type, shift)
        )
    """)
    db.commit()

    # Drawings table — files stored as BYTEA in the database
    db.execute("""
        CREATE TABLE IF NOT EXISTS drawings (
            id            SERIAL PRIMARY KEY,
            title         TEXT NOT NULL,
            original_name TEXT NOT NULL,
            filename      TEXT NOT NULL UNIQUE,
            assembly_mark TEXT DEFAULT '',
            uploaded_by   TEXT DEFAULT '',
            created_at    TIMESTAMPTZ DEFAULT NOW(),
            file_data     BYTEA
        )
    """)
    db.execute("ALTER TABLE drawings ADD COLUMN IF NOT EXISTS file_data BYTEA")
    db.commit()

    # Project settings table
    db.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key   TEXT PRIMARY KEY,
            value TEXT DEFAULT ''
        )
    """)
    db.commit()

    db.close()
    init_raw_materials()
    init_sessions()


def get_project_name():
    c = _conn()
    row = c.execute("SELECT value FROM settings WHERE key='project_name'").fetchone()
    c.close()
    return row['value'] if row else 'Fabrication Tracker'


def set_project_name(name):
    c = _conn()
    c.execute("""
        INSERT INTO settings (key, value) VALUES ('project_name', ?)
        ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value
    """, (name.strip(),))
    c.commit()
    c.close()


def _hash(password):
    import hashlib
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


# ── Users ──────────────────────────────────────────────────────────────────────

def authenticate(username, password):
    c = _conn()
    row = c.execute(
        "SELECT id, username, role FROM users "
        "WHERE LOWER(username)=LOWER(?) AND password_hash=? AND active=1",
        (username.strip(), _hash(password))
    ).fetchone()
    c.close()
    return dict(row) if row else None


def get_users():
    c = _conn()
    rows = c.execute(
        "SELECT id, username, role, active FROM users ORDER BY username"
    ).fetchall()
    c.close()
    return [dict(r) for r in rows]


def add_user(username, password, role='user'):
    c = _conn()
    try:
        c.execute("INSERT INTO users (username, password_hash, role) VALUES (?,?,?)",
                  (username.strip(), _hash(password), role))
        c.commit()
        return True
    except Exception:
        return False
    finally:
        c.close()


def update_user_password(uid, new_password):
    c = _conn()
    c.execute("UPDATE users SET password_hash=? WHERE id=?", (_hash(new_password), uid))
    c.commit()
    c.close()


def update_user_role(uid, role):
    c = _conn()
    c.execute("UPDATE users SET role=? WHERE id=?", (role, uid))
    c.commit()
    c.close()


def toggle_user_active(uid):
    c = _conn()
    c.execute("UPDATE users SET active = 1 - active WHERE id=?", (uid,))
    c.commit()
    c.close()


def delete_user_entry(uid):
    c = _conn()
    c.execute("DELETE FROM users WHERE id=?", (uid,))
    c.commit()
    c.close()


# ── Raw Material Delivery ──────────────────────────────────────────────────────

def init_raw_materials():
    c = _conn()
    c.execute("""
        CREATE TABLE IF NOT EXISTS raw_materials (
            id            SERIAL PRIMARY KEY,
            received_date TEXT NOT NULL,
            do_no         TEXT DEFAULT '',
            description   TEXT DEFAULT '',
            grade         TEXT DEFAULT '',
            qty           DOUBLE PRECISION DEFAULT 0,
            total_kg      DOUBLE PRECISION DEFAULT 0,
            remark        TEXT DEFAULT '',
            created_at    TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    c.execute("ALTER TABLE raw_materials ADD COLUMN IF NOT EXISTS do_no TEXT DEFAULT ''")
    c.execute("ALTER TABLE raw_materials ADD COLUMN IF NOT EXISTS total_kg DOUBLE PRECISION DEFAULT 0")
    c.commit()
    c.close()


def add_raw_material(received_date, do_no, description, grade, qty, total_kg=0, remark=''):
    c = _conn()
    cur = c.execute(
        "INSERT INTO raw_materials (received_date, do_no, description, grade, qty, total_kg, remark) "
        "VALUES (?,?,?,?,?,?,?) RETURNING id",
        (str(received_date), do_no.strip(), description.strip(), grade.strip(), qty, float(total_kg), remark.strip())
    )
    rid = cur.lastrowid
    c.commit()
    c.close()
    return rid


def get_raw_material_summary():
    """Return overall totals: total entries, total qty, total kg received."""
    c = _conn()
    row = c.execute(
        "SELECT COUNT(*) as entries, "
        "COALESCE(SUM(qty),0) as total_qty, "
        "COALESCE(SUM(total_kg),0) as total_kg "
        "FROM raw_materials"
    ).fetchone()
    c.close()
    return dict(row) if row else {'entries': 0, 'total_qty': 0, 'total_kg': 0}


def get_raw_materials(start=None, end=None):
    c = _conn()
    if start and end:
        rows = c.execute(
            "SELECT * FROM raw_materials WHERE received_date BETWEEN ? AND ? "
            "ORDER BY received_date DESC", (str(start), str(end))
        ).fetchall()
    else:
        rows = c.execute(
            "SELECT * FROM raw_materials ORDER BY received_date DESC"
        ).fetchall()
    c.close()
    return [dict(r) for r in rows]


def delete_raw_material(rid):
    c = _conn()
    c.execute("DELETE FROM raw_materials WHERE id=?", (rid,))
    c.commit()
    c.close()
    _reorder_raw_materials()


def _reorder_raw_materials():
    """Renumber raw_materials IDs sequentially after a deletion."""
    c = _conn()
    rows = c.execute(
        "SELECT received_date, do_no, description, grade, qty, total_kg, remark, created_at "
        "FROM raw_materials ORDER BY id"
    ).fetchall()
    c.execute("DELETE FROM raw_materials")
    c.execute("ALTER SEQUENCE raw_materials_id_seq RESTART WITH 1")
    for r in rows:
        c.execute(
            "INSERT INTO raw_materials "
            "(received_date, do_no, description, grade, qty, total_kg, remark, created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (r['received_date'], r['do_no'], r['description'], r['grade'],
             r['qty'], r['total_kg'], r['remark'], r['created_at'])
        )
    c.commit()
    c.close()


def import_raw_materials_excel(file_source):
    """Import raw materials from Excel.
    file_source can be a file path (str) or bytes/BytesIO object.
    Expected columns (row 1 header): Received Date, D.O. Number, Description, Grade, Qty, Remark
    Returns (count, error_message). error_message is None on success.
    """
    try:
        import openpyxl
        from io import BytesIO as _BytesIO
        if isinstance(file_source, (bytes, bytearray)):
            file_source = _BytesIO(file_source)
        wb = openpyxl.load_workbook(file_source, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        # find header row by looking for 'Description' or 'Received Date'
        header_row = next(
            (i for i, r in enumerate(rows)
             if r and any(str(v).strip().lower() in ('description', 'received date')
                          for v in r if v)),
            None
        )
        if header_row is None:
            return 0, "Header row not found. Ensure row 1 has: Received Date, D.O. Number, Description, Grade, Qty, Remark"
        headers = [str(h).strip() if h else '' for h in rows[header_row]]
        col = {h.lower(): i for i, h in enumerate(headers)}

        def _get(row, *names, default=''):
            for name in names:
                idx = col.get(name.lower())
                if idx is not None and idx < len(row) and row[idx] is not None:
                    return row[idx]
            return default

        def _float(v):
            try: return float(v or 0)
            except: return 0.0

        c = _conn()
        count = 0
        for row in rows[header_row + 1:]:
            if not row or not any(v for v in row):
                continue
            desc = str(_get(row, 'description', default='')).strip()
            if not desc:
                continue
            recv     = str(_get(row, 'received date', 'received_date', default='')).strip()
            do_no    = str(_get(row, 'd.o. number', 'do number', 'do no', 'do_no', default='')).strip()
            grade    = str(_get(row, 'grade', default='')).strip()
            qty      = _float(_get(row, 'qty', 'quantity', default=0))
            total_kg = _float(_get(row, 'total kg', 'total_kg', 'total weight', default=0))
            remark   = str(_get(row, 'remark', 'remarks', default='')).strip()
            c.execute(
                "INSERT INTO raw_materials (received_date, do_no, description, grade, qty, total_kg, remark) "
                "VALUES (?,?,?,?,?,?,?)",
                (recv, do_no, desc, grade, qty, total_kg, remark)
            )
            count += 1
        c.commit()
        c.close()
        return count, None
    except Exception as e:
        return 0, str(e)


def replace_import_excel(file_source):
    """Clear all parts & assemblies (keeps progress), then reimport from Excel.
    file_source can be a file path (str) or bytes/BytesIO object."""
    c = _conn()
    # TRUNCATE is instant; DELETE on 4k-row tables hits Supabase statement timeout
    c.execute("TRUNCATE TABLE parts, assemblies")
    c.commit()
    c.close()
    return import_excel(file_source)


def import_excel(file_source):
    try:
        import openpyxl
        from io import BytesIO as _BytesIO
        if isinstance(file_source, (bytes, bytearray)):
            file_source = _BytesIO(file_source)
        wb = openpyxl.load_workbook(file_source, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        header_row = next((i for i, r in enumerate(rows) if r and r[0] == 'Assembly Mark'), None)
        if header_row is None:
            return 0, 0, "Header row 'Assembly Mark' not found."
        headers = [str(h).strip() if h else '' for h in rows[header_row]]
        col = {h: i for i, h in enumerate(headers)}

        def _get(row, *names, default=None):
            for name in names:
                idx = col.get(name)
                if idx is not None and idx < len(row) and row[idx] is not None:
                    return row[idx]
            return default

        def _float(v):
            try: return float(v or 0)
            except: return 0.0

        def _int(v):
            try: return int(float(v or 1))
            except: return 1

        stage_cols = [
            ('FIT UP',              'FIT UP (kg)',               'FIT UP Date'),
            ('WELDING',             'WELDING (kg)',              'WELDING Date'),
            ('BLASTING & PAINTING', 'BLASTING & PAINTING (kg)', 'BLASTING & PAINTING Date'),
            ('SEND TO SITE',        'SEND TO SITE (kg)',         'SEND TO SITE Date'),
        ]

        # ── Pass 1: parse entire Excel into memory lists ──────────────────────
        asm_order    = []          # insertion order preserved
        asm_set      = set()
        parts_rows   = []          # list of 12-tuples for bulk INSERT
        asm_weights  = {}          # asm -> total kg
        progress_map = {}          # (asm, sub, stage) -> (total_kg, date_str)

        for row in rows[header_row + 1:]:
            if not row or not _get(row, 'Assembly Mark'):
                continue

            asm    = str(_get(row, 'Assembly Mark', default='')).strip()
            sub    = str(_get(row, 'Sub Assembly', 'Sub-Assembly Mark', 'Sub Assembly Mark', default='') or '').strip()
            pm     = str(_get(row, 'Part Mark', default='') or '').strip()
            no     = _int(_get(row, 'No.', default=1))
            name   = str(_get(row, 'Name', 'NAME', default='') or '').strip()
            prof   = str(_get(row, 'Profile', default='') or '').strip()
            kgm    = _float(_get(row, 'kg/m', default=0))
            lmm    = _float(_get(row, 'Length (mm)', 'Length', default=0))
            tw     = _float(_get(row, 'Weight (kg)', 'Total weight', default=0))
            prof2  = str(_get(row, 'Profile 2', 'Profile2', default='') or '').strip()
            grade  = str(_get(row, 'Grade', default='') or '').strip()
            remark = str(_get(row, 'Remark', default='') or '').strip()

            if asm not in asm_set:
                asm_order.append(asm)
                asm_set.add(asm)
            asm_weights[asm] = asm_weights.get(asm, 0) + tw
            parts_rows.append((asm, sub, pm, no, name, prof, kgm, lmm, tw, prof2, grade, remark))

            for stage, kg_col, date_col in stage_cols:
                kg = _float(_get(row, kg_col, default=0))
                if kg > 0:
                    raw_date = _get(row, date_col, default=None)
                    if raw_date and hasattr(raw_date, 'strftime'):
                        date_str = raw_date.strftime('%Y-%m-%d')
                    elif raw_date:
                        date_str = str(raw_date).strip()[:10]
                    else:
                        date_str = str(date.today())
                    if (asm, sub, stage) in progress_map:
                        prev_kg, prev_date = progress_map[(asm, sub, stage)]
                        progress_map[(asm, sub, stage)] = (prev_kg + kg, prev_date)
                    else:
                        progress_map[(asm, sub, stage)] = (kg, date_str)

        if not parts_rows:
            return 0, 0, "No valid data rows found."

        # ── Pass 2: bulk DB writes (execute_values = one statement per batch) ─
        db  = _conn()
        raw = db._conn   # underlying psycopg2 connection
        cur = raw.cursor()

        # 1. Assemblies — all 478 in one statement; include final weights
        psycopg2.extras.execute_values(
            cur,
            "INSERT INTO assemblies (assembly_mark, total_weight_kg) VALUES %s "
            "ON CONFLICT(assembly_mark) DO UPDATE SET total_weight_kg = EXCLUDED.total_weight_kg",
            [(asm, asm_weights[asm]) for asm in asm_order],
        )
        raw.commit()

        # 2. Parts — 500-row chunks (each chunk is one fast statement)
        CHUNK = 500
        for i in range(0, len(parts_rows), CHUNK):
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO parts "
                "(assembly_mark, sub_assembly_mark, part_mark, no, name, "
                "profile, kg_per_m, length_mm, total_weight_kg, profile2, grade, remark) "
                "VALUES %s",
                parts_rows[i : i + CHUNK],
            )
            raw.commit()

        # 3. Progress — single DELETE + single INSERT
        # Key includes sub so each sub-assembly gets its own record (export JOIN works)
        prog_rows  = [(ds, asm, sub, stg, kg) for (asm, sub, stg), (kg, ds) in progress_map.items()]
        prog_count = 0
        if prog_rows:
            # Delete by (assembly_mark, stage) only — no sub filter — so old records
            # from previous imports (stored with sub='') are also removed.
            asm_stage_pairs = list({(asm, stg) for asm, sub, stg in progress_map})
            psycopg2.extras.execute_values(
                cur,
                "DELETE FROM progress WHERE (assembly_mark, stage) IN (VALUES %s)",
                asm_stage_pairs,
            )
            raw.commit()
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO progress "
                "(entry_date, assembly_mark, sub_assembly_mark, stage, weight_kg) VALUES %s",
                prog_rows,
            )
            raw.commit()
            prog_count = len(prog_rows)

        cur.close()
        db.close()
        return len(parts_rows), prog_count, None
    except Exception as e:
        return 0, 0, str(e)


def add_assembly(mark, weight, desc=''):
    db = _conn()
    db.execute(
        "INSERT INTO assemblies (assembly_mark, total_weight_kg, description) VALUES (?, ?, ?) "
        "ON CONFLICT(assembly_mark) DO NOTHING",
        (mark.strip().upper(), weight, desc)
    )
    db.commit()
    db.close()


def add_part(asm, sub, pm, no, name, prof, kgm, lmm, tw, prof2, grade, remark=''):
    db = _conn()
    # ensure assembly exists
    db.execute(
        "INSERT INTO assemblies (assembly_mark, total_weight_kg) VALUES (?, 0) "
        "ON CONFLICT(assembly_mark) DO NOTHING",
        (asm,)
    )
    db.execute(
        "INSERT INTO parts (assembly_mark, sub_assembly_mark, part_mark, no, name, "
        "profile, kg_per_m, length_mm, total_weight_kg, profile2, grade, remark) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (asm, sub, pm, no, name, prof, kgm, lmm, tw, prof2, grade, remark)
    )
    # recalculate assembly total weight from sum of all its parts
    db.execute(
        "UPDATE assemblies SET total_weight_kg = "
        "(SELECT COALESCE(SUM(total_weight_kg), 0) FROM parts WHERE assembly_mark = ?) "
        "WHERE assembly_mark = ?",
        (asm, asm)
    )
    db.commit()
    db.close()


def update_part(pid, asm, sub, pm, no, name, prof, kgm, lmm, tw, prof2, grade, remark=''):
    db = _conn()
    old = db.execute("SELECT assembly_mark FROM parts WHERE id = ?", (pid,)).fetchone()
    old_asm = old['assembly_mark'] if old else None
    db.execute("""
        UPDATE parts SET assembly_mark=?, sub_assembly_mark=?, part_mark=?, no=?,
        name=?, profile=?, kg_per_m=?, length_mm=?, total_weight_kg=?, profile2=?, grade=?, remark=?
        WHERE id=?
    """, (asm, sub, pm, no, name, prof, kgm, lmm, tw, prof2, grade, remark, pid))
    # recalculate both old and new assembly weights if assembly changed
    for a in {asm, old_asm} - {None}:
        db.execute(
            "UPDATE assemblies SET total_weight_kg = "
            "(SELECT COALESCE(SUM(total_weight_kg),0) FROM parts WHERE assembly_mark=?) "
            "WHERE assembly_mark=?", (a, a)
        )
    db.commit()
    db.close()


def update_progress(pid, entry_date, mark, sub_mark, stage, weight, qty, remarks, do_no=''):
    db = _conn()
    db.execute("""
        UPDATE progress SET entry_date=?, assembly_mark=?, sub_assembly_mark=?, stage=?,
        weight_kg=?, qty=?, remarks=?, delivery_order_no=? WHERE id=?
    """, (str(entry_date), mark, sub_mark, stage, float(weight), int(qty), remarks, do_no, pid))
    db.commit()
    db.close()


def delete_part(part_id):
    db = _conn()
    row = db.execute("SELECT assembly_mark FROM parts WHERE id = ?", (part_id,)).fetchone()
    if row:
        asm = row['assembly_mark']
        db.execute("DELETE FROM parts WHERE id = ?", (part_id,))
        db.execute(
            "UPDATE assemblies SET total_weight_kg = "
            "(SELECT COALESCE(SUM(total_weight_kg), 0) FROM parts WHERE assembly_mark = ?) "
            "WHERE assembly_mark = ?",
            (asm, asm)
        )
        db.commit()
    db.close()


def get_marks():
    db = _conn()
    rows = db.execute("SELECT assembly_mark FROM assemblies ORDER BY assembly_mark").fetchall()
    db.close()
    return [r['assembly_mark'] for r in rows]


def get_assemblies():
    db = _conn()
    rows = db.execute("SELECT * FROM assemblies ORDER BY assembly_mark").fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_assembly_weight(mark):
    db = _conn()
    row = db.execute("SELECT total_weight_kg FROM assemblies WHERE assembly_mark = ?", (mark,)).fetchone()
    db.close()
    return row['total_weight_kg'] if row else 0


def progress_exists(entry_date, mark, sub_mark, stage):
    """Return True if a progress entry already exists for the given combination."""
    db = _conn()
    row = db.execute(
        "SELECT 1 AS exists FROM progress WHERE entry_date=? AND assembly_mark=? "
        "AND sub_assembly_mark=? AND stage=?",
        (str(entry_date), mark, sub_mark, stage)
    ).fetchone()
    db.close()
    return row is not None


def get_completed_stages(mark, sub_mark):
    """Return set of stages that have at least one progress entry for this assembly/sub-assembly."""
    db = _conn()
    rows = db.execute(
        "SELECT DISTINCT stage FROM progress "
        "WHERE assembly_mark=? AND sub_assembly_mark=?",
        (mark, sub_mark)
    ).fetchall()
    db.close()
    return {r['stage'] for r in rows}


def add_progress(entry_date, mark, sub_mark, stage, weight, qty, remarks, do_no=''):
    db = _conn()
    cur = db.execute(
        "INSERT INTO progress (entry_date, assembly_mark, sub_assembly_mark, stage, "
        "weight_kg, qty, remarks, delivery_order_no) VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
        (str(entry_date), mark, sub_mark, stage, float(weight), int(qty), remarks, do_no)
    )
    rid = cur.lastrowid
    db.commit()
    db.close()
    return rid


def delete_progress(rid):
    db = _conn()
    db.execute("DELETE FROM progress WHERE id = ?", (rid,))
    db.commit()
    db.close()


def clear_all_data():
    """Delete all records from progress, parts, and assemblies tables."""
    db = _conn()
    db.execute("DELETE FROM progress")
    db.execute("DELETE FROM parts")
    db.execute("DELETE FROM assemblies")
    db.commit()
    db.close()


def get_by_date(d):
    db = _conn()
    rows = db.execute(
        "SELECT p.*, a.total_weight_kg as asm_total FROM progress p "
        "JOIN assemblies a ON p.assembly_mark = a.assembly_mark "
        "WHERE p.entry_date = ? ORDER BY p.stage, p.assembly_mark",
        (str(d),)
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_by_range(start, end):
    db = _conn()
    rows = db.execute(
        "SELECT p.*, a.total_weight_kg as asm_total FROM progress p "
        "JOIN assemblies a ON p.assembly_mark = a.assembly_mark "
        "WHERE p.entry_date BETWEEN ? AND ? ORDER BY p.entry_date, p.stage, p.assembly_mark",
        (str(start), str(end))
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_cumulative():
    db = _conn()
    rows = db.execute("""
        SELECT a.assembly_mark, a.total_weight_kg,
            COALESCE(SUM(CASE WHEN p.stage='FIT UP'             THEN p.weight_kg END), 0) as fitup,
            COALESCE(SUM(CASE WHEN p.stage='WELDING'            THEN p.weight_kg END), 0) as welding,
            COALESCE(SUM(CASE WHEN p.stage='BLASTING & PAINTING' THEN p.weight_kg END), 0) as blasting,
            COALESCE(SUM(CASE WHEN p.stage='SEND TO SITE'       THEN p.weight_kg END), 0) as sendsite
        FROM assemblies a
        LEFT JOIN progress p ON a.assembly_mark = p.assembly_mark
        GROUP BY a.assembly_mark, a.total_weight_kg ORDER BY a.assembly_mark
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_cumulative_by_sub():
    """Progress grouped by (assembly, sub-assembly).
    Total weight comes from the sub-assembly's parts weight.
    Assemblies with no sub-assemblies fall back to assembly-level totals.
    """
    db = _conn()
    rows = db.execute("""
        SELECT
            sp.assembly_mark,
            sp.sub_assembly_mark,
            sp.sub_weight AS total_weight_kg,
            COALESCE(SUM(CASE WHEN p.stage='FIT UP'              THEN p.weight_kg END), 0) AS fitup,
            COALESCE(SUM(CASE WHEN p.stage='WELDING'             THEN p.weight_kg END), 0) AS welding,
            COALESCE(SUM(CASE WHEN p.stage='BLASTING & PAINTING' THEN p.weight_kg END), 0) AS blasting,
            COALESCE(SUM(CASE WHEN p.stage='SEND TO SITE'        THEN p.weight_kg END), 0) AS sendsite
        FROM (
            SELECT assembly_mark, sub_assembly_mark, SUM(total_weight_kg) AS sub_weight
            FROM parts
            WHERE sub_assembly_mark != ''
            GROUP BY assembly_mark, sub_assembly_mark
        ) sp
        LEFT JOIN progress p
            ON sp.assembly_mark = p.assembly_mark
           AND sp.sub_assembly_mark = p.sub_assembly_mark
        GROUP BY sp.assembly_mark, sp.sub_assembly_mark, sp.sub_weight

        UNION ALL

        SELECT
            a.assembly_mark,
            '' AS sub_assembly_mark,
            a.total_weight_kg,
            COALESCE(SUM(CASE WHEN p.stage='FIT UP'              THEN p.weight_kg END), 0) AS fitup,
            COALESCE(SUM(CASE WHEN p.stage='WELDING'             THEN p.weight_kg END), 0) AS welding,
            COALESCE(SUM(CASE WHEN p.stage='BLASTING & PAINTING' THEN p.weight_kg END), 0) AS blasting,
            COALESCE(SUM(CASE WHEN p.stage='SEND TO SITE'        THEN p.weight_kg END), 0) AS sendsite
        FROM assemblies a
        LEFT JOIN progress p ON a.assembly_mark = p.assembly_mark
        WHERE a.assembly_mark NOT IN (
            SELECT DISTINCT assembly_mark FROM parts WHERE sub_assembly_mark != ''
        )
        GROUP BY a.assembly_mark, a.total_weight_kg
        ORDER BY 1, 2
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_summary():
    db = _conn()
    total = db.execute(
        "SELECT COALESCE(SUM(total_weight_kg),0) AS total FROM assemblies"
    ).fetchone()['total']
    rows  = db.execute(
        "SELECT stage, COALESCE(SUM(weight_kg),0) as done FROM progress GROUP BY stage"
    ).fetchall()
    db.close()
    result = {'total': total}
    for r in rows:
        result[r['stage']] = r['done']
    return result


def get_sub_assemblies(assembly_mark):
    """Return distinct sub-assembly marks for a given assembly."""
    db = _conn()
    rows = db.execute(
        "SELECT DISTINCT sub_assembly_mark FROM parts "
        "WHERE assembly_mark = ? AND sub_assembly_mark != '' "
        "ORDER BY sub_assembly_mark",
        (assembly_mark,)
    ).fetchall()
    db.close()
    return [r['sub_assembly_mark'] for r in rows]


def get_deliveries():
    db = _conn()
    rows = db.execute(
        "SELECT entry_date, assembly_mark, sub_assembly_mark, stage, "
        "delivery_order_no, weight_kg, qty, remarks "
        "FROM progress WHERE stage IN ('BLASTING & PAINTING','SEND TO SITE') "
        "ORDER BY entry_date DESC, assembly_mark"
    ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_parts(assembly_mark=None):
    db = _conn()
    if assembly_mark:
        rows = db.execute(
            "SELECT * FROM parts WHERE assembly_mark = ? "
            "ORDER BY assembly_mark, part_mark", (assembly_mark,)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM parts ORDER BY assembly_mark, part_mark"
        ).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_master_export():
    """Parts table joined with cumulative progress per (assembly, sub-assembly)."""
    db = _conn()
    rows = db.execute("""
        SELECT
            p.assembly_mark        AS "Assembly Mark",
            p.sub_assembly_mark    AS "Sub Assembly",
            p.part_mark            AS "Part Mark",
            p.no                   AS "No.",
            p.name                 AS "Name",
            p.profile              AS "Profile",
            p.kg_per_m             AS "kg/m",
            p.length_mm            AS "Length (mm)",
            p.total_weight_kg      AS "Weight (kg)",
            p.profile2             AS "Profile 2",
            p.grade                AS "Grade",
            p.remark               AS "Remark",
            CASE WHEN pr.fitup_done    = 1 THEN p.total_weight_kg ELSE 0 END AS "FIT UP (kg)",
            pr.fitup_dates                                                   AS "FIT UP Date",
            CASE WHEN pr.welding_done  = 1 THEN p.total_weight_kg ELSE 0 END AS "WELDING (kg)",
            pr.welding_dates                                                 AS "WELDING Date",
            CASE WHEN pr.blasting_done = 1 THEN p.total_weight_kg ELSE 0 END AS "BLASTING & PAINTING (kg)",
            pr.blasting_dates                                                AS "BLASTING & PAINTING Date",
            CASE WHEN pr.sendsite_done = 1 THEN p.total_weight_kg ELSE 0 END AS "SEND TO SITE (kg)",
            pr.sendsite_dates                                                AS "SEND TO SITE Date"
        FROM parts p
        LEFT JOIN (
            SELECT
                assembly_mark,
                sub_assembly_mark,
                MAX(CASE WHEN stage='FIT UP'              THEN 1 ELSE 0 END) AS fitup_done,
                MAX(CASE WHEN stage='WELDING'             THEN 1 ELSE 0 END) AS welding_done,
                MAX(CASE WHEN stage='BLASTING & PAINTING' THEN 1 ELSE 0 END) AS blasting_done,
                MAX(CASE WHEN stage='SEND TO SITE'        THEN 1 ELSE 0 END) AS sendsite_done,
                STRING_AGG(CASE WHEN stage='FIT UP'              THEN entry_date END, ',') AS fitup_dates,
                STRING_AGG(CASE WHEN stage='WELDING'             THEN entry_date END, ',') AS welding_dates,
                STRING_AGG(CASE WHEN stage='BLASTING & PAINTING' THEN entry_date END, ',') AS blasting_dates,
                STRING_AGG(CASE WHEN stage='SEND TO SITE'        THEN entry_date END, ',') AS sendsite_dates
            FROM progress
            GROUP BY assembly_mark, sub_assembly_mark
        ) pr ON p.assembly_mark = pr.assembly_mark
             AND p.sub_assembly_mark = pr.sub_assembly_mark
        ORDER BY p.assembly_mark, p.sub_assembly_mark, p.part_mark
    """).fetchall()
    db.close()
    return [dict(r) for r in rows]


def get_parts_summary(assembly_mark):
    """Return part count and total weight for an assembly."""
    db = _conn()
    row = db.execute(
        "SELECT COUNT(*) as cnt, COALESCE(SUM(total_weight_kg),0) as total "
        "FROM parts WHERE assembly_mark = ?", (assembly_mark,)
    ).fetchone()
    db.close()
    return dict(row) if row else {'cnt': 0, 'total': 0}


def search_parts(keyword='', assembly_mark=None):
    """Search parts by keyword across all text columns."""
    db = _conn()
    kw = f'%{keyword}%'
    if assembly_mark:
        rows = db.execute("""
            SELECT * FROM parts
            WHERE assembly_mark = ?
              AND (assembly_mark ILIKE ? OR sub_assembly_mark ILIKE ? OR part_mark ILIKE ?
                   OR name ILIKE ? OR profile ILIKE ? OR profile2 ILIKE ? OR grade ILIKE ?)
            ORDER BY assembly_mark, part_mark
        """, (assembly_mark, kw, kw, kw, kw, kw, kw, kw)).fetchall()
    else:
        rows = db.execute("""
            SELECT * FROM parts
            WHERE (assembly_mark ILIKE ? OR sub_assembly_mark ILIKE ? OR part_mark ILIKE ?
                   OR name ILIKE ? OR profile ILIKE ? OR profile2 ILIKE ? OR grade ILIKE ?)
            ORDER BY assembly_mark, part_mark
        """, (kw, kw, kw, kw, kw, kw, kw)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def search_progress(keyword='', stage=None, assembly_mark=None, start=None, end=None):
    """Search progress entries by keyword, stage, assembly and/or date range."""
    db = _conn()
    kw = f'%{keyword}%'
    conditions = ["(p.assembly_mark ILIKE ? OR p.remarks ILIKE ?)"]
    params = [kw, kw]
    if stage:
        conditions.append("p.stage = ?")
        params.append(stage)
    if assembly_mark:
        conditions.append("p.assembly_mark = ?")
        params.append(assembly_mark)
    if start:
        conditions.append("p.entry_date >= ?")
        params.append(str(start))
    if end:
        conditions.append("p.entry_date <= ?")
        params.append(str(end))
    where = " AND ".join(conditions)
    rows = db.execute(f"""
        SELECT p.*, a.total_weight_kg as asm_total
        FROM progress p
        JOIN assemblies a ON p.assembly_mark = a.assembly_mark
        WHERE {where}
        ORDER BY p.entry_date DESC, p.stage, p.assembly_mark
    """, params).fetchall()
    db.close()
    return [dict(r) for r in rows]


def export_csv(rows, path):
    import csv
    if not rows:
        return
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


def export_excel(rows, path):
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    if not rows:
        return
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Progress'
    headers = list(rows[0].keys())
    # header row styling
    hdr_fill = PatternFill('solid', fgColor='1E3A5F')
    hdr_font = Font(bold=True, color='FFFFFF')
    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h.replace('_', ' ').title())
        cell.fill = hdr_fill
        cell.font = hdr_font
        cell.alignment = Alignment(horizontal='center')
    # data rows with alternating fill
    alt_fill = PatternFill('solid', fgColor='EEF2FF')
    for row_i, r in enumerate(rows, 2):
        for col, h in enumerate(headers, 1):
            ws.cell(row=row_i, column=col, value=r.get(h, ''))
        if row_i % 2 == 0:
            for col in range(1, len(headers) + 1):
                ws.cell(row=row_i, column=col).fill = alt_fill
    # auto column width
    for col in ws.columns:
        max_len = max((len(str(c.value or '')) for c in col), default=0)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)
    wb.save(path)


# ── Session / Online Tracking ──────────────────────────────────────────────────

def init_sessions():
    c = _conn()
    c.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id         SERIAL PRIMARY KEY,
            username   TEXT NOT NULL,
            role       TEXT DEFAULT '',
            login_time TEXT NOT NULL,
            last_seen  TEXT NOT NULL,
            active     INTEGER DEFAULT 1
        )
    """)
    c.commit()
    c.close()


def _now_gmt8():
    from datetime import datetime as _dt, timedelta as _td
    return (_dt.utcnow() + _td(hours=8)).strftime('%Y-%m-%d %H:%M:%S')


def create_session(username, role=''):
    now = _now_gmt8()
    c = _conn()
    cur = c.execute(
        "INSERT INTO sessions (username, role, login_time, last_seen) VALUES (?,?,?,?) RETURNING id",
        (username, role, now, now)
    )
    sid = cur.lastrowid
    c.commit()
    c.close()
    return sid


def update_session_heartbeat(session_id):
    now = _now_gmt8()
    c = _conn()
    c.execute("UPDATE sessions SET last_seen=? WHERE id=?", (now, session_id))
    c.commit()
    c.close()


def end_session(session_id):
    c = _conn()
    c.execute("UPDATE sessions SET active=0 WHERE id=?", (session_id,))
    c.commit()
    c.close()


def get_active_sessions(minutes=10):
    """Users active within the last N minutes (GMT+8)."""
    from datetime import datetime as _dt, timedelta as _td
    threshold = (_dt.utcnow() + _td(hours=8) - _td(minutes=minutes)).strftime('%Y-%m-%d %H:%M:%S')
    c = _conn()
    rows = c.execute(
        "SELECT username, role, login_time, last_seen FROM sessions "
        "WHERE active=1 AND last_seen >= ? "
        "ORDER BY last_seen DESC",
        (threshold,)
    ).fetchall()
    c.close()
    return [dict(r) for r in rows]


def get_login_history(limit=100):
    """Recent login sessions, newest first."""
    c = _conn()
    rows = c.execute(
        "SELECT username, role, login_time, last_seen, active FROM sessions "
        "ORDER BY login_time DESC LIMIT ?",
        (limit,)
    ).fetchall()
    c.close()
    return [dict(r) for r in rows]


# ── Manpower ───────────────────────────────────────────────────────────────────

def save_manpower(entry_date, regular, ot1, ot2, ot3, sun_ph,
                  cutting_man=0, supervisor=0, foremen=0, fitter=0, helper=0, semi_skill=0,
                  material_coordinator=0, material_handler=0):
    c = _conn()
    c.execute("""
        INSERT INTO manpower
            (entry_date, regular, ot1, ot2, ot3, sun_ph,
             cutting_man, supervisor, foremen, fitter, helper, semi_skill,
             material_coordinator, material_handler)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(entry_date) DO UPDATE SET
            regular=EXCLUDED.regular, ot1=EXCLUDED.ot1, ot2=EXCLUDED.ot2,
            ot3=EXCLUDED.ot3, sun_ph=EXCLUDED.sun_ph,
            cutting_man=EXCLUDED.cutting_man, supervisor=EXCLUDED.supervisor,
            foremen=EXCLUDED.foremen, fitter=EXCLUDED.fitter,
            helper=EXCLUDED.helper, semi_skill=EXCLUDED.semi_skill,
            material_coordinator=EXCLUDED.material_coordinator,
            material_handler=EXCLUDED.material_handler
    """, (str(entry_date),
          int(regular), int(ot1), int(ot2), int(ot3), int(sun_ph),
          int(cutting_man), int(supervisor), int(foremen),
          int(fitter), int(helper), int(semi_skill),
          int(material_coordinator), int(material_handler)))
    c.commit()
    c.close()


def get_manpower(entry_date):
    c = _conn()
    row = c.execute("SELECT * FROM manpower WHERE entry_date=?", (str(entry_date),)).fetchone()
    c.close()
    return dict(row) if row else None


def save_manpower_grid(entry_date, grid):
    """Save a full grid {worker_type: {shift_key: count}} for a date."""
    c = _conn()
    c.execute("DELETE FROM manpower_detail WHERE entry_date=?", (str(entry_date),))
    for wtype, shifts in grid.items():
        for shift_key, count in shifts.items():
            if int(count) > 0:
                c.execute("""
                    INSERT INTO manpower_detail (entry_date, worker_type, shift, count)
                    VALUES (?,?,?,?)
                """, (str(entry_date), wtype, shift_key, int(count)))
    c.commit()
    c.close()


def get_manpower_grid(entry_date):
    """Return {worker_type: {shift_key: count}} for a date."""
    c = _conn()
    rows = c.execute(
        "SELECT worker_type, shift, count FROM manpower_detail WHERE entry_date=?",
        (str(entry_date),)
    ).fetchall()
    c.close()
    grid = {}
    for r in rows:
        grid.setdefault(r['worker_type'], {})[r['shift']] = r['count']
    return grid


def get_manhour_summary():
    """Total manhours, total days logged, and average manhours per day."""
    c = _conn()
    rows = c.execute(
        "SELECT entry_date, shift, SUM(count) as total FROM manpower_detail GROUP BY entry_date, shift"
    ).fetchall()
    c.close()
    total_days = len({r['entry_date'] for r in rows})
    total_mh   = sum(r['total'] * SHIFT_HOURS.get(r['shift'], 0) for r in rows)
    avg = total_mh / total_days if total_days else 0
    return {'total_manhours': total_mh, 'total_days': total_days, 'avg_per_day': avg}


# ── Drawings (stored as BYTEA in database) ─────────────────────────────────────

def save_drawing(title, assembly_mark, original_name, file_bytes, uploaded_by=''):
    import uuid
    ext      = original_name.rsplit('.', 1)[-1].lower() if '.' in original_name else 'bin'
    filename = f"{uuid.uuid4().hex}.{ext}"
    c = _conn()
    c.execute("""
        INSERT INTO drawings (title, original_name, filename, assembly_mark, uploaded_by, file_data)
        VALUES (?,?,?,?,?,?)
    """, (title.strip(), original_name, filename, assembly_mark or '', uploaded_by,
          psycopg2.Binary(file_bytes)))
    c.commit()
    c.close()


def get_drawings(assembly_mark=None):
    c = _conn()
    if assembly_mark:
        rows = c.execute(
            "SELECT * FROM drawings WHERE assembly_mark=? ORDER BY created_at DESC",
            (assembly_mark,)
        ).fetchall()
    else:
        rows = c.execute("SELECT * FROM drawings ORDER BY created_at DESC").fetchall()
    c.close()
    return [dict(r) for r in rows]


def delete_drawing(did):
    c = _conn()
    c.execute("DELETE FROM drawings WHERE id=?", (did,))
    c.commit()
    c.close()
