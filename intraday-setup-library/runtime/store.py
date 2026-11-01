"""Append-first journal and atomic derived records in a local SQLite WAL."""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import hashlib
from pathlib import Path
import sqlite3
import threading

from .config import canonical, fingerprint, validate_data_dir


class CollisionError(ValueError):
    pass


def _text_sha256(text):
    """Hash canonical UTF-8 without allocating another full checkpoint buffer."""
    digest = hashlib.sha256()
    for offset in range(0, len(text), 65536):
        digest.update(text[offset:offset + 65536].encode("utf-8"))
    return digest.hexdigest()


def _reject_checkpoint_constant(token):
    raise ValueError("checkpoint contains nonfinite JSON number: " + token)


class Store:
    def __init__(self, directory):
        self.directory = validate_data_dir(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "runtime.sqlite3"
        self.lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, timeout=15, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.create_function("known_at",3,self._known_at,deterministic=True)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=15000")
        self.conn.executescript('''
          CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS journal(
            seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, source TEXT NOT NULL,
            event_id TEXT, signature TEXT NOT NULL, payload TEXT NOT NULL,
            recorded_at TEXT NOT NULL, processed INTEGER NOT NULL DEFAULT 0,
            UNIQUE(source,event_id));
          CREATE TABLE IF NOT EXISTS records(
            id INTEGER PRIMARY KEY AUTOINCREMENT, topic TEXT NOT NULL,
            event_seq INTEGER NOT NULL REFERENCES journal(seq), output_index INTEGER NOT NULL,
            identity TEXT, payload TEXT NOT NULL, UNIQUE(topic,event_seq,output_index));
          CREATE INDEX IF NOT EXISTS records_topic_id ON records(topic,id);
          CREATE INDEX IF NOT EXISTS records_identity ON records(topic,identity);
          CREATE TABLE IF NOT EXISTS checkpoints(
            id INTEGER PRIMARY KEY AUTOINCREMENT, cursor INTEGER NOT NULL,
            created_at TEXT NOT NULL, signature TEXT NOT NULL, payload TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS labels(case_id TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS quarantine(
            id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT NOT NULL, event_id TEXT,
            code TEXT NOT NULL, created_at TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS journal_known_at ON journal(known_at(kind,payload,recorded_at),seq);
        ''')

    @staticmethod
    def _known_at(kind,payload,recorded_at):
        from recognition.models import timestamp
        value=json.loads(payload)
        when=value.get("arrival_time") if kind in {"market","clock","backfill"} else value.get("at") if kind in {"control","source_status"} else None
        return timestamp(when or recorded_at).timestamp()

    @contextmanager
    def transaction(self):
        with self.lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise

    @staticmethod
    def now():
        return datetime.now(timezone.utc).isoformat()

    def meta(self, key, default=None):
        with self.lock:
            row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set_meta(self, key, value, connection=None):
        conn = connection or self.conn
        with self.lock:
            conn.execute("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, canonical(value)))

    def append(self, kind, source, payload, event_id=None):
        text = canonical(payload)
        comparison = dict(payload)
        if kind == "market":
            comparison.pop("arrival_time", None)  # retransmission time does not change market content
        elif kind == "control":
            comparison.pop("at", None)  # the first accepted request owns the durable decision time
        signature = fingerprint({"kind": kind, "payload": comparison})
        with self.transaction() as c:
            if event_id is not None:
                prior = c.execute("SELECT seq,signature FROM journal WHERE source=? AND event_id=?", (source, event_id)).fetchone()
                if prior:
                    if prior[1] != signature:
                        c.execute("INSERT INTO quarantine(source,event_id,code,created_at) VALUES(?,?,?,?)", (source,event_id,"event_id_collision",self.now()))
                        collision = True
                    else:
                        return prior[0], False
                else:
                    collision = False
            else:
                collision = False
            if not collision:
                cursor = c.execute("INSERT INTO journal(kind,source,event_id,signature,payload,recorded_at) VALUES(?,?,?,?,?,?)", (kind, source, event_id, signature, text, self.now()))
                seq = cursor.lastrowid
        if collision:
            raise CollisionError("event_id collision; original retained and collision quarantined")
        return seq, True

    def journal(self, after=0, limit=1000, pending=False, start=None, end=None):
        from recognition.models import timestamp
        lo=timestamp(start).timestamp() if start is not None else None
        hi=timestamp(end).timestamp() if end is not None else None
        if lo is not None and hi is not None and lo>hi: raise ValueError("reversed time range")
        query = "SELECT * FROM journal WHERE seq>?" + (" AND processed=0" if pending else "")
        args=[after]
        if lo is not None: query+=" AND known_at(kind,payload,recorded_at)>=?"; args.append(lo)
        if hi is not None: query+=" AND known_at(kind,payload,recorded_at)<=?"; args.append(hi)
        query+=" ORDER BY seq LIMIT ?"; args.append(limit)
        with self.lock:
            rows = self.conn.execute(query, args).fetchall()
        return [dict(r, payload=json.loads(r["payload"])) for r in rows]

    def latest_seq(self):
        with self.lock:
            return self.conn.execute("SELECT COALESCE(MAX(seq),0) FROM journal").fetchone()[0]

    def latest_record_id(self):
        with self.lock:
            return self.conn.execute("SELECT COALESCE(MAX(id),0) FROM records").fetchone()[0]

    def record(self, connection, topic, event_seq, index, payload, identity=None):
        return connection.execute("INSERT INTO records(topic,event_seq,output_index,identity,payload) VALUES(?,?,?,?,?)", (topic,event_seq,index,identity,canonical(payload))).lastrowid

    def derived_hash(self,cursor=None):
        digest=hashlib.sha256()
        query="SELECT id,topic,event_seq,output_index,identity,payload FROM records"
        args=()
        if cursor is not None: query+=" WHERE event_seq<=?"; args=(cursor,)
        with self.lock:
            for row in self.conn.execute(query+" ORDER BY id",args):
                digest.update(canonical(list(row)).encode()+b'\n')
        return digest.hexdigest()

    def collection(self, topic, after=0, limit=100, symbol=None):
        limit = max(1, min(int(limit), 1000))
        query = "SELECT id,event_seq,payload FROM records WHERE topic=? AND id>?"
        args = [topic, int(after)]
        if symbol:
            query += " AND json_extract(payload,'$.symbol')=?"
            args.append(symbol)
        query += " ORDER BY id LIMIT ?"
        args.append(limit)
        with self.lock:
            rows = self.conn.execute(query, args).fetchall()
        return {"items": [dict(json.loads(r["payload"]), record_id=r["id"], event_seq=r["event_seq"]) for r in rows],
                "next_cursor": rows[-1]["id"] if len(rows)==limit else None}

    def history(self,topic,before=None,limit=100,symbol=None):
        limit=max(1,min(int(limit),1000))
        query="SELECT id,event_seq,payload FROM records WHERE topic=?"
        args=[topic]
        if before is not None:
            query+=" AND id<?"; args.append(int(before))
        if symbol:
            query+=" AND json_extract(payload,'$.symbol')=?"; args.append(symbol)
        query+=" ORDER BY id DESC LIMIT ?"; args.append(limit+1)
        with self.lock: rows=self.conn.execute(query,args).fetchall()
        more=len(rows)>limit; rows=rows[:limit]
        return {"items":[dict(json.loads(r["payload"]),record_id=r["id"],event_seq=r["event_seq"]) for r in reversed(rows)],
                "next_cursor":rows[-1]["id"] if rows and more else None}

    def outbox(self, after=0, limit=100):
        with self.lock:
            rows = self.conn.execute("SELECT id,topic,event_seq,payload FROM records WHERE id>? ORDER BY id LIMIT ?", (int(after), min(int(limit),1000))).fetchall()
        return [{"id":r["id"],"topic":r["topic"],"event_seq":r["event_seq"],"data":json.loads(r["payload"])} for r in rows]

    def save_checkpoint(self, cursor, payload):
        with self.lock:
            if self.conn.in_transaction:
                raise RuntimeError("save_checkpoint cannot independently commit inside a main-connection transaction")
            self._save_checkpoint_text(cursor, canonical(payload))

    def _save_checkpoint_text(self, cursor, text):
        """Persist trusted internal JSON text without rebuilding its object tree."""
        with self.lock:
            if not isinstance(text, str):
                raise TypeError("checkpoint text must be str")
            if self.conn.in_transaction:
                raise RuntimeError("save_checkpoint cannot independently commit inside a main-connection transaction")
            signature = _text_sha256(text)
            # The normal connection caches statements and their last bindings.
            # A checkpoint can be tens of MiB: use one uncached, short-lived
            # connection so its UTF-8 binding leaves SQLite before returning.
            connection = sqlite3.connect(self.path, timeout=15, check_same_thread=False,
                                         isolation_level=None, cached_statements=0)
            try:
                connection.row_factory = sqlite3.Row
                connection.create_function("known_at",3,self._known_at,deterministic=True)
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=FULL")
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA busy_timeout=15000")
                connection.execute("BEGIN IMMEDIATE")
                connection.execute("INSERT INTO checkpoints(cursor,created_at,signature,payload) VALUES(?,?,?,?)", (cursor,self.now(),signature,text))
                # Keep two good checkpoints in the same transaction as insertion.
                connection.execute("DELETE FROM checkpoints WHERE id NOT IN (SELECT id FROM checkpoints ORDER BY id DESC LIMIT 2)")
                connection.execute("COMMIT")
            except BaseException:
                if connection.in_transaction:
                    connection.execute("ROLLBACK")
                raise
            finally:
                connection.close()

    def checkpoint(self):
        with self.lock:
            row = self.conn.execute("SELECT * FROM checkpoints ORDER BY id DESC LIMIT 1").fetchone()
        if not row: return None
        canonical_signature = _text_sha256(row["payload"])
        payload = json.loads(row["payload"], parse_constant=_reject_checkpoint_constant)
        # Existing writers store canonical JSON. Preserve compatibility with
        # older, semantically equivalent JSON that was manually reformatted.
        if canonical_signature != row["signature"] and fingerprint(payload) != row["signature"]:
            raise ValueError("checkpoint checksum mismatch")
        return {"cursor":row["cursor"],"payload":payload}

    def labels(self):
        with self.lock:
            rows=self.conn.execute("SELECT payload FROM labels ORDER BY case_id").fetchall()
        return [json.loads(r[0]) for r in rows]

    def label_page(self,before=None,limit=100):
        limit=max(1,min(int(limit),1000))
        query="SELECT id,event_seq,payload FROM records WHERE id IN (SELECT MAX(id) FROM records WHERE topic='labels' GROUP BY identity)"
        args=[]
        if before is not None: query+=" AND id<?"; args.append(int(before))
        query+=" ORDER BY id DESC LIMIT ?"; args.append(limit+1)
        with self.lock: rows=self.conn.execute(query,args).fetchall()
        more=len(rows)>limit; rows=rows[:limit]
        return {"items":[dict(json.loads(r['payload']),record_id=r['id'],event_seq=r['event_seq']) for r in rows],
                "next_cursor":rows[-1]['id'] if rows and more else None}

    def integrity(self):
        with self.lock:
            return self.conn.execute("PRAGMA integrity_check").fetchone()[0]

    def latest_records(self, topic, limit=1000):
        with self.lock:
            rows = self.conn.execute("SELECT payload,id,event_seq FROM records WHERE id IN "
                "(SELECT MAX(id) FROM records WHERE topic=? GROUP BY identity) ORDER BY id DESC LIMIT ?", (topic, min(int(limit),1000))).fetchall()
        return [dict(json.loads(r["payload"]),record_id=r["id"],event_seq=r["event_seq"]) for r in rows]

    def close(self):
        with self.lock:
            self.conn.close()
