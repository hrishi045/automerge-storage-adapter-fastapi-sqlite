from typing import List
from fastapi import APIRouter, Request, HTTPException, Response, Query, status
import sqlite3
import base64

storage_router = APIRouter()

DB_PATH = "automerge_storage.db"
MAX_SEGMENTS = 4


def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)

    # This will soon not be needed. Currently, we're looking at 300 writes per minute, which is well within the limits of WAL journal mode
    # The plan is to do away with saving every second. memory usage/performance of automerge could end up being the bottleneck, not SQLite
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA mmap_size=300000000;")  # 300MB
    conn.execute("PRAGMA busy_timeout=5000;")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS automerge_storage (
            segment0 TEXT NOT NULL,
            segment1 TEXT NOT NULL,
            segment2 TEXT NOT NULL,
            segment3 TEXT NOT NULL,
            data BLOB NOT NULL,
            PRIMARY KEY (segment0, segment1, segment2, segment3)
        );
        """)
    return conn


db = get_db_connection()


def pad_segments(key: List[str]) -> tuple:
    if len(key) > MAX_SEGMENTS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Key cannot have more than {MAX_SEGMENTS} segments",
        )

    padded_key = key + [""] * (MAX_SEGMENTS - len(key))
    return tuple(padded_key)


@storage_router.put("/storage/item")
async def save_chunk(request: Request, key: List[str] = Query(default=[])):

    if not key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Key is required"
        )

    segments = pad_segments(key)
    data = await request.body()
    cursor = db.cursor()

    cursor.execute(
        """
        INSERT INTO automerge_storage (segment0, segment1, segment2, segment3, data)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(segment0, segment1, segment2, segment3) DO UPDATE SET data=excluded.data
    """,
        (*segments, data),
    )
    db.commit()

    return {"status": "ok"}


@storage_router.get("/storage/item")
def load_chunk(key: List[str] = Query(default=[])):
    if not key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Key is required"
        )

    segments = pad_segments(key)
    cursor = db.cursor()
    cursor.execute(
        """
        SELECT data FROM automerge_storage
        WHERE segment0=? AND segment1=? AND segment2=? AND segment3=?
    """,
        segments,
    )
    row = cursor.fetchone()

    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Item not found"
        )

    # `row` is a tuple like (data,). Extract the data bytes for the response.
    return Response(content=row[0], media_type="application/octet-stream")


@storage_router.delete("/storage/item")
def remove_chunk(key: List[str] = Query(default=[])):
    if not key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Key is required"
        )

    segments = pad_segments(key)
    cursor = db.cursor()

    cursor.execute(
        """
        DELETE FROM automerge_storage
        WHERE segment0=? AND segment1=? AND segment2=? AND segment3=?
    """,
        segments,
    )
    db.commit()

    return {"status": "ok"}


@storage_router.get("/storage/range")
def load_range(key: List[str] = Query(default=[])):
    if not key or len(key) > MAX_SEGMENTS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Prefix must have between 1 and {MAX_SEGMENTS} segments",
        )

    query = "SELECT segment0, segment1, segment2, segment3, data FROM automerge_storage WHERE "
    conditions = [f"segment{i} = ?" for i in range(len(key))]
    query += " AND ".join(conditions)

    cursor = db.cursor()
    cursor.execute(query, tuple(key))
    rows = cursor.fetchall()

    results = []

    for row in rows:
        # Reconstruct the string key, skipping our padding empty segments
        original_key = [row[i] for i in range(MAX_SEGMENTS) if row[i] != ""]
        # `row` is (segment0, segment1, segment2, segment3, data)
        data = row[-1]
        encoded_data = base64.b64encode(data).decode("utf-8")
        results.append({"key": original_key, "data": encoded_data})

    return results


@storage_router.delete("/storage/range")
def remove_range(key: List[str] = Query(default=[])):
    if not key or len(key) > MAX_SEGMENTS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Prefix must have between 1 and {MAX_SEGMENTS} segments",
        )

    query = "DELETE FROM automerge_storage WHERE "
    conditions = [f"segment{i} = ?" for i in range(len(key))]
    query += " AND ".join(conditions)

    cursor = db.cursor()
    cursor.execute(query, tuple(key))
    db.commit()

    return {"status": "ok"}
