import os
import json
import gzip
import tempfile
import time

def _safe_replace(src, dst):
    for i in range(10):
        try:
            os.replace(src, dst)
            return
        except PermissionError:
            if i==9:
                raise
            time.sleep(0.1)

def atomic_write_json(path, data):
    dir_name=os.path.dirname(os.path.abspath(path))
    os.makedirs(dir_name, exist_ok=True)
    fd, tmp_path=tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        _safe_replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

def atomic_read_json(path, default=None):
    if not os.path.exists(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return default

def atomic_write_gzip(path, data):
    dir_name=os.path.dirname(os.path.abspath(path))
    os.makedirs(dir_name, exist_ok=True)
    fd, tmp_path=tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb") as gz:
                gz.write(json.dumps(data).encode("utf-8"))
        _safe_replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise

def atomic_read_gzip(path):
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)