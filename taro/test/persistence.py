import sqlite3
from pathlib import Path

from taro import cfg
from taro.jobs import persistence
from taro.jobs.db.sqlite import SQLite


class TestPersistence:
    def __init__(self):
        self.old_values = {}

    def __enter__(self):
        persistence.reset()
        self.old_values = {
            'persistence_enabled': cfg.persistence_enabled,
            'persistence_type': cfg.persistence_type,
            'persistence_database': cfg.persistence_database
        }
        cfg.persistence_enabled = True
        cfg.persistence_type = 'sqlite'
        cfg.persistence_database = test_db_path()
        return None

    def __exit__(self, exc_type, exc_value, traceback):
        cfg.persistence_enabled = self.old_values['persistence_enabled']
        cfg.persistence_type = self.old_values['persistence_type']
        cfg.persistence_database = self.old_values['persistence_database']
        remove_test_db()
        persistence.reset()


def test_sqlite_cfg_vars():
    return f"--set persistence_enabled=1 --set persistence_type=sqlite --set persistence_database={test_db_path()}"


def create_test_sqlite():
    return SQLite(sqlite3.connect(test_db_path()))


def test_db_path() -> Path:
    return Path.cwd() / 'test.db'


def remove_test_db():
    test_db = test_db_path()
    if test_db.exists():
        test_db.unlink()
