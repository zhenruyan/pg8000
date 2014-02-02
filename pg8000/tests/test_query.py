import unittest
import pg8000
from .connection_settings import db_connect
from pg8000.six import u, b
from sys import exc_info
from warnings import filterwarnings


# Tests relating to the basic operation of the database driver, driven by the
# pg8000 custom interface.
class Tests(unittest.TestCase):
    def setUp(self):
        self.db = pg8000.connect(**db_connect)
        filterwarnings("ignore", "DB-API extension cursor.next()")
        filterwarnings("ignore", "DB-API extension cursor.__iter__()")
        self.db.paramstyle = 'format'
        try:
            self.db.execute("DROP TABLE t1")
        except pg8000.DatabaseError:
            e = exc_info()[1]
            # the only acceptable error is:
            self.assertEqual(e.args[1], b('42P01'))  # table does not exist
            self.db.rollback()
        self.db.execute(
            "CREATE TEMPORARY TABLE t1 (f1 int primary key, "
            "f2 bigint not null, f3 varchar(50) null)")

        self.db.commit()

    def tearDown(self):
        self.db.close()

    def testDatabaseError(self):
        self.assertRaises(
            pg8000.ProgrammingError, self.db.execute,
            "INSERT INTO t99 VALUES (1, 2, 3)")

        self.db.rollback()

    def testParallelQueries(self):
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (1, 1, None))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (2, 10, None))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (3, 100, None))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (4, 1000, None))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
            (5, 10000, None))
        try:
            ps1 = self.db.execute("SELECT f1, f2, f3 FROM t1")
            for row in ps1:
                f1, f2, f3 = row
                ps2 = self.db.execute(
                    "SELECT f1, f2, f3 FROM t1 WHERE f1 > :1", (f1,))
                for row in ps2:
                    f1, f2, f3 = row
        finally:
            ps1.close()
            ps2.close()
        self.db.rollback()

    def testInsertReturning(self):
        try:
            self.db.execute("CREATE TABLE t2 (id serial, data text)")

            # Test INSERT ... RETURNING with one row...
            ps = self.db.execute(
                "INSERT INTO t2 (data) VALUES (:1) RETURNING id",
                ("test1",))
            row_id = ps.fetchone()[0]
            ps = self.db.execute(
                "SELECT data FROM t2 WHERE id = :1", (row_id,))
            self.assertEqual("test1", ps.fetchone()[0])

            # In PostgreSQL 8.4 we don't know the row count for a select
            if not self.db._server_version.startswith("8.4"):
                self.assertEqual(ps.rowcount, 1)

            # Test with multiple rows...
            ps = self.db.execute(
                "INSERT INTO t2 (data) VALUES (:1), (:2), (:3) "
                "RETURNING id", ("test2", "test3", "test4"))
            self.assertEqual(ps.rowcount, 3)
            ids = tuple([x[0] for x in ps])
            self.assertEqual(len(ids), 3)
        finally:
            ps.close()
            self.db.rollback()

    def testRowCount(self):
        # In PostgreSQL 8.4 we don't know the row count for a select
        if not self.db._server_version.startswith("8.4"):
            try:
                expected_count = 57
                ps = self.db.executemany(
                    "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
                    tuple((i, i, None) for i in range(expected_count)))
                self.db.commit()

                ps = self.db.execute("SELECT * FROM t1")

                # Check row_count without doing any reading first...
                self.assertEqual(expected_count, ps.rowcount)

                # Check rowcount after reading some rows, make sure it still
                # works...
                for i in range(expected_count // 2):
                    ps.fetchone()
                self.assertEqual(expected_count, ps.rowcount)
            finally:
                ps.close()
                self.db.commit()

            try:
                # Restart the cursor, read a few rows, and then check rowcount
                # again...
                ps = self.db.execute("SELECT * FROM t1")
                for i in range(expected_count // 3):
                    ps.fetchone()
                self.assertEqual(expected_count, ps.rowcount)
                self.db.rollback()

                # Should be -1 for a command with no results
                ps = self.db.execute("DROP TABLE t1")
                self.assertEqual(-1, ps.rowcount)
            finally:
                ps.close()
                self.db.commit()

    def testRowCountUpdate(self):
        try:
            self.db.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
                (1, 1, None))
            self.db.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
                (2, 10, None))
            self.db.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
                (3, 100, None))
            self.db.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
                (4, 1000, None))
            self.db.execute(
                "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)",
                (5, 10000, None))
            ps = self.db.execute(
                "UPDATE t1 SET f3 = :1 WHERE f2 > 101", ("Hello!"))
            self.assertEqual(ps.rowcount, 2)
        finally:
            ps.close()
            self.db.commit()

    def testIntOid(self):
        try:
            # https://bugs.launchpad.net/pg8000/+bug/230796
            ps = self.db.execute(
                "SELECT typname FROM pg_type WHERE oid = :1", (100,))
        finally:
            ps.close()
            self.db.rollback()

    def testUnicodeQuery(self):
        try:
            ps = self.db.execute(
                u(
                    "CREATE TEMPORARY TABLE \u043c\u0435\u0441\u0442\u043e "
                    "(\u0438\u043c\u044f VARCHAR(50), "
                    "\u0430\u0434\u0440\u0435\u0441 VARCHAR(250))"))
        finally:
            ps.close()
            self.db.commit()


if __name__ == "__main__":
    unittest.main()
