import unittest
import pg8000
from .connection_settings import db_connect
from pg8000.six import b, BytesIO
from sys import exc_info


class Tests(unittest.TestCase):
    def setUp(self):
        self.db = pg8000.connect(**db_connect)
        try:
            self.db.execute("DROP TABLE t1")
        except pg8000.DatabaseError:
            e = exc_info()[1]
            # the only acceptable error is:
            self.assertEqual(
                e.args[1], b('42P01'),  # table does not exist
                "incorrect error for drop table")
            self.db.rollback()
        self.db.execute(
            "CREATE TEMPORARY TABLE t1 (f1 int primary key, "
            "f2 int not null, f3 varchar(50) null)")

    def tearDown(self):
        self.db.close()

    def testCopyToWithTable(self):
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)", (1, 1, 1))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)", (2, 2, 2))
        self.db.execute(
            "INSERT INTO t1 (f1, f2, f3) VALUES (:1, :2, :3)", (3, 3, 3))

        stream = BytesIO()
        ps = self.db.copy_to(stream, "t1")
        self.assertEqual(
            stream.getvalue(), b("1\t1\t1\n2\t2\t2\n3\t3\t3\n"))
        self.assertEqual(ps.rowcount, 3)
        self.db.commit()

    def testCopyToWithQuery(self):
        stream = BytesIO()
        ps = self.db.copy_to(
            stream, query="COPY (SELECT 1 as One, 2 as Two) TO STDOUT "
            "WITH DELIMITER 'X' CSV HEADER QUOTE AS 'Y' FORCE QUOTE Two")
        self.assertEqual(stream.getvalue(), b('oneXtwo\n1XY2Y\n'))
        self.assertEqual(ps.rowcount, 1)
        self.db.rollback()

    def testCopyFromWithTable(self):
        stream = BytesIO(b("1\t1\t1\n2\t2\t2\n3\t3\t3\n"))
        ps = self.db.copy_from(stream, "t1")
        self.assertEqual(ps.rowcount, 3)

        ps = self.db.execute("SELECT * FROM t1 ORDER BY f1")
        retval = ps.fetchall()
        self.assertEqual(retval, ([1, 1, '1'], [2, 2, '2'], [3, 3, '3']))
        self.db.rollback()

    def testCopyFromWithQuery(self):
        stream = BytesIO(b("f1Xf2\n1XY1Y\n"))
        ps = self.db.copy_from(
            stream, query="COPY t1 (f1, f2) FROM STDIN WITH DELIMITER "
            "'X' CSV HEADER QUOTE AS 'Y' FORCE NOT NULL f1")
        self.assertEqual(ps.rowcount, 1)

        ps = self.db.execute("SELECT * FROM t1 ORDER BY f1")
        retval = ps.fetchall()
        self.assertEqual(retval, ([1, 1, None],))
        self.db.commit()

    def testCopyWithoutTableOrQuery(self):
        stream = BytesIO()
        self.assertRaises(
            pg8000.CopyQueryOrTableRequiredError, self.db.copy_from, stream)
        self.assertRaises(
            pg8000.CopyQueryOrTableRequiredError, self.db.copy_to, stream)
        self.db.rollback()


if __name__ == "__main__":
    unittest.main()
