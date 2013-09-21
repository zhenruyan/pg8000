import unittest
import pg8000


# Tests of the convert_query function.
class Tests(unittest.TestCase):
    def testQmark(self):
        new_query, make_args = pg8000.core.convert_query(
            "SELECT :1, :2, \"field_:3\" FROM t "
            "WHERE a='say ''what:3''' AND b=:3 AND c=E':3\\'test\\':3'")
        self.assertEqual(
            new_query, "SELECT $1, $2, \"field_:3\" FROM t WHERE "
            "a='say ''what:3''' AND b=$3 AND c=E':3\\'test\\':3'")
        self.assertEqual(make_args((1, 2, 3)), [1, 2, 3])

    def testQmark2(self):
        new_query, make_args = pg8000.core.convert_query(
            "SELECT :1, :2, * FROM t WHERE a=:3 AND b='are you ''sure:3'")
        self.assertEqual(
            new_query,
            "SELECT $1, $2, * FROM t WHERE a=$3 AND b='are you ''sure:3'")
        self.assertEqual(make_args((1, 2, 3)), [1, 2, 3])

    def testNumeric(self):
        new_query, make_args = pg8000.core.convert_query(
            "SELECT :2, :1, * FROM t WHERE a=:3")
        self.assertEqual(new_query, "SELECT $1, $2, * FROM t WHERE a=$3")
        self.assertEqual(make_args((1, 2, 3)), [2, 1, 3])

    def testNamed(self):
        new_query, make_args = pg8000.core.convert_query(
            "SELECT :f_2, :f1 FROM t WHERE a=:f_2")
        self.assertEqual(new_query, "SELECT $1, $2 FROM t WHERE a=$1")
        self.assertEqual(make_args({"f_2": 1, "f1": 2}), [1, 2])

    def testFormat(self):
        new_query, make_args = pg8000.core.convert_query(
            "SELECT :1, :2, \"f1_%\", E'txt_%' "
            "FROM t WHERE a=:3 AND b='75%'")
        self.assertEqual(
            new_query,
            "SELECT $1, $2, \"f1_%\", E'txt_%' FROM t WHERE a=$3 AND b='75%'")
        self.assertEqual(make_args((1, 2, 3)), [1, 2, 3])

if __name__ == "__main__":
    unittest.main()
