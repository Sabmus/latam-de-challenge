import sys
import os
sys.path.append('src')
import unittest
from datetime import date
from src.q1_memory import q1_memory

parquet_file = "tweets.parquet"
data_folder_path = os.path.abspath(os.path.join(os.getcwd(), 'data'))
parquet_path = f"{data_folder_path}\\{parquet_file}"

class Test_Q1_Memory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = q1_memory(f"{parquet_path}")

    # test para verificar si retorna una lista
    def test_return_list(self):
        self.assertIsInstance(self.result, list)

    # test para verificar si retorna una lista de 10 elementos
    def test_return_list_length_10(self):
        self.assertEqual(len(self.result), 10)

    # test para verificar si retorna una lista de tuplas
    def test_return_all_tuples(self):
        for row in self.result:
            self.assertIsInstance(row, tuple)

    # test para verificar si retorna una lista de tuplas con un date y un string
    def test_return_datetime_str_tuples(self):
        for row in self.result:
            self.assertIsInstance(row[0], date)
            self.assertIsInstance(row[1], str)

    # test para verificar que no retorne vacio
    def test_return_not_empty(self):
        self.assertTrue(len(self.result) > 0)
    
    # test para verificar que no retorne None
    def test_return_not_none(self):
        self.assertIsNotNone(self.result)

    # test para verificar que no retorne date vacio
    def test_return_not_empty_date(self):
        for row in self.result:
            self.assertTrue(row[0])

    # test para verificar que no retorne date None
    def test_return_not_none_date(self):
        for row in self.result:
            self.assertIsNotNone(row[0])

    # test para verificar que no retorne string vacio
    def test_return_not_empty_str(self):
        for row in self.result:
            self.assertTrue(row[1])

    # test para verificar que no retorne string None
    def test_return_not_none_str(self):
        for row in self.result:
            self.assertIsNotNone(row[1])

    
if __name__ == "__main__":
    unittest.main()