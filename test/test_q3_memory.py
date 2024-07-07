import sys
import os
sys.path.append('src')
import unittest
from src.q3_memory import q3_memory

parquet_file = "tweets.parquet"
data_folder_path = os.path.abspath(os.path.join(os.getcwd(), 'data'))
parquet_path = f"{data_folder_path}\\{parquet_file}"

class Test_Q3_Memory(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.result = q3_memory(f"{parquet_path}")
    
    # test p
    def test_return_list(self):
        self.assertIsInstance(self.result, list)

    # test para verificar si retorna una lista de 10 elementos
    def test_return_list_length_10(self):
        self.assertEqual(len(self.result), 10)

    # test para verificar si retorna una lista de tuplas
    def test_return_all_tuples(self):
        for row in self.result:
            self.assertIsInstance(row, tuple)

    # test para verificar si retorna una lista de tuplas con un str y un int
    def test_return_str_int_tuples(self):
        for row in self.result:
            self.assertIsInstance(row[0], str)
            self.assertIsInstance(row[1], int)

    # test para verificar que no retorne vacio
    def test_return_not_empty(self):
        self.assertTrue(len(self.result) > 0)
    
    # test para verificar que no retorne None
    def test_return_not_none(self):
        self.assertIsNotNone(self.result)

    # test para verificar que no retorne string vacio
    def test_return_not_empty_str(self):
        for row in self.result:
            self.assertTrue(row[0])

    # test para verificar que no retorne string None
    def test_return_not_none_str(self):
        for row in self.result:
            self.assertIsNotNone(row[0])

    # test para verificar que no retorne int negativo
    def test_return_not_negative_int(self):
        for row in self.result:
            self.assertTrue(row[1] > 0)

    # test para verificar que no retorne int None
    def test_return_not_none_int(self):
        for row in self.result:
            self.assertIsNotNone(row[1])

    
if __name__ == "__main__":
    unittest.main(verbosity=2)