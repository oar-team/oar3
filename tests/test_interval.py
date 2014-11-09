import unittest
from  kao.interval import *

class TestInterval(unittest.TestCase):

    def test_intersec(self):
        x =  [(1, 4), (6,9)]
        y = intersec(x,x)
        self.assertEqual(y,x)

    def test_extract_n_scattered_block_itv_1(self):
        y = [ [(1, 4), (6,9)],  [(10,17)], [(20,30)] ]
        a = extract_n_scattered_block_itv([(1,30)], y, 3)
        self.assertEqual(a, [(1, 4), (6, 9), (10, 17), (20, 30)])

    def test_extract_n_scattered_block_itv_2(self):
        y = [[(1, 4), (10, 17)], [(6, 9), (19, 22)], [(25, 30)]]
        a = extract_n_scattered_block_itv ([(1,30)], y, 2)
        print a
        self.assertEqual(a,  [(1, 4), (6, 9), (10, 17), (19, 22)])

if __name__ == '__main__':
    unittest.main()
