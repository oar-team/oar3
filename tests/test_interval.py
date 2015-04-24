import unittest
from  oar.kao.interval import (intersec, extract_n_scattered_block_itv,
                                ordered_ids2itvs, itvs2ids, add_intervals)

class TestInterval(unittest.TestCase):

    def test_intersec(self):
        x =  [(1, 4), (6, 9)]
        y = intersec(x,x)
        self.assertEqual(y,x)

    def test_extract_n_scattered_block_itv_1(self):
        y = [ [(1, 4), (6,9)],  [(10,17)], [(20,30)] ]
        a = extract_n_scattered_block_itv([(1,30)], y, 3)
        self.assertEqual(a, [(1, 4), (6, 17), (20, 30)])

    def test_extract_n_scattered_block_itv_2(self):
        y = [[(1, 4), (10, 17)], [(6, 9), (19, 22)], [(25, 30)]]
        a = extract_n_scattered_block_itv([(1,30)], y, 2)
        self.assertEqual(a, [(1, 4), (6, 9), (10, 17), (19, 22)])

    def test_ordered_ids2itvs(self):
        y = [1,3,4,5,7,10,11,12,23]
        r = [(1, 1), (3, 5), (7, 7), (10, 12), (23, 23)]
        a = ordered_ids2itvs(y)
        self.assertEqual(a, r)

    def test_itvs2ids(self):
         y = [(1, 1), (3, 5), (7, 7), (10, 12), (23, 23)]
         r = [1,3,4,5,7,10,11,12,23]
         a = itvs2ids(y)
         self.assertEqual(a, r)

    def test_add_intervals1(self):
       r = [(1, 4), (6, 9)]
       x = [(1, 4)]
       y = [(6, 9)]
       a = add_intervals(x,y)
       self.assertEqual(a, r)

    def test_add_intervals2(self):
       r = [(1,9)]
       x = [(1, 4), (6, 9)]
       y = [(2,7)]
       a = add_intervals(x,y)
       self.assertEqual(a, r)

    def test_add_intervals1(self):
       r = [(1, 30)]
       x = [(3, 4), (6, 7), (9, 17), (19, 30)]
       y = [(1, 10), (15, 20), (22, 24)]
       a = add_intervals(x,y)
       self.assertEqual(a, r)

if __name__ == '__main__':
    unittest.main()
