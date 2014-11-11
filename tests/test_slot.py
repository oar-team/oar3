import unittest
from kao.job import *
from kao.slot import *

class TestSlot(unittest.TestCase):
        
    def test_intersec_itvs_slots(self):
        s1 = Slot(1, 0, 2, [(1, 32)], 1, 10)
        s2 = Slot(2, 1, 3, [(1, 16), (24, 28)], 11, 20)
        s3 = Slot(3, 2, 0, [(1, 8), (12, 26)], 21, 30)
        
        slots = {1:s1, 2:s2, 3:s3}

        itvs = intersec_itvs_slots(slots, 1, 3)

        self.assertEqual(itvs, [(1, 8), (12, 16), (24, 26)])


    def test_split_slots(self):
        #TODO
        itvs = []
        self.assertEqual(itvs,[])

if __name__ == '__main__':
    unittest.main()
