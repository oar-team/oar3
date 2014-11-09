import unittest
from  kao.slot import *

class TestSlot(unittest.TestCase):

    def test_intersec_itvs_slots(self):
        s0 = Slot(0, 0, 0, [(1, 32)], 1, 10)
        s1 = Slot(0, 0, 0, [(1, 16), (24, 28)], 11, 20)
        s2 = Slot(0, 0, 0, [(1, 8), (12, 26)], 21, 30)
        
        itvs = intersec_itvs_slots([s0,s1,s2])

        self.assertEqual(itvs, [(1, 8), (12, 16), (24, 26)])

if __name__ == '__main__':
    unittest.main()
