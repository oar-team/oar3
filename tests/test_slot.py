import unittest
from kao.interval import *
from kao.job import *
from kao.slot import *

class TestSlot(unittest.TestCase):
  
    def compare_slots_val_ref(self, slots, v):
        sid = 1
        i = 0
        while True:
            slot = slots[sid]
            (b,e,itvs) = v[i]
            if (slot.b != b) or (slot.e != e) or not equal_itvs(slot.itvs, itvs):
                return False
            sid = slot.next
            if (sid == 0):
                break
            i += 1
        return True
      
    def test_intersec_itvs_slots(self):
        s1 = Slot(1, 0, 2, [(1, 32)], 1, 10)
        s2 = Slot(2, 1, 3, [(1, 16), (24, 28)], 11, 20)
        s3 = Slot(3, 2, 0, [(1, 8), (12, 26)], 21, 30)
        
        slots = {1:s1, 2:s2, 3:s3}

        itvs = intersec_itvs_slots(slots, 1, 3)

        self.assertEqual(itvs, [(1, 8), (12, 16), (24, 26)])

    def test_split_slots_AB(self):        
        v = [ ( 1 , 4 , [(1, 32)] ),( 5 , 20 , [(1, 9), (21, 32)] )]
        j1 = Job(1,"", 5, 16, "", "", "", {}, [(10, 20)], 1, [])
        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 20))
        ss.split_slots(1,1,j1)
        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

    def test_split_slots_ABC(self):        
        v = [ ( 1 , 4 , [(1, 32)] ),( 5 , 14 , [(1, 9), (21, 32)] ),( 15 , 20 , [(1, 32)] )]
        j1 = Job(1,"", 5, 10, "", "", "", {}, [(10, 20)], 1, [])
        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 20))
        ss.split_slots(1,1,j1)
        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

    def test_split_slots_B(self):        
        v = [ ( 1 , 20 , [(1, 9), (21, 32)] ) ]
        j1 = Job(1,"", 1, 21, "", "", "", {}, [(10, 20)], 1, [])
        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 20))
        ss.split_slots(1,1,j1)
        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

    def test_split_slots_BC(self):        
        v = [ ( 1 , 10 , [(1, 9), (21, 32)] ),( 11 , 20 , [(1, 32)] )]
        j1 = Job(1,"", 1, 10, "", "", "", {}, [(10, 20)], 1, [])
        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 20))
        ss.split_slots(1,1,j1)
        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

if __name__ == '__main__':
    unittest.main()
