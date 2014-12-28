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
        j1 = JobPseudo(id=1, start_time=5, walltime=20, res_set=[(10, 20)], moldable_id=1, ts=False, ph=0)
        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 20))
        ss.split_slots(1,1,j1)
        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

    def test_split_slots_ABC(self):
        v = [ ( 1 , 4 , [(1, 32)] ),( 5 , 14 , [(1, 9), (21, 32)] ),( 15 , 20 , [(1, 32)] )]

        j1 = JobPseudo(id=1, start_time=5, walltime=10, res_set=[(10, 20)], moldable_id=1, ts=False, ph=0)
        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 20))
        ss.split_slots(1,1,j1)
        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

    def test_split_slots_B(self):
        v = [ ( 1 , 20 , [(1, 9), (21, 32)] ) ]
        j1 = JobPseudo(id=1, start_time=1, walltime=21, res_set=[(10, 20)], moldable_id=1, ts=False, ph=0)
        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 20))
        ss.split_slots(1,1,j1)
        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

    def test_split_slots_BC(self):
        v = [ ( 1 , 10 , [(1, 9), (21, 32)] ),( 11 , 20 , [(1, 32)] )]
        j1 = JobPseudo(id=1, start_time=1, walltime=10, res_set=[(10, 20)], moldable_id=1, ts=False, ph=0)
        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 20))
        ss.split_slots(1,1,j1)
        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))


    def test_bug_split_slots(self):
        
        v = [ ( 20, 69 , [(31, 32)] ),
              ( 70, 79 , [(1, 15), (31, 32)] ),
              ( 80, 2147483599 , [(1, 32)] ),
              ( 2147483600, 2147483647, [] )
        ]

        res = [(1, 32)]
        s1 = Slot(1,0,4,[(16, 32)],20,69)
        s2 = Slot(2,1,0,[],2147483600,2147483647)
        s4 = Slot(4,1,2,[(1, 32)],70,2147483599)

        slts = { s.id: s for s in [s1,s2,s4] }
        ss = SlotSet(None, slots = slts)

        j2 = JobPseudo(id=2,start_time=20,  walltime=60, res_set=[(16,30)], ts=False, ph=0)

        ss.split_slots(1,4,j2)
        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

if __name__ == '__main__':
    unittest.main()
