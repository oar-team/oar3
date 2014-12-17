import unittest
from kao.job import *
from kao.slot import *
from kao.scheduling import *

class TestScheduling(unittest.TestCase):

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

    def test_set_slots_with_prev_scheduled_jobs_1(self):
        v = [ ( 1 , 4 , [(1, 32)] ),
              ( 5 , 14 , [(1, 9), (21, 32)] ),
              ( 15 , 29 , [(1, 32)] ),
              ( 30 , 49 , [(1, 4), (16, 19), (29, 32)] ),
              ( 50 , 100 , [(1, 32)] )
              ]

        j1 = JobTest(id=1, start_time=5, walltime=10, res_set=[(10, 20)], types={})
        j2 = JobTest(id=2, start_time=30, walltime=20, res_set=[(5, 15),(20, 28)], types={})

        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 100))
        all_ss = {0:ss}

        set_slots_with_prev_scheduled_jobs(all_ss, {1:j1, 2:j2}, [1,2], 10)

        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

    def test_assign_resources_mld_job_split_slots(self):

        v = [ ( 0 , 59 , [(17, 32)] ),( 60 , 100 , [(1, 32)] ) ]

        res = [(1, 32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        #j1 = JobTest(id=1, start_time=0, walltime=0, types={}, key_cache="",
        j1 = JobTest(id=1, types={}, key_cache="",
                     mld_res_rqts=[
                         (1, 60,
                          [  ( [("node", 2)], res)  ]
                      )
                     ]
        )

        assign_resources_mld_job_split_slots(ss, j1, hy)

        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))


    def test_schedule_id_jobs_ct_1(self):
        v = [ ( 0 , 59 , [(17, 32)] ),( 60 , 100 , [(1, 32)] ) ]

        res = [(1, 32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        j1 = JobTest(id=1, types={}, key_cache="",
                     mld_res_rqts=[
                         (1, 60,
                          [  ( [("node", 2)], res)  ]
                      )
                     ]
        )

        schedule_id_jobs_ct(all_ss, {1:j1}, hy, [1], 20)

        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

if __name__ == '__main__':
    unittest.main()
