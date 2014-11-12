import unittest
from kao.job import *
from kao.scheduling import *

class TestScheduling(unittest.TestCase):


    def set_slots_with_prev_scheduled_jobs_1():
        slots = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 1000))

        # Job(id ,state, start_time, walltime, user, name, project, types, res_set, moldable_id, mld_res_rqts)
        j1 = Job(1,"", 10, 100, "", "", "", {}, [(10, 20), (25,30)], 1, [])

        #set_slots_with_prev_scheduled_jobs(slots_sets, jobs, ordered_id_jobs, security_time):
        set_slots_with_prev_scheduled_jobs(slots, {1:j1}, [1],60)



    def schedule_id_jobs_ct(self):
        
        schedule_id_jobs_ct(slots_set, jobs, hy, req_jobs_status???, id_jobs, security_time):
        sched = []
        self.assertEqual(itvs,[])

    def test_split_slots():
        #TODO
        itvs = []
        self.assertEqual(itvs,[])
 

if __name__ == '__main__':
    unittest.main()
