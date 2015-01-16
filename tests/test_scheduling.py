import unittest
from oar.kao.job import *
from oar.kao.slot import *
from oar.kao.scheduling import *
from oar.lib import config, get_logger

config['LOG_FILE'] = '/dev/stdout'
log = get_logger("oar.test")

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

        j1 = JobPseudo(id=1, start_time=5, walltime=10, res_set=[(10, 20)], types={}, ts=False, ph=0)
        j2 = JobPseudo(id=2, start_time=30, walltime=20, res_set=[(5, 15),(20, 28)], types={}, ts=False, ph=0)

        ss = SlotSet(Slot(1, 0, 0, [(1, 32)], 1, 100))
        all_ss = {0:ss}

        set_slots_with_prev_scheduled_jobs(all_ss, [j1,j2],10)

        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

    def test_assign_resources_mld_job_split_slots(self):

        v = [ ( 0 , 59 , [(17, 32)] ),( 60 , 100 , [(1, 32)] ) ]

        res = [(1, 32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        #j1 = JobPseudo(id=1, start_time=0, walltime=0, types={}, key_cache="",
        j1 = JobPseudo(id=1, types={}, key_cache="",
                     mld_res_rqts=[
                         (1, 60,
                          [  ( [("node", 2)], res)  ]
                      )
                     ]
        , ts=False, ph=0)

        assign_resources_mld_job_split_slots(ss, j1, hy, -1)

        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))


    def test_schedule_id_jobs_ct_1(self):
        v = [ ( 0 , 59 , [(17, 32)] ),( 60 , 100 , [(1, 32)] ) ]

        res = [(1, 32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        j1 = JobPseudo(id=1, types={}, key_cache="",
                     mld_res_rqts=[
                         (1, 60,
                          [  ( [("node", 2)], res)  ]
                      )
                     ]
        , ts=False, ph=0)

        schedule_id_jobs_ct(all_ss, {1:j1}, hy, [1], 20, {})

        self.assertTrue(self.compare_slots_val_ref(ss.slots,v))

    def test_schedule_container1(self):

        res = [(1, 32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        j1 = JobPseudo(id=1, types={"container":""}, key_cache="", 
                       mld_res_rqts=[(1, 80, [ ( [("node", 2)], res[:]) ])],
                       ts=False, ph=0)

        j2 = JobPseudo(id=2, types={"inner":"1"}, key_cache="", 
                       mld_res_rqts=[(1, 30, [ ( [("node", 1)], res[:]) ])], 
                       ts=False, ph=0)

        schedule_id_jobs_ct(all_ss, {1:j1,2:j2}, hy, [1,2], 10, {})

        self.assertEqual(j2.res_set, [(1, 8)])

    def test_schedule_container_error1(self):

        res = [(1, 32)]
        res2 = [(17,32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        j1 = JobPseudo(id=1, types={"container":""}, key_cache="", 
                       mld_res_rqts=[(1, 60, [ ( [("node", 2)], res) ])], 
                       ts=False, ph=0)

        j2 = JobPseudo(id=2, types={"inner":"1"}, key_cache="", 
                       mld_res_rqts=[(1, 30, [ ( [("node", 1)], res2) ])], 
                       ts=False, ph=0)

        schedule_id_jobs_ct(all_ss, {1:j1,2:j2}, hy, [1,2], 20, {})

        self.assertEqual(j2.start_time, -1)

    def test_schedule_container_error2(self):
        ''' inner exceeds container's capacity'''

        res = [(1, 32)]
                       
        ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        j1 = JobPseudo(id=1, types={"container":""}, key_cache="", 
                       mld_res_rqts=[(1, 60, [ ( [("node", 2)], res[:]) ])], 
                       ts=False, ph=0)

        j2 = JobPseudo(id=2, types={"inner":"1"}, key_cache="", 
                       mld_res_rqts=[(1, 20, [ ( [("node", 3)], res[:]) ])], 
                       ts=False, ph=0)

        schedule_id_jobs_ct(all_ss, {1:j1,2:j2}, hy, [1,2], 20, {})

        self.assertEqual(j2.start_time, -1)
        
    def test_schedule_container_error3(self):
        ''' inner exceeds time container's capacity'''

        res = [(1, 32)]
                       
        ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        j1 = JobPseudo(id=1, types={"container":""}, key_cache="", 
                       mld_res_rqts=[(1, 60, [ ( [("node", 2)], res[:]) ])], 
                       ts=False, ph=0)

        j2 = JobPseudo(id=2, types={"inner":"1"}, key_cache="", 
                       mld_res_rqts=[(1, 70, [ ( [("node", 1)], res[:]) ])], 
                       ts=False, ph=0)

        schedule_id_jobs_ct(all_ss, {1:j1,2:j2}, hy, [1,2], 20, {})

        self.assertEqual(j2.start_time, -1)

    def test_schedule_container_prev_sched(self):

        res = [(1, 32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        j1 = JobPseudo(id=1, types={"container":""}, key_cache="",
                       res_set = [(7,27)],
                       start_time = 200,
                       walltime = 150,
                       mld_res_rqts=[(1, 60, [ ( [("node", 2)], res[:]) ])], 
                       ts=False, ph=0)

        j2 = JobPseudo(id=2, types={"inner":"1"}, key_cache="", 
                       res_set = [(9,16)],
                       start_time = 210,
                       walltime = 70,
                       mld_res_rqts=[(1, 30, [ ( [("node", 1)], res[:]) ])], 
                       ts=False, ph=0)

        j3 = JobPseudo(id=3, types={"inner":"1"}, key_cache="", 
                       mld_res_rqts=[(1, 30, [ ( [("node", 1)], res[:]) ])], 
                       ts=False, ph=0)

        set_slots_with_prev_scheduled_jobs(all_ss, [j1,j2], 20)

        schedule_id_jobs_ct(all_ss, {3:j3}, hy, [3], 20, {})

        self.assertEqual(j3.start_time, 200)
        self.assertEqual(j3.res_set, [(17, 24)])

    def test_schedule_container_recursif(self):

        res = [(1, 32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 100))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        j1 = JobPseudo(id=1, types={"container":""}, key_cache="", 
                       mld_res_rqts=[(1, 80, [ ( [("node", 2)], res[:]) ])], 
                       ts=False, ph=0)

        j2 = JobPseudo(id=2, types={"container":"","inner":"1"}, key_cache="", 
                       mld_res_rqts=[(1, 50, [ ( [("node", 2)], res[:]) ])], 
                       ts=False, ph=0)

        j3 = JobPseudo(id=2, types={"inner":"2"}, key_cache="", 
                       mld_res_rqts=[(1, 30, [ ( [("node", 1)], res[:]) ])], 
                       ts=False, ph=0)

        schedule_id_jobs_ct(all_ss, {1:j1, 2:j2, 3:j3}, hy, [1,2,3], 10, {})

        self.assertEqual(j3.res_set, [(1, 8)])

    def test_schedule_container_prev_sched_recursif(self):

        res = [(1, 32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }

        j1 = JobPseudo(id=1, types={"container":""}, key_cache="",
                       res_set = [(7,27)],
                       start_time = 200,
                       walltime = 150,
                       ts=False, ph=0)

        j2 = JobPseudo(id=2, types={"container":"","inner":"1"}, key_cache="", 
                       res_set = [(15,25)],
                       start_time = 210,
                       walltime = 70,
                       ts=False, ph=0)

        j3 = JobPseudo(id=3, types={"inner":"2"}, key_cache="", 
                       mld_res_rqts=[(1, 30, [ ( [("node", 1)], res[:]) ])], 
                       ts=False, ph=0)

        set_slots_with_prev_scheduled_jobs(all_ss, [j1,j2], 20)

        schedule_id_jobs_ct(all_ss, {3:j3}, hy, [3], 20, {})

        self.assertEqual(j3.start_time, 210)
        self.assertEqual(j3.res_set, [(17, 24)])

    def test_schedule_timesharing1(self):
        res = [(1, 32)]
        ss = SlotSet(Slot(1, 0, 0, res, 0, 1000))
        all_ss = {0:ss}
        hy = {'node': [ [(1,8)], [(9,16)], [(17,24)], [(25,32)] ] }
        
        j1 = JobPseudo(id=1, types={}, key_cache="", 
                       mld_res_rqts=[(1, 60, [ ( [("node", 4)], res[:]) ])],
                       user = "toto", name="yop",
                       ts=True, ts_user="*", ts_name="*", ph=0)
        
        j2 = JobPseudo(id=2, types={}, key_cache="", 
                       mld_res_rqts=[(1, 80, [ ( [("node", 4)], res[:]) ])],
                       user = "toto", name="yop",
                       ts=True, ts_user="*", ts_name="*", ph=0)
        
        schedule_id_jobs_ct(all_ss, {1:j1,2:j2}, hy, [1,2], 20, {})
       
        print "j1.start_time:", j1.start_time, " j2.start_time:", j2.start_time

        self.assertEqual (j1.start_time, j2.start_time)
     
    def test_schedule_timesharing2(self):
        pass
        
    def test_schedule_timesharing_prev_sched(self):
        pass
        
    def test_schedule_placeholder1(self):
        pass

    def test_schedule_placeholder2(self):
        pass
        
    def test_schedule_placeholder_prev_sched(self):
        pass

if __name__ == '__main__':

    unittest.main()
