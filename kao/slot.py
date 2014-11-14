from job import *
from interval import intersec, sub_intervals
from hierarchy import find_resource_hierarchies_scattered

class Slot:
    def __init__(self, id, prev, next, itvs, b, e):
        self.id = id
        self.prev = prev
        self.next = next
        self.itvs = itvs
        self.b = b
        self.e = e
    def show(self):
        print "(id:", self.id, "p:", self.prev, "n:", self.next, ") b:", self.b, "e:", self.e, "itvs:", self.itvs

    def copy_itvs(self):
        return [itv for itv in self.itvs]
    

# not used TO REMOVE?         
def intersec_slots(slots):
    'Return intersection of intervals from a slot list'
    return reduce(lambda itvs_acc, s: intersec(itvs_acc, s.itvs), slots, slots[0].itvs) 

def intersec_itvs_slots(slots, sid_left, sid_right):
    sid = sid_left
    itvs_acc = slots[sid].itvs
    sid = slots[sid].next

    while(sid != sid_right):
        itvs_acc = intersec(itvs_acc, slots[sid].itvs)
        sid = slots[sid].next

    if (sid_left != sid_right):
        itvs_acc = intersec(itvs_acc, slots[sid_right].itvs)

    return itvs_acc

class SlotSet:
    def __init__(self, first_slot ):
        
        self.slots = {1: first_slot}
        self.last_id = 1


    def show_slots(self):
        for i, slot in self.slots.iteritems():
            slot.show()
        print '---'

    #
    # split slot accordingly with job resource assignment *)
    # new slot A + B + C (A, B and C can be null)         *)
    #   -------
    #   |A|B|C|
    #   |A|J|C|
    #   |A|B|C|
    #   -------

    # generate A slot - slot before job's begin
    def slot_before_job(self, slot, job):
        s_id = slot.id
        self.last_id += 1
        n_id = self.last_id
        a_slot = Slot(s_id, slot.prev, n_id, slot.copy_itvs(), slot.b, job.start_time-1)
        slot.prev = s_id
        self.slots[s_id] = a_slot
        #slot_id is changed so we have always the rightmost slot (min slot.b) w/ sid = 1 r 
        slot.id = n_id
        self.slots[n_id] = slot

    # generate B slot
    def slot_during_job(self, slot, job):
        slot.b = max(slot.b, job.start_time)
        slot.e = min(slot.e, job.start_time + job.walltime - 1)
        slot.itvs = sub_intervals(slot.itvs, job.res_set)

    # generate C slot - slot after job's end
    def slot_after_job(self, slot, job):
        self.last_id += 1
        s_id = self.last_id
        c_slot = Slot(s_id, slot.id, slot.next, slot.copy_itvs(), job.start_time + job.walltime, slot.e)
        slot.next = s_id
        self.slots[s_id] = c_slot             

    def split_slots(self, sid_left, sid_right, job):
        sid = sid_left
        while True:
            slot = self.slots[sid]
            if job.start_time > slot.b:
                # generate AB | ABC 
                if ( job.start_time + job.walltime)  > slot.e:
                    # generate AB
                    self.slot_before_job(slot, job)
                    self.slot_during_job(slot, job)
                else:
                    # generate ABC
                    self.slot_before_job(slot, job)
                    # generate C before modify slot / B
                    self.slot_after_job(slot, job)
                    self.slot_during_job(slot, job)

            else:
                # generate B | BC
                if ((job.start_time + job.walltime) - 1) >= slot.e:
                    # generate B
                    self.slot_during_job(slot, job)
                else:
                    # generate BC
                    # generate C before modify slot / B
                    self.slot_after_job(slot, job)
                    self.slot_during_job(slot, job)

            if (sid == sid_right):
                break
                
#    def split_slots_prev_scheduled_one_job

    def split_slots_prev_scheduled_jobs(self, ordered_jobs):
        ''' function which insert previously occupied slots in slots
        job must be sorted by start_time
        used in kamelot for pseudo_jobs_resources_available_upto splitting'''
        slot = self.slots[1] # 1
        left_sid_2_split = 1
        right_sid_2_split = 1
        
        for job in ordered_jobs:
            # find_first_slot
            while not( (slot.b > job.start_time) or ((slot.b <= job.start_time) and (job.start_time <= slot.e)) ):
                left_sid_2_split = slot.next
                slot = self.slots[slot.next]
                            
            # find_slots_encompass
            while not (slot.e >  (job.start_time + job.walltime)):
                right_sid_2_split = slot.next
                slot = self.slots[slot.next]
                
            self.split_slots(left_sid_2_split, right_sid_2_split, job)


