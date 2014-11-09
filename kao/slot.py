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
        
def intersec_slots(slots):
    'Return intersection of intervals from a slot list'
    return reduce(lambda itvs_acc, s: intersec(itvs_acc, s.itvs), slots, slots[0].itvs) 

def intersec_itvs_slots(slots, sid_left, sid_right):
    sid = sid_left
    itvs_acc = slots[sid].itvs
    sid = slot[sid].next

    while(sid != sid_right):
        intersec(itvs_acc, slot[sid].itvs)
        sid = slot[sid].next
    if (sid_left != sid_right):
        intersec(itvs_acc,slot[sid_right].itvs)
    return itvs_acc

class SlotSet:
    def __init__(self, first_slot ):
        
        self.slots = {1: first_slot}
        self.last_id = 1

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
        self.last_id += 1
        s_id = self.last_id
        a_slot = Slot(s_id, slot.prev, slot.id, slot.itvs, slot.b, job.b-1)
        slot.prev = s_id
        self.slots[s_id] = a_slot

    # generate B slot
    def slot_during_job(self, slot, job):
        slot.s = max(slot.b, job.b)
        slot.e = min(slot.e, job.b + job.walltime - 1)
        slot.itvs = sub_intervals(slot.itvs - job.itvs)

    # generate C slot - slot after job's end
    def slot_after_job(self, slot, job):
        self.last_id += 1
        s_id = self.last_id
        c_slot = Slot(s_id, slot.id, slot.next, slot.itvs, job.b + job.walltime, slot.e)
        slot.next = s_id
        self.slots[s_id] = c_slot             

    def split_slots(self, sid_left, sid_right, job):
        sid = sid_left
        while True:
            slot = self.slots[sid]
            if job.start_time > slot.b:
                # generate AB | ABC 
                if ((job.start_time + job.walltime) - 1) > slot.e:
                    # generate AB
                    slot_before_job(slot, job)
                    slot_during_job(slot, job)
                else:
                    # generate ABC
                    slot_before_job(slot, job)
                    slot_during_job(slot, job)
                    slot_after_job(slot, job)
            else:
                # generate B | BC
                if ((job.start_time + job.walltime) - 1) >= slot.e:
                    # generate B
                    slot_during_job(slot, job)
                else:
                    # generate BC
                    slot_during_job(slot, job)
                    slot_after_job(slot, job)
            if (sid == sid_right):
                break
                
    def split_slots_prev_scheduled_jobs(self, jobs):
        ''' function which insert previously occupied slots in slots
        job must be sorted by start_time
        used in kamelot for pseudo_jobs_resources_available_upto splitting'''
        slot = slots[1] # 1
        id_slots2split = []
        
        for job in jobs:
            # find_first_slot
            while not( (slot.s > job.start_time) or ((slot.s <= job.start_time) and (job.start_time <= slot.e)) ):
                slot = slots[slot.next]
                
            slot_begin = slot 
            # find_slots_encompass
            while not (slot.e >  (job.start_time + job.walltime)): 
                id_slots2split.append[slot.id]
                slot = slots[slot.next]

            # 
            split_slots(id_slots2split, job)
            
            slot = slot_begin

