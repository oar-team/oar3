# coding: utf-8
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta

import simplejson as json

from oar.lib.globals import init_oar
from oar.lib.logging import get_logger
from oar.lib.resource import ResourceSet
from oar.lib.submission import check_reservation
from oar.lib.tools import hms_str_to_duration, local_to_sql

_day2week_offset = {
    "mon": 0,
    "tue": 1,
    "wed": 2,
    "thu": 3,
    "fri": 4,
    "sat": 5,
    "sun": 6,
}

_, _, log = init_oar()
logger = get_logger(log, "oar.kao.quotas")


class Calendar(object):
    def __init__(self, json_quotas, config):
        self.quotas_period = config["QUOTAS_PERIOD"]
        self.period_end = 0  # period_end = period_begin = quotas_period

        self.quotas_window_time_limit = config["QUOTAS_WINDOW_TIME_LIMIT"]
        self.ordered_periodical_ids = []
        self.op_index = 0
        self.periodicals = {}
        self.nb_periodicals = 0

        self.ordered_oneshot_ids = []
        self.oneshots = {}
        self.oneshots_begin = None
        self.oneshots_end = None
        self.nb_oneshots = 0

        self.quotas_rules_list = []
        self.quotas_rules2id = {}
        self.quotas_id2rules = {}
        self.nb_quotas_rules = 0

        # load of quotas rules sets
        for type_temporal_quotas_i in [("periodical", 1), ("oneshot", 2)]:
            type_temporal_quotas, i = type_temporal_quotas_i
            if type_temporal_quotas in json_quotas:
                for p in json_quotas[type_temporal_quotas]:
                    if p[i] not in self.quotas_rules2id:
                        self.quotas_rules_list.append(
                            Quotas.quotas_rules_fromJson(json_quotas[p[i]])
                        )
                        self.quotas_rules2id[p[i]] = self.nb_quotas_rules
                        self.quotas_id2rules[self.nb_quotas_rules] = p[i]
                        self.nb_quotas_rules += 1

        # create periodicals and oneshots data structure from json
        if "periodical" in json_quotas:
            default = None
            for p in json_quotas["periodical"]:
                if p[0] == "*,*,*,*" or p[0] == "default":
                    default = self.quotas_rules2id[p[1]]
                else:
                    self.periodical_fromJson(p)
            self.ordered_periodical_ids = sorted(
                range(self.nb_periodicals), key=lambda k: self.periodicals[k][0]
            )
            if default is not None:
                t = 0
                i = 0
                nb_per = self.nb_periodicals
                while (t < 7 * 86400) and (i < nb_per):
                    t1 = self.periodicals[self.ordered_periodical_ids[i]][0]
                    duration = self.periodicals[self.ordered_periodical_ids[i]][1]
                    # fill the gap w/ default is needed
                    if t != t1:
                        self.periodicals[self.nb_periodicals] = (
                            t,
                            t1 - t,
                            default,
                            "*,*,*,*",
                        )
                        self.nb_periodicals += 1
                    t = t1 + duration
                    i += 1
                if t != 7 * 86400:
                    self.periodicals[self.nb_periodicals] = (
                        t,
                        7 * 86400 - t,
                        default,
                        "*,*,*,*",
                    )
                    self.nb_periodicals += 1
                # reorder the all
                self.ordered_periodical_ids = sorted(
                    range(self.nb_periodicals), key=lambda k: self.periodicals[k][0]
                )

        if "oneshot" in json_quotas:
            for o in json_quotas["oneshot"]:
                self.oneshot_fromJson(o)
            self.ordered_oneshot_ids = sorted(
                range(self.nb_oneshots), key=lambda k: self.oneshots[k][0]
            )

    def periodical_fromJson(self, periodical_json):
        """fill periodical data structures from one json entry
        ex: ["00:00-09:00 mon * *", "quotas_weekend"]
        """
        hm2, wday, mday, month = periodical_json[0].split()
        if (mday != "*") or (month != "*"):
            logger.error(
                "mday and month are not yet implemented: {}".format(periodical_json[0])
            )
            exit(1)

        try:
            # handle hour, minute range
            if hm2 == "*":
                begin = 0
                duration = 24 * 3600
            else:
                hm0, hm1 = hm2.split("-")
                begin = hms_str_to_duration(hm0)
                end = hms_str_to_duration(hm1)
                if end:
                    duration = end - begin
                else:  # end = 00:00
                    duration = 24 * 3600 - begin

            # handle days
            if wday == "*":  # all weekdays
                weekdays = range(7)
            elif "-" in wday:  # days interval
                d0, d1 = wday.split("-")
                d0 = _day2week_offset[d0]
                d1 = _day2week_offset[d1]
                if hm2 == "*":
                    if d0 > d1:
                        d1 += 7
                    weekdays = [d0]
                    duration = 86400 * (d1 - d0 + 1)
                else:
                    weekdays = range(d0, d1 + 1)
            else:  # some days
                weekdays = [_day2week_offset[d] for d in wday.split(",")]

            # fill periodicals (begin, duration, rules_id)
            for i in weekdays:
                t0 = 86400 * i + begin
                # overflow case: ex ["* sun-mon * *", "quotas_weekend", "weekend"],
                if (t0 + duration) > 7 * 86400:
                    duration0 = (t0 + duration) % (7 * 86400)
                    duration -= duration0
                    self.periodicals[self.nb_periodicals] = (
                        0,
                        duration0,
                        self.quotas_rules2id[periodical_json[1]],
                        periodical_json[0],
                    )
                    self.nb_periodicals += 1
                self.periodicals[self.nb_periodicals] = (
                    t0,
                    duration,
                    self.quotas_rules2id[periodical_json[1]],
                    periodical_json[0],
                )
                self.nb_periodicals += 1

        except Exception as e:
            logger.error("Parsing {} failed: {}".format(periodical_json[0], e))
            exit(1)

    def oneshot_fromJson(self, oneshot_json):
        """fill oneshot data structures from one json entry
        ex: ["2020-07-23 19:30", "2012-08-29 8:30", "quotas_holyday", "summer holiday"]
        """
        error0, d0 = check_reservation(oneshot_json[0] + ":00")
        error1, d1 = check_reservation(oneshot_json[1] + ":00")

        if error0[0] or error1[0]:
            logger.error(
                'Parsing error on {} or/and {}, expected format is: "YYYY-MM-DD hh:mm"'.format(
                    d0, d1
                )
            )
            exit(1)

        if (not self.oneshots_begin) or (d0 < self.oneshots_begin):
            self.oneshots_begin = d0

        if (not self.oneshots_end) or (d1 > self.oneshots_end):
            self.oneshots_end = d1

        self.oneshots[self.nb_oneshots] = (
            d0,
            d1,
            self.quotas_rules2id[oneshot_json[2]],
            oneshot_json[0],
            oneshot_json[1],
            oneshot_json[3],
        )
        self.nb_oneshots += 1

    def check_periodicals(self):
        t = 0
        for i in self.ordered_periodical_ids:
            periodical = self.periodicals[i]
            if periodical[0] != t:
                return (False, i)
            t = periodical[0] + periodical[1]
        if t != 7 * 24 * 3600:
            return (False, i)
        return (True, None)

    def oneshot_at(self, t_epoch, periodical_remaining_duration, periodical_rules_id):
        """Determine rules_id and remaining if an oneshot period overlaps t_epoch"""
        remaining_duration = 0
        rules_id = -1
        o_id = -1
        if self.oneshots:
            for r_id in self.ordered_oneshot_ids:
                # begin of the periodical period is overlaped by oneshot
                if (t_epoch >= self.oneshots[r_id][0]) and (
                    t_epoch <= self.oneshots[r_id][1]
                ):
                    remaining_duration = self.oneshots[r_id][1] - t_epoch
                    rules_id = self.oneshots[r_id][2]
                    o_id = r_id
                    break
                # end of the periodical period is overlaped by oneshot
                if (t_epoch < self.oneshots[r_id][0]) and (
                    self.oneshots[r_id][0] < (t_epoch + periodical_remaining_duration)
                ):
                    rules_id = periodical_rules_id
                    remaining_duration = self.oneshots[r_id][0] - t_epoch
                    o_id = r_id
                    break
        return (remaining_duration, rules_id, o_id)

    def periodical_rules_at(self, t_epoch):
        """Determine which periodical apply and return rules_id and the remaining duration"""
        t_dt = datetime.fromtimestamp(t_epoch).date()
        # determine weekstart's day
        t_weekstart_day_dt = t_dt - timedelta(days=t_dt.weekday())
        period_origin = int(
            datetime.combine(t_weekstart_day_dt, datetime.min.time()).timestamp()
        )

        self.period_end = period_origin + self.quotas_period

        # find rules_id and compute remaining_duration
        for i in range(self.nb_periodicals):
            t = t_epoch - period_origin
            t_periodicals_begin = self.periodicals[self.ordered_periodical_ids[i]][0]
            t_periodicals_end = (
                t_periodicals_begin
                + self.periodicals[self.ordered_periodical_ids[i]][1]
                - 1
            )
            if (t_periodicals_begin >= t) and (t <= t_periodicals_end):
                break

        # get rules_id and compute remaining_duration
        self.op_index = i
        index = self.ordered_periodical_ids[self.op_index]
        rules_id = self.periodicals[index][2]
        remaining_duration = self.periodicals[index][1] - (t_epoch - period_origin)

        return (rules_id, remaining_duration)

    def rules_at(self, t_epoch):
        (rules_id, remaining_duration) = self.periodical_rules_at(t_epoch)

        # test if an overshot apply ? If so set remaining duration and rules_id accordingly
        # import pdb; pdb.set_trace()
        (o_remaining_duration, o_rules_id, _) = self.oneshot_at(
            t_epoch, remaining_duration, rules_id
        )
        if o_remaining_duration:
            remaining_duration = o_remaining_duration
            rules_id = o_rules_id

        return (rules_id, remaining_duration)

    def next_rules(self, t_epoch):
        # TODO take into account oneshot case
        if t_epoch >= self.period_end:
            rules_id = None
            remaining_duration = 0
        else:
            self.op_index = (self.op_index + 1) % self.nb_periodicals
            index = self.ordered_periodical_ids[self.op_index]
            rules_id = self.periodicals[index][2]
            remaining_duration = self.periodicals[index][1]

            # test if an overshot apply ? If so set remaining duration and rules_id accordingly
            (o_remaining_duration, o_rules_id, _) = self.oneshot_at(
                t_epoch, remaining_duration, rules_id
            )
            if o_remaining_duration:
                remaining_duration = o_remaining_duration
                rules_id = o_rules_id
        return (rules_id, remaining_duration)

    def show(self, t=None, begin=None, end=None, check=True, json=False):
        t_epoch = None
        if t:
            try:
                t = int(t)
                t_epoch = t
                t = local_to_sql(t_epoch)[:-3]
            except ValueError:
                error, t_epoch = check_reservation(t + ":00")
                if error[0]:
                    logger.error(
                        'Parsing error on {}, expected format is: "YYYY-MM-DD hh:mm"'.format(
                            t
                        )
                    )
                    exit(1)

        if self.periodicals:
            print("\nPeriodicals:\n------------")
            for i in self.ordered_periodical_ids:
                b, d, rules_id, period = self.periodicals[i]
                print(
                    "{:>3}: ({:>6}, {:>6}, {:<23}, {:>2}, {:<15})".format(
                        i, b, d, period, rules_id, self.quotas_id2rules[rules_id]
                    )
                )

            if check:
                check_per, p_id = self.check_periodicals()
                print()
                if not check_per:
                    print("** Periodical issue around: {}".format(p_id))
                else:
                    print("** Periodicals' contiguity checked")
                print()

        if self.oneshots:
            print("Oneshots:\n---------")
            for i in self.ordered_oneshot_ids:
                b, e, rules_id, d0, d1, description = self.oneshots[i]
                print(
                    "{:>3}: ({:>10}, {:>10}, {}, {}, {:>2}, {:<15}, {:<20})".format(
                        i,
                        b,
                        e,
                        d0,
                        d1,
                        rules_id,
                        self.quotas_id2rules[rules_id],
                        description,
                    )
                )

        if t_epoch:
            print("\n** At time: {}, from epoch: {}\n".format(t, t_epoch))
            rules_id, remaining_duration = self.periodical_rules_at(t_epoch)
            index = self.ordered_periodical_ids[self.op_index]

            # import pdb; pdb.set_trace()
            (o_remaining_duration, o_rules_id, o_id) = self.oneshot_at(
                t_epoch, remaining_duration, rules_id
            )
            if o_id != -1:
                oneshot = self.oneshots[o_id]
                if (oneshot[0] <= t_epoch) and (t_epoch <= oneshot[1]):
                    print(
                        "oneshot apply:\n id {}: {}".format(o_id, self.oneshots[o_id])
                    )
                else:
                    print(
                        "periodical apply:\n id {} periodical: {}".format(
                            index, self.periodicals[index]
                        )
                    )
                    print(
                        "oneshot apply by reducing remaining duration:\n id {}: {}".format(
                            o_id, self.oneshots[o_id]
                        )
                    )
                rules_id = o_rules_id
                remaining_duration = o_remaining_duration
            else:
                print(
                    "periodical apply:\n id {} periodical: {}".format(
                        index, self.periodicals[index]
                    )
                )

            print(
                "\nrules_id: {}\nrules: {}".format(
                    rules_id, self.quotas_id2rules[rules_id]
                )
            )
            print("remaining_duration {}".format(remaining_duration))


class Quotas(object):
    """

        Implements quotas on:
           - the amount of busy resources at a time
           - the number of running jobs at a time
           - the resource time in use at a time (nb_resources X hours)
        This can be seen like a surface used by users, projects, types, ...

        depending on:

        - job queue name ("-q" oarsub option)
        - job project name ("--project" oarsub option)
        - job types ("-t" oarsub options)
        - job user

        Syntax is like:

        quotas[queue, project, job_type, user] = [int, int, float];
                                                   |    |     |
                  maximum used resources ----------+    |     |
                  maximum number of running jobs -------+     |
                  maximum resources times (hours) ------------+



           '*' means "all" when used in place of queue, project,
               type and user, quota will encompass all queues or projects or
               users or type
           '/' means "any" when used in place of queue, project and user
               (cannot be used with type), quota will be "per" queue or project or
               user
            -1 means "no quota" as the value of the integer or float field

     The lowest corresponding quota for each job is used (it depends on the
     consumptions of the other jobs). If specific values are defined then it is
     taken instead of '*' and '/'.

     The default quota configuration is (infinity of resources and jobs):

           $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'*'} = [-1, -1, -1] ;

     Examples:

       - No more than 100 resources used by 'john' at a time:

           $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = [100, -1, -1] ;

       - No more than 100 resources used by 'john' and no more than 4 jobs at a
         time:

           $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = [100, 4, -1] ;

       - No more than 150 resources used by jobs of besteffort type at a time:

           $Gantt_quotas->{'*'}->{'*'}->{'besteffort'}->{'*'} = [150, -1, -1] ;

       - No more than 150 resources used and no more than 35 jobs of besteffort
         type at a time:

           $Gantt_quotas->{'*'}->{'*'}->{'besteffort'}->{'*'} = [150, 35, -1] ;

       - No more than 200 resources used by jobs in the project "proj1" at a
         time:

           $Gantt_quotas->{'*'}->{'proj1'}->{'*'}->{'*'} = [200, -1, -1] ;

       - No more than 20 resources used by 'john' in the project 'proj12' at a
         time:

           $Gantt_quotas->{'*'}->{'proj12'}->{'*'}->{'john'} = [20, -1, -1] ;

       - No more than 80 resources used by jobs in the project "proj1" per user
         at a time:

           $Gantt_quotas->{'*'}->{'proj1'}->{'*'}->{'/'} = [80, -1, -1] ;

       - No more than 50 resources used per user per project at a time:

           $Gantt_quotas->{'*'}->{'/'}->{'*'}->{'/'} = [50, -1, -1] ;

       - No more than 200 resource hours used per user at a time:

           $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'/'} = [-1, -1, 200] ;

         For example, a job can take 1 resource for 200 hours or 200 resources for
         1 hour.

     Note: If the value is only one integer then it means that there is no limit
           on the number of running jobs and rsource hours. So the 2 following
           statements have the same meaning:

               $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = 100 ;
               $Gantt_quotas->{'*'}->{'*'}->{'*'}->{'john'} = [100, -1, -1] ;


        Note1: Quotas are applied globally, only the jobs of the type container are not taken in
    account (but the inner jobs are used to compute the quotas).

        Note2: Besteffort jobs are not taken in account except in the besteffort queue.


    """

    enabled = False
    calendar = None
    default_rules = {}
    job_types = ["*"]

    @classmethod
    def enable(cls, config, resource_set=None):
        cls.enabled = True
        if "QUOTAS_ALL_NB_RESOURCES_MODE" in config:
            mode = config["QUOTAS_ALL_NB_RESOURCES_MODE"]
        else:
            mode = "default_not_dead"

        if resource_set:
            all_value = resource_set.nb_resources_default
            if mode == "default_not_dead":
                all_value = resource_set.nb_resources_default_not_dead
        else:
            all_value = None
        cls.load_quotas_rules(config, all_value)

    def __init__(self):
        self.counters = defaultdict(lambda: [0, 0, 0])
        self.rules = Quotas.default_rules

    def deepcopy_from(self, quotas):
        self.counters = deepcopy(quotas.counters)

    def show_counters(self, msg=""):  # pragma: no cover
        print("show_counters:", msg)
        for k, v in self.counters.items():
            print(k, " = ", v)

    def update(self, job, prev_nb_res=0, prev_duration=0):
        queue = job.queue_name
        project = job.project
        user = job.user

        # TOREMOVE ?
        if hasattr(job, "res_set"):
            if not hasattr(self, "nb_res"):
                job.nb_res = len(job.res_set & ResourceSet.default_itvs)
                nb_resources = job.nb_res
        else:
            nb_resources = prev_nb_res

        if hasattr(job, "walltime"):
            duration = job.walltime
        else:
            duration = prev_duration

        for t in Quotas.job_types:
            if (t == "*") or (t in job.types):
                # Update the number of used resources
                self.counters["*", "*", t, "*"][0] += nb_resources
                self.counters["*", "*", t, user][0] += nb_resources
                self.counters["*", project, t, "*"][0] += nb_resources
                self.counters[queue, "*", t, "*"][0] += nb_resources
                self.counters[queue, project, t, user][0] += nb_resources
                self.counters[queue, project, t, "*"][0] += nb_resources
                self.counters[queue, "*", t, user][0] += nb_resources
                self.counters["*", project, t, user][0] += nb_resources
                # Update the number of running jobs
                self.counters["*", "*", t, "*"][1] += 1
                self.counters["*", "*", t, user][1] += 1
                self.counters["*", project, t, "*"][1] += 1
                self.counters[queue, "*", t, "*"][1] += 1
                self.counters[queue, project, t, user][1] += 1
                self.counters[queue, project, t, "*"][1] += 1
                self.counters[queue, "*", t, user][1] += 1
                self.counters["*", project, t, user][1] += 1
                # Update the resource * second
                self.counters["*", "*", t, "*"][2] += nb_resources * duration
                self.counters["*", "*", t, user][2] += nb_resources * duration
                self.counters["*", project, t, "*"][2] += nb_resources * duration
                self.counters[queue, "*", t, "*"][2] += nb_resources * duration
                self.counters[queue, project, t, user][2] += nb_resources * duration
                self.counters[queue, project, t, "*"][2] += nb_resources * duration
                self.counters[queue, "*", t, user][2] += nb_resources * duration
                self.counters["*", project, t, user][2] += nb_resources * duration

    def combine(self, quotas):
        # self.show_counters('combine before')
        for key, value in quotas.counters.items():
            self.counters[key][0] = max(self.counters[key][0], value[0])
            self.counters[key][1] = max(self.counters[key][1], value[1])
            self.counters[key][2] += value[2]
        # self.show_counters('combine after')

    def check(self, job):
        # self.show_counters('before check, job id: ' + str(job.id))
        for rl_fields, rl_quotas in self.rules.items():
            # pdb.set_trace()
            rl_queue, rl_project, rl_job_type, rl_user = rl_fields
            rl_nb_resources, rl_nb_jobs, rl_resources_time = rl_quotas
            for fields, counters in self.counters.items():
                queue, project, job_type, user = fields
                nb_resources, nb_jobs, resources_time = counters
                # match queue
                if (
                    ((rl_queue == "*") and (queue == "*"))
                    or ((rl_queue == queue) and (job.queue_name == queue))
                    or (rl_queue == "/")
                ):
                    # match project
                    if (
                        ((rl_project == "*") and (project == "*"))
                        or ((rl_project == project) and (job.project == project))
                        or (rl_project == "/")
                    ):
                        # match job_typ
                        if ((rl_job_type == "*") and (job_type == "*")) or (
                            (rl_job_type == job_type) and (job_type in job.types)
                        ):
                            # match user
                            if (
                                ((rl_user == "*") and (user == "*"))
                                or ((rl_user == user) and (job.user == user))
                                or (rl_user == "/")
                            ):
                                # test quotas values plus job's ones
                                # 1) test nb_resources
                                if (rl_nb_resources > -1) and (
                                    rl_nb_resources < nb_resources
                                ):
                                    return (
                                        False,
                                        "nb resources quotas failed",
                                        rl_fields,
                                        rl_nb_resources,
                                    )
                                # 2) test nb_jobs
                                if (rl_nb_jobs > -1) and (rl_nb_jobs < nb_jobs):
                                    return (
                                        False,
                                        "nb jobs quotas failed",
                                        rl_fields,
                                        rl_nb_jobs,
                                    )
                                # 3) test resources_time (work)
                                if (rl_resources_time > -1) and (
                                    rl_resources_time < resources_time
                                ):
                                    return (
                                        False,
                                        "resources hours quotas failed",
                                        rl_fields,
                                        rl_resources_time,
                                    )
        return (True, "quotas ok", "", 0)

    @staticmethod
    def check_slots_quotas(slots, sid_left, sid_right, job, job_nb_resources, duration):
        # loop over slot_set
        slots_quotas = Quotas()
        slots_quotas.rules = slots[sid_left].quotas.rules
        sid = sid_left
        while True:
            slot = slots[sid]
            # slot.quotas.show_counters('check_slots_quotas, b e: ' + str(slot.b) + ' ' + str(slot.e))
            slots_quotas.combine(slot.quotas)

            if sid == sid_right:
                break
            else:
                sid = slot.next
                if slot.next and (
                    slot.quotas_rules_id != slots[slot.next].quotas_rules_id
                ):
                    return (False, "different quotas rules over job's time", "", 0)
        # print('slots b e :' + str(slots[sid_left].b) + " " + str(slots[sid_right].e))
        slots_quotas.update(job, job_nb_resources, duration)
        return slots_quotas.check(job)

    def set_rules(self, rules_id):
        """Use for temporal calendar, when rules must be change from default"""
        if Quotas.calendar:
            self.rules = Quotas.calendar.quotas_rules_list[rules_id]

    @staticmethod
    def quotas_rules_fromJson(json_quotas_rules, all_value=None):
        def alphanum_to_int(v):
            """eval simple expression: int|float|ALL|int*ALL|float*ALL -> int"""
            if isinstance(v, int):
                return v
            else:
                try:
                    f = float(v)
                except ValueError:
                    c = 1
                    if "*" in v:
                        c, v = v.split("*")
                        c = float(c)
                    if v == "ALL":
                        v = all_value
                    else:
                        v = float(v)
                    f = c * v
            return int(f)

        rules = {}
        for k, v in json_quotas_rules.items():
            rules[tuple(k.split(","))] = [
                alphanum_to_int(v[0]),
                alphanum_to_int(v[1]),
                int(3600 * alphanum_to_int(v[2])),
            ]
        return rules

    @classmethod
    def load_quotas_rules(cls, config, all_value=None):
        """
        Simple example
        --------------

        {
            "quotas": {
                   "*,*,*,*": [120,-1,-1],
                    "*,*,*,john": [150,-1,-1]
            }
            "job_types": ['besteffort','deploy','console']
        }

        Temporal example
        ----------------
        {
            "periodical": [
                ["08:00-19:00 mon-fri * *", "quotas_workday", "workdays"],
                ["19:00-00:00 mon-thu * *", "quotas_night", "nights of workdays"],
                ["00:00-08:00 tue-fri * *", "quotas_night", "nights of workdays"],
                ["19:00-00:00 fri * *", "quotas_weekend", "weekend"],
                ["* sat-sun * *", "quotas_weekend", "weekend"],
                ["00:00-08:00 mon * *", "quotas_weekend", "weekend"]
            ],
            "oneshot": [
                ["2020-07-23 19:30", "2020-08-29 8:30", "quotas_holiday", "summer holiday"],
                ["2020-03-16 19:30", "2020-05-10 8:30", "quotas_holiday", "confinement"]
            ],
            "quotas_workday": {
                "*,*,*,john": [100,-1,-1],
                "*,projA,*,*": [200,-1,-1]
            },
            "quotas_night": {
                "*,*,*,john": [100,-1,-1],
                "*,projA,*,*": [200,-1,-1]
            },
            "quotas_weekend": {
                "*,*,*,john": [100,-1,-1],
                "*,projA,*,*": [200,-1,-1]
            },
            "quotas_holiday": {
                "*,*,*,john": [100,-1,-1],
                "*,projA,*,*": [200,-1,-1]
            }
        }

        """
        quotas_rules_filename = config["QUOTAS_CONF_FILE"]
        with open(quotas_rules_filename) as json_file:
            json_quotas = json.load(json_file)
            if ("periodical" in json_quotas) or ("oneshot" in json_quotas):
                cls.calendar = Calendar(json_quotas, config)
            if "quotas" in json_quotas:
                cls.default_rules = cls.quotas_rules_fromJson(
                    json_quotas["quotas"], all_value
                )
            if "job_types" in json_quotas:
                cls.job_types.extend(json_quotas["job_types"])
