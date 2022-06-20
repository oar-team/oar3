# coding: utf-8
# import oar.kao.job
import os
import tempfile
import time

import redis

from oar.kao.job import JobPseudo, save_assigns
from oar.kao.simsim import ResourceSetSimu
from oar.lib import GanttJobsPrediction, config, db
from oar.lib.interval import itvs2ids
from oar.lib.psycopg2 import pg_bulk_insert

nb_max_res = 200000
rs = ResourceSetSimu(rid_o2i=range(nb_max_res))


def create_db():
    config.clear()
    tempdir = tempfile.mkdtemp()
    config["LOG_FILE"] = os.path.join(tempdir, "oar.log")
    config["DB_TYPE"] = "Pg"
    config["DB_PORT"] = "5432"
    config["DB_BASE_NAME"] = "oar"
    config["DB_BASE_PASSWD"] = "oar"
    config["DB_BASE_LOGIN"] = "oar"
    config["DB_HOSTNAME"] = "localhost"

    db.create_all()


def delete_tables():
    db.session.execute(db["gantt_jobs_predictions"].delete())
    db.session.execute(db["gantt_jobs_resources"].delete())


def generate_jobs(nb_jobs, size_max, mode="same"):
    # print('mode: ', mode)
    jobs = {}
    if mode == "same":
        for i in range(nb_jobs):
            res = [(j, j) for j in range(size_max)]
            jobs[i + 1] = JobPseudo(id=i, moldable_id=i, start_time=10, res_set=res)
    else:
        print("mode :", mode, " does not exist")
        exit(1)
    return jobs


def save_assigns_bulk_0(jobs, resource_set):

    if len(jobs) > 0:
        mld_id_start_time_s = []
        mld_id_rid_s = []
        for j in jobs.values():
            mld_id_start_time_s.append((j.moldable_id, j.start_time))
            riods = itvs2ids(j.res_set)
            mld_id_rid_s.extend(
                [(j.moldable_id, resource_set.rid_o2i[rid]) for rid in riods]
            )

        with db.engine.connect() as to_conn:
            cursor = to_conn.connection.cursor()
            pg_bulk_insert(
                cursor,
                db["gantt_jobs_predictions"],
                mld_id_start_time_s,
                ("moldable_job_id", "start_time"),
                binary=True,
            )
            pg_bulk_insert(
                cursor,
                db["queues"],
                mld_id_rid_s,
                ("moldable_job_id", "resource_id"),
                binary=True,
            )


def save_assigns_redis_0(jobs, resource_set):
    if len(jobs) > 0:
        r = redis.Redis()
        mld_id_start_time_s = []
        for j in jobs.values():
            mld_id_start_time_s.append(
                {"moldable_job_id": j.moldable_id, "start_time": j.start_time}
            )
            riods = itvs2ids(j.res_set)
            str_mld_id_rids = ",".join(
                map(lambda x: str(resource_set.rid_o2i[x]), riods)
            )

            r.set(str(j.moldable_id), str_mld_id_rids)

        db.session.execute(GanttJobsPrediction.__table__.insert(), mld_id_start_time_s)


def save_assigns_redis_pipeline_0(jobs, resource_set):
    print("# save_assigns_redis_pipeline_0")
    t = 0
    if len(jobs) > 0:
        r = redis.Redis()
        pipe = r.pipeline()
        mld_id_start_time_s = []
        for j in jobs.values():
            t0 = time.time()
            mld_id_start_time_s.append(
                {"moldable_job_id": j.moldable_id, "start_time": j.start_time}
            )
            riods = itvs2ids(j.res_set)
            str_mld_id_rids = ",".join(
                map(lambda x: str(resource_set.rid_o2i[x]), riods)
            )
            t += time.time() - t0
            pipe.set(str(j.moldable_id), str_mld_id_rids)

        db.session.execute(GanttJobsPrediction.__table__.insert(), mld_id_start_time_s)
        pipe.execute()
    print("Cumlated mld_id_start_time_s.append time:", t)


def save_assigns_redis_pipeline_1(jobs, resource_set):
    print("# save_assigns_redis_pipeline_1 compact")
    t = 0
    if len(jobs) > 0:
        r = redis.Redis()
        pipe = r.pipeline()
        mld_id_start_time_s = []
        for j in jobs.values():
            t0 = time.time()
            mld_id_start_time_s.append(
                {"moldable_job_id": j.moldable_id, "start_time": j.start_time}
            )
            # riods = itvs2ids(j.res_set)
            str_mld_id_rids = j.res_set

            t += time.time() - t0
            pipe.set(str(j.moldable_id), str_mld_id_rids)

        db.session.execute(GanttJobsPrediction.__table__.insert(), mld_id_start_time_s)
        pipe.execute()
    print("Cumlated mld_id_start_time_s.append time:", t)


def bench_job_same(nb_job_exp=10, job_size=100, save_assign="default"):
    delete_tables()
    print("# bench: ", save_assign)
    print("# nb_j, job_size, time")
    for j in range(nb_job_exp):
        nb_j = 2**j
        jobs = generate_jobs(nb_j, job_size)
        start = time.time()
        # getattr(oar.kao.job, "save_assigns_bulk")(jobs, rs)

        if save_assign == "bulk_0":
            save_assigns_bulk_0(jobs, rs)
        elif save_assign == "redis_0":
            save_assigns_redis_0(jobs, rs)
        elif save_assign == "redis_pipeline_0":
            save_assigns_redis_pipeline_0(jobs, rs)
        elif save_assign == "redis_pipeline_1":
            save_assigns_redis_pipeline_1(jobs, rs)
        else:
            save_assigns(jobs, rs)
        end = time.time()
        print(nb_j, job_size, end - start)
        delete_tables()


create_db()
nb_jobs_exp = 10
job_size = 10000
# bench_job_sxsame(12, 100, 'redis_0')
# bench_job_same(16, 100, 'redis_pipeline_0')
bench_job_same(nb_jobs_exp, job_size, "default")
bench_job_same(nb_jobs_exp, job_size, "bulk_0")
# bench_job_same(nb_jobs_exp, job_size, 'save_assigns_redis')
# bench_job_same(nb_jobs_exp, job_size, 'redis_0')
# bench_job_same(nb_jobs_exp, job_size, 'redis_pipeline_0')
bench_job_same(nb_jobs_exp, job_size, "redis_pipeline_1")
# bench_job_same(nb_jobs_exp, job_size, 'save_assigns_redis_0')
