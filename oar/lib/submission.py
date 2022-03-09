# -*- coding: utf-8 -*-
import copy
import os
import random
import re
import sys
from socket import gethostname

from procset import ProcSet
from sqlalchemy import exc, text

import oar.lib.tools as tools
from oar.lib import (
    AdmissionRule,
    Challenge,
    Job,
    JobDependencie,
    JobResourceDescription,
    JobResourceGroup,
    JobStateLog,
    JobType,
    MoldableJobDescription,
    Queue,
    Resource,
    config,
    db,
)
from oar.lib.hierarchy import find_resource_hierarchies_scattered
from oar.lib.resource import ResourceSet
from oar.lib.tools import (
    PIPE,
    Popen,
    format_job_message_text,
    get_date,
    hms_str_to_duration,
    sql_to_local,
)


def lstrip_none(s):
    if s:
        return s.lstrip()
    else:
        return None


# TODO to remove/modifiy
def print_info(*objs):
    print("# INFO: ", *objs, file=sys.stderr)


def job_key_management(
    use_job_key,
    import_job_key_inline,
    import_job_key_file,
    export_job_key_file,
    user=None,
):
    """Manage the job key if option is activated.
    Read job key file if import from file and generate a job key if no import.
    This function returns with job_key_priv and job_key_pub set if use_job_key is set.
    """
    # import pdb; pdb.set_trace()
    error = (0, "")

    job_key_priv = ""
    job_key_pub = ""

    if (
        use_job_key
        and (not import_job_key_inline)
        and (not import_job_key_file)
        and "OAR_JOB_KEY_FILE" in os.environ
    ):
        import_job_key_file = os.environ["OAR_JOB_KEY_FILE"]

    if not use_job_key and (
        import_job_key_inline or import_job_key_file or export_job_key_file
    ):
        error = (
            -15,
            "You must set the --use-job-key (or -k) option in order to use other job key related options.",
        )
        return (error, "", "")
    if use_job_key:
        if import_job_key_inline and import_job_key_file:
            error = (
                -15,
                "You cannot import a job key both inline and from a file at the same time.",
            )
            return (error, "", "")

        tmp_job_key_file = (
            config["OAREXEC_DIRECTORY"] + "/oarsub_" + str(os.getpid()) + ".jobkey"
        )
        if import_job_key_inline or import_job_key_file:
            # job key is imported
            if import_job_key_inline:
                # inline import
                print(
                    "# Info: importing job key inline."
                )  # TODO use a wrapped function
                import_job_key = import_job_key_inline
            else:
                # file import
                print("# Info: import job key from file: " + import_job_key_file)
                if not user:
                    user = os.environ["OARDO_USER"]
                # read key files: oardodo su - user needed in order to be able to read the file for sure
                # safer way to do a `cmd`, see perl cookbook (come for OAR2)

                os.environ["OARDO_BECOME_USER"] = user

                try:
                    process = tools.Popen(
                        ["oardodo", "cat", import_job_key_file], stdout=PIPE
                    )
                except Exception:
                    error = (-14, "Unable to read: " + import_job_key_file)
                    return (error, "", "")

                stdout = process.communicate()[0]
                # TODO never used import_job_key
                import_job_key = stdout.decode()  # noqa

            # Write imported_job_key in tmp_job_key_file
            try:
                with os.fdopen(
                    os.open(tmp_job_key_file, os.O_WRONLY | os.O_CREAT, 0o600), "w"
                ) as f:
                    f.write(import_job_key_inline)
            except Exception as e:
                error = (
                    -14,
                    "Cannot open tmp file and write in: "
                    + tmp_job_key_file
                    + ". Raised exception: "
                    + str(e),
                )
                return (error, "", "")

            # Extract the public key from the private one
            cmd = (
                'bash -c "SSH_ASKPASS=/bin/true ssh-keygen -y -f '
                + tmp_job_key_file
                + " < /dev/null 2> /dev/null > "
                + tmp_job_key_file
                + '.pub"'
            )
            base_error_msg = ""
            try:
                retcode = tools.call(cmd, shell=True)
                if retcode != 0:
                    base_error_msg = "Child was terminated by signal: " + str(-retcode)
            except OSError as e:
                base_error_msg = "Execution failed: " + str(e)

            if base_error_msg:
                # Remove tmp_job_key_file, tmp_job_key_file.pub if they exist
                try:
                    os.remove(tmp_job_key_file)
                except OSError:
                    pass
                try:
                    os.remove(tmp_job_key_file + ".pub")
                except OSError:
                    pass
                return (
                    (-14, "Fail to extract the public key. " + base_error_msg),
                    "",
                    "",
                )

        else:
            # The key must be generated
            print("# Info: generating a job key...")

            # ssh-keygen: no passphrase, smallest key (1024 bits), ssh2 rsa faster than dsa.
            cmd = (
                "bash -c \"ssh-keygen -b 1024 -N '' -t rsa -f "
                + tmp_job_key_file
                + ' > /dev/null"'
            )

            base_error_msg = ""
            try:
                retcode = tools.call(cmd, shell=True)
                if retcode != 0:
                    base_error_msg = "Child was terminated by signal: " + str(-retcode)
            except OSError as e:
                base_error_msg = "Execution failed: " + str(e)

            if base_error_msg:
                return ((-14, "Job key generation failed. " + base_error_msg), "", "")

        # Priv and pub key file must now exist
        try:
            with open(tmp_job_key_file) as f:
                job_key_priv = f.read()
        except OSError as e:
            return ((-14, " Fail to read private key. " + str(e)), "", "")

        try:
            with open(tmp_job_key_file + ".pub") as f:
                job_key_pub = f.read()
        except OSError as e:
            return ((-14, " Fail to read public key. " + str(e)), "", "")

        os.remove(tmp_job_key_file)
        os.remove(tmp_job_key_file + ".pub")

    # Last checks
    if use_job_key:
        if job_key_pub == "":
            error = (-15, "Missing job public key (private key found);")
            return (error, "", "")
        if job_key_priv == "":
            error = (-15, "Missing job private key.")
            return (error, "", "")

        if not re.match(r"^(ssh-rsa|ssh-dss)\s.+\n*$", job_key_pub):
            error = (
                -14,
                "Bad job key format. The public key must begin with either 'ssh-rsa' "
                "or 'ssh-dss' and is only 1 line.",
            )
            return (error, "", "")

        job_key_pub = job_key_pub.replace("\n", "")

    return (error, job_key_priv, job_key_pub)


def scan_script(submitted_filename, initial_request_str, user=None):
    result = {}
    error = (0, "")
    process = None

    if not user:
        user = os.environ["OARDO_USER"]
    os.environ["OARDO_BECOME_USER"] = user

    try:
        process = tools.Popen(["oardodo", "cat", submitted_filename], stdout=PIPE)
    except Exception:
        error = (-70, "Unable to read: " + submitted_filename)
        return (error, result)

    stdout = process.communicate()[0]
    output = stdout.decode()

    for line in output.split("\n"):
        if re.match(r"^#OAR\s+", line):
            line = line.strip()
            m = re.match(r"^#OAR\s+(-l|--resource)\s*(.+)\s*$", line)
            if m:
                if "resource" in result:
                    result["resource"].append(m.group(2))
                else:
                    result["resource"] = [m.group(2)]
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-q|--queue)\s*(.+)\s*$", line)
            if m:
                result["queue"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-p|--property)\s*(.+)\s*$", line)
            if m:
                result["property"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(--checkpoint)\s*(\d+)\s*$", line)
            if m:
                result["checkpoint"] = int(m.group(2))
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(--notify)\s*(.+)\s*$", line)
            if m:
                result["notify"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-t|--type)\s*(.+)\s*$", line)
            if m:
                if "types" in result:
                    result["types"].append(m.group(2))
                else:
                    result["types"] = [m.group(2)]
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-d|--directory)\s*(.+)\s*$", line)
            if m:
                result["directory"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-n|--name)\s*(.+)\s*$", line)
            if m:
                result["name"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(--project)\s*(.+)\s*$", line)
            if m:
                result["project"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(--hold)\s*$", line)
            if m:
                result["hold"] = True
                initial_request_str += " " + m.group(1)
                continue
            # TODO modify documentation
            m = re.match(r"^#OAR\s+(-a|--after)\s*(\d+)\s*$", line)
            if m:
                if "dependencies" in result:
                    result["dependencies"].append(int(m.group(2)))
                else:
                    result["dependencies"] = [int(m.group(2))]
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(--signal)\s*(\d+)\s*$", line)
            if m:
                result["signal"] = int(m.group(2))
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-O|--stdout)\s*(.+)\s*$", line)
            if m:
                result["stdout"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-E|--stderr)\s*(.+)\s*$", line)
            if m:
                result["stderr"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-k|--use-job-key)\s*$", line)
            if m:
                result["use_job_key"] = True
                initial_request_str += " " + m.group(1)
                continue
            m = re.match(r"^#OAR\s+(--import-job-key-inline-priv)\s*(.+)\s*$", line)
            if m:
                result["import_job_key_inline"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-i|--import-job-key-from-file)\s*(.+)\s*$", line)
            if m:
                result["import_job_key_file"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-e|--export-job-key-to-file)\s*(.+)\s*$", line)
            if m:
                result["export_job_key_file"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(-s|--stagein)\s*(.+)\s*$", line)
            if m:
                result["stagein"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(--stagein-md5sum)\s*(.+)\s*$", line)
            if m:
                result["stagein_md5sum"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(--array)\s*(\d+)\s*$", line)
            if m:
                result["array"] = int(m.group(2))
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            m = re.match(r"^#OAR\s+(--array-param-file)\s*(.+)\s*$", line)
            if m:
                result["array_param_file"] = m.group(2)
                initial_request_str += " " + m.group(1) + " " + m.group(2)
                continue
            error = (-72, "Not able to parse line: " + line)
        result["initial_request"] = initial_request_str
    return (error, result)


def parse_resource_descriptions(
    str_resource_request_list, default_resources, nodes_resources
):
    """Parse and transform a cli oar resource request in python structure which is manipulated
    in admission process

    Resource request output composition:

         resource_request = [moldable_instance , ...]
         moldable_instance =  ( resource_desc_lst , walltime)
         walltime = int|None
         resource_desc_lst = [{property: prop, resources: res}]
         property = string|''|None
         resources = [{resource: r, value: v}]
         r = string
         v = int

    Example:

     - oar cli resource request:
         "{ sql1 }/prop1=1/prop2=3+{sql2}/prop3=2/prop4=1/prop5=1+...,walltime=60"

     - str_resource_request_list input:
         ["/switch=2/nodes=10+{lic_type = 'mathlab'}/licence=2, walltime = 60"]

     - resource_request output:
         [
            ([{property: '', resources:  [{resource: 'switch', value: 2}, {resource: 'nodes', value: 10}]},
              {property: "lic_type = 'mathlab'", resources: [{resource: 'licence', value: 2}]}
             ], 60)
         ]
    """

    if not str_resource_request_list:
        str_resource_request_list = [default_resources]

    resource_request = []  # resource_request = [moldable_instance , ...]
    for str_resource_request in str_resource_request_list:
        res_req_walltime = str_resource_request.split(",")
        # only walltime is provided
        if re.match(r"walltime=.*", res_req_walltime[0]):
            req_walltime = res_req_walltime[0]
            res_req_walltime = [default_resources, req_walltime]
        str_prop_res_req = res_req_walltime[0]

        walltime = None
        if len(res_req_walltime) == 2:
            walltime_desc = res_req_walltime[1].split("=")
            if len(walltime_desc) == 2:
                walltime = hms_str_to_duration(walltime_desc[1])

        prop_res_reqs = str_prop_res_req.split("+")

        resource_desc = []  # resource_desc = [{property: prop, resources: res}]
        for prop_res_req in prop_res_reqs:
            # Extract propertie if any
            m = re.search(r"^\{(.+?)\}(.*)$", prop_res_req)
            if m:
                property = m.group(1)
                str_res_req = m.group(2)
            else:
                property = ""
                str_res_req = prop_res_req

            str_res_value_lst = str_res_req.split("/")

            resources = []  # resources = [{resource: r, value: v}]

            for str_res_value in str_res_value_lst:
                if (
                    str_res_value.lstrip()
                ):  # to filter first and last / if any "/nodes=1" or "/nodes=1/
                    # remove  first and trailing spaces"
                    res, value = str_res_value.lstrip().split("=")

                    if res == "nodes":
                        res = nodes_resources
                    if value == "ALL":
                        v = -1
                    elif value == "BESTHALF":
                        v = -2
                    elif value == "BEST":
                        v = -3
                    else:
                        v = str(value)
                    resources.append({"resource": res, "value": v})

            resource_desc.append({"property": property, "resources": resources})

        resource_request.append((resource_desc, walltime))

    return resource_request


def estimate_job_nb_resources(resource_request, j_properties):
    """returns an array with an estimation of the number of resources that can be used by a job:
    (resources_available, [(nbresources => int, walltime => int)])
    """
    # estimate_job_nb_resources
    estimated_nb_resources = []
    is_resource_available = False
    resource_set = ResourceSet()
    resources_itvs = resource_set.roid_itvs

    for mld_idx, mld_resource_request in enumerate(resource_request):

        resource_desc, walltime = mld_resource_request

        if not walltime:
            walltime = str(config["DEFAULT_JOB_WALLTIME"])

        estimated_nb_res = 0

        for prop_res in resource_desc:
            jrg_grp_property = prop_res["property"]
            resource_value_lst = prop_res["resources"]

            #
            # determine resource constraints
            #
            if (not j_properties) and (
                not jrg_grp_property or (jrg_grp_property == "type='default'")
            ):  # TODO change to re.match
                # copy itvs
                constraints = copy.copy(resource_set.roid_itvs)
            else:
                and_sql = ""
                if j_properties and jrg_grp_property:
                    and_sql = " AND "
                if j_properties is None:
                    j_properties = ""
                if jrg_grp_property is None:
                    jrg_grp_property = ""

                sql_constraints = j_properties + and_sql + jrg_grp_property

                try:
                    request_constraints = (
                        db.query(Resource.id).filter(text(sql_constraints)).all()
                    )
                except exc.SQLAlchemyError:
                    error_code = -5
                    error_msg = (
                        "Bad resource SQL constraints request:"
                        + sql_constraints
                        + "\n"
                        + "SQLAlchemyError: "
                        + str(exc)
                    )
                    error = (error_code, error_msg)
                    return (error, None, None)

                roids = [resource_set.rid_i2o[int(y[0])] for y in request_constraints]
                constraints = ProcSet(*roids)

            hy_levels = []
            hy_nbs = []
            for resource_value in resource_value_lst:
                res_name = resource_value["resource"]
                value = resource_value["value"]
                hy_levels.append(resource_set.hierarchy[res_name])
                hy_nbs.append(int(value))

            cts_resources_itvs = constraints & resources_itvs
            res_itvs = find_resource_hierarchies_scattered(
                cts_resources_itvs, hy_levels, hy_nbs
            )
            if res_itvs:
                estimated_nb_res = len(res_itvs)
            else:
                estimated_nb_res = 0
            break

        if estimated_nb_res > 0:
            is_resource_available = True

        estimated_nb_resources.append((estimated_nb_res, walltime))
        print_info(
            "Moldable instance: ",
            mld_idx + 1,
            " Estimated nb resources: ",
            estimated_nb_res,
            " Walltime: ",
            walltime,
        )

    if not is_resource_available:
        error = (-5, "There are not enough resources for your request")
        return (error, None, None)

    return ((0, ""), is_resource_available, estimated_nb_resources)


def add_micheline_subjob(
    job_parameters, ssh_private_key, ssh_public_key, array_id, array_index, command
):

    # Estimate_job_nb_resources and incidentally test if properties and resources request are coherent
    # against available resources

    date = get_date()
    properties = job_parameters.properties
    resource_request = job_parameters.resource_request

    error, resource_available, estimated_nb_resources = estimate_job_nb_resources(
        resource_request, properties
    )
    if error[0] != 0:
        return (error, -1)

    # Add admin properties to the job
    if hasattr(job_parameters, "properties_applied_after_validation:"):
        if properties:
            properties = (
                "("
                + properties
                + ") AND "
                + job_parameters.properties_applied_after_validation
            )
        else:
            properties = job_parameters.properties_applied_after_validation
    job_parameters.properties = properties

    name = job_parameters.name
    stdout = job_parameters.stdout
    if not stdout:
        stdout = "OAR"
        if name is not None:
            stdout += "." + name
        stdout += ".%jobid%.stdout"
    elif name is not None:
        stdout = re.sub(r"%jobname%", name, stdout)

    stderr = job_parameters.stderr
    if not stderr:
        stderr = "OAR"
        if name is not None:
            stderr += "." + name
        stderr += ".%jobid%.stderr"
    elif name is not None:
        stderr = re.sub(r"%jobname%", name, stderr)

    # Insert job

    kwargs = job_parameters.kwargs(command, date)
    estimated_nbr, estimated_walltime = estimated_nb_resources[0]
    kwargs["message"] = format_job_message_text(
        name,
        estimated_nbr,
        estimated_walltime,
        job_parameters.job_type,
        job_parameters.reservation_date,
        job_parameters.queue,
        job_parameters.project,
        job_parameters.types,
        "",
    )
    if job_parameters.reservation_date:
        kwargs["reservation"] = "toSchedule"
    else:
        kwargs["reservation"] = "None"

    kwargs["array_index"] = array_index
    kwargs["stdout_file"] = stdout
    kwargs["stderr_file"] = stderr

    if array_id > 0:
        kwargs["array_id"] = array_id

    ins = Job.__table__.insert().values(**kwargs)
    result = db.session.execute(ins)
    job_id = result.inserted_primary_key[0]

    if array_id <= 0:
        db.query(Job).filter(Job.id == job_id).update(
            {Job.array_id: job_id}, synchronize_session=False
        )
        db.commit()

    random_number = random.randint(1, 1000000000000)
    ins = Challenge.__table__.insert().values(
        {
            "job_id": job_id,
            "challenge": random_number,
            "ssh_private_key": ssh_private_key,
            "ssh_public_key": ssh_public_key,
        }
    )
    db.session.execute(ins)

    # print(resource_request)

    # Insert resources request in DB
    mld_jid_walltimes = []
    resource_desc_lst = []
    for moldable_instance in resource_request:
        resource_desc, walltime = moldable_instance
        if not walltime:
            walltime = config["DEFAULT_JOB_WALLTIME"]
        mld_jid_walltimes.append(
            {"moldable_job_id": job_id, "moldable_walltime": walltime}
        )
        resource_desc_lst.append(resource_desc)

    # Insert MoldableJobDescription job_id and walltime
    # print('mld_jid_walltimes)
    result = db.session.execute(
        MoldableJobDescription.__table__.insert(), mld_jid_walltimes
    )

    # Retrieve MoldableJobDescription.ids
    if len(mld_jid_walltimes) == 1:
        mld_ids = [result.inserted_primary_key[0]]
    else:
        res = (
            db.query(MoldableJobDescription.id)
            .filter(MoldableJobDescription.job_id == job_id)
            .all()
        )
        mld_ids = [e[0] for e in res]
    #
    # print(mld_ids, resource_desc_lst)
    for mld_idx, resource_desc in enumerate(resource_desc_lst):
        # job_resource_groups
        mld_id_property = []
        res_lst = []

        moldable_id = mld_ids[mld_idx]

        for prop_res in resource_desc:
            prop = prop_res["property"]
            res = prop_res["resources"]

            mld_id_property.append(
                {"res_group_moldable_id": moldable_id, "res_group_property": prop}
            )

            res_lst.append(res)

        # print(mld_id_property)
        # Insert property for moldable
        result = db.session.execute(
            JobResourceGroup.__table__.insert(), mld_id_property
        )

        if len(mld_id_property) == 1:
            grp_ids = [result.inserted_primary_key[0]]
        else:
            res = (
                db.query(JobResourceGroup.id)
                .filter(JobResourceGroup.moldable_id == moldable_id)
                .all()
            )
            grp_ids = [e[0] for e in res]

        # print('grp_ids, res_lst)
        # Insert job_resource_descriptions
        for grp_idx, res in enumerate(res_lst):
            res_description = []
            for idx, res_value in enumerate(res):
                res_description.append(
                    {
                        "res_job_group_id": grp_ids[grp_idx],
                        "res_job_resource_type": res_value["resource"],
                        "res_job_value": res_value["value"],
                        "res_job_order": idx,
                    }
                )
            # print(res_description)
            db.session.execute(
                JobResourceDescription.__table__.insert(), res_description
            )

    # types of job
    types = job_parameters.types
    if types:
        ins = [{"job_id": job_id, "type": typ} for typ in types]
        db.session.execute(JobType.__table__.insert(), ins)

    # Set insert job dependencies
    dependencies = job_parameters.dependencies
    if dependencies:
        ins = [{"job_id": job_id, "job_id_required": dep} for dep in dependencies]
        db.session.execute(JobDependencie.__table__.insert(), ins)

    if not job_parameters.hold:
        req = db.insert(JobStateLog).values(
            {"job_id": job_id, "job_state": "Waiting", "date_start": date}
        )
        db.session.execute(req)
        db.commit()

        db.query(Job).filter(Job.id == job_id).update(
            {Job.state: "Waiting"}, synchronize_session=False
        )
        db.commit()
    else:
        req = db.insert(JobStateLog).values(
            {"job_id": job_id, "job_state": "Hold", "date_start": date}
        )
        db.session.execute(req)
        db.commit()

    return ((0, ""), job_id)


def add_micheline_simple_array_job(
    job_parameters,
    ssh_private_key,
    ssh_public_key,
    array_id,
    array_index,
    array_commands,
):

    job_id_list = []
    date = get_date()

    # Check the jobs are no moldable
    resource_request = job_parameters.resource_request
    if len(resource_request) > 1:
        error = (-30, "array jobs cannot be moldable")
        return (error, [])

    # Estimate_job_nb_resources and incidentally test if properties and resources request are coherent
    # against avalaible resources
    properties = job_parameters.properties
    error, resource_available, estimated_nb_resources = estimate_job_nb_resources(
        resource_request, properties
    )

    # Add admin properties to the job
    if hasattr(job_parameters, "properties_applied_after_validation:"):
        if properties:
            properties = (
                "("
                + properties
                + ") AND "
                + job_parameters.properties_applied_after_validation
            )
        else:
            properties = job_parameters.properties_applied_after_validation
    job_parameters.properties = properties

    name = job_parameters.name
    stdout = job_parameters.stdout
    if not stdout:
        stdout = "OAR"
        if name is not None:
            stdout += "." + name
        stdout += ".%jobid%.stdout"
    elif name is not None:
        stdout = re.sub(r"%jobname%", name, stdout)

    stderr = job_parameters.stderr
    if not stderr:
        stderr = "OAR"
        if name is not None:
            stderr += "." + name
        stderr += ".%jobid%.stderr"
    elif name is not None:
        stderr = re.sub(r"%jobname%", name, stderr)

    # Insert job
    kwargs = job_parameters.kwargs(array_commands[0], date)
    estimated_nbr, estimated_walltime = estimated_nb_resources[0]
    kwargs["message"] = format_job_message_text(
        name,
        estimated_nbr,
        estimated_walltime,
        job_parameters.job_type,
        job_parameters.reservation_date,
        job_parameters.queue,
        job_parameters.project,
        job_parameters.types,
        "",
    )
    kwargs["array_index"] = array_index

    kwargs["stdout_file"] = stdout
    kwargs["stderr_file"] = stderr
    # print(kwargs)

    ins = Job.__table__.insert().values(**kwargs)
    result = db.session.execute(ins)
    first_job_id = result.inserted_primary_key[0]

    # Update array_id
    array_id = first_job_id
    db.query(Job).filter(Job.id == first_job_id).update(
        {Job.array_id: array_id}, synchronize_session=False
    )
    db.commit()

    # Insert remaining array jobs with array_id
    jobs_data = []
    kwargs["array_id"] = array_id
    for command in array_commands[1:]:
        kwargs["array_index"] += 1
        job_data = kwargs.copy()
        job_data["command"] = command
        jobs_data.append(job_data)

    db.session.execute(Job.__table__.insert(), jobs_data)
    db.commit()

    # Retrieve job_ids thanks to array_id value
    result = db.query(Job.id).filter(Job.array_id == array_id).all()
    job_id_list = [r[0] for r in result]

    # TODO Populate challenges and moldable_job_descriptions tables (DONE?)
    challenges = []
    moldable_job_descriptions = []

    walltime = resource_request[0][1]
    if not walltime:
        walltime = config["DEFAULT_JOB_WALLTIME"]

    for job_id in job_id_list:
        random_number = random.randint(1, 1000000000000)
        challenges.append({"job_id": job_id, "challenge": random_number})
        moldable_job_descriptions.append(
            {"moldable_job_id": job_id, "moldable_walltime": walltime}
        )

    db.session.execute(Challenge.__table__.insert(), challenges)
    db.session.execute(
        MoldableJobDescription.__table__.insert(), moldable_job_descriptions
    )
    db.commit()

    # Retrieve moldable_ids thanks to job_ids
    result = (
        db.query(MoldableJobDescription.id)
        .filter(MoldableJobDescription.job_id.in_(tuple(job_id_list)))
        .order_by(MoldableJobDescription.id)
        .all()
    )
    moldable_ids = [r[0] for r in result]

    # Populate job_resource_groups table
    job_resource_groups = []
    resource_desc_lst = resource_request[0][0]

    for moldable_id in moldable_ids:
        for resource_desc in resource_desc_lst:
            prop = resource_desc["property"]
            job_resource_groups.append(
                {"res_group_moldable_id": moldable_id, "res_group_property": prop}
            )

    db.session.execute(JobResourceGroup.__table__.insert(), job_resource_groups)
    db.commit()

    # Retrieve res_group_ids thanks to moldable_ids
    result = (
        db.query(JobResourceGroup.id)
        .filter(JobResourceGroup.moldable_id.in_(tuple(moldable_ids)))
        .order_by(JobResourceGroup.id)
        .all()
    )
    res_group_ids = [r[0] for r in result]

    # Populate job_resource_descriptions table
    job_resource_descriptions = []
    k = 0
    for i in range(len(array_commands)):  # Nb jobs
        for resource_desc in resource_desc_lst:
            order = 0
            for res_val in resource_desc["resources"]:
                job_resource_descriptions.append(
                    {
                        "res_job_group_id": res_group_ids[k],
                        "res_job_resource_type": res_val["resource"],
                        "res_job_value": res_val["value"],
                        "res_job_order": order,
                    }
                )
                order += 1
            k += 1

    db.session.execute(
        JobResourceDescription.__table__.insert(), job_resource_descriptions
    )
    db.commit()

    # Populate job_types table
    types = job_parameters.types
    if types:
        jobs_types = []
        for job_id in job_id_list:
            for typ in types:
                jobs_types.append({"job_id": job_id, "type": typ})
        db.session.execute(JobType.__table__.insert(), jobs_types)
        db.commit()

    # Set insert job dependencies
    dependencies = job_parameters.dependencies
    if dependencies:
        jobs_dependencies = []
        for job_id in job_id_list:
            for dep in dependencies:
                jobs_dependencies.append({"job_id": job_id, "job_id_required": dep})
        db.session.execute(JobDependencie.__table__.insert(), jobs_dependencies)
        db.commit()

    # Hold/Waiting management, job_state_log setting
    # Job is inserted with hold state first
    state_log = "Hold"
    if not job_parameters.hold:
        state_log = "Waiting"
        db.query(Job).filter(Job.array_id == array_id).update(
            {Job.state: state_log}, synchronize_session=False
        )
        db.commit()

    # Update array_id field and set job to state if waiting and insert job_state_log
    job_state_logs = [
        {"job_id": job_id, "job_state": state_log, "date_start": date}
        for job_id in job_id_list
    ]
    db.session.execute(JobStateLog.__table__.insert(), job_state_logs)
    db.commit()

    return ((0, ""), job_id_list)


def add_micheline_jobs(
    job_parameters, import_job_key_inline, import_job_key_file, export_job_key_file
):
    """Adds a new job(or multiple in case of array-job) to the table Jobs applying
    the admission rules from the base  parameters : base, jobtype, nbnodes,
    , command, infotype, walltime, queuename, jobproperties,
    startTimeReservation
    return value : ref. of array of created jobids
    side effects : adds an entry to the table Jobs
                 the first jobid is found taking the maximal jobid from
                 jobs in the table plus 1, the next (if any) takes the next
                 jobid. Array-job submission is atomic and array_index are
                 sequential
                 the rules in the base are pieces of python code directly
                 evaluated here, so in theory any side effect is possible
                 in normal use, the unique effect of an admission rule should
                 be to change parameters
    """

    array_id = 0

    # TODO can we remove it ?
    if job_parameters.reservation_date:
        job_parameters.start_time = job_parameters.reservation_date

    # job_vars['user'] = os.environ['OARDO_USER']

    # Check the user validity
    if not re.match(r"^[a-zA-Z0-9_-]+$", job_parameters.user):
        error = (-11, "invalid username:", job_parameters.user)
        return (error, [])
    # Verify notify syntax
    if job_parameters.notify and not re.match(
        r"^\s*(\[\s*(.+)\s*\]\s*)?(mail|exec)\s*:.+$", job_parameters.notify
    ):

        error = (-6, "bad syntax for the notify option.")
        return (error, [])

    # Check the stdout and stderr path validity
    if job_parameters.stdout and not re.match(
        r"^[a-zA-Z0-9_.\/\-\%\\ ]+$", job_parameters.stdout
    ):
        error = (12, "invalid stdout file name (bad character)")
        return (error, [])

    if job_parameters.stderr and not re.match(
        r"^[a-zA-Z0-9_.\/\-\%\\ ]+$", job_parameters.stderr
    ):
        error = (-13, "invalid stderr file name (bad character)")
        return (error, [])

    # Remove no_quotas must set within admission rules or automatically for admin jobs
    if config["QUOTAS"] == "yes" and "no_quotas" in job_parameters.types:
        # TODO print Warning
        job_parameters.types.remove("no_quotas")

    # Retrieve Micheline's rules
    str_rules = ""
    if ("ADMISSION_RULES_IN_FILES" in config) and (
        config["ADMISSION_RULES_IN_FILES"] == "yes"
    ):
        # Read admission_rules from files
        rules_dir = "/etc/oar/admission_rules.d/"
        file_names = os.listdir(rules_dir)

        file_names.sort()
        for file_name in file_names:
            if re.match(r"^\d+_.*", file_name):
                with open(rules_dir + file_name, "r") as rule_file:
                    for line in rule_file:
                        str_rules += line
    else:
        # Retrieve Micheline's rules from database
        rules = (
            db.query(AdmissionRule.rule)
            .filter(AdmissionRule.enabled == "YES")
            .order_by(AdmissionRule.priority, AdmissionRule.id)
            .all()
        )
        str_rules = "\n".join([r[0] for r in rules])

    # Apply rules
    code = compile(str_rules, "<string>", "exec")

    try:
        exec(code, globals(), job_parameters.__dict__)
    except Exception:
        err = sys.exc_info()
        error = (
            -2,
            str(err[1]) + ", a failed admission rule prevented submitting the job.",
        )
        return (error, [])

    # Test if the queue exists
    if not db.query(Queue).filter(Queue.name == job_parameters.queue).all():
        error = (-8, "queue " + job_parameters.queue + " does not exist")
        return (error, [])

    # Automatically add no quotas restriction for admin job
    if config["QUOTAS"] == "yes" and job_parameters.queue == "admin":
        job_parameters.types.append("no_quotas")

    # TODO move to job class ?
    if job_parameters.array_params:
        array_commands = [
            job_parameters.command + " " + params
            for params in job_parameters.array_params
        ]
        job_parameters.array_nb = len(array_commands)
    else:
        array_commands = [job_parameters.command] * job_parameters.array_nb

    array_index = 1
    job_id_list = []
    ssh_private_key = ""
    ssh_public_key = ""
    if job_parameters.array_nb > 1 and not job_parameters.use_job_key:
        # Simple array job submission is used
        (error, job_id_list) = add_micheline_simple_array_job(
            job_parameters,
            ssh_private_key,
            ssh_public_key,
            array_id,
            array_index,
            array_commands,
        )

    else:
        # Single job to submit or when job key is used with array job
        for cmd in array_commands:
            (error, ssh_private_key, ssh_public_key) = job_key_management(
                job_parameters.use_job_key,
                import_job_key_inline,
                import_job_key_file,
                export_job_key_file,
            )
            if error[0] != 0:
                return (error, job_id_list)

            (error, job_id) = add_micheline_subjob(
                job_parameters,
                ssh_private_key,
                ssh_public_key,
                array_id,
                array_index,
                cmd,
            )

            if error[0] == 0:
                job_id_list.append(job_id)
            else:
                return (error, job_id_list)

            if array_id <= 0:
                array_id = job_id_list[0]
            array_index += 1

            if job_parameters.use_job_key and export_job_key_file:
                # Copy the keys in the directory specified with the right name
                export_job_key_file_tmp = export_job_key_file
                export_job_key_file_tmp = export_job_key_file_tmp.replace(
                    "%jobid%", str(job_id)
                )

                # Write the private job key with the user ownership
                # TODO never used
                user = job_parameters.user  # noqa
                os.environ["OARDO_BECOME_USER"]
                prev_umask = os.umask(0o117)

                try:
                    p = Popen(
                        ["oardodo", "dd", "of=" + export_job_key_file_tmp], stdin=PIPE
                    )
                    p.stdin.write(
                        ssh_priv_key.encode()  # noqa TODO variable doesn't exist + write corresponding test
                    )
                    p.stdin.flush()
                    p.stdin.close()
                    while p.returncode is None:
                        p.poll()
                    if p.returncode:
                        error = (
                            60,
                            "Return code for the private job key writing is not null:"
                            + str(p.returncode),
                        )
                        return (error, job_id_list)
                except Exception as e:
                    error = (60, "Cannot write the private job key: " + str(e))

                os.umask(prev_umask)

                print("# Info: export job key to file: " + export_job_key_file_tmp)

    return ((0, ""), job_id_list)


def check_reservation(reservation_date_str):
    reservationn_date_str = lstrip_none(reservation_date_str)
    if reservationn_date_str:
        m = re.search(
            r"^\s*(\d{4}\-\d{1,2}\-\d{1,2})\s+(\d{1,2}:\d{1,2}:\d{1,2})\s*$",
            reservationn_date_str,
        )
        if m:
            reservation_date = sql_to_local(m.group(1) + " " + m.group(2))
            return ((0, ""), reservation_date)

    error = (
        7,
        'syntax error for the advance reservation start date \
    specification. Expected format is:"YYYY-MM-DD hh:mm:ss"',
    )
    return (error, None)


class JobParameters:
    def __init__(self, **kwargs):

        self.error = (0, "")
        self.array_params = []

        scanscript_values = {}
        scanscript = False
        if "scanscript" in kwargs and kwargs["scanscript"]:
            scanscript = True
            initial_request = ""
            user = None
            if "initial_request" in kwargs:
                initial_request = kwargs["initial_request"]
                if "user" in kwargs:
                    user = kwargs["user"]
            error, scanscript_values = scan_script(
                kwargs["command"], initial_request, user
            )
            if error[0] != 0:
                self.error = error
                return

        for key in [
            "job_type",
            "resource",
            "command",
            "info_type",
            "queue",
            "properties",
            "checkpoint",
            "signal",
            "notify",
            "name",
            "types",
            "directory",
            "dependencies",
            "stdout",
            "stderr",
            "hold",
            "project",
            "initial_request",
            "user",
            "interactive",
            "reservation_date",
            "connect",
            "scanscript",
            "array",
            "array_params",
            "array_param_file",
            "use_job_key",
            "import_job_key_inline",
            "import_job_key_file",
            "export_job_key_file",
        ]:

            if key in kwargs:
                setattr(self, key, kwargs[key])
            else:
                setattr(self, key, None)

            if (
                scanscript
                and (key in kwargs and not kwargs[key])
                and key in scanscript_values
            ):
                setattr(self, key, scanscript_values[key])

        if not self.types:
            self.types = []

        if not self.initial_request:
            self.initial_request = self.command

        if scanscript:
            self.initial_request = scanscript_values["initial_request"]

        if self.array:
            self.array_nb = self.array
        else:
            self.array_nb = 1

        if not self.queue:
            self.queue = config["QUEUE"]

        if not self.project:
            self.project = config["PROJECT"]

        if not self.signal:
            self.signal = config["SIGNAL"]

        if self.directory:
            self.launching_directory = self.directory
        else:
            self.launching_directory = os.path.expanduser("~" + self.user)

        if not self.info_type:
            self.info_type = gethostname() + ":"

        self.array_id = 0
        self.start_time = 0

        # prepare and build resource_request
        default_resources = config["OARSUB_DEFAULT_RESOURCES"]
        nodes_resources = config["OARSUB_NODES_RESOURCES"]
        self.resource_request = parse_resource_descriptions(
            self.resource, default_resources, nodes_resources
        )

        # Check the default name of the key if we have to generate it
        try:
            getattr(self, "use_job_key")
        except AttributeError:
            if ("OARSUB_FORCE_JOB_KEY" in config) and (
                config["OARSUB_FORCE_JOB_KEY"] == "yes"
            ):
                self.use_job_key = True
            else:
                self.use_job_key = False

    def check_parameters(self):
        if self.error[0] != 0:
            return self.error

        if (
            not self.command
            and not self.interactive
            and not self.reservation_date
            and not self.connect
            # Noop type doesn't need any command
            and "noop" not in self.types
        ):
            return (
                5,
                "Command or interactive flag or advance reservation time or connection directive must be provided",
            )

        if self.interactive and self.reservation_date:
            return (7, "An advance reservation cannot be interactive.")

        if self.interactive and "desktop_computing" in self.types:
            return (17, "A desktop computing job cannot be interactive")

        if any(re.match(r"^noop$", t) for t in self.types):
            if self.interactive:
                return (17, "a NOOP job cannot be interactive.")
            elif self.connect:
                return (17, "A NOOP job does not have a shell to connect to.")

        # notify : check insecure character
        if self.notify and re.match(r"^.*exec\s*:.+$", self.notify):
            m = re.search(r".*exec\s*:([a-zA-Z0-9_.\/ -]+)$", self.notify)
            if not m:
                return (
                    16,
                    "insecure characters found in the notification method (the allowed regexp is: [a-zA-Z0-9_.\\/ -]+).",
                )

        return (0, "")

    def read_array_param_file(self):
        process = None

        self.array_nb = 0
        self.array_params = []

        try:
            process = tools.Popen(
                ["oardodo", "cat", self.array_param_file], stdout=PIPE
            )
        except Exception:
            return (12, "Cannot open the parameter file " + self.array_param_file)

        stdout = process.communicate()[0]
        output = stdout.decode()

        for line in output.split("\n"):
            # Ignore comments and blank line
            if not re.match(r"#.*|^\s*$", line):
                self.array_params.append(line)
                self.array_nb += 1

        if self.array_nb == 0:
            return (6, "An array of job must have a number of sub-jobs greater than 0.")

        return (0, "")

    def kwargs(self, command, date):
        kwargs = {}
        kwargs["submission_time"] = date
        kwargs["command"] = command
        kwargs["state"] = "Hold"

        for key in [
            "job_type",
            "info_type",
            "properties",
            "launching_directory",
            "start_time",
            "checkpoint",
            "notify",
            "project",
            "initial_request",
            "array_id",
        ]:

            kwargs[key] = getattr(self, key)

        kwargs["job_user"] = self.user
        kwargs["queue_name"] = self.queue
        kwargs["job_name"] = self.name
        kwargs["checkpoint_signal"] = self.signal

        return kwargs


class Submission:
    def __init__(self, job_parameters):
        self.job_parameters = job_parameters

    def submit(self):
        import_job_key_inline = self.job_parameters.import_job_key_inline
        import_job_key_file = self.job_parameters.import_job_key_file
        export_job_key_file = self.job_parameters.export_job_key_file

        (err, job_id_lst) = add_micheline_jobs(
            self.job_parameters,
            import_job_key_inline,
            import_job_key_file,
            export_job_key_file,
        )
        return (err, job_id_lst)
