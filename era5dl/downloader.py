"""
Contains the core functions needed for the downlaoder
=====================================================

(c) 2026 Bedartha Goswami <bedartha.goswami@iiserpune.ac.in>
"""

import os
import time
import csv
from tqdm import tqdm
import logging
from ecmwf.datastores import Client

from . import params

logging.basicConfig(level="INFO")
client = Client()
client.check_authentication()




def _pprint(stuff, verbose=True):
    """prints given stuff depending on the verbosity boolean"""
    if verbose:
        print(stuff)
    return None


def _print_info(remote, rid, args):
    """Prints job information in human readable form for given remote"""
    request = remote.request
    result_status = (not remote.results_ready) * "NOT " + "READY"
    _pprint(f"\nGetting info for request with ID {rid}")
    _pprint(f"Submitted at: {remote.created_at}")
    _pprint(f"For data from : {remote.collection_id}")
    _pprint(f"Particulars of the data requested")
    _pprint(f"{request['variable'][0]}, {request['year'][0]}, {remote.status}")
    _pprint(f"Job with ID {rid} is {result_status} for download")
    return None


def _select_job(client, task, args):
    """prints list of successful jobs to stdout"""
    jobs = client.get_jobs(limit=args.num_jobs,
                           sortby="-created",
                           status="successful")
    request_ids = jobs.request_ids
    year, var = [], []
    for rid in request_ids:
        remote = client.get_remote(rid)
        var.extend(remote.request['variable'])
        year.extend(remote.request['year'])
    _pprint("\nThe following jobs are marked as successful:")
    for i, zipped  in enumerate(zip(request_ids, var, year)):
        rid, y, v = zipped
        _pprint(f"\t[{i}] RID: {rid}\tYEAR: {y}\tVAR: {v}")
    _pprint(f"Which job would you like to {task}? (Enter the number)\n")
    i = int(input())
    assert i < len(request_ids), "Invalid choice!"
    return request_ids[i], year[i], var[i]


def _set_logging(quiet):
    """set logging based on verbosity"""
    if quiet is False:
        logging.basicConfig(level="CRITICAL")
    return None


def build_db(args):
    """Retrieves job info for all jobs and stores it locally in JSON"""
    logging.getLogger().setLevel(logging.ERROR)
    status_all = ["accepted", "running", "successful", "failed"]
    njobs = {}
    _pprint("pre-fetching details ...", args.quiet)
    for status in tqdm(status_all):
        jobs = client.get_jobs(limit=args.num_jobs,
                               sortby="-created",
                               status=status)
        njobs[status] = len(jobs.request_ids)
    status_with_jobs = [st for st in njobs.keys() if njobs[st] > 0]
    _pprint("\nlooping over statuses with > 0 jobs ...", args.quiet)
    for status in status_with_jobs:
        jobs_list = []
        jobs = client.get_jobs(limit=args.num_jobs,
                               sortby="created",
                               status=status)
        _pprint(f"getting info on {status} jobs ...", args.quiet)
        for job in tqdm(jobs.json["jobs"]):
            remote = client.get_remote(job["jobID"])
            job_dict = {
                    "request_id" : job["jobID"],
                    "status"  : job["status"],
                    "created" : job["created"],
                    "updated" : job["updated"],
                    "year" : remote.request["year"][0],
                    "variable": remote.request["variable"][0],
                    }
            jobs_list.append(job_dict)
        # # read inthe jobs list currently stored in the database
        # f_db = f"./db/{status}.csv"
        # jobs_list_db = []
        # with open(f_db, mode ='r')as file:
        #     csvFile = csv.reader(file)
        #     for lines in csvFile:
        #         jobs_list_db.append(lines)
        # jobs_list_db = jobs_list_db[1:]             # skip header row
        # rid_db = [job[0] for job in jobs_list_db]
        # # remove jobs that exist in the database
        # print(len(jobs_list))
        # for job in jobs_list:
        #     if job["request_id"] in rid_db:
        #         jobs_list.remove(job)
        # print(len(jobs_list))
        # os.sys.exit()

        # save dictionary as a csv file to disk
        headers = [k for k in job_dict.keys()]
        fout = f"./db/{status}.csv"
        with open(fout, mode='w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()  # Write header row
            writer.writerows(jobs_list)  # Write data rows
        _pprint(f"saved to {fout}", args.quiet)
    _pprint("\ncleaning db for statuses with = 0 jobs ...", args.quiet)
    status_wo_jobs = [st for st in njobs.keys() if njobs[st] == 0]
    for status in status_wo_jobs:
        f =  f"./db/{status}.csv"
        if os.path.exists(f):
            os.remove(f)
            _pprint(f"removed {f}", args.quiet)
    return None


def check(args):
    """checks status of submitted jobs"""
    logging.getLogger().setLevel(logging.ERROR)
    assert args.status is not None, "Please specify the status of job(s)"
    if args.status == "all":
        status_all = ["accepted", "running", "successful", "failed"]
        request_ids = {}
        njobs = 0
        for status in status_all:
            jobs = client.get_jobs(limit=args.num_jobs,
                                   sortby="-created",
                                   status=status)
            request_ids[status] = jobs.request_ids
            njobs += len(request_ids[status])
    else:
        jobs = client.get_jobs(limit=args.num_jobs,
                               sortby="-created",
                               status=args.status)
        request_ids = jobs.request_ids

    if args.status == "all":
        for status in status_all:
            _pprint(f"Fetching status of {status} jobs ...")
            for rid in request_ids[status]:
                remote = client.get_remote(rid)
                _print_info(remote, rid, args)
    else:
        _pprint(f"Fetching status of {args.status} jobs ...")
        for rid in request_ids:
            remote = client.get_remote(rid)
            _print_info(remote, rid, args)
    return None


def delete(args):
    """delete specified job requests"""
    _set_logging(args.quiet)
    rid, year, var = _select_job(client, "delete", args)
    _pprint(f"\tDeleting ...")
    _pprint(f"\tRID: {rid}\tYEAR: {year}\tVAR: {var}")
    client.delete(rid)
    print("Done.", args.quiet)
    return None


def download(args):
    """downloads jobs that are ready"""
    _set_logging(args.quiet)
    f = "./db/successful.csv"
    timediff = os.path.getmtime(f) - time.time()
    assert timediff < 14400, "Outdated database! Rebuild with ``-t build_db``"
    jobs_list = []
    with open(f, mode ='r')as file:
        csvFile = csv.reader(file)
        for lines in csvFile:
            jobs_list.append(lines)
    hdr = jobs_list[0]
    jobs_list = jobs_list[-int(args.num_jobs):][::-1]
    _pprint("\nThe following jobs are marked as successful:")
    _pprint(f"No.\t{hdr[4]}\t\t{hdr[5]}\t\t\t{hdr[0]}")
    for i, job in enumerate(jobs_list[1:]):
        print(f"[{i+1}]\t{job[4]}\t{job[5]}\t{job[0]}")
    _pprint(f"Which job would you like to download? (Enter the number)")
    i = int(input())
    job = jobs_list[i]
    rid = job[0]
    year = job[4]
    var = job[5]
    remote = client.get_remote(rid)
    _pprint(f"\n\tDownloading ...")
    _pprint(f"\tRID: {rid}\tYEAR: {year}\tVAR: {var}")
    OUTPATH = args.path_to_output
    target = f"{OUTPATH}/{var}_{year}.nc"
    remote.download(target)
    _pprint(f"\tSaved to: {target}")
    return None


def retrieve(args):
    """submits and downloads the data immediately when ready"""
    _set_logging(args.quiet)
    collection_id, request = structure_request(args)
    target = f"{OUTPATH}/{args.var}_{args.year}.nc"
    client.retrieve(collection_id, request, target)
    return None


def structure_request(args):
    """downloads data for single levels"""
    # retrieve the general request params that remain broadly unchanged
    collection_id = params.COLLECTION_ID
    request = params.REQUEST_ARGS
    # add the variable name and year as specified by user
    request["variable"] = args.var
    request["year"] = args.year
    return collection_id, request


def submit(args):
    """submit a job"""
    _set_logging(args.quiet)
    collection_id, request = structure_request(args)
    remote = client.submit(collection_id, request)
    return remote


