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


def run_del_or_dl(args):
    """Runs the delete or download tasks"""
    _set_logging(args.quiet)
    jobs_list, hdr = _job_ls(args, jobtype="successful")
    jobs = _job_sel(jobs_list, hdr, args)
    _job_loop(jobs, args)
    return None


def _job_ls(args, jobtype="successful"):
    """returns the job list from the current database"""
    # check if file exists in database
    f = f"./db/{jobtype}.csv"
    assert os.path.isfile(f),\
            f"there are no {jobtype} jobs. Try running `-t build_db`"

    # check if file has been modified recently, else rebuild database
    timediff = time.time() - os.path.getmtime(f)
    assert timediff < 14400, "Outdated database! Rebuild with ``-t build_db``"

    # get jobs list from database file
    jobs_list = []
    with open(f, mode ='r') as file:
        csvFile = csv.reader(file)
        for lines in csvFile:
            jobs_list.append(lines)

    # separate header and job details, retain only njobs, reverse order
    hdr = jobs_list[0]
    jobs_list = jobs_list[1:]
    jobs_list = jobs_list[-int(args.num_jobs):][::-1]

    return jobs_list, hdr


def _job_sel(jobs_list, hdr, args):
    """asks for user input to select the job from given job list"""
    _pprint("\nThe following jobs are marked as successful:")
    _pprint(f"No.\t{hdr[4]}\t\t{hdr[5]}\t\t\t{hdr[0]}")
    for i, job in enumerate(jobs_list):
        print(f"[{i+1}]\t{job[4]}\t{job[5]}\t{job[0]}")
    _pprint(f"Which job(s) would you like to {args.task}?")
    _pprint("Enter the number(s) separated by commas")
    ids = input().split(",")
    selected = [jobs_list[int(i)-1] for i in ids]
    return selected


def _job_loop(jobs, args):
    """Executes specified args.task (delete or download) on the jobs"""
    execute = {
            "delete": __delete,
            "download": __download
            }
    for job in jobs:
        _pprint(f"{args.task} ...")
        _pprint(f"RID: {job[0]}\tYEAR: {job[4]}\tVAR: {job[5]}")
        execute[args.task](job, args)
    return None


def __delete(job, args):
    client.delete(job[0])
    _pprint("done.", args.quiet)
    return None


def __download(job, args):
    target = f"{args.path_to_output}/{job[5]}_{job[4]}.nc"
    client.get_remote(job[0]).download(target)
    _pprint(f"\tSaved to: {target}", args.quiet)
    return None


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
    jobs_list, hdr = _job_ls(args, jobtype=args.status)
    _pprint(f"No.\t{hdr[4]}\t\t{hdr[5]}\t\t\t{hdr[0]}")
    for i, job in enumerate(jobs_list):
        print(f"[{i+1}]\t{job[4]}\t{job[5]}\t{job[0]}")
    return None


def delete(args):
    """delete specified job requests"""
    run_del_or_dl(args)
    return None


def download(args):
    """downloads jobs that are ready"""
    run_del_or_dl(args)
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


