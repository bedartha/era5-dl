"""
Contains the core functions needed for the downlaoder
=====================================================

(c) 2026 Bedartha Goswami <bedartha.goswami@iiserpune.ac.in>
"""

from tqdm import tqdm
import logging
from ecmwf.datastores import Client


logging.basicConfig(level="INFO")
client = Client()
client.check_authentication()

CHK_MAX_JOBS = 100
MONTHS = [str(m) for m in range(1, 13)]
DAYS = [str(d) for d in range(1, 32)]



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


def _select_job(client, task):
    """prints list of successful jobs to stdout"""
    jobs = client.get_jobs(sortby="-created", status="successful")
    request_ids = jobs.request_ids
    year, var = [], []
    for rid in request_ids:
        remote = client.get_remote(rid)
        var.extend(remote.request['variable'])
        year.extend(remote.request['year'])
    _pprint("\nThe following jobs are marked as succssful:")
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


def check(args):
    """checks status of submitted jobs"""
    # logging.basicConfig(level="ERROR")
    logging.getLogger().setLevel(logging.ERROR)
    assert args.status is not None, "Please specify the status of job(s)"
    if args.status == "all":
        status_all = ["accepted", "running", "successful", "failed"]
        request_ids = {}
        njobs = 0
        for status in status_all:
            jobs = client.get_jobs(limit=CHK_MAX_JOBS,
                                   sortby="created",
                                   status=status)
            request_ids[status] = jobs.request_ids
            njobs += len(request_ids[status])
    else:
        jobs = client.get_jobs(limit=CHK_MAX_JOBS,
                               sortby="created",
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
    rid, year, var = _select_job(client, "delete")
    _pprint(f"\tDeleting ...")
    _pprint(f"\tRID: {rid}\tYEAR: {year}\tVAR: {var}")
    client.delete(rid)
    print("Done.", args.quiet)
    return None


def download(args):
    _set_logging(args.quiet)
    """downloads jobs that are ready"""
    rid, year, var = _select_job(client, "download")
    remote = client.get_remote(rid)
    _pprint(f"\tDownloading ...")
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
    if args.data_type == "single-level":
        collection_id = "derived-era5-single-levels-daily-statistics"
        request = {
            "product_type": "reanalysis",
            "variable": args.var,
            "year": args.year,
            "month": MONTHS,
            "day": DAYS,
            "daily_statistic": "daily_mean",
            "time_zone": "utc+00:00",
            "frequency": "6_hourly",
            "data_format": "netcdf",
        }
    elif args.data_type == "pressure-level":
        plevel = args.plevel
        collection_id, request = pressure_level(variable, plevel, args.year)
    return collection_id, request


def submit(args):
    """submit a job"""
    _set_logging(args.quiet)
    collection_id, request = structure_request(args)
    remote = client.submit(collection_id, request)
    return remote


