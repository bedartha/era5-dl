"""
User friendly wrapper for getting ERA5 data with the ECMWF DataStores Client
============================================================================

https://github.com/ecmwf/ecmwf-datastores-client


(c) 2026 Bedartha Goswami <bedartha.gowami@iiserpune.ac.in>
"""


import argparse
import logging

from era5dl import downloader as edl



def _parser():
    """Parse arguments"""
    doc_split = __doc__.split("(c) ")
    helpdoc, epilog = doc_split[0], doc_split[-1]
    parser = argparse.ArgumentParser(
                    prog=f'python {__file__.split('/')[-1]}',
                    description=helpdoc,
                    formatter_class=argparse.RawTextHelpFormatter,
                    epilog=f"(c) {epilog}")
    # parser.add_argument("-dt", "--data-type",
    #                     help="Type of data to download",
    #                     choices=["single-level", "pressure-level"],
    #                     )
    parser.add_argument("-v", "--var",
                        help="Variable name (See --help for options)",
                        )
    parser.add_argument("-y", "--year",
                        help="Year between 1940 and 2025"
                        )
    parser.add_argument("-l", "--plevel",
                        help="Pressure level (See --help for options)"
                        )
    parser.add_argument("-o", "--path-to-output",
                        help="path to output"
                        )
    parser.add_argument("-t", "--task",
                        help="Specify the task to execute",
                        choices=["submit", "check", "download",
                            "retrieve", "delete", "build_db"]
                        )
    parser.add_argument("-st", "--status",
                        help="Status of job (to be used with ``-t check``",
                        choices=["accepted", "running", "successful", "failed"]
                        )
    parser.add_argument("-n", "--num-jobs",
                        default=100,
                        help="Number of jobs to query from the data store"
                        )
    parser.add_argument("-q", "--quiet",
                        action="store_false",
                        help="Suppress output printed to screen by default"
                        )
    return parser


if __name__ == "__main__":
    args = _parser().parse_args()
    _func = eval(f"edl.{args.task}")
    _func(args)
