#!/usr/bin/python3

"""Python script to parse bluecoat proxy logs using multiprocessing for performance"""

__author__      = "Jan Goebel <jan-go@gmx.de>"
__copyright__   = "Copyright 2020, Pi-One"
__version__ = "1.0.0"

import csv
import sys
import multiprocessing
from itertools import islice
import io
import os
import json
import time
import hashlib


def mp_worker(data):
    """function to be run by each process with its own 20.000 lines of logs to process
    """
    res = []
    data = [data]
    # Required: adjust columns to your bluecoat log (order can be different depending on bluecoat configuration)
    bfields = ("date", "time", "time-taken", "c-ip", "s-action", "s-ip", "s-supplier-name", "s-sitename", "cs-user", "cs-username", "cs-auth-group", "cs-categories", "cs-method", "cs-host", "cs-uri", "cs-uri-scheme", "cs-uri-port", "cs-uri-path", "cs-uri-query", "cs-uri-extension", "cs(Referer)", "cs(User-Agent)", "cs-bytes", "sc-status", "sc-bytes", "sc-filter-result", "sc-filter-category", "x-virus-id", "x-exception-id", "rs(Content-Type)", "duration", "s-supplier-ip", "cs(Cookie)", "s-computername", "s-port", "cs-uri-stem", "cs-version")
    fp = io.StringIO("\r\n".join(data))
    # iterate over the log lines
    for row in csv.DictReader(fp, fieldnames=bfields, delimiter=' ', quotechar='"'):
        # do something with the loglines, e.g. ignore http reponse 407 lines
        if row['sc-status'] == '407':
            continue
        res.append(row)
    return res

if __name__ == '__main__':
    # provide logfile name as input
    try:
        fn = sys.argv[1]
    except:
        print(">> no input file given!")
        sys.exit(255)
    # spawn as much processes as available but one less
    p = multiprocessing.Pool(multiprocessing.cpu_count()-1)
    # each process will crunch 20.000 log lines
    n = 20000
    # write results to out.file in json format
    with open('out.file', 'w') as wp:
        with open(fn, newline='', encoding="iso-8859-1") as fp:
            while True:
                try:
                    next_n_lines = list(islice(fp, n))
                    if not next_n_lines:
                        break
                    nres = p.map(mp_worker, next_n_lines)
                    for item in nres:
                        for entry in item:
                            jres = dict(entry)
                            wp.write(json.dumps(jres)+'\r\n')
                except Exception as e:
                    print(e)
                    break
