#!/usr/bin/env python
#
# Copyright 2013-2018 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
This script synchronises the users and VO's from the HPC account page to the Slurm database.

The script must result in an idempotent execution, to ensure nothing breaks.
"""

import logging
import sys
import tempfile
import subprocess

from collections import namedtuple, Mapping
from datetime import datetime

from vsc.accountpage.client import AccountpageClient
from vsc.accountpage.wrappers import mkVo
from vsc.accountpage.wrappers import mkNamedTupleInstance
from vsc.utils.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.utils import fancylogger
from vsc.utils.missing import nub
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.run import run_simple
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = "sync_slurm_acct"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 60 * 60  # 60 minutes

SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)
SYNC_SLURM_ACCT_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)

SLURM_SACCT_MGR = "/usr/bin/sacctmgr"

logger = fancylogger.getLogger()
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

ACCOUNTS = "accounts"
USERS = "users"


# https://stackoverflow.com/questions/11351032/namedtuple-and-default-values-for-optional-keyword-arguments
def namedtuple_with_defaults(typename, field_names, default_values=()):
    T = namedtuple(typename, field_names)
    T.__new__.__defaults__ = (None,) * len(T._fields)
    if isinstance(default_values, Mapping):
        prototype = T(**default_values)
    else:
        prototype = T(*default_values)
    T.__new__.__defaults__ = tuple(prototype)
    return T


SacctUserFields = ["User", "Def_Acct", "Admin", "Cluster", "Account", "Partition", "Share",
                   "MaxJobs", "MaxNodes", "MaxCPUs", "MaxSubmit", "MaxWall", "MaxCPUMins",
                   "QOS", "Def_QOS"]
SacctAccountFields = ["Account", "Descr", "Org", "Cluster", "Par_Name", "User", "Share",
                      "GrpJobs", "GrpNodes", "GrpCPUs", "GrpMem", "GrpSubmit", "GrpWall", "GrpCPUMins",
                      "MaxJobs", "MaxNodes", "MaxCPUs", "MaxSubmit", "MaxWall", "MaxCPUMins",
                      "QOS", "Def_QOS"]

SlurmAccount = namedtuple_with_defaults('SlurmAccount', SacctAccountFields)
SlurmUser = namedtuple_with_defaults('SlurmUser', SacctUserFields)


def mkSlurmAccount(fields):
    mkNamedTupleInstance(fields, SlurmAccount)


def mkSlurmUser(fields):
    mkNamedTupleInstance(fields, SlurmUser)


def parse_slurm_acct_line(header, line, creator):
    fields = line.split("|")
    print "fields (%d): %s" % (len(fields), fields)
    return creator(dict(zip(header, fields)))


def parse_slurm_acct_dump(lines, info_type):
    """
    Parse the accounts from the listing
    """
    info = set()

    if info_type == ACCOUNTS:
        creator = mkSlurmAccount
    elif info_type == USERS:
        creator = mkSlurmUser

    header = [w.replace(' ', '_') for w in lines[0].rstrip().split("|")]

    print "Headers: %s" % header

    for line in lines[1:]:
        line = line.rstrip()
        try:
            info.add(parse_slurm_acct_line(header, line, creator))
        except Exception, err:
            logging.warning("Slurm acct sync: could not process line %s [%s]", line, err)

    return info


def get_slurm_acct_info(info_type):
    """Get slurm account info for the given clusterself.

    @param cluster: the cluster for which to get information
    @param info_type: this is either "accounts" or "users"
    """
    contents = None
    outputFile = tempfile.NamedTemporaryFile(delete=True)
    with open(outputFile.name, 'r+') as f:
        try:
            subprocess.check_call([
                SLURM_SACCT_MGR,
                "-s",
                "-P",
                "list",
                "%s" % info_type,
                ],
                stdout=f
            )
        except subprocess.CalledProcessError, err:
            logging.error("Could not get sacctmgr output: error %d", err.returncode)
            raise

        f.flush()
        f.seek(0)
        contents = f.readlines()
        print "read %d lines" % len(contents)

    print "header: %s" % contents[0]
    info = parse_slurm_acct_dump(contents, info_type)

    return info


def main():
    """
    Main script. The usual.
    """

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'account_page_url': ('URL of the account page where we can find the REST API', str, 'store', 'https://apivsc.ugent.be/django'),
        'clusters': ('Cluster(s) (comma-separated) to get the info for', str, 'store', 'banette'),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        now = datetime.utcnow()
        client = AccountpageClient(
            token=opts.options.access_token,
            url=opts.options.account_page_url + "/api/")

        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except Exception:
            logging.exception("Error reading from %s" % SYNC_TIMESTAMP_FILENAME)
            last_timestamp = "200901010000Z"  # the beginning of time
        if last_timestamp is None:
            last_timestamp = "201804010000Z"  # the beginning of time
        last_timestamp = "201804010000Z"  # the beginning of time

        logging.info("Last recorded timestamp was %s" % (last_timestamp))

        slurm_account_info = get_slurm_acct_info(ACCOUNTS)
        slurm_users_info = get_slurm_acct_info(USERS)

        print slurm_account_info
        print slurm_users_info

        # All users belong to a VO, so fetching the VOs is necessary and sufficient.
        # This assumes correct timestamps on the VOs if members join and leave
        account_page_vos = [mkVo(v) for v in client.vo.modified[last_timestamp[:12]].get()[1]]

        account_page_members = {}
        for vo in account_page_vos:
            account_page_members[vo.vsc_id] = vo.members

        

    except Exception as err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("Accounts synced to slurm", stats)


if __name__ == '__main__':
    main()
