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

import sys
import tempfile

from collections import namedtuple
from datetime import datetime

from vsc.accountpage.client import AccountpageClient
from vsc.accountpage.wrappers import mkVo
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
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


from vsc.accountpage.wrappers import mkNamedTupleInstance


SlurmAccount = namedtuple('SlurmAccount', ['Description', 'Organization', 'Fairshare'])
SlurmUser = namedtuple('SlurmUser', ['DefaultAccount', 'Fairshare'])


def mkSlurmAccount(fields):
    mkNamedTupleInstance(fields, SlurmAccount)


def mkSlurmUser(fields):
    mkNamedTupleInstance(fields, SlurmUser)


def parse_slurm_acct_line(line):
    return ('Account', 'gvo00002', {'Description': 'gvo00002', 'Organization': 'gvo00002', 'Fairshare': 1})


def parse_slurm_acct_dump(lines):
    """
    Parse the accounts and users from the dump

    sacctmgr: Account - 'gvo00002':Description='gvo00002':Organization='gvo00002':Fairshare=1
    sacctmgr: User - 'vsc40002':DefaultAccount='gvo00002':Fairshare=1
    """
    accounts = set()
    users = set()

    print "Processing %d lines" % len(lines)
    for l in lines:
        (kind, name, fields) = parse_slurm_acct_line(l)
        if kind == 'Account':
            accounts.add((name, mkSlurmAccount(fields)))
        if kind == 'User':
            users.add((name, mkSlurmUser(fields)))

    return (accounts, users)

def get_slurm_account_info(cluster):
    """Get slurm account info for the given cluster"""

    contents = None
    with tempfile.NamedTemporaryFile() as f:
        (ec, output) = run_simple([SLURM_SACCT_MGR, "dump", cluster, "File=%s" % f.name])
        f.flush()
        f.seek(0)
        contents = f.readlines()
        print "ec: %s" % ec
        print "read %d lines" % len(contents)

    (accounts, users) = parse_slurm_acct_dump(contents)


def main():
    """
    Main script. The usual.
    """

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'account_page_url': ('URL of the account page where we can find the REST API', str, 'store', 'https://apivsc.ugent.be/django'),
        'cluster': ('Cluster to get the info for', str, 'store', 'banette'),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        now = datetime.utcnow()
        client = AccountpageClient(token=opts.options.access_token, url=opts.options.account_page_url + "/api/")

        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except Exception:
            logger.exception("Something broke reading the timestamp from %s" % SYNC_TIMESTAMP_FILENAME)
            last_timestamp = "200901010000Z"  # the beginning of time

        logger.info("Last recorded timestamp was %s" % (last_timestamp))

        # All users belong to a VO, so fetching the VOs is necessary and sufficient.
        # This assumes correct timestamps on the VOs if members join and leave
        account_page_vos = [mkVo(v) for v in client.vo.modified[last_timestamp[:12]].get()[1]]

        account_page_members = {}
        for vo in account_page_vos:
            account_page_members[vo.vsc_id] = vo.members

        slurm_account_info = get_slurm_account_info(opts.options.cluster)

    except Exception as err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("Accounts synced to slurm", stats)


if __name__ == '__main__':
    main()
