#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2013-2017 Ghent University
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
Get existing Django accountpage users and sync them to the VSC LDAP
"""
import grp
import os
import pwd
import sys

import datetime
from vsc.config.base import VSC_CONF_DEFAULT_FILENAME

from vsc.accountpage.client import AccountpageClient

from vsc.administration.ldapsync import LdapSyncer, ERROR

from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = "sync_django_to_ldap"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes
SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)

fancylogger.setLogLevelInfo()
fancylogger.logToScreen(True)
_log = fancylogger.getLogger(NAGIOS_HEADER)


def main():

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'start-timestamp': ("The timestamp form which to start, otherwise use the cached value", None, "store", None),
        'access_token': ('OAuth2 token identifying the user with the accountpage', None, 'store', None),
        'account_page_url': ('url for the account page', None, 'store', None),
        }
    # get access_token from conf file
    ExtendedSimpleOption.CONFIGFILES_INIT = ['/etc/account_page.conf']
    opts = ExtendedSimpleOption(options)
    stats = {}

    # Creating this here because this is a singleton class
    _ = LdapQuery(VscConfiguration(VSC_CONF_DEFAULT_FILENAME))

    last_timestamp = opts.options.start_timestamp
    if not last_timestamp:
        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except Exception:
            _log.warning("Something broke reading the timestamp from %s", SYNC_TIMESTAMP_FILENAME)
            last_timestamp = "201710230000Z"
            _log.warning("We will resync from a hardcoded know working sync a while back : %s", last_timestamp)

    _log.info("Using timestamp %s", last_timestamp)
    # record starttime before starting, and take a 10 sec safety buffer so we don't get gaps where users are approved
    # in between the requesting of modified users and writing out the start time
    start_time = datetime.datetime.now() + datetime.timedelta(seconds=-10)
    _log.info("startime %s", start_time)

    try:
        parent_pid = os.fork()
        _log.info("Forked.")
    except OSError:
        _log.exception("Could not fork")
        parent_pid = 1
    except Exception:
        _log.exception("Oops")
        parent_pid = 1

    if parent_pid == 0:
        try:
            global _log
            _log = fancylogger.getLogger(NAGIOS_HEADER)
            # drop privileges in the child
            try:
                apache_uid = pwd.getpwnam('apache').pw_uid
                apache_gid = grp.getgrnam('apache').gr_gid

                os.setgroups([])
                os.setgid(apache_gid)
                os.setuid(apache_uid)

                _log.info("Now running as %s" % (os.geteuid(),))
            except OSError:
                _log.raiseException("Could not drop privileges")

            client = AccountpageClient(token=opts.options.access_token, url=opts.options.account_page_url + '/api/')
            syncer = LdapSyncer(client)
            last = int((datetime.datetime.strptime(last_timestamp, "%Y%m%d%H%M%SZ") - datetime.datetime(1970, 1, 1)).total_seconds())
            altered_accounts = syncer.sync_altered_accounts(last, opts.options.dry_run)

            _log.debug("Altered accounts: %s", altered_accounts)

            altered_groups = syncer.sync_altered_groups(last, opts.options.dry_run)

            _log.debug("Altered groups: %s" % altered_groups)

            if not altered_accounts[ERROR] \
                    and not altered_groups[ERROR]:
                _log.info("Child process exiting correctly")
                sys.exit(0)
            else:
                _log.info("Child process exiting with status -1")
                _log.warning("Error occured in %s" % (
                    ["%s: %s\n" % (k, v) for (k, v) in [
                        ("altered accounts", altered_accounts[ERROR]),
                        ("altered groups", altered_groups[ERROR]),
                    ]]
                ))
                sys.exit(-1)
        except Exception:
            _log.exception("Child caught an exception")
            sys.exit(-1)

    else:
        # parent
        (_, result) = os.waitpid(parent_pid, 0)
        _log.info("Child exited with exit code %d" % (result,))

        if not result:
            if not opts.options.start_timestamp:
                (_, ldap_timestamp) = convert_timestamp(start_time)
                if not opts.options.dry_run:
                    write_timestamp(SYNC_TIMESTAMP_FILENAME, ldap_timestamp)
            else:
                _log.info("Not updating the timestamp, since one was provided on the command line")
            opts.epilogue("Synchronised LDAP users to the Django DB", stats)
        else:
            _log.info("Not updating the timestamp, since it was given on the command line for this run")
            sys.exit(NAGIOS_EXIT_CRITICAL)


if __name__ == '__main__':
    main()
