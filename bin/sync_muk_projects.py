#!/usr/bin/env python
##
#
# Copyright 2012-2013 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
This script checks the project entries in the LDAP that have changed since a given timestamp
and that are in the muk autogroup.

For these, the home and other shizzle should be set up.

@author Andy Georges
"""

import os
import sys

from vsc.administration.group import Group
from vsc.administration.user import MukUser
from vsc.config.base import Muk, ANTWERPEN, BRUSSEL, GENT, LEUVEN
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.filters import CnFilter, InstituteFilter, LdapFilter
from vsc.ldap.timestamp import convert_timestamp, write_timestamp
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.availability import proceed_on_ha_service
from vsc.utils.generaloption import simple_option
from vsc.utils.lock import lock_or_bork, release_or_bork
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_CRITICAL, NAGIOS_EXIT_WARNING
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile

NAGIOS_HEADER = 'sync_muk_projects'
NAGIOS_CHECK_FILENAME = "/var/cache/%s.nagios.json.gz" % (NAGIOS_HEADER)
NAGIOS_CHECK_INTERVAL_THRESHOLD = 24 * 60 * 60 # 1 day

SYNC_TIMESTAMP_FILENAME = "/var/run/%s.timestamp" % (NAGIOS_HEADER)
SYNC_MUK_PROJECTS_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)
SYNC_MUK_PROJECTS_LOCKFILE = "/gpfs/scratch/user/%s.lock" % (NAGIOS_HEADER)

fancylogger.logToFile(SYNC_MUK_PROJECTS_LOGFILE)
fancylogger.setLogLevelInfo()

logger = fancylogger.getLogger(name=NAGIOS_HEADER)


def process_project(options, project):

    muk = Muk()  # Singleton class, so no biggie
    logger.info("Processing project %{project_id}".format(project_id=project.project_id))

    try:
        project.create_scratch_fileset()
        return True
    except:
        logger.exception("Cannot create scratch fileset for project %{project_id}".format(project_id=project.project_id,))
        return False



def main():
    """
    Main script.
    - loads the previous timestamp
    - build the filter
    - fetches the users
    - process the users
    - write the new timestamp if everything went OK
    - write the nagios check file
    """

    options = {
        'dry-run': ("Do not make any updates whatsoever", None, "store_true", False),
        'nagios': ('print out nagion information', None, 'store_true', False, 'n'),
        'nagios-check-filename': ('filename of where the nagios check data is stored', str, 'store', NAGIOS_CHECK_FILENAME),
        'nagios-check-interval-threshold': ('threshold of nagios checks timing out', None, 'store', NAGIOS_CHECK_INTERVAL_THRESHOLD),
        'ha': ('high-availability master IP address', None, 'store', None),
    }

    opts = simple_option(options)

    nagios_reporter = NagiosReporter(NAGIOS_HEADER, NAGIOS_CHECK_FILENAME, NAGIOS_CHECK_INTERVAL_THRESHOLD)

    if opts.options.nagios:
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    logger.info("Starting synchronisation of Muk users.")

    if not proceed_on_ha_service(opts.options.ha):
        logger.warning("Not running on the target host in the HA setup. Stopping.")
        nagios_reporter.cache(NAGIOS_EXIT_WARNING,
                        NagiosResult("Not running on the HA master."))
        sys.exit(NAGIOS_EXIT_WARNING)

    lockfile = TimestampedPidLockfile(SYNC_MUK_PROJECTS_LOCKFILE)
    lock_or_bork(lockfile, nagios_reporter)

    try:
        muk = Muk()

        l = LdapQuery(VscConfiguration())  # Initialise LDAP binding

        muk_project_group_filter = CnFilter(muk.muk_project_group)

        # all currently _running_ projects are elements is the projects autogroup
        # these values are set elsewhere
        try:
            muk_group = Group.lookup(muk_project_group_filter)[0]
            logger.info("Muk projects = %s" % (muk_group.autogroup))
        except IndexError:
            logger.raiseException("Could not find a group with cn %s. Cannot proceed synchronisation" % muk.muk_user_group)

        muk_projects = [MukProject(project_id) for project_id in muk_group.memberUid]

        projects_ok = 0
        projects_fail = 0

        for project in muk_projects:
            if process_project(opts.options, project):
                projects_ok += 1
            else:
                projects_fail += 1

    except Exception:
        logger.exception("Fail during muk users synchronisation")
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL,
                              NagiosResult("Script failed, check log file ({logfile})".format(
                                  logfile=SYNC_MUK_USERS_LOGFILE)))
        lockfile.release()
        sys.exit(NAGIOS_EXIT_CRITICAL)

    result_dict = {
        'projects_ok': projects_ok,
        'projects_fail': projects_fail,
        'projects_fail_warning': 1,
        'projects_fail_critical': 3,
    }

    if projects_fail > 0:
        result = NagiosResult("several projects were not synched", **result_dict)
        nagios_reporter.cache(NAGIOS_EXIT_WARNING, result)
    else:
        try:
            result = NagiosResult("muk projects synchronised", **result_dict)
            (_, ldap_timestamp) = convert_timestamp()
            if not opts.options.dry_run:
                write_timestamp(SYNC_TIMESTAMP_FILENAME, ldap_timestamp)
            nagios_reporter.cache(NAGIOS_EXIT_OK, result)
        except:
            logger.exception("Something broke writing the timestamp")
            result.message = "muk projects synchronised, filestamp not written"
            nagios_reporter.cache(NAGIOS_EXIT_WARNING, result)

    result.message = "muk projects synchronised, lock release failed"
    release_or_bork(lockfile, nagios_reporter, result)

    logger.info("Finished synchronisation of the Muk users from the LDAP with the filesystem.")


if __name__ == '__main__':
    main()
