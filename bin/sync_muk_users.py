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
This script checks the users entries in the LDAP that have changed since a given timestamp
and that are in the muk autogroup.

For these, the home and other shizzle should be set up.

@author Andy Georges
"""

import os
import sys
from lockfile import FileLock, AlreadyLocked

from vsc.utils import fancylogger
from vsc.administration.group import Group
from vsc.administration.user import MukUser
from vsc.config.base import Muk
from vsc.ldap.configuration import LumaConfiguration
from vsc.ldap.filters import CnFilter, InstituteFilter, LdapFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.timestamp import convert_timestamp, write_timestamp
from vsc.utils.generaloption import simple_option
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_CRITICAL, NAGIOS_EXIT_WARNING

NAGIOS_HEADER = 'sync_muk_users'
NAGIOS_CHECK_FILENAME = "/var/log/pickles/%s.nagios.json.gz" % (NAGIOS_HEADER)
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = "/var/run/%s.timestamp" % (NAGIOS_HEADER)
SYNC_MUK_USERS_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)
SYNC_MUK_USERS_LOCKFILE = "/gpfs/scratch/user/%s.lock" % (NAGIOS_HEADER)

fancylogger.logToFile(SYNC_MUK_USERS_LOGFILE)
fancylogger.setLogLevelInfo()

logger = fancylogger.getLogger(name=NAGIOS_HEADER)


def process_institute(options, institute, users_filter):

    muk = Muk()  # Singleton class, so no biggie
    changed_users = MukUser.lookup(users_filter & InstituteFilter(institute))
    logger.info("Processing the following users from {institute}: {users}".format(institute=institute,
                users=[u.user_id for u in changed_users]))

    try:
        nfs_location = muk.nfs_link_pathnames[institute]['home']
        logger.info("Checking link to NFS mount at %s" % (nfs_location))
        os.stat(nfs_location)
        try:
            error_users = process(options, changed_users)
        except:
            logger.exception("Something went wrong processing users from %s" % (institute))
    except:
        logger.exception("Cannot process users from institute %s, cannot stat link to NFS mount" % (institute))
        error_users = changed_users

    fail_usercount = len(error_users)
    ok_usercount = len(changed_users) - fail_usercount

    return { 'ok': ok_usercount,
             'fail': fail_usercount
           }


def process(options, users):
    """
    Actually do the tasks for a changed or new user:

    - create the user's fileset
    - set the quota
    - create the home directory as a link to the user's fileset
    """

    error_users = []
    for user in users:
        if options.dry_run:
            user.dry_run = True
        try:
            user.create_scratch_fileset()
            user.populate_scratch_fallback()
            user.create_home_dir()
        except:
            logger.exception("Cannot process user %s" % (user.user_id))
            error_users.append(user)

    return error_users

def force_nfs_mounts(muk):

    nfs_mounts = []
    for institute in muk.institutes:
        try:
            os.stat(muk.nfs_link_pathnames[institute]['home'])
            nfs_mounts.append(institute)
        except:
            logger.exception("Cannot stat %s, not adding institute" % muk.nfs_link_pathnames[institute]['home'])

    return nfs_mounts


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
    }

    opts = simple_option(options)

    nagios_reporter = NagiosReporter(NAGIOS_HEADER, NAGIOS_CHECK_FILENAME, NAGIOS_CHECK_INTERVAL_THRESHOLD)

    if opts.options.nagios:
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    logger.info("Starting synchronisation of Muk users.")

    try:
        logger.info("Trying to acquire lockfile {lockfile}".format(lockfile=SYNC_MUK_USERS_LOGFILE))
        lockfile = FileLock(SYNC_MUK_USERS_LOCKFILE)
        lockfile.acquire(timeout=60)
        logger.info("Lock acquired.")
    except AlreadyLocked:
        logger.exception("Cannot acquire lock, bailing.")
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL, NagiosResult("Cannot acquire lock on {lockfile}. Bailing.".format(lockfile=SYNC_MUK_USERS_LOCKFILE)))
        sys.exit(NAGIOS_EXIT_CRITICAL)
    except Exception:
        logger.exception("Failed taking the lock on {lockfile}".format(lockfile=SYNC_MUK_USERS_LOCKFILE))
        sys.exit(NAGIOS_EXIT_CRITICAL)


    try:
        muk = Muk()
        nfs_mounts = force_nfs_mounts(muk)
        logger.info("Forced NFS mounts")

        LdapQuery(LumaConfiguration())  # Initialise LDAP binding

        muk_group_filter = CnFilter(muk.muk_users_group)
        try:
            muk_group = Group.lookup(muk_group_filter)[0]
            logger.info("Muk group = %s" % (muk_group.memberUid))
        except IndexError:
            logger.raiseException("Could not find a group with cn %s. Cannot proceed synchronisation" % muk.muk_user_group)

        muk_users = [MukUser(user_id) for user_id in muk_group.memberUid]
        logger.debug("Found the following Muk users: {users}".format(users=muk_group.memberUid))

        muk_users_filter = LdapFilter.from_list(lambda x, y: x | y, [CnFilter(u.user_id) for u in muk_users])

        users_ok = {}
        for institute in nfs_mounts:
            users_ok[institute] = process_institute(options, institute, muk_users_filter)

    except Exception:
        logger.exception("Fail during muk users synchronisation")
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL,
                              NagiosResult("Script failed, check log file ({logfile})".format(
                                  logfile=SYNC_MUK_USERS_LOGFILE)))
        lockfile.release()
        sys.exit(NAGIOS_EXIT_CRITICAL)

    result_dict = {
        'a': users_ok['antwerpen']['ok'],
        'b': users_ok['brussel']['ok'],
        'g': users_ok['gent']['ok'],
        'l': users_ok['leuven']['ok'],
        'a_critical': users_ok['antwerpen']['fail'],
        'b_critical': users_ok['brussel']['fail'],
        'g_critical': users_ok['gent']['fail'],
        'l_critical': users_ok['leuven']['fail']
    }

    if any([result_dict[i + '_critical'] for i in ['a', 'b', 'g', 'l']]):
        result = NagiosResult("several users were not synched", result_dict)
        exit_value = NAGIOS_EXIT_WARNING
    else:
        write_timestamp(SYNC_TIMESTAMP_FILENAME, convert_timestamp()[1])
        result = NagiosResult("muk users synchronised", result_dict)
        exit_value = NAGIOS_EXIT_OK

    nagios_reporter.cache(exit_value, result)

    lockfile.release()
    logger.info("Finished synchronisation of the Muk users from the LDAP with the filesystem.")


if __name__ == '__main__':
    main()
