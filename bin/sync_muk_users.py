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

import logging
import os
import sys
from lockfile import FileLock, AlreadyLocked
from optparse import OptionParser

from vsc import fancylogger
from vsc.administration.group import Group
from vsc.administration.user import MukUser
from vsc.config.base import Muk
from vsc.ldap.configuration import LumaConfiguration
from vsc.ldap.filters import CnFilter, InstituteFilter, LdapFilter, NewerThanFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_CRITICAL, NAGIOS_EXIT_WARNING
from vsc.utils.pickle_files import TimestampPickle

NAGIOS_CHECK_FILENAME = '/var/log/pickles/sync_muk_users.pickle'
NAGIOS_HEADER = 'sync_muk_users'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = '/var/run/sync_muk_users.timestamp'
SYNC_MUK_USERS_LOGFILE = '/var/log/sync_muk_users.log'
SYNC_MUK_USERS_LOCKFILE = '/gpfs/scratch/user/sync_muk_users.lock'

fancylogger.logToFile(SYNC_MUK_USERS_LOGFILE)
fancylogger.setLogLevel(logging.DEBUG)

logger = fancylogger.getLogger(name='sync_muk_users')


def process_institute(options, institute, users_filter, timestamp_filter):

    muk = Muk()  # Singleton class, so no biggie
    changed_users = MukUser.lookup(timestamp_filter & users_filter & InstituteFilter(institute))
    logger.info("Processing the following users from {institute}: {users}".format(institute=institute,
                users=map(lambda u: u.user_id, changed_users)))

    try:
        nfs_location = muk.nfs_link_pathnames[institute]['home']
        logger.info("Checking link to NFS mount at %s" % (nfs_location))
        os.stat(nfs_location)
        try:
            error_users = process(options, changed_users)
        except:
            logger.exception("Something went wrong processing users from %s" % (insitute))
            pass
    except:
        logger.exception("Cannot process users from institute %s, cannot stat link to NFS mount" % (institute))
        error_users = changed_users

    fail_usercount = len(error_users)
    ok_usercount = len(changed_users) - fail_usercount

    return (ok_usercount, fail_usercount)


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

def force_nfs_mounts():
    os.stat('/nfsmuk/user/home/gent')
    os.stat('/nfsmuk/user/antwerpen')
    os.stat('/nfsmuk/user/leuven')


def main(argv):
    """
    Main script.
    - loads the previous timestamp
    - build the filter
    - fetches the users
    - process the users
    - write the new timestamp if everything went OK
    - write the nagios check file
    """

    parser = OptionParser()
    parser.add_option("-d", "--dry-run", dest="dry_run", default=False, action="store_true",
                      help="Do not make any updates whatsoever.")
    parser.add_option("", "--debug", dest="debug", default=False, action="store_true",
                      help="Enable debug output to log.")
    parser.add_option("-n", "--nagios", dest="nagios", default=False, action="store_true",
                      help="Print out the nagios result message and exit accordingly.")

    (options, args) = parser.parse_args(argv)

    if options.debug:
        fancylogger.setLogLevel(logging.DEBUG)
    else:
        fancylogger.setLogLevel(logging.INFO)

    nagios_reporter = NagiosReporter(NAGIOS_HEADER, NAGIOS_CHECK_FILENAME, NAGIOS_CHECK_INTERVAL_THRESHOLD)

    if options.nagios:
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    logger.info("Starting synchronisation of Muk users.")

    try:
        logger.info("Trying to acquire lockfile {lockfile}".format(lockfile=SYNC_MUK_USERS_LOGFILE))
        lockfile = FileLock(SYNC_MUK_USERS_LOCKFILE)
        lockfile.acquire(timeout=60)
        logger.info("Lock acquired.")
    except AlreadyLocked, _:
        logger.exception("Cannot acquire lock, bailing.")
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL, NagiosResult("Cannot acquire lock on {lockfile}. Bailing.".format(lockfile=SYNC_MUK_USERS_LOCKFILE)))
        sys.exit(NAGIOS_EXIT_CRITICAL)
    except Exception, err:
        logger.exception("Oops.")
        sys.exit(NAGIOS_EXIT_CRITICAL)


    try:
        force_nfs_mounts()

        logger.info("Forced NFS mounts")

        LdapQuery(LumaConfiguration())  # Initialise LDAP binding
        muk = Muk()

        last_timestamp = "20090101000000Z" # read_timestamp(SYNC_TIMESTAMP_FILENAME) or "20090101000000Z"
        logger.info("Last recorded timestamp was %s" % (last_timestamp))

        timestamp_filter = NewerThanFilter("objectClass=*", last_timestamp)
        logger.info("Filter for looking up new Muk users = %s" % (timestamp_filter))

        muk_group_filter = CnFilter("gt1_mukallusers")  # FIXME: this should preferably be placed in a constant
        try:
            muk_group = Group.lookup(muk_group_filter)[0]
            logger.info("Muk group = %s" % (muk_group.memberUid))
        except IndexError, _:
            logger.error("Could not find a group with cn mukusers. Cannot proceed synchronisation")
            raise

        muk_users = [MukUser(id) for id in muk_group.memberUid]
        logger.debug("Found the following Muk users: {users}".format(users=muk_group.memberUid))

        muk_users_filter = LdapFilter.from_list(lambda x, y: x | y, [CnFilter(u.user_id) for u in muk_users])

        (antwerpen_users_ok, antwerpen_users_fail) = process_institute(options, 'antwerpen', muk_users_filter, timestamp_filter)
        (brussel_users_ok, brussel_users_fail) = process_institute(options, 'brussel', muk_users_filter, timestamp_filter)
        (gent_users_ok, gent_users_fail) = process_institute(options, 'gent', muk_users_filter, timestamp_filter)
        (leuven_users_ok, leuven_users_fail) = process_institute(options, 'leuven', muk_users_filter, timestamp_filter)

    except Exception, err:
        logger.exception("Fail during muk users synchronisation: {err}".format(err=err))
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL,
                              NagiosResult("Script failed, check log file ({logfile})".format(logfile=SYNC_MUK_USERS_LOGFILE)))
        lockfile.release()
        sys.exit(NAGIOS_EXIT_CRITICAL)

    if len([us for us in [antwerpen_users_fail, brussel_users_fail, gent_users_fail, leuven_users_fail] if us > 0]):
        result = NagiosResult("several users were not synched",
                              a = antwerpen_users_ok, a_critical = antwerpen_users_fail,
                              b = brussel_users_ok, b_critical = brussel_users_fail,
                              g = gent_users_ok, g_critical = gent_users_fail,
                              l = leuven_users_ok, l_critical = leuven_users_fail)
        nagios_reporter.cache(NAGIOS_EXIT_WARNING, result)
    else:
        write_timestamp(SYNC_TIMESTAMP_FILENAME, convert_timestamp()[0])
        result = NagiosResult("muk users synchronised",
                              a = antwerpen_users_ok, a_critical = antwerpen_users_fail,
                              b = brussel_users_ok, b_critical = brussel_users_fail,
                              g = gent_users_ok, g_critical = gent_users_fail,
                              l = leuven_users_ok, l_critical = leuven_users_fail)
        nagios_reporter.cache(NAGIOS_EXIT_OK, result)

    lockfile.release()
    logger.info("Finished synchronisation of the Muk users from the LDAP with the filesystem.")


if __name__ == '__main__':
    main(sys.argv)
