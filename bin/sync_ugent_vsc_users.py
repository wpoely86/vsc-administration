#!/usr/bin/env python
##
#
# Copyright 2013 Ghent University
# Copyright 2013 Andy Georges
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
This script synchronises the users and VO's from the HPC LDAP to the central
UGent storage for home and data.

For each (active) user, the following tasks are done:
    - create a directory in the home filesystem
    - chown this directory to the user
    - create the basic directories and scripts if they do not yet exist (.ssh, .bashrc, ...)
    - drop the user's public keys in the appropriate location
    - chmod the files to the correct value
    - chown the files (only changes things upopn first invocation and new files)

The script should result in an idempotent execution, to ensure nothing breaks.
"""

# --------------------------------------------------------------------
import logging
import os
import sys

# --------------------------------------------------------------------
from vsc import fancylogger
from vsc.administration.group import Group
from vsc.administration.user import MukUser
from vsc.config.base import Muk
from vsc.ldap.configuration import LumaConfiguration
from vsc.ldap.entities import VscLdapUser, VscLdapGroup
from vsc.ldap.filters import CnFilter, InstituteFilter, LdapFilter, NewerThanFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.utils.generaloption import simple_option
from vsc.utils.lock import lock_or_bork, release_or_bork
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_CRITICAL, NAGIOS_EXIT_WARNING
from vsc.utils.pickle_files import TimestampPickle

NAGIOS_CHECK_FILENAME = '/var/log/pickles/sync_muk_users.pickle'
NAGIOS_HEADER = 'sync_muk_users'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = '/var/run/sync_ugent_users.timestamp'
SYNC_UGENT_USERS_LOGFILE = '/var/log/sync_ugent_users.log'
SYNC_UGENT_USERS_LOCKFILE = '/gpfs/scratch/user/sync_ugent_users.lock'

fancylogger.logToFile(SYNC_UGENT_USERS_LOGFILE)
fancylogger.setLogLevel(logging.DEBUG)

logger = fancylogger.getLogger(name='sync_vsc_ugent_users')


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
            logger.error("Oops")
            pass
    except:
        logger.error("Cannot process users from institute %s, cannot stat link to NFS mount" % (institute))
        error_users = changed_users

    fail_usercount = len(error_users)
    ok_usercount = len(changed_users) - fail_usercount

    return (ok_usercount, fail_usercount)


def process_users(options, users):
    """
    Process the users.

    - make their home directory
    - populate their home directory
    - make their data directory
    """
    error_users = []
    for user in users:
        if options.dry_run:
            user.dry_run = True
        try:
            user.create_home_dir()
            user.populate_home_dir()

            user.create_data_dir()
        except:
            logger.exception("Cannot process user %s" % (user.user_id))
            error_users.append(user)

    return error_users


def process_vos(options, vos):
    pass


def process(options, users):
    """
    Actually do the tasks for a changed or new user:

    - created the user's fileset
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


def main(argv):
    """
    Main script.
    - build the filter
    - fetches the users
    - process the users
    - write the new timestamp if everything went OK
    - write the nagios check file
    """

    options = {
        'dry-run': ('do not make any updates whatsoever', None, 'store_true', False),
        'nagios': ('print out nagion information', None, 'store_true', False, 'n'),
        'nagios_check_filename': ('filename of where the nagios check data is stored', str, 'store', NAGIOS_CHECK_FILENAME),
        'nagios_check_interval_threshold': ('threshold of nagios checks timing out', None, 'store', NAGIOS_CHECK_INTERVAL_THRESHOLD),
    }

    opts = simple_option(options)

    nagios_reporter = NagiosReporter(NAGIOS_HEADER,
                                     opts.options.nagios_check_filename,
                                     opts.options.nagios_check_interval_threshold)

    if options.nagios:
        logger.debug("Producing Nagios report and exiting.")
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    logger.info("Starting synchronisation of UGent users.")

    lock_or_bork(lockfile, nagios_reporter)

    try:
        LdapQuery(LumaConfiguration())  # Initialise LDAP binding
        vsc =Vsc()

        last_timestamp = "20090101000000Z" # read_timestamp(SYNC_TIMESTAMP_FILENAME) or "20090101000000Z"
        logger.info("Last recorded timestamp was %s" % (last_timestamp))

        timestamp_filter = NewerThanFilter("objectClass=*", last_timestamp)
        logger.info("Filter for looking up new UGent users = %s" % (timestamp_filter))

        ugent_users_filter = InstituteFilter("gent")  # FIXME: this should preferably be placed in a constant
        ugent_users = VscLdapUser.lookup(ugent_users_filter)

        logger.debug("Found the following UGent users: {users}".format(users=[u.user_id for u in ugent_users]))

        process_users(ugent_users)

        ugent_vo_filter = InsituteFilter("gent") & CnFilter("gvo*")
        ugent_vos = VscLdapGroup.lookup(ugent_vo_filter)

        process_vos(ugent_vos)

    except Exception, err:
        logger.error("Fail during ugent users synchronisation: {err}".format(err=err))
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
