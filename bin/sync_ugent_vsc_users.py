#!/usr/bin/env python
##
#
# Copyright 2013-2013 Ghent University
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
    - chown the files (only changes things upon first invocation and new files)

The script should result in an idempotent execution, to ensure nothing breaks.
"""

import copy
import logging
import sys

from vsc import fancylogger
from vsc.administration.user import VscUser
from vsc.administration.vo import VscVo
from vsc.config.base import CentralStorage, VSC
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.filters import CnFilter, InstituteFilter, NewerThanFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.utils.generaloption import simple_option
from vsc.utils.lock import lock_or_bork, release_or_bork
from vsc.utils.missing import Monoid, MonoidDict
from vsc.utils.nagios import NagiosReporter, NagiosResult, NAGIOS_EXIT_OK, NAGIOS_EXIT_CRITICAL, NAGIOS_EXIT_WARNING
from vsc.utils.timestamp_pid_lockfile import TimestampedPidLockfile

NAGIOS_CHECK_FILENAME = '/var/log/pickles/sync_muk_users.pickle'
NAGIOS_HEADER = 'sync_muk_users'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = '/var/run/sync_ugent_users.timestamp'
SYNC_UGENT_USERS_LOGFILE = '/var/log/sync_ugent_users.log'
SYNC_UGENT_USERS_LOCKFILE = '/var/run/sync_ugent_users.lock'

fancylogger.logToFile(SYNC_UGENT_USERS_LOGFILE)
fancylogger.setLogLevel(logging.DEBUG)

logger = fancylogger.getLogger(name='sync_vsc_ugent_users')


def process_users(options, users, storage):
    """
    Process the users.

    - make their home directory
    - populate their home directory
    - make their data directory
    """
    error_users = []
    ok_users = []
    for user in users:
        if options.dry_run:
            user.dry_run = True
        try:
            user.create_home_dir()
            user.set_home_quota()
            user.populate_home_dir()

            user.create_data_dir()
            user.set_data_quota()
            # At this point, the user's data quota are still wrong, since we upped them to work around the
            # fileset mess-up on the old gengar shared storage. We need to fix these once deployed.
            ok_users.append(user)
        except:
            logger.exception("Cannot process user %s" % (user.user_id))
            error_users.append(user)

    return (ok_users, error_users)


def process_vos(options, vos, storage):
    """Process the virtual organisations.

    - make the fileset per VO
    - set the quota for the complete fileset
    - set the quota on a per-user basis for all VO members
    """

    listm = Monoid([], lambda xs, ys: xs + ys)
    ok_vos = MonoidDict(copy.deepcopy(listm))
    error_vos = MonoidDict(copy.deepcopy(listm))

    for vo in vos:
        try:
            vo.status # force LDAP attribute load
            vo.create_data_fileset()
            vo.set_data_quota()

            for user in vo.memberUid:
                try:
                    vo.set_member_data_quota(VscUser(user))  # half of the VO quota
                    vo.set_member_data_symlink(VscUser(user))
                    ok_vos[vo.vo_id] = user
                except:
                    logger.exception("Failure at setting up the member %s VO %s data" % (user.user_id, vo.vo_id))
                    error_vos[vo.vo_id] = user
        except:
            logger.exception("Oops. Something went wrong setting up the VO on the filesystem")

    return (ok_vos, error_vos)


def main():
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
        'nagios-check-filename': ('filename of where the nagios check data is stored', str, 'store', NAGIOS_CHECK_FILENAME),
        'nagios-check-interval-threshold': ('threshold of nagios checks timing out', None, 'store', NAGIOS_CHECK_INTERVAL_THRESHOLD),
    }

    opts = simple_option(options)

    nagios_reporter = NagiosReporter(NAGIOS_HEADER,
                                     opts.options.nagios_check_filename,
                                     opts.options.nagios_check_interval_threshold)

    if opts.options.nagios:
        logger.debug("Producing Nagios report and exiting.")
        nagios_reporter.report_and_exit()
        sys.exit(0)  # not reached

    logger.info("Starting synchronisation of UGent users.")

    lockfile = TimestampedPidLockfile(SYNC_UGENT_USERS_LOCKFILE)
    lock_or_bork(lockfile, nagios_reporter)

    try:
        LdapQuery(VscConfiguration())  # Initialise LDAP binding
        vsc = VSC()
        storage = CentralStorage()
        backup_storage = copy.deepcopy(storage)
        backup_storage.home_name = "backup%s" % (storage.home_name)
        backup_storage.data_name = "backup%s" % (storage.data_name)

        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except:
            logger.exception("Something broke reading the timestamp")
            last_timestamp = "200901010000Z"

        logger.info("Last recorded timestamp was %s" % (last_timestamp))

        timestamp_filter = NewerThanFilter("objectClass=*", last_timestamp)
        logger.info("Filter for looking up new UGent users = %s" % (timestamp_filter))

        ugent_users_filter = InstituteFilter("gent") & timestamp_filter  # FIXME: this should preferably be placed in a constant
        ugent_users = VscUser.lookup(ugent_users_filter)

        logger.debug("Found the following UGent users: {users}".format(users=[u.user_id for u in ugent_users]))

        (users_ok, users_critical) = process_users(opts.options, ugent_users, storage)

        ugent_vo_filter = InstituteFilter("gent") & CnFilter("gvo*") & timestamp_filter
        ugent_vos = [vo for vo in VscVo.lookup(ugent_vo_filter) if vo.vo_id not in vsc.institute_vos.values()]

        (vos_ok, vos_critical) = process_vos(opts.options, ugent_vos, storage)

    except Exception, err:
        logger.exception("Fail during UGent users synchronisation: {err}".format(err=err))
        nagios_reporter.cache(NAGIOS_EXIT_CRITICAL,
                              NagiosResult("Script failed, check log file ({logfile})".format(logfile=SYNC_UGENT_USERS_LOGFILE)))
        lockfile.release()
        sys.exit(NAGIOS_EXIT_CRITICAL)

    result = NagiosResult("UGent users synchronised",
                          users_ok=len(users_ok),
                          users_critical=len(users_critical),
                          vos_ok=len(vos_ok),
                          vos_critical=len(vos_critical))
    try:
        write_timestamp(SYNC_TIMESTAMP_FILENAME, convert_timestamp())
        nagios_reporter.cache(NAGIOS_EXIT_OK, result)
    except:
        logger.exception("Something broke writing the timestamp")
        result.message = "UGent users synchronised, filestamp not written"
        nagios_reporter.cache(NAGIOS_EXIT_WARNING, result)
    finally:
        result.message = "UGent users synchronised, lock release failed"
        release_or_bork(lockfile, nagios_reporter, result)

    logger.info("Finished synchronisation of the UGent VSC users from the LDAP with the filesystem.")


if __name__ == '__main__':
    main()
