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

from vsc.administration.group import Group
from vsc.administration.user import MukUser
from vsc.config.base import Muk, ANTWERPEN, BRUSSEL, GENT, LEUVEN
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.filters import CnFilter, InstituteFilter, LdapFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.timestamp import convert_timestamp, write_timestamp
from vsc.utils import fancylogger
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = 'sync_muk_users'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = "/var/run/%s.timestamp" % (NAGIOS_HEADER)
SYNC_MUK_USERS_LOCKFILE = "/gpfs/scratch/user/%s.lock" % (NAGIOS_HEADER)

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


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
        if institute == BRUSSEL:
            logger.warning("Not performing any action for institute %s" % (BRUSSEL,))
            continue
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
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'locking-filename': SYNC_MUK_USERS_LOCKFILE,
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        muk = Muk()
        nfs_mounts = force_nfs_mounts(muk)
        logger.info("Forced NFS mounts")

        l = LdapQuery(VscConfiguration())  # Initialise LDAP binding

        muk_group_filter = CnFilter(muk.muk_user_group)
        try:
            muk_group = Group.lookup(muk_group_filter)[0]
            logger.info("Muk group = %s" % (muk_group.memberUid))
        except IndexError:
            logger.raiseException("Could not find a group with cn %s. Cannot proceed synchronisation" % muk.muk_user_group)

        muk_users = [MukUser(user_id) for user_id in muk_group.memberUid]
        logger.debug("Found the following Muk users: {users}".format(users=muk_group.memberUid))

        muk_users_filter = LdapFilter.from_list(lambda x, y: x | y, [CnFilter(u.user_id) for u in muk_users])

        for institute in nfs_mounts:
            users_ok = process_institute(opts.options, institute, muk_users_filter)
            total_institute_users = len(l.user_filter_search(InstituteFilter(institute)))
            stats["%s_users_sync" % (institute,)] = users_ok.get(institute).get('ok',0)
            stats["%s_users_sync_warning" % (institute,)] = int(total_institute_users / 5)  # 20% of all users want to get on
            stats["%s_users_sync_critical" % (institute,)] = int(total_institute_users / 2)  # 30% of all users want to get on
            stats["%s_users_sync_fail" % (institute,)] = users_ok.get(institute).get('fail',0)
            stats["%s_users_sync_fail_warning" % (institute,)] = users_ok.get(institute).get('fail',0)
            stats["%s_users_sync_fail_warning" % (institute,)] = 1
            stats["%s_users_sync_fail_critical" % (institute,)] = 3
    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("Muk users synchronisation completed", stats)


if __name__ == '__main__':
    main()
