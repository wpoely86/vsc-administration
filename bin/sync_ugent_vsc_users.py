#!/usr/bin/env python
#
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
import sys
import urllib
import urllib2


from vsc.administration.user import VscUser
from vsc.administration.vo import VscVo
from vsc.config.base import GENT, VscStorage, VSC
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.filters import CnFilter, InstituteFilter, NewerThanFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.utils import fancylogger
from vsc.utils.missing import Monoid, MonoidDict
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.rest_oauth import request_access_token, make_api_request
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = "sync_ugent_users"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = "/var/run/%s.timestamp" % (NAGIOS_HEADER)
SYNC_UGENT_USERS_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


STORAGE_USERS_LIMIT_WARNING = 1
STORAGE_USERS_LIMIT_CRITICAL = 10
STORAGE_VO_LIMIT_WARNING = 1
STORAGE_VO_LIMIT_CRITICAL = 10


def notify_user_directory_created(user, options, opener, access_token, dry_run=True):
    """Make sure the rest of the subsystems know the user status has changed.

    Currently, this is tailored to our LDAP-based setup.
    - if the LDAP state is new:
        change the state to notify
    - if the LDAP state is modify:
        change the state to active
    - otherwise, the user account already was active in the past, and we simply have an idempotent script.
    """
    if dry_run:
        logger.info("User %s has LDAP status %s. Dry-run so not changing anything" % (user.user_id, user.status))
        return

    payload = '{"status": "active"}'
    if user.status == 'new':
        response = make_api_request(opener, "%s/api/account/%s/" % (options.account_page_url, user.cn), 'PATCH', payload, access_token)
        if response.get('status', None) not in ('active'):
            logger.error("Status for %s was not set to active" % (user.cn,))
        else:
            logger.info("User %s changed LDAP status from new to notify" % (user.user_id))
    elif user.status == 'modify':
        response = make_api_request(opener, "%s/api/account/%s/" % (options.account_page_url, user.cn), 'PATCH', payload, access_token)
        if response.get('status', None) not in ('active'):
            logger.error("Status for %s was not set to active" % (user.cn,))
        else:
            logger.info("User %s changed LDAP status from modify to active" % (user.user_id))
    else:
        logger.info("User %s has LDAP status %s, not changing" % (user.user_id, user.status))

def notify_vo_directory_created(vo, dry_run=True):
    """Make sure the rest of the subsystems know that the VO status has changed.

    Currently, this is tailored to our LDAP-based setup.
    - if the LDAP state is new:
        change the state to notify
    - if the LDAP state is modify:
        change the state to active
    - otherwise, the VO already was active in the past, and we simply have an idempotent script.
    """
    if dry_run:
        logger.info("VO %s has LDAP status %s. Dry-run so not changing anything" % (vo.vo_id, vo.status))
        return

    if vo.status == 'new':
        vo.status = 'notify'
        logger.info("VO %s changed LDAP status from new to notify" % (vo.vo_id))
    elif vo.status == 'modify':
        vo.status = 'active'
        logger.info("VO %s changed LDAP status from modify to active" % (vo.vo_id))
    else:
        logger.info("VO %s has LDAP status %s, not changing" % (vo.vo_id, vo.status))

def process_users(options, users, storage_name, opener, access_token):
    """
    Process the users.

    We make a distinction here between three types of filesystems.
        - home (unique)
            - create and populate the home directory
        - data (unique)
            - create the grouping fileset if needed
            - create the user data directory
        - scratch (multiple)
            - create the grouping fileset if needed
            - create the user scratch directory

    The following are done everywhere:
        - set quota and permissions
    """
    error_users = []
    ok_users = []

    for user in users:
        if options.dry_run:
            user.dry_run = True

        try:
            if storage_name in ['VSC_HOME']:
                user.create_home_dir()
                user.set_home_quota()
                user.populate_home_dir()
                notify_user_directory_created(user, options, opener, access_token, options.dry_run)

            if storage_name in ['VSC_DATA']:
                user.create_data_dir()
                user.set_data_quota()

            if storage_name in ['VSC_SCRATCH_DELCATTY', 'VSC_SCRATCH_GENGAR', 'VSC_SCRATCH_GULPIN']:
                user.create_scratch_dir(storage_name)
                user.set_scratch_quota(storage_name)

            ok_users.append(user)
        except:
            logger.exception("Cannot process user %s" % (user.user_id))
            error_users.append(user)

    return (ok_users, error_users)


def process_vos(options, vos, storage, storage_name):
    """Process the virtual organisations.

    - make the fileset per VO
    - set the quota for the complete fileset
    - set the quota on a per-user basis for all VO members
    """

    listm = Monoid([], lambda xs, ys: xs + ys)
    ok_vos = MonoidDict(copy.deepcopy(listm))
    error_vos = MonoidDict(copy.deepcopy(listm))

    for vo in vos:
        if options.dry_run:
            vo.dry_run = True
        try:
            vo.status  # force LDAP attribute load

            if storage_name in ['VSC_DATA']:
                vo.create_data_fileset()
                vo.set_data_quota()
                notify_vo_directory_created(vo, options.dry_run)

            if storage_name in ['VSC_SCRATCH_GENGAR', 'VSC_SCRATCH_DELCATTY', 'VSC_SCRATCH_GULPIN']:
                vo.create_scratch_fileset(storage_name)
                vo.set_scratch_quota(storage_name)

            for user in vo.memberUid:
                try:
                    member = VscUser(user)
                    if storage_name in ['VSC_DATA']:
                        vo.set_member_data_quota(member)  # half of the VO quota
                        vo.create_member_data_dir(member)

                    if storage_name in ['VSC_SCRATCH_GENGAR', 'VSC_SCRATCH_DELCATTY', 'VSC_SCRATCH_GULPIN']:
                        vo.set_member_scratch_quota(storage_name, member)  # half of the VO quota
                        vo.create_member_scratch_dir(storage_name, member)

                    ok_vos[vo.vo_id] = [user]
                except:
                    logger.exception("Failure at setting up the member %s of VO %s on %s" %
                                     (user, vo.vo_id, storage_name))
                    error_vos[vo.vo_id] = [user]
        except:
            logger.exception("Something went wrong setting up the VO %s on the storage %s" % (vo.vo_id, storage_name))
            error_vos[vo.vo_id] = vo.memberUid

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
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'storage': ('storage systems on which to deploy users and vos', None, 'extend', []),
        'user': ('process users', None, 'store_true', False),
        'vo': ('process vos', None, 'store_true', False),
        'client_id': ('ID of the registered application', None, 'store', None),
        'client_secret': ('secret key', None, 'store', None),
        'account_page_url': ('Base URL of the account page', None, 'store', 'https://account.vscentrum.be/django')
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        oauth_path = "%s/oauth/token/" % (opts.options.account_page_url,)
        access_token_info = request_access_token(opener, oauth_path, opts.options.client_id, opts.options.client_secret )
        access_token = access_token_info['access_token']


        LdapQuery(VscConfiguration())  # Initialise LDAP binding
        vsc = VSC()
        storage = VscStorage()

        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except:
            logger.exception("Something broke reading the timestamp from %s" % SYNC_TIMESTAMP_FILENAME)
            last_timestamp = "200901010000Z"

        logger.info("Last recorded timestamp was %s" % (last_timestamp))
        timestamp_filter = NewerThanFilter("objectClass=*", last_timestamp)
        logger.debug("Timestamp filter = %s" % (timestamp_filter))

        (users_ok, users_fail) = ([], [])
        if opts.options.user:
            ugent_users_filter = timestamp_filter & InstituteFilter(GENT)
            logger.debug("Filter for looking up changed UGent users %s" % (ugent_users_filter))

            ugent_users = VscUser.lookup(ugent_users_filter)
            logger.info("Found %d UGent users that have changed in the LDAP since %s" %
                        (len(ugent_users), last_timestamp))
            logger.debug("Found the following UGent users: {users}".format(users=[u.user_id for u in ugent_users]))

            for storage_name in opts.options.storage:
                (users_ok, users_fail) = process_users(opts.options,
                                                       ugent_users,
                                                       storage_name,
                                                       opener,
                                                       access_token)
                stats["%s_users_sync" % (storage_name,)] = len(users_ok)
                stats["%s_users_sync_fail" % (storage_name,)] = len(users_fail)
                stats["%s_users_sync_fail_warning" % (storage_name,)] = STORAGE_USERS_LIMIT_WARNING
                stats["%s_users_sync_fail_critical" % (storage_name,)] = STORAGE_USERS_LIMIT_CRITICAL

        (vos_ok, vos_fail) = ([], [])
        if opts.options.vo:
            ugent_vo_filter = timestamp_filter & InstituteFilter(GENT) & CnFilter("gvo*")
            logger.info("Filter for looking up changed UGent VOs = %s" % (ugent_vo_filter))

            ugent_vos = [vo for vo in VscVo.lookup(ugent_vo_filter) if vo.vo_id not in vsc.institute_vos.values()]
            logger.info("Found %d UGent VOs that have changed in the LDAP since %s" % (len(ugent_vos), last_timestamp))
            logger.debug("Found the following UGent VOs: {vos}".format(vos=[vo.vo_id for vo in ugent_vos]))

            for storage_name in opts.options.storage:
                (vos_ok, vos_fail) = process_vos(opts.options,
                                                     ugent_vos,
                                                     storage[storage_name],
                                                     storage_name)
                stats["%s_vos_sync" % (storage_name,)] = len(vos_ok)
                stats["%s_vos_sync_fail" % (storage_name,)] = len(vos_fail)
                stats["%s_vos_sync_fail_warning" % (storage_name,)] = STORAGE_VO_LIMIT_WARNING
                stats["%s_vos_sync_fail_critical" % (storage_name,)] = STORAGE_VO_LIMIT_CRITICAL

        if not (users_fail or vos_fail):
            (_, ldap_timestamp) = convert_timestamp()
            if not opts.options.dry_run:
                write_timestamp(SYNC_TIMESTAMP_FILENAME, ldap_timestamp)
    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("UGent users and VOs synchronised", stats)


if __name__ == '__main__':
    main()
