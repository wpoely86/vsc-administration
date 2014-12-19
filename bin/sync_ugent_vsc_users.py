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


from vsc.accountpage.client import AccountpageClient
from vsc.administration.user import VscTier2AccountpageUser
from vsc.administration.vo import VscTier2AccountpageVo
from vsc.config.base import GENT, VscStorage, VSC
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.utils import fancylogger
from vsc.utils.missing import Monoid, MonoidDict, nub
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
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

ACTIVE = 'active'
MODIFIED = 'modified'
MODIFY = 'modify'
NEW = 'new'
NOTIFY = 'notify'

def notify_user_directory_created(user, options, client):
    """Make sure the rest of the subsystems know the user status has changed.

    Currently, this is tailored to our LDAP-based setup.
    - if the LDAP state is new:
        change the state to notify
    - if the LDAP state is modify:
        change the state to active
    - otherwise, the user account already was active in the past, and we simply have an idempotent script.
    """
    if user.dry_run:
        logger.info("User %s has account status %s. Dry-run so not changing anything" % (user.user_id, user.account.status))
        return

    payload = {"status": ACTIVE}
    if user.account.status == NEW:
        response = client.account[user.user_id].patch(body=payload)
        if response[0] != 200 or response[1].get('status', None) != ACTIVE:
            logger.error("Status for %s was not set to active" % (user.user_id,))
        else:
            logger.info("Account %s changed status from new to notify" % (user.user_id))
    elif user.account.status in (MODIFIED, MODIFY):
        response = client.account[user.user_id].patch(body=payload)
        if response[0] != 200 or response[1].get('status', None) != ACTIVE:
            logger.error("Status for %s was not set to active" % (user.user_id,))
        else:
            logger.info("Account %s changed status from modify to active" % (user.user_id))
    else:
        logger.info("Account %s has status %s, not changing" % (user.user_id, user.account.status))


def notify_vo_directory_created(vo, client):
    """Make sure the rest of the subsystems know that the VO status has changed.

    Currently, this is tailored to our LDAP-based setup.
    - if the LDAP state is new:
        change the state to notify
    - if the LDAP state is modify:
        change the state to active
    - otherwise, the VO already was active in the past, and we simply have an idempotent script.
    """
    if vo.dry_run:
        logger.info("VO %s has status %s. Dry-run so not changing anything" % (vo.vo_id, vo.vo.status))
        return

    if vo.vo.status == NEW:
        payload = {"status": NOTIFY }
        response = client.vo[vo.vo_id].patch(body=payload)
        if response[0] != 200 or response[1].get('status', None) != NOTIFY:
            logger.error("Status for %s was not set to notify" % (vo.vo_id,))
        else:
            logger.info("VO %s changed accountpage status from new to notify" % (vo.vo_id))
    elif vo.vo.status in (MODIFIED, MODIFY):
        payload = {"status": ACTIVE }
        response = client.vo[vo.vo_id].patch(body=payload)
        if response[0] != 200 or response[1].get('status', None) != ACTIVE:
            logger.error("Status for %s was not set to active" % (vo.vo_id,))
        else:
            logger.info("VO %s changed accountpage status from modify to active" % (vo.vo_id))
    else:
        logger.info("VO %s has accountpage status %s, not changing" % (vo.vo_id, vo.vo.status))


def process_users(options, account_ids, storage_name, client):
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

    for vsc_id in account_ids:

        user = VscTier2AccountpageUser(vsc_id, rest_client=client)
        user.dry_run = options.dry_run

        try:
            if storage_name in ['VSC_HOME']:
                user.create_home_dir()
                user.set_home_quota()
                user.populate_home_dir()
                notify_user_directory_created(user, options, client)

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


def process_vos(options, vo_ids, storage, storage_name, client):
    """Process the virtual organisations.

    - make the fileset per VO
    - set the quota for the complete fileset
    - set the quota on a per-user basis for all VO members
    """

    listm = Monoid([], lambda xs, ys: xs + ys)
    ok_vos = MonoidDict(copy.deepcopy(listm))
    error_vos = MonoidDict(copy.deepcopy(listm))

    for vo_id in vo_ids:

        vo = VscTier2AccountpageVo(vo_id, rest_client=client)
        vo.dry_run = options.dry_run

        try:
            if storage_name in ['VSC_HOME']:
                continue

            if storage_name in ['VSC_DATA']:
                vo.create_data_fileset()
                vo.set_data_quota()
                notify_vo_directory_created(vo, client)

            if storage_name in ['VSC_SCRATCH_GENGAR', 'VSC_SCRATCH_DELCATTY', 'VSC_SCRATCH_GULPIN']:
                vo.create_scratch_fileset(storage_name)
                vo.set_scratch_quota(storage_name)

            for user_id in vo.vo.members:
                try:
                    member = VscTier2AccountpageUser(user_id, rest_client=client)
                    member.dry_run = options.dry_run
                    if storage_name in ['VSC_DATA']:
                        vo.set_member_data_quota(member)  # half of the VO quota
                        vo.create_member_data_dir(member)

                    if storage_name in ['VSC_SCRATCH_GENGAR', 'VSC_SCRATCH_DELCATTY', 'VSC_SCRATCH_GULPIN']:
                        vo.set_member_scratch_quota(storage_name, member)  # half of the VO quota
                        vo.create_member_scratch_dir(storage_name, member)

                    ok_vos[vo.vo_id] = [user_id]
                except:
                    logger.exception("Failure at setting up the member %s of VO %s on %s" %
                                     (user_id, vo.vo_id, storage_name))
                    error_vos[vo.vo_id] = [user_id]
        except:
            logger.exception("Something went wrong setting up the VO %s on the storage %s" % (vo.vo_id, storage_name))
            error_vos[vo.vo_id] = vo.members

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
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
        'account_page_url': ('URL of the account page REST API', None, 'store', None)
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        client = AccountpageClient(token=opts.options.access_token)

        vsc = VSC()
        storage = VscStorage()

        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except:
            logger.exception("Something broke reading the timestamp from %s" % SYNC_TIMESTAMP_FILENAME)
            last_timestamp = "200901010000Z"

        logger.info("Last recorded timestamp was %s" % (last_timestamp))

        (users_ok, users_fail) = ([], [])
        if opts.options.user:
            ugent_changed_accounts = client.account.institute['gent'].modified[last_timestamp[:8]].get()[1]
            ugent_changed_pubkey_accounts = client.account.pubkey.institute['gent'].modified[last_timestamp[:8]].get()[1]
            ugent_changed_quota = client.quota.user.modified[last_timestamp[:8]].get()[1]

            logger.info("Found %d UGent accounts that have changed in the accountpage since %s" %
                        (len(ugent_changed_accounts), last_timestamp[:8]))
            logger.info("Found %d UGent accounts that have changed pubkeys in the accountpage since %s" %
                        (len(ugent_changed_pubkey_accounts), last_timestamp[:8]))
            logger.info("Found %d UGent accounts that have changed quota in the accountpage since %s" %
                        (len(ugent_changed_quota), last_timestamp[:8]))

            ugent_accounts = [u['vsc_id'] for u in ugent_changed_accounts] \
                           + [u['vsc_id'] for u in ugent_changed_pubkey_accounts if u['vsc_id']] \
                           + [u['user'] for u in ugent_changed_quota]
            ugent_accounts = nub(ugent_accounts)

            for storage_name in opts.options.storage:
                (users_ok, users_fail) = process_users(opts.options,
                                                       ugent_accounts,
                                                       storage_name,
                                                       client)
                stats["%s_users_sync" % (storage_name,)] = len(users_ok)
                stats["%s_users_sync_fail" % (storage_name,)] = len(users_fail)
                stats["%s_users_sync_fail_warning" % (storage_name,)] = STORAGE_USERS_LIMIT_WARNING
                stats["%s_users_sync_fail_critical" % (storage_name,)] = STORAGE_USERS_LIMIT_CRITICAL

        (vos_ok, vos_fail) = ([], [])
        if opts.options.vo:
            ugent_changed_vos = client.vo.modified[last_timestamp[:8]].get()[1]
            ugent_changed_vo_quota = client.quota.vo.modified[last_timestamp[:8]].get()[1]

            ugent_vos = [v['vsc_id'] for v in ugent_changed_vos] \
                      + [v['virtual_organisation'] for v in ugent_changed_vo_quota]

            logger.info("Found %d UGent VOs that have changed in the accountpage since %s" %
                        (len(ugent_changed_vos), last_timestamp[:8]))
            logger.info("Found %d UGent VOs that have changed quota in the accountpage since %s" %
                        (len(ugent_changed_vo_quota), last_timestamp[:8]))
            logger.debug("Found the following UGent VOs: {vos}".format(vos=ugent_vos))

            for storage_name in opts.options.storage:
                (vos_ok, vos_fail) = process_vos(opts.options,
                                                 ugent_vos,
                                                 storage[storage_name],
                                                 storage_name,
                                                 client)
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
