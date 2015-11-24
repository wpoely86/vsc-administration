#!/usr/bin/env python
#
# Copyright 2013-2015 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
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

from datetime import datetime
from urllib2 import HTTPError

from vsc.accountpage.client import AccountpageClient
from vsc.accountpage.wrappers import mkVscAccount, mkUserGroup
from vsc.administration.user import VscTier2AccountpageUser
from vsc.config.base import VscStorage, NEW, MODIFIED, MODIFY, ACTIVE
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.utils import fancylogger
from vsc.utils.missing import nub
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = "sync_ugent_users"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)
SYNC_UGENT_USERS_LOGFILE = "/var/log/%s.log" % (NAGIOS_HEADER)

logger = fancylogger.getLogger()
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

STORAGE_USERS_LIMIT_WARNING = 1
STORAGE_USERS_LIMIT_CRITICAL = 10
STORAGE_VO_LIMIT_WARNING = 1






class UserGroupStatusUpdateError(Exception):
    pass


def update_user_status(user, options, client):
    """
    Change the status of the user's account in the account page to active. Do the same for the corresponding UserGroup.
    """
    if user.dry_run:
        logger.info("User %s has account status %s. Dry-run, not changing anything", user.user_id, user.account.status)
        return

    if user.account.status not in (NEW, MODIFIED, MODIFY):
        logger.info("Account %s has status %s, not changing" % (user.user_id, user.account.status))
        return

    payload = {"status": ACTIVE}
    try:
        response_account = client.account[user.user_id].patch(body=payload)
    except HTTPError, err:
        logger.error("Account %s and UserGroup %s status were not changed", user.user_id, user.user_id)
        raise UserStatusUpdateError("Account %s status was not changed - received HTTP code %d" % err.code)
    else:
        account = mkVscAccount(response_account[1])
        if account.status == ACTIVE:
            logger.info("Account %s status changed to %s" % (user.user_id, ACTIVE))
        else:
            logger.error("Account %s status was not changed", user.user_id)
            raise UserStatusUpdateError("Account %s status was not changed, still at %s" %
                                        (user.user_id, account.status))
    try:
        response_usergroup = client.account[user.user_id].usergroup.patch(body=payload)
    except HTTPError, err:
        logger.error("UserGroup %s status was not changed", user.user_id)
        raise UserStatusUpdateError("UserGroup %s status was not changed - received HTTP code %d" % (user.user_id, err.code))

    else:
        usergroup = mkUserGroup(response_usergroup[1])
        if usergroup.status == ACTIVE:
            logger.info("UserGroup %s status changed to %s" % (user.user_id, ACTIVE))
        else:
            logger.error("UserGroup %s status was not changed", user.user_id)
            raise UserStatusUpdateError("UserGroup %s status was not changed, still at %s" %
                                        (user.user_id, usergroup.status))


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

    for vsc_id in sorted(account_ids):

        user = VscTier2AccountpageUser(vsc_id, rest_client=client)
        user.dry_run = options.dry_run

        try:
            if storage_name in ['VSC_HOME']:
                user.create_home_dir()
                user.set_home_quota()
                user.populate_home_dir()
                update_user_status(user, options, client)

            if storage_name in ['VSC_DATA']:
                user.create_data_dir()
                user.set_data_quota()

            if storage_name in ['VSC_SCRATCH_DELCATTY', 'VSC_SCRATCH_GENGAR', 'VSC_SCRATCH_GULPIN']:
                user.create_scratch_dir(storage_name)
                user.set_scratch_quota(storage_name)

            if storage_name in ['VSC_SCRATCH_PHANPY']:
                user.create_scratch_dir(storage_name)

            ok_users.append(user)
        except:
            logger.exception("Cannot process user %s" % (user.user_id))
            error_users.append(user)

    return (ok_users, error_users)


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

        now = datetime.utcnow()

        client = AccountpageClient(token=opts.options.access_token)

        storage = VscStorage()

        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except:
            logger.exception("Something broke reading the timestamp from %s" % SYNC_TIMESTAMP_FILENAME)
            last_timestamp = "200901010000Z"

        logger.info("Last recorded timestamp was %s" % (last_timestamp))

        (users_ok, users_fail) = ([], [])
        if opts.options.user:
            ugent_changed_accounts = client.account.institute['gent'].modified[last_timestamp[:12]].get()[1]
            ugent_changed_pubkey_accounts = client.account.pubkey.institute['gent'].modified[last_timestamp[:12]].get()[1]
            ugent_changed_quota = client.quota.user.modified[last_timestamp[:12]].get()[1]

            logger.info("Found %d UGent accounts that have changed in the accountpage since %s" %
                        (len(ugent_changed_accounts), last_timestamp[:8]))
            logger.info("Found %d UGent accounts that have changed pubkeys in the accountpage since %s" %
                        (len(ugent_changed_pubkey_accounts), last_timestamp[:12]))
            logger.info("Found %d UGent accounts that have changed quota in the accountpage since %s" %
                        (len(ugent_changed_quota), last_timestamp[:12]))

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
            ugent_changed_vos = client.vo.modified[last_timestamp[:12]].get()[1]
            ugent_changed_vo_quota = client.quota.vo.modified[last_timestamp[:12]].get()[1]

            ugent_vos = [v['vsc_id'] for v in ugent_changed_vos] + [v['virtual_organisation'] for v in ugent_changed_vo_quota]

            logger.info("Found %d UGent VOs that have changed in the accountpage since %s" %
                        (len(ugent_changed_vos), last_timestamp[:12]))
            logger.info("Found %d UGent VOs that have changed quota in the accountpage since %s" %
                        (len(ugent_changed_vo_quota), last_timestamp[:12]))
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
            (_, ldap_timestamp) = convert_timestamp(now)
            if not opts.options.dry_run:
                write_timestamp(SYNC_TIMESTAMP_FILENAME, ldap_timestamp)
    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("UGent users and VOs synchronised", stats)


if __name__ == '__main__':
    main()
