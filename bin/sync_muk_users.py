#!/usr/bin/env python
#
# Copyright 2012-2015 Ghent University
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
This script checks the users entries in the LDAP that have changed since a given timestamp
and that are in the muk autogroup.

For these, the home and other shizzle should be set up.

@author Andy Georges
"""

import logging
import os
import sys
import time

from urllib2 import HTTPError

from vsc.administration.user import MukAccountpageUser
from vsc.accountpage.client import AccountpageClient
from vsc.config.base import Muk
from vsc.utils import fancylogger
from vsc.utils.cache import FileCache
from vsc.utils.mail import VscMail
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = 'sync_muk_users'
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes

SYNC_TIMESTAMP_FILENAME = "/var/run/%s.timestamp" % (NAGIOS_HEADER)
SYNC_MUK_USERS_LOCKFILE = "/gpfs/scratch/user/%s.lock" % (NAGIOS_HEADER)

PURGE_NOTIFICATION_TIMES = (7 * 86400, 12 * 86400)
PURGE_DEADLINE_TIME = 14 * 86400

logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()

TIER1_GRACE_GROUP_SUFFIX = "t1_mukgraceusers"
TIER1_HELPDESK_ADDRESS = "tier1@ugent.be"
UGENT_SMTP_ADDRESS = "smtp.ugent.be"

REINSTATEMENT_MESSAGE = """
Dear %(gecos)s,

You have been reinstated to regular status on the VSC Tier-1 cluster at Ghent. This means you can
again submit jobs to the scheduler.

Should you have any questions, please contact us at %(tier1_helpdesk)s or reply to
this email which will open a ticket in our helpdesk system for you.

Kind regards,
-- The UGent HPC team
"""

GRACE_MESSAGE = """
Dear %(gecos)s,

Your allocated compute time on the VSC Tier-1 cluster at Ghent has expired.
You are now in a grace state for the next %(grace_time)s.  This means you can
no longer submit new jobs to the scheduler.  Jobs running at this moment will
not be killed and should likely finish.

Please make sure you copy back all required results from the dedicated
$VSC_SCRATCH storage on the Tier-1 to your home institution's long term
storage, since you will no longer be able to log in to this machine once
the grace period expires.

Should you have any questions, please contact us at %(tier1_helpdesk)s or reply to
this email which will open a ticket in our helpdesk system for you.

Kind regards,
-- The UGent HPC team
"""

FINAL_MESSAGE = """
Dear %(gecos)s,

The grace period for your compute time on the VSC Tier-1 cluster at Ghent
has expired.  From this point on, you will not be able to log in to the
machine anymore, nor will you be able to reach its dedicated $VSC_SCRATCH
storage.

Should you have any questions, please contact us at %(tier1_helpdesk)s or reply to
this email which will open a ticket in our helpdesk system for you.

Kind regards,
-- The UGent HPC team
"""


def process_institute(options, institute, institute_users, client):
    """
    Sync the users from the given institute to the system
    """
    muk = Muk()  # Singleton class, so no biggie

    try:
        nfs_location = muk.nfs_link_pathnames[institute]['home']
        logger.info("Checking link to NFS mount at %s" % (nfs_location))
        os.stat(nfs_location)
        try:
            error_users = process(options, institute_users, client)
        except:
            logger.exception("Something went wrong processing users from %s" % (institute))
            error_users = institute_users
    except:
        logger.exception("Cannot process users from institute %s, cannot stat link to NFS mount" % (institute))
        error_users = institute_users

    fail_usercount = len(error_users)
    ok_usercount = len(institute_users) - fail_usercount

    return {
        'ok': ok_usercount,
        'fail': fail_usercount,
        }


def process(options, users, client):
    """
    Actually do the tasks for a changed or new user:

    - create the user's fileset
    - set the quota
    - create the home directory as a link to the user's fileset
    """

    error_users = []
    for user_id in sorted(users):
        user = MukAccountpageUser(user_id, rest_client=client)
        user.dry_run = options.dry_run
        try:
            user.create_scratch_fileset()
            user.populate_scratch_fallback()
            user.create_home_dir()
        except:
            logger.exception("Cannot process user %s" % (user_id))
            error_users.append(user_id)

    return error_users


def force_nfs_mounts(muk):
    """
    Make sure that the NFS mounts are there
    """

    nfs_mounts = []
    for institute in muk.institutes:
        try:
            os.stat(muk.nfs_link_pathnames[institute]['home'])
            nfs_mounts.append(institute)
        except:
            logger.exception("Cannot stat %s, not adding institute" % muk.nfs_link_pathnames[institute]['home'])

    return nfs_mounts


def cleanup_purgees(current_users, purgees, client, dry_run):
    """
    Remove users from purgees if they are in the current users list.
    """
    purgees_undone = 0
    for user_id in current_users:
        logger.debug("Checking if %s is in purgees", (user_id,))
        if user_id in purgees:
            del purgees[user_id]
            purgees_undone += 1
            logger.info("Removed %s from the list of purgees: found in list of current users" % (user_id,))
            user = MukAccountpageUser(user_id, rest_client=client)
            user.dry_run = dry_run
            notify_reinstatement(user)

            group_name = "%st1_mukgraceusers" % user.person.institute['site']
            if not user.dry_run:
                try:
                    client.group[group_name].member[user.account.vsc_id].delete()
                except HTTPError, err:
                    logging.error("Return code %d: could not remove %s from group %s [%s].",
                                  err.code, user.account.vsc_id, group_name, err)
                    continue
                else:
                    logging.info("Account %s removed to group %s", user.account.vsc_id, group_name)
            else:
                logging.info("Dry-run: not removing user %s from grace users group %s" %
                             (user.account.vsc_id, group_name))

    return purgees_undone


def add_users_to_purgees(previous_users, current_users, purgees, now, client, dry_run):
    """
    Add the users that are out to the purgees
    """
    purgees_first_notify = 0
    for user_id in previous_users:
        user = MukAccountpageUser(user_id, rest_client=client)
        user.dry_run = dry_run
        if user_id not in current_users and user_id not in purgees:
            if not user.dry_run:
                group_name = "%st1_mukgraceusers" % user.person.institute['site'][0]
                try:
                    client.group[group_name].member[user_id].post()
                except HTTPError, err:
                    logging.error(
                        "Return code %d: could not add %s to group %s [%s]. Not notifying user or adding to purgees.",
                        err.code, user_id, group_name, err)
                    continue
                else:
                    logging.info("Account %s added to group %s", user_id, group_name)
            notify_user_of_purge(user, now, now)
            purgees[user_id] = (now, None, None)  # in a dry run we will not store these in the cache
            purgees_first_notify += 1
            logger.info("Added %s to the list of purgees with timestamp %s" % (user_id, (now, None, None)))

        else:
            logger.info("User %s in both previous users and current users lists, not eligible for purge." % (user_id,))

    return purgees_first_notify


def purge_obsolete_symlinks(path, current_users, client, dry_run):
    """
    The symbolic links to home directories must vanish for people who no longer have access.

    For this we use a cache with the following items.
    - previous list of tier-1 members
    - to-be-purged dict of member, timestamp pairs

    @type path: string
    @type current_users: list of user login names

    @param path: path to the cache of purged or to be purged users
    @param current_users: VSC members who are entitled to compute on the Tier-1 at this point in time
    """

    now = time.time()
    cache = FileCache(path)

    purgees_undone = 0
    purgees_first_notify = 0
    purgees_second_notify = 0
    purgees_final_notify = 0
    purgees_begone = 0

    previous_users = cache.load('previous_users')
    if previous_users:
        (previous_users_timestamp, previous_users) = previous_users
    else:
        logger.warning("Purge cache has no previous_users")
        previous_users = []

    purgees = cache.load('purgees')
    if purgees:
        (purgees_timestamp, purgees) = purgees
    else:
        logger.warning("Purge cache has no purgees")
        purgees = dict()

    logger.info("Starting purge at time %s" % (now,))
    logger.debug("Previous users: %s", (previous_users,))
    logger.debug("Purgees: %s", (purgees,))

    # if a user is added again before his grace ran out, remove him from the purgee list and from the grace group
    purgees_undone = cleanup_purgees(current_users, purgees, client, dry_run)

    # warn those still on the purge list if needed
    for (user_id, (first_warning, second_warning, final_warning)) in purgees.items():
        logger.debug("Checking if we should warn %s at %d, time since purge: %d", user_id, now, now - first_warning)

        user = MukAccountpageUser(user_id, rest_client=client)
        user.dry_run = dry_run
        if now - first_warning > PURGE_DEADLINE_TIME:
            notify_user_of_purge(user, first_warning, now)
            purge_user(user, client)
            del purgees[user_id]
            purgees_begone += 1
            logger.info("Removed %s from the list of purgees - time's up " % (user_id, ))
        elif not second_warning and now - first_warning > PURGE_NOTIFICATION_TIMES[0]:
            notify_user_of_purge(user, first_warning, now)
            purgees[user_id] = (first_warning, now, None)
            purgees_second_notify += 1
            logger.info("Updated %s in the list of purgees with timestamps %s" % (user_id, (first_warning, now, None)))
        elif not final_warning and now - first_warning > PURGE_NOTIFICATION_TIMES[1]:
            notify_user_of_purge(user, first_warning, now)
            purgees[user_id] = (first_warning, second_warning, now)
            purgees_final_notify += 1
            logger.info("Updated %s in the list of purgees with timestamps %s" %
                        (user_id, (first_warning, second_warning, now)))
        else:
            logger.info("Time difference does not warrant sending a new mail already.")

    # add those that went to the other side and warn them
    purgees_first_notify = add_users_to_purgees(previous_users, current_users, purgees, now, client, dry_run)

    if not dry_run:
        cache.update('previous_users', current_users, 0)
        cache.update('purgees', purgees, 0)
        logger.info("Purge cache updated")
    else:
        logger.info("Dry run: not updating the purgees cache")

    cache.close()

    return {
        'purgees_undone': purgees_undone,
        'purgees_first_notify': purgees_first_notify,
        'purgees_second_notify': purgees_second_notify,
        'purgees_final_notify': purgees_final_notify,
        'purgees_begone': purgees_begone,
    }


def notify_user_of_purge(user, grace_time, current_time):
    """
    Send out a notification mail to the user letting him know he will be purged in n days or k hours.

    @type user: MukAccountpageUser
    """
    time_left = grace_time + PURGE_DEADLINE_TIME - current_time

    logger.debug("Time time_left for %s: %d seconds", user, time_left)

    if time_left < 0:
        time_left = 0
        left_unit = None
    if time_left <= 86400:
        time_left /= 3600
        left_unit = "hours"
    else:
        time_left /= 86400
        left_unit = "days"

    logger.info("Sending notification mail to %s - time time_left before purge %d %s" % (user, time_left, left_unit))
    if time_left:
        notify_purge(user, time_left, left_unit)
    else:
        notify_purge(user, None, None)


def notify_reinstatement(user):
    """
    Send out a mail notifying the user who was removed from grace and back to regular mode on muk.

    @type user: MukAccountpageUser
    """
    mail = VscMail(mail_host=UGENT_SMTP_ADDRESS)

    message = REINSTATEMENT_MESSAGE % ({'gecos': user.person.gecos,
                                        'tier1_helpdesk': TIER1_HELPDESK_ADDRESS,
                                        })
    mail_subject = "%(user_id)s VSC Tier-1 access reinstated" % ({'user_id': user.account.vsc_id})

    if user.dry_run:
        logger.info("Dry-run, would send the following message to %s: %s" % (user, message,))
    else:
        mail.sendTextMail(mail_to=user.account.email,
                          mail_from=TIER1_HELPDESK_ADDRESS,
                          reply_to=TIER1_HELPDESK_ADDRESS,
                          mail_subject=mail_subject,
                          message=message)
        logger.info("notification: recipient %s [%s] sent expiry mail with subject %s" %
                    (user.account.vsc_id, user.person.gecos, mail_subject))


def notify_purge(user, grace=0, grace_unit=""):
    """Send out the actual notification"""
    mail = VscMail(mail_host=UGENT_SMTP_ADDRESS)

    if grace:
        message = GRACE_MESSAGE % ({'gecos': user.person.gecos,
                                    'grace_time': "%d %s" % (grace, grace_unit),
                                    'tier1_helpdesk': TIER1_HELPDESK_ADDRESS,
                                    })
        mail_subject = "%s compute on the VSC Tier-1 entering grace period" % user.account.vsc_id

    else:
        message = FINAL_MESSAGE % ({'gecos': user.person.gecos,
                                    'tier1_helpdesk': TIER1_HELPDESK_ADDRESS,
                                    })
        mail_subject = "%(user_id)s compute time on the VSC Tier-1 expired" % ({'user_id': user.account.vsc_id})

    if user.dry_run:
        logger.info("Dry-run, would send the following message to %s: %s" % (user.account.vsc_id, message,))
    else:
        mail.sendTextMail(mail_to=user.account.email,
                          mail_from=TIER1_HELPDESK_ADDRESS,
                          reply_to=TIER1_HELPDESK_ADDRESS,
                          mail_subject=mail_subject,
                          message=message)
        logger.info("notification: recipient %s [%s] sent expiry mail with subject %s" %
                    (user.account.vsc_id, user.person.gecos, mail_subject))


def purge_user(user, client):
    """
    Really purge the user by removing the symlink to his home dir.
    """
    if not user.dry_run:
        logger.info("Purging %s" % (user.account.vsc_id,))
        # remove the user from the grace users
        group_name = user.get_institute_prefix() + TIER1_GRACE_GROUP_SUFFIX
        try:
            client.group[group_name].member[user.account.vsc_id].delete()
        except HTTPError, err:
            logging.error("Return code %d: could not remove %s from group %s [%s]",
                          err.code, user.account.vsc_id, group_name, err)
        else:
            logging.info("Account %s removed from group %s", user.account.vsc_id, group_name)

        user.cleanup_home_dir()
    else:
        logging.info("Dry-run: not removing user %s from grace group" % (user.account.vsc_id,))
        logging.info("Dry-run: not cleaning up home dir symlink for user %s" % (user.account.vsc_id))


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
        'purge-cache': ('Location of the cache with users that should be purged', None, 'store', None),
        'access_token': ('OAuth2 token identifying the user with the accountpage', None, 'store', None),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        muk = Muk()
        nfs_mounts = force_nfs_mounts(muk)
        logger.info("Forced NFS mounts")

        client = AccountpageClient(token=opts.options.access_token)

        muk_users_set = client.autogroup[muk.muk_user_group].get()[1]['members']
        logger.debug("Found the following Muk users: {users}".format(users=muk_users_set))

        for institute in nfs_mounts:

            (status, institute_users) = client.account.institute[institute].get()
            if status == 200:
                muk_institute_users = set([u['vsc_id'] for u in institute_users]).intersection(muk_users_set)
                users_ok = process_institute(opts.options, institute, muk_institute_users, client)
            else:
                # not sure what to do here.
                continue

            total_institute_users = len(muk_institute_users)
            stats["%s_users_sync" % (institute,)] = users_ok.get('ok', 0)
            # 20% of all users want to get on
            stats["%s_users_sync_warning" % (institute,)] = int(total_institute_users / 5)
            # 30% of all users want to get on
            stats["%s_users_sync_critical" % (institute,)] = int(total_institute_users / 2)
            stats["%s_users_sync_fail" % (institute,)] = users_ok.get('fail', 0)
            stats["%s_users_sync_fail_warning" % (institute,)] = users_ok.get('fail', 0)
            stats["%s_users_sync_fail_warning" % (institute,)] = 1
            stats["%s_users_sync_fail_critical" % (institute,)] = 3

        purgees_stats = purge_obsolete_symlinks(opts.options.purge_cache, muk_users_set, client, opts.options.dry_run)
        stats.update(purgees_stats)

    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("Muk users synchronisation completed", stats)


if __name__ == '__main__':
    main()
