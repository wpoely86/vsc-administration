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
import time

from vsc.administration.group import Group
from vsc.administration.user import MukUser
from vsc.config.base import Muk, ANTWERPEN, BRUSSEL, GENT, LEUVEN
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.filters import CnFilter, InstituteFilter, LdapFilter
from vsc.ldap.utils import LdapQuery
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

def cleanup_purgees(current_users, purgees, dry_run):
    """
    Remove users from purgees if they are in the current users list.
    """
    purgees_undone = 0
    for user in current_users:
        logger.debug("Checking if %s is in purgees", (user,))
        if user in purgees:
            del purgees[user]
            purgees_undone += 1
            logger.info("Removed %s from the list of purgees: found in list of current users" % (user,))
            notify_reinstatement(MukUser(user), dry_run)

    return purgees_undone


def add_users_to_purgees(previous_users, current_users, purgees, now, dry_run):
    """
    Add the users that are out to the purgees
    """
    purgees_first_notify = 0
    for user in previous_users:
        if not user in current_users and not user in purgees:
            notify_user_of_purge(MukUser(user), now, now, dry_run)
            purgees[user] = (now, None, None)
            purgees_first_notify += 1
            logger.info("Added %s to the list of purgees with timestamp %s" % (user, (now, None, None)))
        else:
            logger.info("User %s in both previous users and current users lists, not eligible for purge." % (user,))

    return purgees_first_notify



def purge_obsolete_symlinks(path, current_users, dry_run):
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
    if not previous_users:
        logger.warning("Purge cache has no previous_users")
        previous_users = []
        previous_users_timestamp = now
    else:
        (previous_users_timestamp, previous_users) = previous_users

    purgees = cache.load('purgees')
    if not purgees:
        logger.warning("Purge cache has no purgees")
        purgees = dict()
        purgees_timestamp = now
    else:
        (purgees_timestamp, purgees) = purgees

    logger.info("Starting purge at time %s" % (now,))
    logger.debug("Previous users: %s", (previous_users,))
    logger.debug("Purgees: %s", (purgees,))

    # if a user is added again before his grace ran out, remove him from the purgee list
    purgees_undone = cleanup_purgees(current_users, purgees, dry_run)

    # warn those still on the purge list if needed
    for (user, (first_warning, second_warning, final_warning)) in purgees.items():
        logger.debug("Checking if we should warn %s at %d, time since purge entry %d", user, now, now - first_warning)

        if now - first_warning > PURGE_DEADLINE_TIME:
            m_user = MukUser(user)
            notify_user_of_purge(m_user, first_warning, now, dry_run)
            purge_user(m_user, dry_run)
            del purgees[user]
            purgees_begone += 1
            logger.info("Removed %s from the list of purgees - time's up " % (user, ))

            continue

        if not second_warning and now - first_warning > PURGE_NOTIFICATION_TIMES[0]:
            notify_user_of_purge(MukUser(user), first_warning, now, dry_run)
            purgees[user] = (first_warning, now, None)
            purgees_second_notify += 1
            logger.info("Updated %s in the list of purgees with timestamps %s" % (user, (first_warning, now, None)))
        elif not final_warning and now - first_warning > PURGE_NOTIFICATION_TIMES[1]:
            notify_user_of_purge(MukUser(user), first_warning, now, dry_run)
            purgees[user] = (first_warning, second_warning, now)
            purgees_final_notify += 1
            logger.info("Updated %s in the list of purgees with timestamps %s" % (user, (first_warning, second_warning,
                                                                                         now)))

    # add those that went to the other side and warn them
    purgees_first_notify = add_users_to_purgees(previous_users, current_users, purgees, now, dry_run)
    cache.update('previous_users', current_users, 0)
    cache.update('purgees', purgees, 0)
    cache.close()

    logger.info("Purge cache updated")

    return {
        'purgees_undone': purgees_undone,
        'purgees_first_notify': purgees_first_notify,
        'purgees_second_notify' :purgees_second_notify,
        'purgees_final_notify': purgees_final_notify,
        'purgees_begone': purgees_begone,
    }


def notify_user_of_purge(user, grace_time, current_time, dry_run):
    """
    Send out a notification mail to the user letting him know he will be purged in n days or k hours.
    """
    left = grace_time + PURGE_DEADLINE_TIME - current_time

    logger.debug("Time left for %s: %d seconds", user, left)

    if left < 0:
        left = 0
        left_unit = None
    if left <= 86400:
        left /= 3600
        left_unit = "hours"
    else:
        left /= 86400
        left_unit = "days"

    logger.info("Sending notification mail to %s - time left before purge %d %s" % (user, left, left_unit))
    if left:
        notify_purge(user, left, left_unit, dry_run)
    else:
        notify_purge(user, None, None, dry_run)


def notify_reinstatement(user, dry_run):
    """
    Send out a mail notifying the user he was removed from grace and back to regular mode on muk.
    """
    mail = VscMail(mail_host="smtp.ugent.be")

    message = """
Dear %(gecos)s,

You have been reinstated to regular status on the VSC Tier-1 cluster at Ghent. This means you can
again submit jobs to the scheduler.

Should you have any questions, please contact us at hpc@ugent.be or reply to
this email which will open a ticket in our helpdesk system for you.

Kind regards,
-- The UGent HPC team
""" % ({'gecos': user.gecos,})
    mail_subject = "%(user_id)s VSC Tier-1 access reinstated" % ({'user_id': user.cn})

    if dry_run:
        logger.info("Dry-run, would send the following message to %s: %s" % (user, message,))
    else:
        mail.sendTextMail(mail_to=user.mail,
                          mail_from="hpc@ugent.be",
                          reply_to="hpc@ugent.be",
                          mail_subject=mail_subject,
                          message=message)
        logger.info("notification: recipient %s [%s] sent expiry mail with subject %s" %
                    (user.cn, user.gecos, mail_subject))



def notify_purge(user, grace=0, grace_unit="", dry_run=True):
    """Send out the actual notification"""
    mail = VscMail(mail_host="smtp.ugent.be")

    if grace:
        message = """
Dear %(gecos)s,

Your allocated compute time on the VSC Tier-1 cluster at Ghent has expired.
You are now in a grace state for the next %(grace_time)s.  This means you can
no longer submit new jobs to the scheduler.  Jobs running at this moment will
not be killed and should likely finish.

Please make sure you copy back all required results from the dedicated
$VSC_SCRATCH storage on the Tier-1 to your home institution's long term
storage, since you will no longer be able to log in to this machine once
the grace period expires.

Should you have any questions, please contact us at hpc@ugent.be or reply to
this email which will open a ticket in our helpdesk system for you.

Kind regards,
-- The UGent HPC team
""" % ({'gecos': user.gecos,
        'grace_time': "%d %s" % (grace, grace_unit),
        })
        mail_subject = "%(user_id)s compute on the VSC Tier-1 entering grace period" % ({'user_id': user.cn})

    else:
        message = """
Dear %(gecos)s,

The grace period for your compute time on the VSC Tier-1 cluster at Ghent
has expired.  From this point on, you will not be able to log in to the
machine anymore, nor will you be able to reach its dedicated $VSC_SCRATCH
storage.

Should you have any questions, please contact us at hpc@ugent.be or reply to
this email which will open a ticket in our helpdesk system for you.

Kind regards,
-- The UGent HPC team
""" % ({'gecos': user.gecos,
        })
        mail_subject = "%(user_id)s compute time on the VSC Tier-1 expired" % ({'user_id': user.cn})

    if dry_run:
        logger.info("Dry-run, would send the following message to %s: %s" % (user, message,))
    else:
        mail.sendTextMail(mail_to=user.mail,
                          mail_from="hpc@ugent.be",
                          reply_to="hpc@ugent.be",
                          mail_subject=mail_subject,
                          message=message)
        logger.info("notification: recipient %s [%s] sent expiry mail with subject %s" %
                    (user.cn, user.gecos, mail_subject))


def purge_user(user, dry_run):
    """
    Really purge the user by removing the symlink to his home dir.
    """
    logger.info("Purging %s" % (user.cn,))
    if dry_run:
        user.dry_run = True
    user.cleanup_home_dir()


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
            stats["%s_users_sync" % (institute,)] = users_ok.get('ok',0)
            stats["%s_users_sync_warning" % (institute,)] = int(total_institute_users / 5)  # 20% of all users want to get on
            stats["%s_users_sync_critical" % (institute,)] = int(total_institute_users / 2)  # 30% of all users want to get on
            stats["%s_users_sync_fail" % (institute,)] = users_ok.get('fail',0)
            stats["%s_users_sync_fail_warning" % (institute,)] = users_ok.get('fail',0)
            stats["%s_users_sync_fail_warning" % (institute,)] = 1
            stats["%s_users_sync_fail_critical" % (institute,)] = 3

        purgees_stats = purge_obsolete_symlinks(opts.options.purge_cache, [u.cn for u in muk_users], opts.options.dry_run)
        stats.update(purgees_stats)

    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("Muk users synchronisation completed", stats)


if __name__ == '__main__':
    main()
