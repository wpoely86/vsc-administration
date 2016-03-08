# -*- coding: latin-1 -*-
#
# Copyright 2012-2016 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
This file contains the tools for automated administration w.r.t. the VSC
Original Perl code by Stijn De Weirdt

@author: Andy Georges (Ghent University)
"""

import logging
import os
import stat

from lockfile.pidlockfile import PIDLockFile
from urllib2 import HTTPError

from vsc.utils import fancylogger
from vsc.utils.mail import VscMail


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

logger = fancylogger.getLogger(__name__)
mailer = VscMail()


def create_stat_directory(path, permissions, uid, gid, posix, override_permissions=True):
    """
    Create a new directory if it does not exist and set permissions, ownership. Otherwise,
    check the permissions and ownership and change if needed.
    """

    created = False
    try:
        statinfo = os.stat(path)
        logging.debug("Path %s found.", path)
    except OSError:
        created = posix.make_dir(path)
        logging.info("Created directory at %s" % (path,))

    if created or (override_permissions and stat.S_IMODE(statinfo.st_mode) != permissions):
        posix.chmod(permissions, path)
        logging.info("Permissions changed for path %s to %s", path, permissions)
    else:
        logging.debug("Path %s already exists with correct permissions" % (path,))

    if created or statinfo.st_uid != uid or statinfo.st_gid != gid:
        posix.chown(uid, gid, path)
        logging.info("Ownership changed for path %s to %d, %d", path, uid, gid)
    else:
        logging.debug("Path %s already exists with correct ownership" % (path,))

    return created


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


def cleanup_purgees(current_users, purgees, client, dry_run):
    """
    Remove users from purgees if they are in the current users list.
    """
    from vsc.administration.user import MukAccountpageUser
    purgees_undone = 0
    for user_id in current_users:
        logger.debug("Checking if %s is in purgees", (user_id,))
        if user_id in purgees:
            purgees.remove(user_id)
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
