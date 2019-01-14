# -*- coding: latin-1 -*-
#
# Copyright 2012-2019 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
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
