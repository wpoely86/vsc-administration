#!/usr/bin/env python
##
#
# Copyright 2012 Andy Georges
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
This file contains the tools for automated administration w.r.t. the VSC
Original Perl code by Stijn De Weirdt

@author Andy Georges

@created May 8, 2012
"""

__author__ = 'ageorges'
__date__ = 'May 8, 2012'

import logging
import os

from lockfile.pidlockfile import PIDLockFile
from vsc import fancylogger
from vsc.util.mail import VscMail

from vsc.administration.group import Group
from vsc.administration.user import User
from vsc.administration.vo import Vo
from vsc.administration.institute import Institute

logger = fancylogger.getLogger(__name__)
logger.setLevel(logging.DEBUG)

mailer = VscMail()


def add_group(group_name, institute_name, user_names):
    """Original code add_group.pl. Adds a group and fills it with the given users.

    @type group_name: string that matches the rules for a group name.
    @type insititute_name: string that represents an existing institute in the VSC.
    @type user_names: list of strings with VSC user names.
    """
    if not user_names:
        return None

    group = Group(institute_name)

    #the first user given automagically becomes the moderator for the new group.
    moderator_name = user_names[0]

    moderator = User().load(moderator_name, institute_name)
    if not moderator.exists:
        ## FIXME: raise error
        return None

    group.add(group_name, moderator_name)

    for user_name in user_names:
        user = User().load(user_name, institute_name)
        if user.exists:
            group.add_member(user_name)



def process_new_users(institute_name):
    # FIXME: Needs to use the new VscUser class.
    """Original code new_user.pl. Checks the LDAP for new users and sets up their stuff.

    - Only check users belonging to the host institute?
    - See if we can remap the directories to someplace root can write?
        * Not necessary, since we are going to run this where root can write to the filesystem
          and we're going to use numerical IDs since the user logins are potentially not known
          where we run the scripts.
    - Must be protected by a lock
    """
    institute = Institute(institute_name)
    new_users = institute.get_new_users()

    for user in new_users:
        try:
            set_up_user_home(user)
            user.modify_status('notify_user_status')

        ## FIXME: there's a post-add phase that's still missing.

        except Exception, err:
            ## FIXME: we need to clean up the mess and report the error
            pass


def process_new_vos(institute_name):
    # FIXME: Needs to use the new VscVO class.
    """Original code new_vo.pl. Checks the LDAP for new VOs and sets up their stuff.

    - Only check for VOs belonging to the host institute
    - Do not forget about GOLD/MAW, whichever we're going to use
    """
    institute = Institute(institute_name)
    new_vos = institute.get_new_vos()

    for vo in new_vos:
        ## GOLD!!
        vo.modify_status('notify_user_status')


def reject_user(user, message=None):
    """Send the rejection mail to the user.
    """
    text = "\n".join([
        "Dear %s," % ()
    ])



