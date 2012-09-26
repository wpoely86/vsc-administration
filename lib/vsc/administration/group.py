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
This file contains the utilities for dealing with VOs on the VSC.
Original Perl code by Stijn De Weirdt

@author Andy Georges

@created Apr 24, 2012
"""

__author__ = 'ageorges'
__date__ = 'Apr 26, 2012'

## STUFF TO PAY ATTENTION TO
##
## LDAP: use the right one, since there are three: VSC, UGent, and replicas on the masters

from lockfile.pidlockfile import PIDLockFile

import vsc.fancylogger as fancylogger
from vsc.ldap import *

from vsc.administration.institute import Institute
from vsc.ldap.utils import LdapQuery
from vsc.ldap.group import LdapGroup

logger = fancylogger.getLogger(__name__)


class GroupBase(LdapGroup):
    """Class representing a group in the VSC administration library.
    """
    def __init__(self, vsc_group_id):
        """Initialisation

        @type vsc_group_id: string representing the VSC group ID in the HPC LDAP database. The group data is loaded
                            lazily, when first required as per the LdapGroup functionality.
        """
        super(GroupBase, self).__init__(vsc_group_id)

        # FIXME: unsure if needed
        self.GROUP_LOCKFILE_NAME = "/tmp/lock.%s.pid" % (self.__class__.__name__)
        self.lockfile = PIDLockFile(self.GROUP_LOCKFILE_NAME)

    @staticmethod
    def add(self, group_name, moderator_name):
        """Add a group to the LDAP database.

        @type group_name: string representing the name of a group in the LDAP database.
        @type moderator_name: name of the moderator in the VSC

        @returns:
        """
        group = self.ldap_query.group_filter_search("(&(cn=%s) (institute=%s))" % (group_name, self.institute.institute_name))
        if group:
            ## FIXME: should we load the group data here?
            self.logger.error("%s.add: trying to add group %s that already exists" % (self.__class__.__name__, group_name))
            raise GroupAlreadyExistsError(group_name)

        group_number = self.get_next_group_id(institute=self.institute.institute_name)

        vsc = self.ldap_query.ldap.vsc
        attributes = {
            'objectclass': ['top', 'posixGroup', 'vscgroup'],
            'cn': group_name,
            'gidNumber': str(group_number),
            'institute': self.institute.institute_name,
            'moderator': moderator_name,
            'status': vsc.defaults['new_user_status']
        }

        self.ldap_query.group_add(cn=group_name, attributes=attributes)
        self.__setup(attributes)
        return self

    def add_member(self, member_uid):
        """Add a member to a group.

        @type member_uid: the user id of the user on the VSC

        @returns: The user_id if the user was added. None otherwise.
        """
        if not self.exists:
            return None

        # check if the user already belongs to this group
        # FIXME: should this throw an error? It is not really an error, but we should have some indication upstack.
        if member_uid in self.members:
            return None

        # modify will REPLACE the current data with the new, so make sure we retain existing members :-)
        result = self.ldap_query.group_modify(self.cn, {'memberUid': self.members + [member_uid] })
        if result:
            self.members += [member_uid]
            return self
        else:
            return None

    def delete_member(self, member_uid):
        """Remove a user from a group.

        @type member_uid: the VSC member login (vscXXXXX)
        """
        if not self.exists:
            self.logger.error("%s.delete_member: trying to delete %s from non-initialised group" % (self.__class__.__name__, member_uid))
            return None

        if not member_uid in self.members:
            return None

        members = self.members
        members.remove(member_uid)
        result = self.ldap_query.group_modify(self.cn, {'memberUid': members})
        if result:
            self.members = members
            return self
        else:
            return None

    def get_next_group_id(self, institute="vsc"):
        """Determine the next available group ID for a user at the given institute.

        This function needs to acquire a global lock, we cannot risk two groups
        ending up with the same ID.

        @type institute: a string representing the name of the institue

        @returns: an integer representing the ID

        @raise: NoAvailableGroupID
        """
        # FIXME: this is a pretty inefficient way to get the next gid. Also, needs a lock someplace.
        self.lockfile.acquire()
        if not self.ldap_query.ldap.vsc.user_extra_gid_institute_map.has_key(institute):
            self.lockfile.release()
            raise NoSuchInstituteError(institute)

        ## FIXME: we may have different non-adjacent ranges in the future
        minimum_gid = self.ldap_query.ldap.vsc.user_extra_gid_institute_map[institute][0]
        maximum_gid = minimum_gid + self.ldap_query.ldap.vsc.user_extra_gid_range

        groups = self.ldap_query.group_filter_search(filter="(institute=%s)" % (institute),
                                                     attributes=['gidNumber'])

        gids = [int(g['gidNumber']) for g in groups]
        current_max_gid = max(max(gids), minimum_gid)  # GIDs are > UIDs (and this user GIDs)

        new_gid = current_max_gid + 1
        if new_gid > maximum_gid:
            self.lockfile.release()
            raise NoAvailableGroupId(new_gid, maximum_gid)

        self.lockfile.release()
        return new_gid


