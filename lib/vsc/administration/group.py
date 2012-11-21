#!/usr/bin/env python
##
#
# Copyright 2012 Ghent University
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
"""

## STUFF TO PAY ATTENTION TO
##
## LDAP: use the right one, since there are three: VSC, UGent, and replicas on the masters

from lockfile.pidlockfile import PIDLockFile

import vsc.fancylogger as fancylogger

from vsc.administration.institute import Institute
from vsc.config.base import VSC
from vsc.ldap.utils import LdapQuery
from vsc.ldap.entities import VscLdapGroup

logger = fancylogger.getLogger(__name__)


class Group(VscLdapGroup):
    """Class representing a group in the VSC administration library.

    All groups have the PosixGroup, vscgroup object classes.

    This is a general base class for any group type we have in the VSC:
        - User groups corresponding to single users in the people subtree
        - VO groups corresponding to a VO in the VO subtree
        - Project groups corresponding a project in the projects subtree
    """
    lockfile = PIDLockFile("/tmp/lock.%s.pid" % ('vsc.administration.group.Group'))

    def __init__(self, group_id):
        """Initialisation

        @type group_id: string representing the VSC group ID in the HPC LDAP database. The group data is loaded
                            lazily, when first required as per the LdapGroup functionality.
        """
        super(Group, self).__init__(group_id)

    @classmethod
    def lock(cls):
        """Take a global lock to avoid other instances from messing things up."""
        cls.LOCKFILE.acquire()

    @classmethod
    def unlock(cls):
        """Release the global lock."""
        cls.LOCKFILE.release()

    @classmethod
    def add(cls, institute, group_name, moderator_name):
        """Add a group to the LDAP database.

        @type group_name: string representing the name of a group in the LDAP database.
        @type moderator_name: name of the moderator in the VSC

        @returns: the newly added group.
        """
        vsc = VSC()
        if not institute in vsc.institutes:
            log.raiseException("Institute %s does not exist in the VSC." % (institute), NoSuchInstituteError)

        try:
            cls.lock()

            cn_filter = CnFilter(group_name)
            groups = LdapQuery(VscConfiguration()).group_filter_search(cn_filter)

            if len(groups) > 0:
                log.raiseException("Group %s already exists" % (group_name))

            group = VscGroup(None)

            #FIXME: this should preferably come from a more reliable source, such as the accounts DB
            group_number = cls.get_next_group_id(institute)

            attributes = {
                'objectclass': ['top', 'posixGroup', 'vscgroup'],
                'cn': group_name,                                   # posixGroup
                'gidNumber': str(group_number),                     # posixGroup
                'institute': institute,                             # vscgroup
                'moderator': moderator_name,                        # vscgroup
                'status': vsc.defaults['new_user_status']           # vscgroup
            }

            group.add(attributes)
            return group
        finally:
            cls.unlock()

    def add_member(self, member_uid):
        """Add a member to a group.

        @type member_uid: the user id of the user on the VSC (i.e., vscXYZUV)

        @returns: The user_id if the user was added. None otherwise.
        """

        self.lock()

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

    @classmethod
    def get_next_group_id(cls, institute):
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


