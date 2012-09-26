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
__date__ = 'Apr 24, 2012'

## STUFF TO PAY ATTENTION TO
##
## LDAP: use the right one, since there are three: VSC, UGent, and replicas on the masters

import re
from lockfile.pidlockfile import PIDLockFile

import vsc.fancylogger as fancylogger
from vsc.ldap import *

from vsc.administration.group import Group
from vsc.administration.institute import Institute

logger = fancylogger.getLogger(__name__)

class Vo(Group):
    """Class representing a VO in the VSC administrative library.

    A VO is a special kind of group.
    """
    def __init__(self, institute_name):
        super(Vo, self).__init__(institute_name)
        self.logger = fancylogger.getLogger(self.__class__.__name__)
        self.USER_LOCKFILE_NAME = "/tmp/lock.%s.pid" % (self.__class__.__name__)
        self.lockfile = PIDLockFile(self.USER_LOCKFILE_NAME)

        self.description = None
        self.fairshare = None
        self.members = None

    def __setup(self, ldap_attributes):
        """Fill in the instance values from a retrieved VO.

        @type ldap_attributes: dictionary with the required VO attributes as keys.

        Sets self.exists to True if the data is filled in.
        """
        if ldap_attributes is not None:
            self.cn = ldap_attributes['cn'],
            self.description = ldap_attributes['description'],
            self.gid_number = ldap_attributes['gidNumber'],
            self.institute = Institute(ldap_attributes['institute']),
            self.fairshare = ldap_attributes['fairshare'],
            self.moderators = ldap_attributes['moderator'],
            self.members = ldap_attributes.get('memberUid', [])
            self.exists = True
        else:
            self.exists = False

    def load(self, vo_name):
        """Load the VO data from the LDAP.

        @type vo_name: string representing a VO in the VSC.

        @raise NoSuchInstituteError, NoSuchVoError
        """
        vo = self.ldap_query.vo_filter_search("(&(cn=%s) (institute=%s))" % (vo_name, self.institute.institute_name))
        if not vo:
            self.exists = False
            raise NoSuchVoError(vo_name)
        self.__setup(vo[0])
        return self

    def __generate_name(self):
        """Generate a name for a new VO.

        - The generated name depends on the institute.
        - This function is not protected by a lock, so make sure you protect in
          upwards in the call stack.

        @returns: a string representing the name of a new VO.
        """
        vos = self.ldap_query.vo_filter_search("(institute=%s)" % (self.institute.institute_name))
        if vos is None:
            highest = 0
        else:
            vo_number_regex = re.compile('.vo(?P<number>\d+)')
            vo_numbers = [vo_number_regex.search(v).groups('number')[0] for v in vos]
            highest = int(max(vo_numbers))

        highest += 1
        name = self.institute.institute_name[0] + 'vo' + '0' * (5-len(str(highest))) + str(highest)

        logger.info("Generated new VO name: %s" % (name))
        return name

    def add(self, moderator_uid, description):
        """Add a VO to the VSC LDAP database.

        Note that the VO name is automagically generated

        @type moderator_uid: string representing the user ID on the VSC of a VO moderator
        @type description: description of the VO

        @raise: NoSuchInstitute
        """
        vo_group_id = self.get_next_group_id(self.institute.institute_name)
        vo_name = self.__generate_name()
        fairshare = "100"
        attributes = {
            'objectclass': [ 'top', 'posixGroup', 'vscgroup' ],
            'cn': vo_name,
            'description': description,
            'gidNumber': str(vo_group_id),
            'institute': self.institute.institute_name,
            'fairshare': fairshare,
            'moderator': moderator_uid,
            'status': self.ldap_query.ldap.vsc.defaults['new_user_status']
        }
        self.ldap_query.group_add(vo_name, attributes)
        self.__setup(attributes)

    def add_member(self, member_uid):
        """Add a member to a VO.

        @type member_uid: the user id of the user on the VSC

        @returns: The user_id if the user was added. None otherwise.
        """
        if not self.exists:
            self.logger.error("%s.add_member: trying to add %s to non-initialised group" % (self.__class__.__name__, member_uid))
            return None

        # first remove the member from the default VO unless we're the default VO
        (default_vo_name, _) = self.institute.get_default_vo_admin_info()
        if self.cn != default_vo_name:
            default_vo = Vo(self.institute).load(default_vo_name)
            default_vo.delete_member(member_uid)

        super(Vo, self).add_member(member_uid)

    def delete_member(self, member_uid):
        """Remove a user from a VO.

        @type member_uid: string representing the id of a VSC member
        """
        if not self.exists:
            self.logger.error("%s.delete_member: trying to delete %s from non-initialised group" % (self.__class__.__name__, member_uid))
            return None

        super(Vo, self).delete_member(member_uid)

        # move the member back to the default VO.
        (default_vo_name, _) = self.institute.get_default_vo_admin_info()
        if self.cn != default_vo_name:
            default_vo = Vo(self.institute).load(default_vo_name)
            default_vo.add_member(member_uid)


    def add_moderator(self, member_uid):
        """Add the user with given ID to the moderator set for the given VO.

        Note that checking if the user actually exists (in the institute the
        VO belongs to!) is the responsibility of the calling code.

        @type member_uid: string representing the user id on the VSC

        @returns: the member_uid if the member has been added as a moderator, None otherwise.
        """
        if not self.exists:
            return None

        # there certainly is at least one VO the user belongs to (e.g., the default VO),
        # and there should be no more than one
        (default_vo_name, _) = self.institute.get_default_vo_admin_info()
        vos = self.ldap_query.vo_filter_search("(|(memberUid=%s) (moderator=%s))" % (member_uid, member_uid), attributes=['cn'])
        for v in vos:
            if v['cn'] == self.cn or v['cn'] == default_vo_name:
                return None

        # if the member is not yet a member of this VO, add him
        # this will also remove the member from the default VO
        self.add_member(member_uid)
        if not member_uid in self.moderators:
            moderators = self.moderators + [member_uid]
            self.ldap_query.group_modify(self.cn, { 'moderator' : moderators})
            self.moderators = moderators
        return self

    def delete_moderator(self, member_uid):
        """Remove the given user as a moderator for this VO.

        @type member_uid: string representing the member on the VSC

        @raise: UserDoesNotExist
        """
        if not self.exists:
            self.logger.error("%s.delete_moderator: trying to delete %s from non-initialised vo" % (self.__class__.__name__, member_uid))
            return None

        if not member_uid in self.moderators:
            raise NoSuchVoModeratorError(self.cn, member_uid)

        moderators = self.moderators
        moderators.remove(member_uid)
        ## safety check!
        if not moderators:
            return None
        else:
            self.ldap_query.group_modify(self.cn, { 'moderator': moderators })
            self.moderators = moderators
            return self

    def modify_quota(self, data_quota=None, scratch_quota=None):
        """Change the quota for the given VO.

        If there have been no quota set for the VO, they are
        added to the LDAP entry.

        @type data_quota: integer indicating the quota on the data filesystem
        @type scratch_quota: integer indicating the quota on the scratch filesystem
        """
        if not self.exists:
            self.logger.error("%s.modify_quota: trying to change quota (%d, %d) to non-initialised vo" % (self.__class__.__name__, data_quota, scratch_quota))
            return None

        self.logger.info("Changing quota for vo %s to %d [data] and %d [scratch]" % (self.cn, data_quota, scratch_quota))

        if data_quota is not None:
            self.__modify_quota('dataDirectory', self.ldap_query.ldap.vsc.vo_pathnames()['data'], 'dataQuota', data_quota)

        if scratch_quota is not None:
            self.__modify_quota('scratchDirectory', paths['scratch'], 'scratchQuota', scratch_quota)

    def __modify_quota(self, path_name, path, quota_name, quota):
        """Change the quota settings in the LDAP entry for the given VO.

        If there have been no quota set, add the required entries.

        @type path_name: string representing the name of the entry in both the LDAP and the VO class instance
        @type path: string representing the location on which the quota should be placed
        @type quota_name: string representing the name of the entry in both the LDAP and the VO class instance
        @type quota: integer representing the quota
        """
        self.ldap_query.group_modify(self.cn, { path_name: path })
        self.__dict__[path_name] = path
        self.ldap_query.group_modify(self.cn, { quota_name: quota})
        self.__dict__[quota_name] = quota


