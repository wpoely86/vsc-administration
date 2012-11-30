#!/usr/bin/env python
##
#
# Copyright 2009-2012 Ghent University
# Copyright 2009-2012 Stijn De Weirdt
# Copyright 2012 Andy Georges
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
This file contains the utilities for dealing with institutes on the VSC.
Original Perl code by Stijn De Weirdt

The following functionality is available for institutes:
- rebuild_default_vo:
    - checks for all users if they belong to a non-default VO.
    - adds users without non-default VO to the default
    - removes users from the default if they belong to a non-default
- create_default_vo: set up the default VO for an institute
- get_default_vo_admin_info: get the admin information for the default VO
- get_external_default_vo_admin_info: get the admin information for a default VO
                                      from an external institute
"""

## STUFF TO PAY ATTENTION TO
##
## LDAP: use the right one, since there are three: VSC, UGent, and replicas on the masters

from lockfile.pidlockfile import PIDLockFile
import vsc.fancylogger as fancylogger

from vsc.ldap.utils import LdapQuery

logger = fancylogger.getLogger(__name__)


class Institute(object):
    """Class representing an institute in the VSC administration library.

    FIXME: Maybe requires a layer in between to avoid using LdapQuery directly?
    """
    #LOCKFILE_NAME = "/tmp/lock.%s.pid" % (__name__)
    #LOCKFILE = PIDLockFile(Institute.LOCKFILE_NAME)

    def __init__(self, institute_name):
        self.logger = fancylogger.getLogger(self.__class__.__name__)
        self.ldap_query = LdapQuery()

        if not institute_name in self.ldap_query.ldap.vsc.institutes:
            self.logger.error("Failed to initialise Intitute instance with institute = %s" % (institute_name))
            raise NoSuchInstituteError(institute_name)
        self.institute_name = institute_name

    @classmethod
    def lock(cls):
        pass

    @classmethod
    def unlock(cls):
        pass

    def rebuild_default_vo(self):
        """Rebuild the default VO for the given institute.

        The default VO holds the users that are not assigned to any other VO, since a user cannot be a member of
        more than one VO at the same time.

        This has the side effect of creating a default VO if none exists.

        @raise InstituteDoesNotExistError, CreateVoError
        """
        default_vo_name = None
        try:
            (default_vo_name, _) = self.get_default_vo_admin_info()
        except NoSuchInstituteError, err:
            ## This should NEVER happen!
            logger.error("Rebuilding default VO failed: no such institute: %s [%s]" % (self.institute_name, err))
            raise
        except (NoVoAdminForInstituteError, NoDefaultVoForInstituteError), _:
            try:
                self.create_default_vo()
            except Exception, err:
                logger.error("Could not create default VO for institute: %s [%s]" % (self.institute_name, err))
                raise CreateVoError(err)

        # first, get all known VOs and account for their users
        all_vos = self.ldap_query.vo_filter_search("(institute=%s)" % (self.institute_name))

        default_vo_members = set()
        other_vo_members = set()

        for vo in all_vos:
            vo_name = vo['cn']
            vo_member_uids = vo['memberUid']
            if vo_name == default_vo_name:
                default_vo_members += vo_member_uids
            else:
                other_vo_members += vo_member_uids

        # second, get all users and see if there are any not in some VO list
        all_member_uids = [u['cn'] for u in self.ldap_query.user_filter_search(filter="(institute=%s)" % (self.institute_name), attributes=['cn'])]

        # FIXME: fugly!
        from vsc.administration.vo import Vo
        default_vo = Vo(default_vo_name)
        for member_uid in all_member_uids:
            if member_uid in other_vo_members:
                if member_uid in default_vo_members:
                    default_vo.delete_member(member_uid)
                    logger.info("Deleted member %s from the default VO" % (member_uid))
            else:
                if member_uid not in default_vo_members:
                    default_vo.add_member(member_uid)
                    logger.info("Added member %s to the default VO" % (member_uid))

    def create_default_vo(self):
        pass

    def get_new_users(self):
        """Find the users that still have the new status for the institute.

        @returns: list of users with the new status.
        """
        # FIXME: fugly!
        from vsc.administration.user import User
        all_member_uids = self.ldap_query.user_filter_search(filter="(&(institute=%s) (status=%s))" % (self.institute_name, self.ldap_query.ldap.vsc.defaults['new_user_status']))
        return [User().load(m['cn'], self.institute_name) for m in all_member_uids]

    def get_default_vo_admin_info(self):
        """Get the default VO and its admin for the given institute.

        @returns: a tuple with the default VO name and it's admin name

        @raise InstituteDoesNotExistError, InstituteHasNoVoAdmin, InstituteHasNoDefaultVo
        """
        ## FIXME: this does not seem to belong here. Maybe put in an institute module?

        if not self.institute_name in self.ldap_query.ldap.vsc.institutes:
            ## This should never happen!
            raise NoSuchInstituteError(self.institute_name)

        # FIXME: this is not the cleanest solution, I think, but to avoid circular references ...
        vo_admin = self.ldap_query.user_filter_search( filter="(&(instituteLogin=voadmin) (institute=%s))" % (self.institute_name)
                                                     , attributes=['cn'])

        if not vo_admin:
            raise NoVoAdminForInstituteError(self.institute_name)

        vo_admin_name = vo_admin[0]['cn']

        # FIXME: this might yield the wrong results, since there was a base as well in the Perl code.
        default_vo = self.ldap_query.vo_filter_search( filter="(&(moderator=%s) (description=DEFAULT))" % vo_admin_name
                                                        , attributes=['cn'])

        if not default_vo:
            raise NoDefaultVoForInstituteError(self.institute_name)

        return (default_vo[0]['cn'], vo_admin_name)


    def get_external_default_vo_admin_info(self, host_institute, external_institute):
        """Get the default VO and its admin for the given external institute.

        @type host_institute: string representing the institute that is hosting the VSC accounts
        @type external_institute: string representing the institute for which we want the VSC accounts

        @returns: a tuple with the VO name and it's admin name

        @raise InstituteDoesNotExistError, InstituteHasNoVoAdmin, InstituteHasNoDefaultVo
        """
        ## FIXME: not clear at this point how to decently implement this functionality

        if not external_institute in self.ldap_query.ldap.vsc.institutes:
            raise NoSuchInstituteError(external_institute)

        if not host_institute in self.ldap_query.ldap.vsc.institutes:
            raise NoSuchInstituteError(host_institute)

        ## Get the VO admin name for the host institute
        vo_admin = self.ldap_query.user_filter_search( filter="(&(instituteLogin=vodamin) (institute=%s))" % (host_institute)
                                                     , attributes=['cn]'])

        if vo_admin is None:
            raise NoVoAdminForInstituteError(host_institute)

        vo_admin_name = vo_admin[0]['cn']

        # FIXME: this might yield the wrong results, since there was a base as well in the Perl code.
        # The host VO admin is also the VO admin for external institute VOs
        external_default_vo = self.ldap_query.vo_filter_search( filter="(&(moderator=%s) (description=DEFAULT%s))" % (vo_admin_name, external_institute.upper())
                                                              , attributes=['cn'])

        if external_default_vo is None:
            raise NoDefaultVoForInstituteError(external_institute)

        return (external_default_vo[0]['cn'], vo_admin_name)

    def get_next_member_uid(self):
        """Returns the next available member user ID for this institute.

        This function uses its own lock, but since it does not add a user,
        it still needs to be protected by the adding function!

        @returns: integer representing a VAC member user ID.
        """
        self.__class__.lock()

        vsc = VSC()

        institute_filter = InstituteFilter(self.institute_name)

        # stay low-level, no need to create higher-level structures
        institute_users = self.ldap_query.user_filter_search(institute_filter, attributes=['uid', 'uidNumber'])

        if institute_users is None:
            current_max = vsc.user_uid_institute_map[self.institute_name]
        else:
            current_max = max([int(user['uidNumber']) for user in institute_users])

        # this should take multiple ranges into account
        maximum_uid = vsc.user_uid_institute_map[self.institute_name][0] + vsc.user_uid_range
        new_uid = current_max + 1
        if new_uid > maximum_uid:
            self.lockfile.release()
            raise NoAvailableUserId(new_uid, maximum_uid)

        self.__class__.unlock()
        return new_uid

    def get_admin_info(self, institute):
        """Get the administrator's information for the given institute.

        @type institute: a string representing a VSC institute

        @return: a dictionary with 'email' and 'logins' as keys
        """
        ## FIXME: need to do proper error checking and logging!
        return self.__get_group_contact_logins_info(institute, "(&(instituteLogin=admin) (institute=%s))" % (institute))


    def get_vo_admin_info(self, institute):
        """Get the administrator's information for the given institute.

        @type institute: a string representing a VSC institute

        @return: a dictionary with 'email' and 'logins' as keys
        """
        ## FIXME: need to do proper error checking and logging!
        return self.__get_group_contact_logins_info(institute, "(&(instituteLogin=voadmin) (institute=%s))" % (institute))


    def __get_group_contact_logins_info(self, institute, filter):
        """General function to get information about a group.

        Contact (mail email address) and group member logins.

        @type institute: string representing a VSC institute
        @filter: string representing an LDAP filter
        """
        user = self.ldap_query.user_filter_search(filter=filter, attributes=['cn', 'gecos', 'mail'])

        info = { 'email': None
               , 'logins': None }

        if user:
            user = user[0] # there can be only one
            info['email'] = "%s <%s>" % (user['gecos'], user['mail'])

            # check which 'people' are members of the admin group
            # we get the VSC logins, NOT the institute logins! Thus, we need a filtered search.
            # this should NOT go wrong if the LDAP database is correct
            ## FIXME: add error handling
            group = self.ldap_query.group_filter_search("(cn=%s)" % (user['cn']), attributes=['cn', 'memberUid'])
            if not group is None:
                info['logins'] = [self.ldap_query.user_filter_search( filter="(&(cn=%s) (institute=%s))" % (member, institute)
                                                                    , attributes=['instituteLogin'])[0]['instituteLogin']
                                        for member in group[0]['memberUid']]

        return info


