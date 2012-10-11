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
This file contains the utilities for dealing with users on the VSC.
Original Perl code by Stijn De Weirdt.

The following actions are available for users:
- add: Add a user. Requires: institute, gecos, mail address, public key
- modify_quota: Change the personal quota for a user (data and scratch only)


@author Andy Georges

@created Apr 23, 2012
"""

__author__ = 'ageorges'
__date__ = 'Apr 24, 2012'

import os
from lockfile.pidlockfile import PIDLockFile

import vsc.fancylogger as fancylogger
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.posix import PosixOperations
from vsc.gpfs.quota.mmfs_utils import set_gpfs_user_quota
from vsc.ldap import NoSuchInstituteError, NoSuchUserError
from vsc.ldap.user import LdapUser

#from vsc.administration.group import GroupBase
from vsc.administration.institute import Institute

logger = fancylogger.getLogger(__name__)


class VscUser(LdapUser):
    """Classs representing a user in the VSC administrative library.

    This is a VSC user with full capabilities. Intended to be used on a machine that has write access to _all_ LDAP
    attributes.

    - add a user to the VSC LDAP
    - set up the user's directories

    """
    def __init__(self, vsc_user_id):
        super(VscUser, self).__init__(vsc_user_id)

        self.USER_LOCKFILE_NAME = "/var/run/lock.%s.pid" % (self.__class__.__name__)
        self.lockfile = PIDLockFile(self.USER_LOCKFILE_NAME)

    @staticmethod
    def load(self, login, institute):
        """Loads a user with given institute and institute login.

        @type login: string representing the login name of the user at his institute
        @type institute: string representing the institute

        @raise NoSuchInstituteError, NoSuchUserError
        """
        if not institute in self.ldap_query.ldap.vsc.institutes:
            self.logger.error("Institute %s does not exist in the VSC." % (institute))
            raise NoSuchInstituteError(institute)

        user_ldap_info = self.ldap.user_search(login, institute)

        if user_ldap_info is None:
            self.logger.error("There is no user in the HPC LDAP who matches login=%s and institute=%s." % (login, institute))
            raise NoSuchUserError(login)

        user = LdapUser(user_ldap_info['cn'])
        return user

    def modify_status(self, status):
        """Modify the status from the user."""

        #FIXME: Should be in the LdapUser superclass
        self.status  # force load
        self.status = status

    @staticmethod
    def add(login, institute_name, gecos, mail_address, key):
        """Add a user to the VSC LDAP.

        This method performs multiple actions:
            - it adds an entry for the user in the people LDAP subtree
            - it adds an entry for the corresponding group in the LDAP groups subtree

        @type login: string representing the login name of the user in the institute
        @type institute_name: string representing the institute
        @type gecos: string representing the user's name
        @type mail_address: string representing the user's email address
        @type key: string representing the user's public ssh key

        @returns: VscUser instance. If the users exists, the corresponding instance is returned.

        @raise: NoSuchInstituteError if the institute does not exist.
        @raise: FIXME: should raise an error if the user already exists in the LDAP
        """
        # check if the institute is valid
        user = VscUser(None)  # placeholder, but defines the LdapQuery instance

        # FIXME: this is simply fugly
        if not institute_name in user.ldap.ldap.vsc.institutes:
            logger.error("Institute %s does not exist in the VSC" % (institute_name))
            raise NoSuchInstituteError(institute_name)

        institute = Institute(institute_name)

        try:
            user.lockfile.acquire()
            # Check if the user is already active on the VSC. We assume no two people
            # carry the same login in a single institute.
            user_ldap_info = user.ldap.user_search(login, institute_name)
            if user_ldap_info is not None:
                user.logger.warning("User %s (%s, %s) already exists in the HPC LDAP." %
                                    (user_ldap_info['cn'], login, institute_name))
                user.__fill_from_ldap_info(user_ldap_info)
                user.lockfile.release()

            user.logger.info("%s.add: did not find user %s@%s to add, proceeding" % (user.__class__.__name__, login, institute_name))

            # Determine VSC-specific attributes to set in the LDAP.
            u_id = institute.get_next_member_uid()  # numerical ID
            g_id = u_id  # each user gets a group with the same numerical ID
            u_login = user.__generate_name(u_id)
            vsc = user.ldap.ldap.vsc
            pathnames = vsc.user_pathnames(str(u_id), institute_name)  # the canonical pathnames for the user in his institute (home, data, scratch)

            attributes = {
                'objectClass': ['top', 'posixAccount', 'vscuser'],
                'cn': u_login,
                'uid': u_login,
                'uidNumber': str(u_id),
                'gidNumber': str(g_id),
                'researchField': 'unknown',
                'mailList': ['hpc-announce@lists.ugent.be'],  # obligatory
                'homeDirectory': pathnames['home'],
                'dataDirectory': pathnames['data'],
                'scratchDirectory': pathnames['scratch'],
                'institute': institute_name,
                'instituteLogin': login,
                'gecos': gecos,
                'mail': mail_address,
                'pubkey': key,
                'loginShell': vsc.user_shell,
                'homeQuota': str(vsc.user_quota_home),
                'dataQuota': str(vsc.user_quota_data),
                'scratchQuota': str(vsc.user_quota_default_scratch),
                'status': vsc.defaults['new_user_status']
            }

            user.logger.info("Adding user [%s] with attributes %s" % (u_login, attributes))
            user.vsc_user_id = u_login  # fixing placeholder
            super(VscUser, user).add(attributes)

            # each user has a corresponding group
            group = Group(institute_name)
            group.add(group_name=u_login, moderator_name=u_login)
            group.add_member(member_uid=u_login)
            institute_group_all = Group(institute_name).load('%sall' % (institute_name))
            institute_group_all.add_member(member_uid=u_login)
            user.lockfile.release()
            return user
        finally:
            if user.lockfile.is_locked():
                user.lockfile.release()
        return None

    def post_add(self, user, institute, gecos, mail_address):
        """Do some post processing of the user, depending on his institute.

        @type user
        @type institute
        @type gecos
        @type mail_address
        """
        if institute == "gent":
            self.__post_add_gent(user, institute, gecos, mail_address)

    def modify_quota(self, data_quota=None, scratch_quota=None):
        """Modify the data or scratch quota for a given user.

        @type data_quota:
        @type scratch_quota:

        @raise NoSuchUserError
        """
        user = self.ldap_query.user_filter_search(filter="cn=%s" % (self.cn))
        if not user:
            raise NoSuchUserError(self.cn)

        logger.info("Changing quota for user %s to %d [data] and %d [scratch]" % (user, data_quota, scratch_quota))

        # FIXME: there should be a better way to do this.
        if data_quota is not None:
            # FIXME: use the LdapUser super instance for these.
            self.ldap_query.user_modify(self.cn, {'dataQuota': data_quota})
            self.data_quota = data_quota
        if scratch_quota is not None:
            self.ldap_query.user_modify(self.cn, {'scratchQuota': scratch_quota})
            self.scratch_quota = scratch_quota

    def __post_add_gent(self, user, institute, gecos, mail_address):
        """Do some post-processing for added users if they belong to the gent institute.

        @type user
        @type institute
        @type gecos
        @type mail_address
        """
        pass
        #FIXME: this generates output, so we'll do this afterwards

    def set_home(self):
        """Create all required files in the (future) user's home directory.

        Note that:
        - we can do this as root
        - we need to use the numerical IDs from the LDAP database, since the
        user as such does not necessarily exist on the machine where we are
        running this.

        TODO:
        - check for errors and clean up if required!
        """
        ## Create directories
        os.mkdir(os.path.join(self.homeDirectory, '.ssh'), mode=0700)
        os.mkdir(self.dataDirectory, mode=0700)
        os.mkdir(self.scratchDirectory, mode=0700)

        ## SSH keys
        fp = open(os.path.join(self.homeDirectory, '.ssh', 'authorized_keys'), 'w')
        for key in self.pubkey:
            fp.write(key + "\n")
        fp.close()
        os.chmod(os.path.join(self.homeDirectory, '.ssh', 'authorized_keys'), 0644)

        ## bash shizzle
        open(os.path.join(self.homeDirectory, '.bashrc')).close()
        fp = open(os.path.join(self.homeDirectory), '.bash_profile')
        fp.write('if [ -f ~/.bashrc ]; then\n . ~/.bashrc\nfi\n')
        fp.close()

        # when we added a user, we also added groups in the LDAP database with the
        # same name. So, given that these groups exist, we can change group
        # ownership for all created files and directories to this group
        for f in [os.path.join(self.homeDirectory, '.ssh'),
                  self.data_directory,
                  self.scratch_directory,
                  os.path.join(self.homeDirectory, '.ssh', 'authorized_keys'),
                  os.path.join(self.homeDirectory, '.bashrc'),
                  os.path.join(self.homeDirectory, '.bash_profile')]:
            os.chown(f, self.user_id, self.group_id)

    def set_quota(self):
        ## set the quota for the user
        set_gpfs_user_quota(self.user_id, self.homeDirectory, self.homeQuota)
        set_gpfs_user_quota(self.user_id, self.dataDirectory, self.dataQuota)
        set_gpfs_user_quota(self.user_id, self.scratchDirectory, self.scratchQuota)

        ## FIXME: what about gold?

    def __generate_name(self, numerical_user_id):
        """Generate a name for a VSC user based on the numerical VSC user ID.

        @type numerical_user_id: integer representing a numeric user ID on the VSC

        @returns: name of the user on the VSC as a string
        """
        id = numerical_user_id % 100000  # retain last 5 digits
        return ''.join(['vsc', str(id)])


class MukUser(LdapUser):
    """A VSC user who is allowed to execute on the Tier 1 machine(s).

    This class provides functionality for administrating users on the
    Tier 1 machine(s).

    - Provide a fileset for the user on scratch ($VSC_SCRATCH)
    - Set up quota (scratch)
    - Symlink the user's home ($VSC_HOME) to the real home
        - AFM cached mount (GPFS) of the NFS path
        - NFS mount of the home institute's directory for the user
        - Local scratch location
      This is more involved than it seems since Ghent has a different
      path compared to the other institutes.
      Also, /scratch needs to remain the real scratch.
    - All changes should be an idempotent operation, i.e., f . f = f.
    - All changes should be made based on the timestamp of the LDAP entry,
      i.e., only if the modification time is more recent, we update the
      deployed settings.
    """

    def __init__(self, user_id):
        """Initialisation.

        @type vsc_user_id: string representing the user's VSC ID (vsc[0-9]{5})
        """
        super(MukUser, self).__init__(user_id)
        self.gpfs = GpfsOperation()
        self.posix = PosixOperations()

        self.user_scratch_quota = 250 * 1024 * 1024 * 1024  # 250 GiB

    def _scratch_path():
        """Determines the path (relative to the scratch mount point)

        For a user with ID vscXYZUV this becomes users/vscXYZ/vscXYZUV.

        @returns: string representing the relative path for this user.
        """
        path = os.path.join(['users', self.user_id[:-2], self.user_id])
        return path

    def create_scratch_fileset(self):
        """Create a fileset for the user on the scratch filesystem.

        - creates the fileset if it does not already exist
        - sets the (fixed) quota on this fileset
        - no user quota on scratch! only per-fileset quota
        """
        fileset_name = self.user_id
        path = self._scratch_path

        self.gpfs.list_filesets()

        if not fileset_name in gpfs.gpfslocalfilesets:
            self.log.info("Creating new fileset on Muk scratch with name %s and path %s" % (fileset_name, path))
            self.gpfs.make_fileset(path, fileset_name)
        else:
            self.log.info("Fileset %s already exists for user %s ... not doing anything." % (fileset_name, self.user_id))

        # FIXME: this is not going to work yet.
        self.gpfs_fileset_quota(soft = self.user_scratch_quota, path)

    def set_home(self,):
        """FIXME.

        - check the 'real_path_storage_type' and make the home symlink accordingly
        """
        pass

    def set_quota(self,):
        """FIXME.

        - set the quota for the user, fixed values. if more is required, go see a doctor, erm a project.
        """
        pass

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - othwerwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(MukUser, self).__setattr__('dry_run', value)








