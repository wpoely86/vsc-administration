#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2012-2013 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
This file contains the utilities for dealing with users on the VSC.
Original Perl code by Stijn De Weirdt.

The following actions are available for users:
- add: Add a user. Requires: institute, gecos, mail address, public key
- modify_quota: Change the personal quota for a user (data and scratch only)

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
"""

import os

from vsc import fancylogger
from vsc.administration.institute import Institute
from vsc.config.base import VSC, Muk, VscStorage
from vsc.filesystem.ext import ExtOperations
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.posix import PosixOperations
from vsc.ldap.filters import InstituteFilter, LoginFilter
from vsc.ldap import NoSuchUserError
from vsc.ldap.entities import VscLdapUser
from vsc.utils.fs_store import store_pickle_data_at_user_home, store_pickle_data_at_user


log = fancylogger.getLogger(__name__)


class VscUser(VscLdapUser):
    """Classs representing a user in the VSC administrative library.

    - add a user to the VSC LDAP
    - set up the user's directories
    - FIXME: This should be revamped once we decide on the central location for the
      user information (e.g., LDAP or a real DB, for example, the django backend.)
    """

    # lock attributes on a class basis (should be reachable from static and class methods
    #USER_LOCKFILE_NAME = "/var/run/lock.%s.pid" % (__class__.__name__)
    #LOCKFILE = PIDLockFile(USER_LOCKFILE_NAME)

    def __init__(self, user_id, storage=None):
        super(VscUser, self).__init__(user_id)

        self.vsc = VSC()

        if not storage:
            self.storage = VscStorage()
        else:
            self.storage = storage

        self.gpfs = GpfsOperations()  # Only used when needed
        self.posix = PosixOperations()

    def pickle_path(self):
        """Provide the location where to store pickle files for this user."""
        return self.homeDirectory

    @classmethod
    def lock(cls):
        """Take a global lock to avoid other instances from messing things up."""
        cls.LOCKFILE.acquire()

    @classmethod
    def unlock(cls):
        """Release the global lock."""
        cls.LOCKFILE.release()

    @classmethod
    def load(cls, login, institute):
        """Instantiate a user for the given (institute, login) pair.

        @type login: string
        @type institute: string

        @param login: the user's login at the home institute
        @param institute: the user's home institute name

        @raise NoSuchInstituteError, NoSuchUserError

        @return: a VscUser instance or None if no such user exists
        """
        if not institute in VSC.institutes:
            log.raiseException("Institute %s does not exist in the VSC." % (institute), NoSuchInstituteError)

        login_filter = LoginFilter(login)
        institute_filter = InstituteFilter(institute)

        result = cls.lookup(login_filter & institute_filter)
        if len(result) > 0:
            return result[0]
        else:
            return None

    @classmethod
    def add(cls, login, institute, gecos, mail_address, key):
        """Add a user to the LDAP.

        This method performs multiple actions:
            - it adds an entry for the user in the people LDAP subtree
            - it adds an entry for the corresponding group in the LDAP groups subtree

        @type login: string representing the login name of the user in the institute
        @type institute: string representing the institute
        @type gecos: string representing the user's name
        @type mail_address: string representing the user's email address
        @type key: string representing the user's public ssh key

        @returns: VscUser instance. If the users exists, the corresponding instance is returned.

        @raise: NoSuchInstituteError if the institute does not exist.
        @raise: FIXME: should raise an error if the user already exists in the LDAP

        FIXME: Since we have the data in the django DB, we can pass it along here and use this for synchronising the LDAP.
        """
        vsc = VSC()
        if not institute in vsc.institutes:
            log.raiseException("Institute %s does not exist in the VSC." % (institute), NoSuchInstituteError)

        user = VscUser(None)  # placeholder, but defines the LdapQuery instance

        try:
            VscUser.lock()
            # Check if the user is already active on the VSC. We assume no two people
            # carry the same login in a single institute.
            user = VscUser.load(login, institute)
            if not user is None:
                log.raiseException("User %s@%s already exists in LDAP" % (login, institute))

            log.info("VscUser.add: did not find user %s@%s to add, proceeding" % (login, institute))

            # Determine VSC-specific attributes to set in the LDAP.
            user_id = Institute(institute).get_next_member_uid()  # numerical ID
            group_id = user_id  # each user gets a group with the same numerical ID
            user_name = VscUser.__generate_name(user_id)
            group_name = user_name  # group name for this user is the same
            pathnames = vsc.user_pathnames(str(user_id), institute)  # the canonical pathnames for the user in his institute (home, data, scratch)

            attributes = {
                'objectClass': ['top', 'posixAccount', 'vscuser'],
                'cn': user_name,
                'uid': user_name,
                'uidNumber': str(user_id),
                'gidNumber': str(group_id),
                'researchField': 'unknown',
                'mailList': ['hpc-announce@lists.ugent.be'],  # obligatory
                'homeDirectory': pathnames['home'],
                'dataDirectory': pathnames['data'],
                'scratchDirectory': pathnames['scratch'],
                'institute': institute,
                'instituteLogin': login,
                'gecos': gecos,
                'mail': mail_address,
                'pubkey': key,
                'loginShell': vsc.user_shell,
                'homeQuota': str(vsc.user_quota_home),  # default
                'dataQuota': str(vsc.user_quota_data),  # default
                'scratchQuota': str(vsc.user_quota_default_scratch),  # default
                'status': vsc.defaults['new_user_status']  # indicatesd progress through scripts
            }

            log.info("Adding user [%s] with attributes %s" % (user_name, attributes))
            user.vsc_user_id = user_name  # fixing placeholder
            super(VscUser, user).add(attributes)

            # each user has a corresponding group
            group = GroupBase(group_name)
            group.add(group_name=group_name, moderator_name=user_name)
            group.add_member(member_uid=user_name)
            institute_group_all = Group(institute).load('%sall' % (institute))
            institute_group_all.add_member(member_uid=user_name)
            VscUser.unlock()
            return user
        finally:
            VscUser.unlock()
        return None

    def modify_quota(self, data_quota=None, scratch_quota=None):
        """Modify the data or scratch quota for a given user.

        @type data_quota:
        @type scratch_quota:

        @raise NoSuchUserError
        """
        user = self.ldap_query.user_filter_search(filter="cn=%s" % (self.cn))
        if not user:
            raise NoSuchUserError(self.cn)

        log.info("Changing quota for user %s to %d [data] and %d [scratch]" % (user, data_quota, scratch_quota))

        # FIXME: there should be a better way to do this.
        if data_quota is not None:
            # FIXME: use the VscLdapUser super instance for these.
            self.ldap_query.user_modify(self.cn, {'dataQuota': data_quota})
            self.data_quota = data_quota
        if scratch_quota is not None:
            self.ldap_query.user_modify(self.cn, {'scratchQuota': scratch_quota})
            self.scratch_quota = scratch_quota

    def _create_grouping_fileset(self, filesystem_name, path):
        """Create a fileset for a group of 100 user accounts

        - creates the fileset if it does not already exist
        """
        self.gpfs.list_filesets()
        fileset_name = self.vsc.user_grouping(self.user_id)
        self.log.info("Trying to create the grouping fileset %s with link path %s" % (fileset_name, path))

        if not self.gpfs.get_fileset_info(filesystem_name, fileset_name):
            self.log.info("Creating new fileset on %s with name %s and path %s" % (filesystem_name,
                                                                                   fileset_name,
                                                                                   path))
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            self.log.info("Fileset %s already exists for VO %s ... not creating again." % (fileset_name, self.user_id))

        self.gpfs.chmod(0755, path)

    def _get_path(self, storage, mount_point="gpfs"):
        """Get the path for the (if any) user directory on the given storage."""

        template = self.storage.path_templates[storage]['user']
        if mount_point == "login":
            mount_path = self.storage[storage].login_mount_point
        elif mount_point == "gpfs":
            mount_path = self.storage[storage].gpfs_mount_point
        else:
            self.log.raiseException("mount_point (%s) is not login or gpfs" % (mount_point))

        return os.path.join(mount_path, template[0], template[1](self.user_id))

    def _get_grouping_path(self, storage_name, mount_point="gpfs"):
        """Get the path for the user group directory (and associated fileset)."""

        template = self.storage.path_templates[storage_name]['user_grouping']
        if mount_point == "login":
            mount_path = self.storage[storage_name].login_mount_point
        elif mount_point == "gpfs":
            mount_path = self.storage[storage_name].gpfs_mount_point
        else:
            self.log.raiseException("mount_point (%s) is not login or gpfs" % (mount_point))

        return os.path.join(mount_path, template[0], template[1](self.user_id))

    def _home_path(self, mount_point="gpfs"):
        """Return the path to the home dir."""

        return self._get_path('VSC_HOME', mount_point)

    def _data_path(self, mount_point="gpfs"):
        """Return the path to the data dir."""

        return self._get_path('VSC_DATA', mount_point)

    def _scratch_path(self, storage_name, mount_point="gpfs"):
        """Return the path to the scratch dir"""

        return self._get_path(storage_name, mount_point)

    def _grouping_data_path(self, mount_point="gpfs"):
        """Return the path to the grouping fileset for the users on data."""

        return self._get_grouping_path('VSC_DATA', mount_point)

    def _grouping_scratch_path(self, storage_name, mount_point="gpfs"):
        """Return the path to the grouping fileset for the users on the given scratch filesystem."""

        return self._get_grouping_path(storage_name, mount_point)

    def create_home_dir(self):
        """Create all required files in the (future) user's home directory.

        Requires to be run on a system where the appropriate GPFS is mounted.
        Always set the quota.
        """
        try:
            path = self._home_path()
            self._create_user_dir(path)
        except:
            self.log.raiseException("Could not create home dir for user %s" % (self.user_id))

    def create_data_dir(self):
        """Create the user's directory on the HPC data filesystem.

        Required to be run on a system where the appropriate GPFS is mounted."""
        try:
            path = self._grouping_data_path()
            self._create_grouping_fileset(self.storage['VSC_DATA'].filesystem, path)

            path = self._data_path()
            self._create_user_dir(path)
        except:
            self.log.raiseException("Could not create data dir for user %s" % (self.user_id))

    def create_scratch_dir(self, storage_name):
        """Create the user's directory on the given scratch filesystem

        @type storage_name: string

        @param storage_name: name of the storage system as defined in /etc/filesystem_info.conf
        """
        try:
            if self.storage[storage_name].user_grouping_fileset:
                path = self._grouping_scratch_path(storage_name)
                self._create_grouping_fileset(self.storage[storage_name].filesystem, path)

            path = self._scratch_path(storage_name)
            self._create_user_dir(path)
        except:
            self.log.raiseException("Could not create scratch dir for user %s" % (self.user_id))

    def _create_user_dir(self, path):
        """Create a user owned directory on the GPFS."""
        self.gpfs.make_dir(path)
        self.gpfs.chmod(0700, path)
        self.gpfs.chown(int(self.uidNumber), int(self.gidNumber), path)

    def _set_quota(self, quota, path):
        """Set quota on the target path.

        @type quota: int
        @type path: path into a GPFS mount
        """
        quota *= 1024
        soft = int(self.vsc.quota_soft_fraction * quota)

        self.log.info("Setting quota on %s to %d" % (path, quota))

        # LDAP information is expressed in KiB, GPFS wants bytes.
        self.gpfs.set_user_quota(soft, int(self.uidNumber), path, quota)
        self.gpfs.set_user_grace(path, self.vsc.user_storage_grace_time)  # 7 days

    def set_home_quota(self):
        """Set USR quota on the home FS in the user fileset."""
        path = self._home_path()
        self._set_quota(self.vsc.user_quota_home * self.vsc.quota_soft_fraction, path)

    def set_data_quota(self):
        """Set USR quota on the data FS in the user fileset."""
        path = self._grouping_data_path()
        self._set_quota(self.vsc.user_quota_data * self.vsc.quota_soft_fraction, path)

    def set_scratch_quota(self, storage_name):
        """Set USR quota on the scratch FS in the user fileset.

        FIXME: this information will have to come from the Django DB at some point
        """
        if self.storage[storage_name].user_grouping_fileset:
            path = self._grouping_scratch_path(storage_name)
        else:
            # Hack; this should actually become the link path of the fileset that contains the path (the file, not the followed symlink)
            path = os.path.normpath(os.path.join(self._scratch_path(), '..'))

        self._set_quota(self.vsc.user_quota_scratch * self.vsc.quota_soft_fraction, path)

    def populate_home_dir(self):
        """Store the required files in the user's home directory.

        Does not overwrite files that may contain user defined content.
        """
        path = self._home_path()
        self.gpfs.populate_home_dir(int(self.uidNumber), int(self.gidNumber), path, self.pubkey)

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - otherwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(VscUser, self).__setattr__(name, value)


class MukUser(VscLdapUser):
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

        self.muk = Muk()

        self.ext = ExtOperations()
        self.gpfs = GpfsOperations()
        self.posix = PosixOperations()

        self.user_scratch_quota = 250 * 1024 * 1024 * 1024  # 250 GiB
        self.scratch = self.gpfs.get_filesystem_info(self.muk.scratch_name)

    def pickle_path(self):
        return self._scratch_path()

    def _scratch_path(self):
        """Determines the path (relative to the scratch mount point)

        For a user with ID vscXYZUV this becomes users/vscXYZ/vscXYZUV. Note that the 'user' dir on scratch is
        different, that is there to ensure the home dir symlink tree can be present on all nodes.

        @returns: string representing the relative path for this user.
        """
        path = os.path.join(self.scratch['defaultMountPoint'], 'users', self.user_id[:-2], self.user_id)
        return path

    def create_scratch_fileset(self):
        """Create a fileset for the user on the scratch filesystem.

        - creates the fileset if it does not already exist
        - sets the (fixed) quota on this fileset
        - no user quota on scratch! only per-fileset quota
        """
        self.gpfs.list_filesets()

        fileset_name = self.user_id
        path = self._scratch_path()

        if not self.gpfs.get_fileset_info(self.muk.scratch_name, fileset_name):
            self.log.info("Creating new fileset on Muk scratch with name %s and path %s" % (fileset_name, path))
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            self.log.info("Fileset %s already exists for user %s ... not doing anything." % (fileset_name, self.user_id))

        self.gpfs.set_fileset_quota(self.user_scratch_quota, path, fileset_name)

        # We will always populate the scratch directory of the user as if it's his home directory
        # In this way, if the user moves to home on scratch, everything will be up to date and in place.

    def populate_scratch_fallback(self):
        """The scratch fileset is populated with the

        - ssh keys,
        - a clean .bashrc script,
        - a clean .bash_profile.

        The user can then always log in to the scratch, should the synchronisation fail to detect
        a valid NFS mount point and avoid setting home on Muk.
        """
        path = self._scratch_path()
        self.gpfs.populate_home_dir(int(self.uidNumber), int(self.gidNumber), path, self.pubkey)

    def create_home_dir(self):
        """Create the symlink to the real user's home dir that is

        - mounted somewhere over NFS
        - has an AFM cache covering the real NFS mount
        - sits on scratch (as indicated by the LDAP attribute).
        """
        try:
            source = self.homeDirectory
            base_home_dir_hierarchy = os.path.dirname(source.rstrip('/'))
        except AttributeError, _:
            self.log.raiseException("homeDirectory attribute missing in LDAP for user %s" % (self.user_id))  # FIXME: add the right exception type

        target = None
        try:
            if self.mukHomeOnScratch:
                self.log.info("User %s has his home on Muk scratch" % (self.user_id))
                target = self._scratch_path()
            elif self.mukHomeOnAFM:
                self.log.info("User %s has his home on Muk AFM" % (self.user_id))
                target = self.muk.user_afm_home_mount(self.user_id, self.institute)
        except AttributeError, _:
            pass


        if target is None:
            # This is the default case
            target = self.muk.user_nfs_home_mount(self.user_id, self.institute)

        self.gpfs.ignorerealpathmismatch = True
        self.gpfs.make_dir(base_home_dir_hierarchy)
        self.gpfs.make_symlink(target, source)
        self.gpfs.ignorerealpathmismatch = False

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - othwerwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(MukUser, self).__setattr__(name, value)


cluster_user_pickle_location_map = {
    'gengar': VscUser,
    'muk': MukUser
}

cluster_user_pickle_store_map = {
    'gengar': store_pickle_data_at_user_home,
    'muk': store_pickle_data_at_user
}
