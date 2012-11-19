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
This file contains the utilities for dealing with users on the VSC.
Original Perl code by Stijn De Weirdt.

The following actions are available for users:
- add: Add a user. Requires: institute, gecos, mail address, public key
- modify_quota: Change the personal quota for a user (data and scratch only)
"""

import os
from lockfile.pidlockfile import PIDLockFile

import vsc.fancylogger as fancylogger
from vsc.config.base import VSC, Muk
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.posix import PosixOperations
from vsc.ldap.filter import InstituteFilter, LoginFilter
from vsc.ldap import NoSuchInstituteError, NoSuchUserError
from vsc.ldap.entities import VscLdapUser

from vsc.administration.institute import Institute

log = fancylogger.getLogger(__name__)


class VscUser(VscLdapUser):
    """Classs representing a user in the VSC administrative library.

    - add a user to the VSC LDAP
    - set up the user's directories
    """

    # lock attributes on a class basis (should be reachable from static and class methods
    USER_LOCKFILE_NAME = "/var/run/lock.%s.pid" % (__class__.__name__)
    LOCKFILE = PIDLockFile(USER_LOCKFILE_NAME)

    def __init__(self, user_id):
        super(VscUser, self).__init__(user_id)

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

    def modify_status(self, status):
        """Modify the status from the user."""

        #FIXME: Should be in the LdapUser superclass
        self.status  # force load
        self.status = status

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

            log.info("VscUser.add: did not find user %s@%s to add, proceeding" % (user.__class__.__name__, login, institute))

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

        log.info("Changing quota for user %s to %d [data] and %d [scratch]" % (user, data_quota, scratch_quota))

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
        #set_gpfs_user_quota(self.user_id, self.homeDirectory, self.homeQuota)
        #set_gpfs_user_quota(self.user_id, self.dataDirectory, self.dataQuota)
        #set_gpfs_user_quota(self.user_id, self.scratchDirectory, self.scratchQuota)

        ## FIXME: what about gold?
        pass

    @classmethod
    def __generate_name(cls, numerical_user_id):
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

        self.muk = Muk()

        self.gpfs = GpfsOperations()
        self.posix = PosixOperations()

        self.user_scratch_quota = 250 * 1024 * 1024 * 1024  # 250 GiB

    def _scratch_path(self):
        """Determines the path (relative to the scratch mount point)

        For a user with ID vscXYZUV this becomes users/vscXYZ/vscXYZUV. Note that the 'user' dir on scratch is
        different, that is there to ensure the home dir symlink tree can be present on all nodes.

        @returns: string representing the relative path for this user.
        """
        scratch = self.gpfs.get_filesystem_info(self.muk.scratch_name)
        path = os.path.join(scratch['defaultMountPoint'], 'users', self.user_id[:-2], self.user_id)
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

        if not self.gpfs.get_fileset_info('scratch', fileset_name):
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
            target = self.muk.user_home_mount(self.user_id, self.institute)

        self.gpfs.ignorerealpathmismatch = True
        if target:
            base_home_dir_hierarchy = os.path.dirname(source.rstrip('/'))
            # we should check that the real path (/user) sits on the GPFS, i.e., is a symlink to /gpfs/scratch/user
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
