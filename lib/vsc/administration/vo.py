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
#!/usr/bin/env python
##
"""
This file contains the utilities for dealing with VOs on the VSC.
Original Perl code by Stijn De Weirdt

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
"""

import errno
import os
import pwd

from vsc import fancylogger
from vsc.administration.user import VscUser
from vsc.config.base import VSC, VscStorage
from vsc.filesystem.gpfs import GpfsOperations, GpfsOperationError, PosixOperations, PosixOperationError
from vsc.ldap.entities import VscLdapGroup

logger = fancylogger.getLogger(__name__)

VO_PREFIX = 'gvo'
DEFAULT_VO = 'gvo000012'
INSTITUTE_VOS = ['gvo00012', 'gvo00016', 'gvo00017', 'gvo00018']


class VscVo(VscLdapGroup):
    """Class representing a VO in the VSC.

    A VO is a special kind of group, identified mainly by its name.
    """

    def __init__(self, vo_id, storage=None, **kwargs):
        """Initialise"""
        super(VscVo, self).__init__(vo_id)

        # Normally, we could use the group_id, but since we're in VO, we should use the right terms
        self.vo_id = vo_id
        self.vsc = VSC()

        if not storage:
            self.storage = VscStorage()
        else:
            self.storage = storage

        self.gpfs = GpfsOperations()
        self.posix = PosixOperations()

    def _lock(self):
        """Take a global lock (on a file), to avoid other instances
        ruining things :-)
        """
        pass

    def members(self):
        """Return a list with all the VO members in it."""
        return [VscUser(m) for m in self.memberUid]

    def _get_path(self, storage, mount_point="gpfs"):
        """Get the path for the (if any) user directory on the given storage."""

        template = self.storage.path_templates[storage]['vo']
        if mount_point == "login":
            mount_path = self.storage[storage].login_mount_point
        elif mount_point == "gpfs":
            mount_path = self.storage[storage].gpfs_mount_point
        else:
            self.log.raiseException("mount_point (%s)is not login or gpfs" % (mount_point))

        return os.path.join(mount_path, template[0], template[1](self.vo_id))

    def _data_path(self, mount_point="gpfs"):
        """Return the path to the VO data fileset on GPFS"""
        return self._get_path('VSC_DATA', mount_point)

    def _scratch_path(self, storage, mount_point="gpfs"):
        """Return the path to the VO scratch fileset on GPFS.

        @type storage: string
        @param storage: name of the storage we are looking at.
        """
        return self._get_path(storage, mount_point)

    def _create_fileset(self, filesystem_name, path, parent_fileset=None):
        """Create a fileset for the VO on the data filesystem.

        - creates the fileset if it does not already exist
        - sets the (fixed) quota on this fileset for the VO

        The parent_fileset is used to support older (< 3.5.x) GPFS setups still present in our system
        """
        self.gpfs.list_filesets()
        fileset_name = self.vo_id

        if not self.gpfs.get_fileset_info(filesystem_name, fileset_name):
            self.log.info("Creating new fileset on %s with name %s and path %s" % (filesystem_name,
                                                                                   fileset_name,
                                                                                   path))
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)

            # HACK to support versions older than 3.5 in our setup
            if parent_fileset is None:
                self.gpfs.make_fileset(path, fileset_name)
            else:
                self.gpfs.make_fileset(path, fileset_name, parent_fileset)
        else:
            self.log.info("Fileset %s already exists for VO %s ... not creating again." % (fileset_name, self.vo_id))

        moderators = [m for m in [VscUser(m_) for m_ in self.moderator] if m.status == 'active']

        self.gpfs.chmod(0770, path)

        if moderators:
            self.gpfs.chown(int(moderators[0].uidNumber), int(self.gidNumber), path)
        else:
            self.gpfs.chown(pwd.getpwnam('nobody').pw_uid, int(self.gidNumber), path)

    def create_data_fileset(self):
        """Create the VO's directory on the HPC data filesystem. Always set the quota."""
        try:
            path = self._data_path()
            self._create_fileset(self.storage['VSC_DATA'].filesystem, path)
        except AttributeError:
            self.log.exception("Trying to access non-existent attribute 'filesystem' in the storage instance")
        except KeyError:
            self.log.exception("Trying to access non-existent field 'VSC_DATA' in the storage dictionary")

    def create_scratch_fileset(self, storage_name):
        """Create the VO's directory on the HPC data filesystem. Always set the quota."""
        try:
            path = self._scratch_path(storage_name)
            if self.storage[storage_name].version >= (3,5,0,0):
                self._create_fileset(self.storage[storage_name].filesystem, path)
            else:
                self._create_fileset(self.storage[storage_name].filesystem, path, 'root')
        except AttributeError:
            self.log.exception("Trying to access non-existent attribute 'filesystem' in the storage instance")
        except KeyError:
            self.log.exception("Trying to access non-existent field %s in the storage dictionary" % (storage_name))

    def _create_vo_dir(self, path):
        """Create a user owned directory on the GPFS."""
        self.gpfs.make_dir(path)

    def _set_quota(self, path, quota):
        """Set FILESET quota on the FS for the VO fileset.

        @type quota: int

        @param quota: soft quota limit expressed in KiB
        """
        try:
            quota *= 1024
            soft = int(quota * self.vsc.quota_soft_fraction)

            # LDAP information is expressed in KiB, GPFS wants bytes.
            self.gpfs.set_fileset_quota(soft, path, self.vo_id, quota)
            self.gpfs.set_fileset_grace(path, self.vsc.vo_storage_grace_time)  # 7 days
        except GpfsOperationError:
            self.log.raiseException("Unable to set quota on path %s" % (path))

    def set_data_quota(self):
        """Set FILESET quota on the data FS for the VO fileset."""
        if self.dataQuota:
            self._set_quota(self._data_path(), int(self.dataQuota))
        else:
            quota = 16 * 1024**2  # default not used from the filesystem_info/conf file at thes moment.
            self._set_quota(self._data_path(), quota)

    def set_scratch_quota(self, storage_name):
        """Set FILESET quota on the scratch FS for the VO fileset."""
        if self.scratchQuota:
            # FIXME: temp fix for the delcatty storage rsync
            if storage_name.startswith('VSC_SCRATCH_DELCATTY'):
                multiplier = 1.3
            else:
                multiplier = 1
            self._set_quota(self._scratch_path(storage_name), int(int(self.scratchQuota) * multiplier))
        else:
            self._set_quota(self._scratch_path(storage_name), self.storage[storage_name].quota_vo)

    def _set_member_quota(self, path, member, quota):
        """Set USER quota on the FS for the VO fileset

        @type member: VscUser instance
        """
        try:
            soft = int(quota * self.vsc.quota_soft_fraction)
            self.gpfs.set_user_quota(soft, int(member.uidNumber), path, quota)
        except GpfsOperationError:
            self.log.raiseException("Unable to set USR quota for member %s on path %s" % (member.user_id, path))

    def set_member_data_quota(self, member):
        """Set the quota on the data FS for the member in the VO fileset.

        @type member: VscUser instance

        The user can have up to half of the VO quota.
        FIXME: This should probably be some variable in a config setting instance
        """
        if self.dataQuota and int(self.dataQuota) > 0:
            quota = int(self.dataQuota) / 2 * 1024  # half the VO space expressed in bytes
        else:
            quota = 2 * 1024**2 # 2 MiB, with a replication factor of 2

        self.log.info("Setting the data quota for VO %s member %s to %d GiB" %
                      (self.vo_id, member, quota / 1024 / 1024))
        self._set_member_quota(self._data_path(), member, quota)

    def set_member_scratch_quota(self, storage_name, member):
        """Set the quota on the scratch FS for the member in the VO fileset.

        @type member: VscUser instance

        The user can have up to half of the VO quota.
        FIXME: This should probably be some variable in a config setting instance
        """
        if self.scratchQuota and int(self.scratchQuota) > 0:
            # FIXME: temp fix for the delcatty storage rsync
            if storage_name.startswith('VSC_SCRATCH_DELCATTY'):
                multiplier = 1.3
            else:
                multiplier = 1
            quota = int(int(self.scratchQuota or 2) / 2 * 1024 * multiplier)
        else:
            quota = 2 * 1024**2 # 2 MiB, with a replication factor of 2


        self.log.info("Setting the scratch quota on %s for VO %s member %s to %d GiB" %
                      (storage_name, self.vo_id, member, quota / 1024 / 1024))
        self._set_member_quota(self._scratch_path(storage_name), member, quota)

    def _set_member_symlink(self, member, origin, target, fake_target):
        """Create a symlink for this user from origin to target"""
        self.log.info("Trying to create a symlink for %s from %s to %s [%s]. Deprecated. Not doing anything." % (member.user_id, origin, fake_target, target))

    def _create_member_dir(self, member, target):
        """Create a member-owned directory in the VO fileset."""
        created = self.gpfs.make_dir(target)
        self.gpfs.chown(int(member.uidNumber), int(member.gidNumber), target)
        if created:
            self.gpfs.chmod(0700, target)

        self.log.info("Created directory %s for member %s" % (target, member.user_id))

    def create_member_data_dir(self, member):
        """Create a directory on data in the VO fileset that is owned by the member with name $VSC_DATA_VO/<vscid>."""
        target = os.path.join(self._data_path(), member.user_id)
        self._create_member_dir(member, target)

    def create_member_scratch_dir(self, storage_name, member):
        """Create a directory on scratch in the VO fileset that is owned by the member with name $VSC_SCRATCH_VO/<vscid>."""
        target = os.path.join(self._scratch_path(storage_name), member.user_id)
        self._create_member_dir(member, target)

    def set_member_data_symlink(self, member):
        """(Re-)creates the symlink that points from $VSC_DATA to $VSC_DATA_VO/<vscid>."""
        self.log.warning("Trying to set a symlink for a VO member on %s. Deprecated. Not doing anything" % (storage_name,))

    def set_member_scratch_symlink(self, storage_name, member):
        """(Re-)creates the symlink that points from $VSC_SCRATCH to $VSC_SCRATCH_VO/<vscid>.

        @deprecated. We should not create new symlinks.
        """
        self.log.warning("Trying to set a symlink for a VO member on %s. Deprecated. Not doing anything" % (storage_name,))

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - otherwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(VscVo, self).__setattr__(name, value)


