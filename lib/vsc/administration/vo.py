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

import os

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
            self.log.raiseException("mount_point is not login or gpfs")

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

    def _create_fileset(self, filesystem_name, path):
        """Create a fileset for the VO on the data filesystem.

        - creates the fileset if it does not already exist
        - sets the (fixed) quota on this fileset for the VO
        """
        self.gpfs.list_filesets()
        fileset_name = self.vo_id

        if not self.gpfs.get_fileset_info(filesystem_name, fileset_name):
            self.log.info("Creating new fileset on %s scratch with name %s and path %s" % (filesystem_name,
                                                                                           fileset_name,
                                                                                           path))
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            self.log.info("Fileset %s already exists for VO %s ... not creating again." % (fileset_name, self.vo_id))

        self.gpfs.chmod(0700, path)
        self.gpfs.chown(int(self.gidNumber), int(self.gidNumber), path)

    def create_data_fileset(self):
        """Create the VO's directory on the HPC data filesystem. Always set the quota."""
        try:
            path = self._data_path()
            self._create_fileset(self.storage['VSC_DATA'].filesystem, path)
        except AttributeError:
            self.log.exception("Trying to access non-existent attribute 'filesystem' in the storage instance")
        except KeyError:
            self.log.exception("Trying to access non-existent field 'VSC_DATA' in the storage dictionary")

    def create_scratch_fileset(self, storage):
        """Create the VO's directory on the HPC data filesystem. Always set the quota."""
        try:
            path = self._scratch_path(storage)
            self._create_fileset(self.storage[storage].filesystem, path, self.dataQuota)
        except AttributeError:
            self.log.exception("Trying to access non-existent attribute 'filesystem' in the storage instance")
        except KeyError:
            self.log.exception("Trying to access non-existent field %s in the storage dictionary" % (storage))

    def _create_vo_dir(self, path):
        """Create a user owned directory on the GPFS."""
        self.gpfs.make_dir(path)

    def _set_quota(self, path_function, quota):
        """Set FILESET quota on the FS for the VO fileset.

        @type quota: int

        @param quota: soft quota limit expressed in KiB
        """
        try:
            path = path_function()
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
            self._set_quota(self._data_path, int(self.dataQuota))
        else:
            self._set_quota(self._data_path, 0)

    def set_scratch_quota(self, storage):
        """Set FILESET quota on the scratch FS for the VO fileset."""
        if self.scratchQuota:
            self._set_quota(self._scratch_path, int(self.scratchQuota))
        else:
            self._set_quota(self._scratch_path, 0)

    def _set_member_quota(self, path_function, member, quota):
        """Set USER quota on the FS for the VO fileset

        @type member: VscUser instance
        """
        try:
            path = path_function()
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
        if self.dataQuota:
            quota = int(self.dataQuota) / 2 * 1024  # expressed in bytes
        else:
            quota = 0
        self._set_member_quota(self._data_path, member, quota)

    def set_member_scratch_quota(self, member):
        """Set the quota on the scratch FS for the member in the VO fileset.

        @type member: VscUser instance

        The user can have up to half of the VO quota.
        FIXME: This should probably be some variable in a config setting instance
        """
        if self.scratchQuota:
            quota = int(self.scratchQuota or 0) / 2 * 1024
        else:
            quota = 0
        self._set_member_quota(self._scratch_path, member, quota)

    def _set_member_symlink(self, member, origin, target):
        """Create a symlink for this user from origin to target"""
        try:
            self.gpfs.make_dir(target)
            self.gpfs.chown(int(member.uidNumber), int(member.gidNumber), target)
            if not self.gpfs.is_symlink(origin):
                self.gpfs.remove_obj(origin)
                self.gpfs.make_symlink(target, origin)
            self.gpfs.ignorerealpathmismatch = True
            self.gpfs.chown(int(member.uidNumber), int(member.gidNumber), origin)
            self.gpfs.ignorerealpathmismatch = False
        except PosixOperationError:
            self.log.exception("Could not create the symlink from %s to %s for %s" % (origin, target, member.user_id))

    def set_member_data_symlink(self, member):
        """(Re-)creates the symlink that points from $VSC_DATA to $VSC_DATA_VO/<vscid>."""
        if member.dataMoved:
            origin = member._data_path()
            target = os.path.join(self._data_path(), member.user_id)
            self._set_member_symlink(member, origin, target)

    def set_member_scratch_symlink(self, member):
        """(Re-)creates the symlink that points from $VSC_SCRATCH to $VSC_SCRATCH_VO/<vscid>."""
        if member.scratchMoved:
            origin = member._scratch_path()
            target = os.path.join(self._scratch_path(), member.user_id)
            self._set_member_symlink(member, origin, target)
