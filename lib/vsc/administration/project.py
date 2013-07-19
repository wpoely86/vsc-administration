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
This file provides utilities to set up projects on the VSC clusters.

@author: Andy Georges (Ghent University)
"""
import os


from vsc.config.base import Muk, VscStorage
from vsc.filesystem.ext import ExtOperations
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.posix import PosixOperations
from vsc.ldap.entities import VscLdapGroup


class MukProject(VscLdapGroup):
    """Project that will be run on Muk.

    - Check if the project has scratch requirements
    - Set up the project scratch dir and associated quota
    - Set up credits?

    """

    def __init__(self, project_id, storage=None):
        """Initialisation.

        @type project_id: string
        @param project_id: the unique ID of the project, i.e.,  the LDAP cn entry
        """
        super(MukProject, self).__init__(project_id)

        self.project_id = project_id  # since we still do not have a proper project LDAP tree
        self.muk = Muk()

        self.ext = ExtOperations()
        self.gpfs = GpfsOperations()
        self.posix = PosixOperations()

        if not storage:
            self.storage = VscStorage()
        else:
            self.storage = storage

        # quota are obtained through LDAP
        self.scratch = self.gpfs.get_filesystem_info(self.muk.scratch_name)

    def _scratch_path(self, mount_point="gpfs"):
        """Determines the path (relative to the scratch mount point)

        For a project with ID projectXYZUV this becomes projects/projectXYZ/projectYZUV.

        @returns: string representing the relative path for this project.
        """
        template = self.storage.path_templates[self.muk.storage_name]['project']
        if mount_point == "login":
            mount_path = self.storage[self.muk.storage_name].login_mount_point
        elif mount_point == "gpfs":
            mount_path = self.storage[self.muk.storage_name].gpfs_mount_point
        else:
            self.log.raiseException("mount_point (%s) is not login or gpfs" % (mount_point))

        return os.path.join(mount_path, template[0], template[1](self.project_id))


    def create_scratch_fileset(self):
        """Create a fileset for the VO on the data filesystem.

        - creates the fileset if it does not already exist
        - sets the (fixed) quota on this fileset for the VO
        """
        self.gpfs.list_filesets()
        fileset_name = self.project_id
        filesystem_name = self.muk.scratch_name
        path = self._scratch_path()

        if not self.gpfs.get_fileset_info(filesystem_name, fileset_name):
            self.log.info("Creating new fileset for project %s on %s with name %s and path %s" % (self.project_id,
                                                                                                  filesystem_name,
                                                                                                  fileset_name,
                                                                                                  path))
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            self.log.info("Fileset %s already exists for project %s ... not creating again." % (fileset_name,
                                                                                                self.project_id))

        moderators = [m for m in [VscUser(m_) for m_ in self.moderator] if m.status == 'active']

        self.gpfs.chmod(0770, path)

        if moderators:
            self.gpfs.chown(int(moderators[0].uidNumber), int(self.gidNumber), path)
        else:
            self.gpfs.chown(pwd.getpwnam('nobody').pw_uid, int(self.gidNumber), path)

        self.gpfs.set_fileset_quota(self.scratchQuota, path, fileset_name)

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - othwerwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(MukProject, self).__setattr__(name, value)
