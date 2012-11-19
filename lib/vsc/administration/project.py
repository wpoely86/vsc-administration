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
This file provides utilities to set up projects on the VSC clusters.
"""
import os

import vsc.fancylogger as fancylogger
from vsc.config.base import Muk
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.posix import PosixOperations
from vsc.ldap.entities import VscLdapProject

class MukProject(VscLdapProject):
    """Project that will be run on Muk.

    - Check if the project has scratch requirements
    - Set up the project scratch dir and associated quota
    - Set up credits?

    """

    def __init__(self, project_id):
        """Initialisation.

        @type vsc_user_id: string representing the user's VSC ID (vsc[0-9]{5})
        """
        super(MukProject, self).__init__(project_id)

        self.muk = Muk()

        self.gpfs = GpfsOperations()
        self.posix = PosixOperations()

    def scratch_path(self):
        """Determines the path (relative to the scratch mount point)

        For a user with ID vscXYZUV this becomes users/vscXYZ/vscXYZUV.

        @returns: string representing the relative path for this user.
        """
        scratch = self.gpfs.get_filesystem_info(self.muk.scratch_name)
        path = os.path.join(scratch['defaultMountPoint'], 'projects', self.project_id[:-2], self.project_id)
        return path

    def create_scratch_fileset(self):
        """Create a fileset for the project on the scratch filesystem.

        - creates the fileset if it does not already exist
        - sets the quota on this fileset
        - no user quota on scratch! only per-fileset quota
        """
        self.gpfs.list_filesets()

        fileset_name = self.project_id
        path = self._scratch_path()

        if not self.gpfs.get_fileset_info('scratch', fileset_name):
            self.log.info("Creating new fileset on Muk scratch with name %s and path %s" % (fileset_name, path))
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            self.log.info("Fileset %s already exists for user %s ... not doing anything." % (fileset_name, self.project_id))

        self.gpfs.set_fileset_quota(self.user_scratch_quota, path, fileset_name)
        moderator = MukUser(self.moderator)
        self.gpfs.chown(os.path.join(path, fileset_name), moderator.uidNumber, self.gidNumber) # FIXME: the gidNumber prolly comes from elsewhere

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - othwerwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(MukProject, self).__setattr__(name, value)
