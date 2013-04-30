#!/usr/bin/env python
##
#
# Copyright 2012-2013 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
##
"""
Sets up the baseline directory structure we need to have on muk.

- a user fileset
- an apps fileset
- creates symlinks to these if they do not yet exist (on the node where the script is being run
    - /user -> /gpfs/scratch/user
    - /apps -> /gpfs/scratch/apps

@author Andy Georges
"""

import os
import stat
import sys

from vsc.filesystem.gpfs import GpfsOperations
from vsc.ldap.utils import LdapQuery
from vsc.ldap.configuration import VscConfiguration
from vsc.config.base import CentralStorage
from vsc.utils import fancylogger


log = fancylogger.getLogger('create_directory_trees_tier2_home_data')
fancylogger.setLogLevelDebug()

def set_up_filesystem(gpfs, filesystem_info, filesystem_name, vo_support=False):
    """Set up the filesets and directories such that user, vo directories and friends can be created."""

    # Create the basic user fileset
    user_fileset_path = os.path.join(filesystem_info['defaultMountPoint'], 'users')
    if not 'users' in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
        gpfs.make_fileset(user_fileset_path, 'users')
        gpfs.chmod(0755, user_fileset_path)

    if vo_support:
        # Create the basic vo fileset
        vo_fileset_path = os.path.join(filesystem_info['defaultMountPoint'], 'vos')
        if not 'vos' in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
            gpfs.make_fileset(vo_fileset_path, 'vos')
            gpfs.chmod(0755, vo_fileset_path)


def main(args):

    l = LdapQuery(VscConfiguration())  # initialise
    storage_settings = CentralStorage()

    gpfs = GpfsOperations()
    gpfs.list_filesystems()
    gpfs.list_filesets()
    home = gpfs.get_filesystem_info(storage_settings.home_name)
    data = gpfs.get_filesystem_info(storage_settings.data_name)
    #apps = gpfs.get_filesystem_info(storage_settings.apps_name)

    set_up_filesystem(gpfs, home, storage_settings.home_name)
    set_up_filesystem(gpfs, data, storage_settings.data_name, vo_support=True)

    # for now
    set_up_filesystem(gpfs, home, storage_settings.home_name)
    set_up_filesystem(gpfs, data, storage_settings.data_name, vo_support=True)

    # Examples
    # for a single user:
    #u = MukUser('vsc40075')  # ageorges
    #u.create_scratch_fileset()
    #u.populate_scratch_fallback()  # we should always do this, so we can shift the symlinks around at leisure.
    #u.create_home_dir()  # this creates the symlink from the directory hierarchy in the scratch to the actual home

    # for an UGent user with regular home on the gengar storage, NFS mounted
    #u = MukUser('vsc40528')  # lmunoz
    #u.create_scratch_fileset()
    #u.populate_scratch_fallback()  # we should always do this, so we can shift the symlinks around at leisure.
    #u.create_home_dir()  # this creates the symlink from the directory hierarchy in the scratch to the actual home


if __name__ == '__main__':
    main(sys.argv)
