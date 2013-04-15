#!/usr/bin/env python
##
#
# Copyright 2012 Ghent University
# Copyright 2012 Andy Georges
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
"""

import os
import stat
import sys

from vsc import fancylogger
from vsc.filesystem.gpfs import GpfsOperations
from vsc.ldap.filters import CnFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.configuration import LumaConfiguration
from vsc.administration.user import VscUser
from vsc.config.base import CentralStorage

log = fancylogger.getLogger('create_directory_trees_tier2_home_data')


def set_up_filesystem(gpfs, filesystem_info, filesystem_name, vo_support=False):
    """Set up the filesets and directories such that user, vo directories and friends can be created."""

    # Create the basic user fileset
    user_fileset_path = os.path.join(filesystem_info['defaultMountPoint'], 'users')
    if not 'user' in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
        gpfs.make_fileset(user_fileset_path, 'users')
        os.chmod(user_fileset_path,
                 stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                 stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                 stat.S_IROTH | stat.S_IXOTH)

    if vo_support:
        # Create the basic vo fileset
        vo_fileset_path = os.path.join(filesystem_info['defaultMountPoint'], 'vos')
        if not 'virtualorg' in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
            gpfs.make_fileset(vo_fileset_path, 'vos')
            os.chmod(vo_fileset_path,
                    stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                    stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                    stat.S_IROTH | stat.S_IXOTH)


def main(args):

    l = LdapQuery(LumaConfiguration())  # initialise
    storage_settings = CentralStorage()

    gpfs = GpfsOperations()
    gpfs.list_filessystems()
    gpfs.list_filesets()
    home = gpfs.get_filesystem_info(storage_settings.home_name)
    data = gpfs.get_filesystem_info(storage_settings.data_name)
    apps = gpfs.get_filesystem_info(storage_settings.apps_name)

    set_up_filesystem(gpfs, home, storage_settings.home_name)
    set_up_filesystem(gpfs, data, storage_settings.data_name)

    # If users are to log in, there should be a symlink to the GPFS directory hierarchy
    if not os.path.lexists('/user'):
        os.symlink(user_fileset_path, '/user')

    # If the apps are to be reachable in a similar vein as on the Tier-2, we need a symlink
    # from the FS root to the real fileset path
    if not os.path.lexists('/apps'):
        os.symlink(apps_fileset_path, '/apps')

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
