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

from vsc import fancylogger
from vsc.filesystem.gpfs import GpfsOperations
from vsc.ldap.filters import CnFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.configuration import LumaConfiguration
from vsc.administration.user import MukUser
from vsc.config.base import Muk

log = fancylogger.getLogger('create_directory_trees_muk')


def main(args):

    l = LdapQuery(LumaConfiguration())  # initialise
    muk = Muk()

    gpfs = GpfsOperations()
    gpfs.list_filesets()
    scratch = gpfs.get_filesystem_info(muk.scratch_name)

    # Create the base user fileset that will be used to store the directory
    # hierarchy to mimic the login/home directories of users
    user_fileset_path = os.path.join(scratch['defaultMountPoint'], 'user')
    if not 'user' in [f['filesetName'] for f in gpfs.gpfslocalfilesets[muk.scratch_name].values()]:
        gpfs.make_fileset(user_fileset_path, 'user')
        gpfs.chmod(0755, user_fileset_path)

    # Create the applications fileset that will be used for storing apps and tools
    # that can be used by the users on muk
    apps_fileset_path = os.path.join(scratch['defaultMountPoint'], 'apps')
    if not 'apps' in [f['filesetName'] for f in gpfs.gpfslocalfilesets[muk.scratch_name].values()]:
        gpfs.make_fileset(apps_fileset_path, 'apps')
        gpfs.chmod(0755, apps_fileset_path)

    # Create the projects fileset that will be used to store the directory
    # hierarchy for all project spaces on muk scratch
    projects_fileset_path = os.path.join(scratch['defaultMountPoint'], 'projects')
    if not 'projects' in [f['filesetName'] for f in gpfs.gpfslocalfilesets[muk.scratch_name].values()]:
        gpfs.make_fileset(projects_fileset_path, 'projects')
        gpfs.chmod(0755, projects_fileset_path)

    # If users are to log in, there should be a symlink to the GPFS directory hierarchy
    if not os.path.lexists('/user'):
        os.symlink(user_fileset_path, '/user')

    # If the apps are to be reachable in a similar vein as on the Tier-2, we need a symlink
    # from the FS root to the real fileset path
    if not os.path.lexists('/apps'):
        os.symlink(apps_fileset_path, '/apps')

    # In the pilot phase, we have 4 project filesets that need to be owned by the pilot groups
    # moderator and be group rw for said pilot group
    pilot_projects = {
        'a': 'project_apilot',
        'b': 'project_bpilot',
        'g': 'project_gpilot',
        'l': 'project_lpilot',
    }

    for institute in ['a', 'b', 'g', 'l']:
        group_name = "%st1_mukusers" % institute
        try:
            group = l.group_filter_search(CnFilter(group_name))[0]
        except:
            continue
        owner = l.user_filter_search(CnFilter(group['moderator'][0]))[0]

        project_fileset_name = pilot_projects[institute]
        project_fileset_path = os.path.join(scratch['defaultMountPoint'], 'projects', project_fileset_name)

        if not project_fileset_name in [f['filesetName'] for f in gpfs.gpfslocalfilesets[muk.scratch_name].values()]:
            gpfs.make_fileset(project_fileset_path, project_fileset_name)

        gpfs.chmod(0755, project_fileset_path)
        os.chown(project_fileset_path, int(owner['uidNumber']), int(group['gidNumber']))

	    project_quota = 70 * 1024 * 1024 * 1024 * 1024
        gpfs.set_fileset_quota(project_quota, project_fileset_path, project_fileset_name)

    # Exmaples
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
