#!/usr/bin/env python
#
# Copyright 2012-2016 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
Sets up the baseline directory structure we need to have on muk.

- a user fileset
- an apps fileset
- creates symlinks to these if they do not yet exist (on the node where the script is being run
    - /user -> /gpfs/scratch/user
    - /apps -> /gpfs/scratch/apps

@author: Andy Georges
"""

import os

from vsc.filesystem.gpfs import GpfsOperations
from vsc.ldap.filters import CnFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.configuration import LumaConfiguration
from vsc.administration.user import MukAccountpageUser
from vsc.config.base import Muk
from vsc.utils import fancylogger

log = fancylogger.getLogger('create_directory_trees_muk')

PILOT_PROJECTS = {
    'a': 'project_apilot',
    'b': 'project_bpilot',
    'g': 'project_gpilot',
    'l': 'project_lpilot',
}


def main():
    """Main."""

    ldap_query = LdapQuery(LumaConfiguration())  # initialise
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
        log.info("Fileset user created and linked at %s" % (user_fileset_path))

    # Create the applications fileset that will be used for storing apps and tools
    # that can be used by the users on muk
    apps_fileset_path = os.path.join(scratch['defaultMountPoint'], 'apps')
    if not 'apps' in [f['filesetName'] for f in gpfs.gpfslocalfilesets[muk.scratch_name].values()]:
        gpfs.make_fileset(apps_fileset_path, 'apps')
        gpfs.chmod(0755, apps_fileset_path)
        log.info("Fileset apps created and linked at %s" % (apps_fileset_path))

    # Create the projects fileset that will be used to store the directory
    # hierarchy for all project spaces on muk scratch
    projects_fileset_path = os.path.join(scratch['defaultMountPoint'], 'projects')
    if not 'projects' in [f['filesetName'] for f in gpfs.gpfslocalfilesets[muk.scratch_name].values()]:
        gpfs.make_fileset(projects_fileset_path, 'projects')
        gpfs.chmod(0755, projects_fileset_path)
        log.info("Fileset projects created and linked at %s" % (projects_fileset_path))

    # If users are to log in, there should be a symlink to the GPFS directory hierarchy
    if not os.path.lexists('/user'):
        os.symlink(user_fileset_path, '/user')
        log.info("Linking /user to %s" % (user_fileset_path))

    # If the apps are to be reachable in a similar vein as on the Tier-2, we need a symlink
    # from the FS root to the real fileset path
    if not os.path.lexists('/apps'):
        os.symlink(apps_fileset_path, '/apps')
        log.info("Linking /apps to %s" % (apps_fileset_path))

    # In the pilot phase, we have 4 project filesets that need to be owned by the pilot groups
    # moderator and be group rw for said pilot group
    pilot_projects = PILOT_PROJECTS

    for institute in pilot_projects.keys():
        group_name = "%st1_mukusers" % institute
        try:
            group = ldap_query.group_filter_search(CnFilter(group_name))[0]
        except:
            log.error("No LDAP group with the name %s found" % (group_name))
            continue

        owner = ldap_query.user_filter_search(CnFilter(group['moderator'][0]))[0]

        project_fileset_name = pilot_projects[institute]
        project_fileset_path = os.path.join(scratch['defaultMountPoint'], 'projects', project_fileset_name)

        if not project_fileset_name in [f['filesetName'] for f in gpfs.gpfslocalfilesets[muk.scratch_name].values()]:
            try:
                gpfs.make_fileset(project_fileset_path, project_fileset_name)
                log.info("Created new fileset %s with link path %s" % (project_fileset_name, project_fileset_path))
            except:
                log.exception("Failed to create a new fileset with the name %s and link path %s" %
                              (project_fileset_name, project_fileset_path))

        gpfs.chmod(0755, project_fileset_path)
        os.chown(project_fileset_path, int(owner['uidNumber']), int(group['gidNumber']))

        project_quota = 70 * 1024 * 1024 * 1024 * 1024
        gpfs.set_fileset_quota(project_quota, project_fileset_path, project_fileset_name)


def add_example_users():
    """Usage example on how to add a user.

    This creates paths and filesets for an UGent user with regular home on the gengar storage, NFS mounted
    """
    u = MukAccountpageUser('vsc40075')  # ageorges
    u.create_scratch_fileset()
    u.populate_scratch_fallback()  # we should always do this, so we can shift the symlinks around at leisure.
    u.create_home_dir()  # this creates the symlink from the directory hierarchy in the scratch to the actual home


if __name__ == '__main__':
    main()
