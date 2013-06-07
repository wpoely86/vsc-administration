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
Sets up the basic structure on the UGent home and data storage

@author: Andy Georges
"""

import os

from vsc.filesystem.gpfs import GpfsOperations
from vsc.ldap.utils import LdapQuery
from vsc.ldap.configuration import VscConfiguration
from vsc.config.base import VscStorage
from vsc.utils import fancylogger


log = fancylogger.getLogger('create_directory_trees_tier2_home_data')
fancylogger.setLogLevelInfo()


def set_up_filesystem(gpfs, storage_settings, storage, filesystem_info, filesystem_name, vo_support=False):
    """Set up the filesets and directories such that user, vo directories and friends can be created."""

    # Create the basic gent fileset
    log.info("Setting up for storage %s" % (storage))
    fileset_name = storage_settings.path_templates[storage]['user'][0]
    fileset_path = os.path.join(filesystem_info['defaultMountPoint'], fileset_name)
    if not fileset_name in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
        gpfs.make_fileset(fileset_path, fileset_name)
        gpfs.chmod(0755, fileset_path)
        log.info("Fileset users created and linked at %s" % (fileset_path))

    if vo_support:
        # Create the basic vo fileset
        fileset_name = storage_settings.path_templates[storage]['vo'][0]
        vo_fileset_path = os.path.join(filesystem_info['defaultMountPoint'], fileset_name)
        if not fileset_name in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
            gpfs.make_fileset(vo_fileset_path, 'vos')
            gpfs.chmod(0755, vo_fileset_path)
            log.info("Fileset vos created and linked at %s" % (vo_fileset_path))


def main():

    LdapQuery(VscConfiguration())  # initialise
    storage_settings = VscStorage()

    gpfs = GpfsOperations()
    gpfs.list_filesystems()
    gpfs.list_filesets()

    home_name = storage_settings['VSC_HOME'].filesystem
    data_name = storage_settings['VSC_DATA'].filesystem

    home = gpfs.get_filesystem_info(home_name)
    data = gpfs.get_filesystem_info(data_name)
    #apps = gpfs.get_filesystem_info(storage_settings.apps_name)

    set_up_filesystem(gpfs, storage_settings, 'VSC_HOME', home, home_name)
    set_up_filesystem(gpfs, storage_settings, 'VSC_DATA', data, data_name, vo_support=True)


if __name__ == '__main__':
    main()
