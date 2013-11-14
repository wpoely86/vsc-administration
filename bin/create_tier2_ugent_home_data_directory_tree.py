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
from ConfigParser import SafeConfigParser

from vsc.filesystem.gpfs import GpfsOperations
from vsc.ldap.utils import LdapQuery
from vsc.ldap.configuration import VscConfiguration
from vsc.config.base import VscStorage
from vsc.utils import fancylogger

QUOTA_CONF_FILE = '/etc/quota_check.conf'

log = fancylogger.getLogger('create_directory_trees_tier2_home_data')
fancylogger.setLogLevelInfo()


def set_up_apps(gpfs, storage_settings, storage, filesystem_info, filesystem_name):
    """Set up the apps fileset."""
    log.info("Setting up the apps fileset on storage %s" % (storage))
    fileset_name = storage_settings.path_templates[storage]['apps'][0]
    fileset_path = os.path.join(filesystem_infp['defaultMountPoint'], fileset_name)
    if not fileset_name in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
        gpfs.make_fileset(fileset_path, fileset_name)
        gpfs.chmod(0755, fileset_path)
        log.info("Fileset %s created and linked at %s" % (fileset_name, fileset_path))


def set_up_filesystem(gpfs, storage_settings, storage, filesystem_info, filesystem_name, vo_support=False):
    """Set up the filesets and directories such that user, vo directories and friends can be created."""

    # Create the basic gent fileset
    log.info("Setting up for storage %s" % (storage))
    fileset_name = storage_settings.path_templates[storage]['user'][0]
    fileset_path = os.path.join(filesystem_info['defaultMountPoint'], fileset_name)
    if not fileset_name in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
        gpfs.make_fileset(fileset_path, fileset_name)
        gpfs.chmod(0755, fileset_path)
        log.info("Fileset %s created and linked at %s" % (fileset_name, fileset_path))

    if vo_support:
        # Create the basic vo fileset
        fileset_name = storage_settings.path_templates[storage]['vo'][0]
        vo_fileset_path = os.path.join(filesystem_info['defaultMountPoint'], fileset_name)
        if not fileset_name in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
            gpfs.make_fileset(vo_fileset_path, fileset_name)
            gpfs.chmod(0755, vo_fileset_path)
            log.info("Fileset %s created and linked at %s" % (fileset_name, vo_fileset_path))


def main():

    LdapQuery(VscConfiguration())  # initialise
    storage_settings = VscStorage()

    local_storage_conf = SafeConfigParser()
    local_storage_conf.read(QUOTA_CONF_FILE)

    gpfs = GpfsOperations()
    gpfs.list_filesystems()
    gpfs.list_filesets()

    for storage_name in local_storage_conf.get('MAIN', 'storage').split(','):

        filesystem_name = storage_settings[storage_name].filesystem
        filesystem_info = gpfs.get_filesystem_info(filesystem_name)

        if storage_name in ('VSC_HOME'):
            set_up_filesystem(gpfs, storage_settings, storage_name, filesystem_info, filesystem_name)
            set_up_apps(gpfs, storage_settings, storage_name, filesystem_info, filesystem_name)
        else:
            set_up_filesystem(gpfs, storage_settings, storage_name, filesystem_info, filesystem_name, vo_support=True)


if __name__ == '__main__':
    main()
