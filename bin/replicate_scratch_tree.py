#!/usr/bin/env python
#
#
# Copyright 2013-2013 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
This script replicates the users and VO's onto the scratch storage in
a temporary tree.
"""

import copy
import pwd
import sys

from vsc.administration.user import VscUser
from vsc.administration.vo import VscVo
from vsc.config.base import GENT, VscStorage, VSC
from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.filters import CnFilter, InstituteFilter, NewerThanFilter
from vsc.ldap.utils import LdapQuery
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.utils import fancylogger
from vsc.utils.missing import Monoid, MonoidDict
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption


NAGIOS_HEADER = "replicate_scratch_tree"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes


SYNC_TIMESTAMP_FILENAME = "/var/run/%s.timestamp" % (NAGIOS_HEADER)


logger = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.setLogLevelInfo()


def set_up_filesystem(gpfs, storage_settings, storage, filesystem_info, filesystem_name, vo_support=False):
    """Set up the filesets and directories such that user, vo directories and friends can be created."""

    # Create the basic gent fileset
    log.info("Replicating up for storage %s" % (storage))
    fileset_name = storage_settings.path_templates[storage]['replica'][0]
    fileset_path = os.path.join(filesystem_info['defaultMountPoint'], fileset_name)

    # if the replicat fileset does not exist, we create it, like a BAWS
    if not fileset_name in [f['filesetName'] for f in gpfs.gpfslocalfilesets[filesystem_name].values()]:
        gpfs.make_fileset(fileset_path, fileset_name)
        gpfs.chmod(0755, fileset_path)
        log.info("Fileset %s created and linked at %s" % (fileset_name, fileset_path))

    # create directories up to vsc42000
    for group in xrange(0,20):

        group_path = os.path.join(fileset_path, "vsc4%02d" % group)
        if not os.path.exists(group_path):
            log.info("Path %s does not exist. Creating directory." % (group_path,))
            try:
                os.mkdir(group_path)
                os.chown(group_path, "0755")
            except (IOError, OSError), err:
                log.error("Problem creating dir %s" % (group_path,))

        for user in xrange(0,99):

            user_name = "vsc%02d%02d" % (group, user)
            user_id = 2540000 + group * 100 + user
            user_path = os.path.join(group_path, user_name)
            if not os.path.exists(user_path):
                log.info("Path %s does not exist. Creating directory." % (user_path,))
                try:
                    os.mkdir(user_path)
                    os.chown(user_path, user_id, user_id)
                except (IOError, OSError), err:
                    log.error("Problem creating dir %s" % (user_path,))

    if vo_support:

        for vo in xrange(1,99):

            vo_name =  "gvo%03d" % (vo,)
            try:
                vo_group = grp.getgrnam(vo_name)
            except:
                continue

            vo_path = os.path.join(fileset_path, 'gvo000', vo_name)
            vo_members = vo_group.gr_mem

            vo_moderator = None
            for member_name in vo_members:
                try:
                    vo_moderator = pwd.getpwnam(member_name)
                    break
                except KeyError, err:
                    continue

            if not vo_moderator:
                log.error("Cannot find a moderator for VO %s" % (vo_name,))
            else:
                if not os.exists(vo_path):
                    log.info("Path %s does not exist. Creating directory." % (vo_path,))
                    try:
                        os.mkdir(vo_path)
                        os.chown(vo_path, vo_moderator.pw_uid, vo_group.gr_gid)
                        os.chmod(vo_path, 770)
                    except (IOError, OSError), err:
                        log.error("Problem creating dir %s" % (vo_path,))

                for member_name in vo_members:

                    member_path = os.path.join(vo_path, member_name)
                    try:
                        member = pwd.getpwnam(member_name)
                    except KeyError, err:
                        continue

                    if not os.exists(member_path):
                        log.info("Path %s does not exist. Creating directory" % (member_path,))
                        try:
                            os.mkdir(member_path)
                            os.chown(member_path, member.pw_uid, member.pw_gid)
                            os.chmod(member_path, 700)
                        except:
                            log.error("Cannot create dir %s" % (member_path,))







def main():
    """
    Main script.
    - process the users and VOs
    - write the new timestamp if everything went OK
    - write the nagios check file
    """

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'storage': ('storage systems on which to deploy users and vos', None, 'extend', []),
        'user': ('process users', None, 'store_true', False),
        'vo': ('process vos', None, 'store_true', False),
    }

    opts = ExtendedSimpleOption(options)
    stats = {}

    try:
        storage_settings = VscStorage()
        gpfs = GpfsOperations()
        gpfs.list_filesystems()
        gpfs.list_filesets()

        for storage_name in opts.options.storage:

            filesystem_name = storage_settings[storage_name].filesystem
            filesystem_info = gpfs.get_filesystem_info(filesystem_name)


            set_up_filesystem(gpfs, storage_settings, storage_name, filesystem_info, filesystem_name, vo_support=True)

    except Exception, err:
        logger.exception("critical exception caught: %s" % (err))
        opts.critical("Script failed in a horrible way")
        sys.exit(NAGIOS_EXIT_CRITICAL)

    opts.epilogue("UGent users and VOs synchronised", stats)


if __name__ == '__main__':
    main()
