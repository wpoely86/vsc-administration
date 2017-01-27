# -*- coding: latin-1 -*-
#
# Copyright 2012-2017 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
This file contains the utilities for dealing with users on the VSC.

@author: Stijn De Weirdt (Ghent University)
@author: Andy Georges (Ghent University)
"""

import errno
import logging
import os

from urllib2 import HTTPError

from vsc.utils import fancylogger
from vsc.accountpage.wrappers import mkVscAccountPubkey, mkVscHomeOnScratch, mkUserGroup
from vsc.accountpage.wrappers import mkVscAccount
from vsc.accountpage.wrappers import mkGroup, mkVscUserSizeQuota
from vsc.administration.tools import create_stat_directory
from vsc.config.base import VSC, Muk, VscStorage, VSC_DATA, VSC_HOME
from vsc.config.base import NEW, MODIFIED, MODIFY, ACTIVE
from vsc.filesystem.ext import ExtOperations
from vsc.filesystem.gpfs import GpfsOperations
from vsc.filesystem.posix import PosixOperations


log = fancylogger.getLogger(__name__)


class UserStatusUpdateError(Exception):
    pass


class VscAccountPageUser(object):
    """
    A user who gets his own information from the accountpage through the REST API.
    """

    def __init__(self, user_id, rest_client, account=None, pubkeys=None):
        """
        Initialise.

        @param account: can be a VscAccount namedtuple, to avoid calling the REST api.
        @param pubkeys: can be a VscAccountPubkey namedtuple, to avoid calling the REST api.
        """
        self.user_id = user_id
        self.rest_client = rest_client
        self._pubkey_cache = pubkeys
        self._account_cache = account
        self._usergroup_cache = None
        self._home_on_scratch_cache = None

    @property
    def account(self):
        if not self._account_cache:
            self._account_cache = mkVscAccount((self.rest_client.account[self.user_id].get())[1])
        return self._account_cache

    @property
    def person(self):
        return self.account.person

    @property
    def usergroup(self):
        if not self._usergroup_cache:
            if self.person.institute_login in ('x_admin', 'admin', 'voadmin'):
                # TODO to be removed when magic site admin usergroups are purged from code
                self._usergroup_cache = mkGroup((self.rest_client.group[self.user_id].get())[1])
            else:
                self._usergroup_cache = mkUserGroup((self.rest_client.account[self.user_id].usergroup.get()[1]))

        return self._usergroup_cache

    @property
    def home_on_scratch(self):
        if self._home_on_scratch_cache is None:
            hos = self.rest_client.account[self.user_id].home_on_scratch.get()[1]
            self._home_on_scratch_cache = [mkVscHomeOnScratch(h) for h in hos]
        return self._home_on_scratch_cache

    @property
    def pubkeys(self):
        if self._pubkey_cache is None:  # an empty list is allowed :)
            ps = self.rest_client.account[self.user_id].pubkey.get()[1]
            self._pubkey_cache = [mkVscAccountPubkey(p) for p in ps if not p['deleted']]
        return self._pubkey_cache

    def get_institute_prefix(self):
        """
        Get the first letter of the institute the user belongs to.
        """
        return self.person.institute['site'][0]


class VscTier2AccountpageUser(VscAccountPageUser):
    """
    A user on each of our Tier-2 system using the account page REST API
    to retrieve its information.
    """
    def __init__(self, user_id, storage=None, pickle_storage='VSC_SCRATCH_DELCATTY', rest_client=None,
                 account=None, pubkeys=None):
        """
        Initialisation.
        @type vsc_user_id: string representing the user's VSC ID (vsc[0-9]{5})
        """
        super(VscTier2AccountpageUser, self).__init__(user_id, rest_client, account, pubkeys)

        self._quota_cache = {}
        self.pickle_storage = pickle_storage
        if not storage:
            self.storage = VscStorage()
        else:
            self.storage = storage

        self.vsc = VSC()
        self.gpfs = GpfsOperations()  # Only used when needed
        self.posix = PosixOperations()

    @property
    def user_home_quota(self):
        if not self._quota_cache:
            self._init_quota_cache()
        return self._quota_cache['home']

    @property
    def user_data_quota(self):
        if not self._quota_cache:
            self._init_quota_cache()
        return self._quota_cache['data']

    @property
    def user_scratch_quota(self):
        if not self._quota_cache:
            self._init_quota_cache()
        return self._quota_cache['scratch']

    @property
    def vo_data_quota(self):
        if not self._quota_cache:
            self._init_quota_cache()
        return self._quota_cache['vo']['data']

    @property
    def vo_scratch_quota(self):
        if not self._quota_cache:
            self._init_quota_cache()
        return self._quota_cache['vo']['scratch']

    def _init_quota_cache(self):
        all_quota = [mkVscUserSizeQuota(q) for q in self.rest_client.account[self.user_id].quota.get()[1]]
        # we no longer set defaults, since we do not want to accidentally revert people to some default
        # that is lower than their actual quota if the accountpage goes down in between retrieving the users
        # and fetching the quota
        institute_quota = filter(lambda q: q.storage['institute'] == self.person.institute['site'], all_quota)
        fileset_name = self.vsc.user_grouping(self.account.vsc_id)

        def user_proposition(quota, storage_type):
            return quota.fileset == fileset_name and quota.storage['storage_type'] == storage_type

        self._quota_cache['home'] = [q.hard for q in institute_quota if user_proposition(q, 'home')][0]
        self._quota_cache['data'] = [q.hard for q in institute_quota if user_proposition(q, 'data')][0]
        self._quota_cache['scratch'] = filter(lambda q: user_proposition(q, 'scratch'), institute_quota)

        fileset_name = 'gvo'

        def user_proposition(quota, storage_type):
            return quota.fileset.startswith(fileset_name) and quota.storage['storage_type'] == storage_type
        self._quota_cache['vo'] = {}
        self._quota_cache['vo']['data'] = [q for q in institute_quota if user_proposition(q, 'data')]
        self._quota_cache['vo']['scratch'] = [q for q in institute_quota if user_proposition(q, 'scratch')]

    def pickle_path(self):
        """Provide the location where to store pickle files for this user.

        This location is the user'path on the pickle_storage specified when creating
        a VscTier2AccountpageUser instance.
        """
        template = self.storage.path_templates[self.pickle_storage]['user']
        return os.path.join(self.storage[self.pickle_storage].gpfs_mount_point,
                            template[0],
                            template[1](self.account.vsc_id)
                            )

    def _create_grouping_fileset(self, filesystem_name, path):
        """Create a fileset for a group of 100 user accounts

        - creates the fileset if it does not already exist
        """
        self.gpfs.list_filesets()
        fileset_name = self.vsc.user_grouping(self.account.vsc_id)
        logging.info("Trying to create the grouping fileset %s with link path %s", fileset_name, path)

        if not self.gpfs.get_fileset_info(filesystem_name, fileset_name):
            logging.info("Creating new fileset on %s with name %s and path %s", filesystem_name, fileset_name, path)
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            logging.info("Fileset %s already exists for user group of %s ... not creating again.",
                         fileset_name, self.account.vsc_id)

        self.gpfs.chmod(0o755, path)

    def _get_path(self, storage_name, mount_point="gpfs"):
        """Get the path for the (if any) user directory on the given storage_name."""

        template = self.storage.path_templates[storage_name]['user']
        if mount_point == "login":
            mount_path = self.storage[storage_name].login_mount_point
        elif mount_point == "gpfs":
            mount_path = self.storage[storage_name].gpfs_mount_point
        else:
            logging.error("mount_point (%s) is not login or gpfs", mount_point)
            raise Exception("mount_point (%s) is not designated as gpfs or login" % (mount_point,))

        return os.path.join(mount_path, template[0], template[1](self.account.vsc_id))

    def _get_grouping_path(self, storage_name, mount_point="gpfs"):
        """Get the path for the user group directory (and associated fileset)."""

        template = self.storage.path_templates[storage_name]['user_grouping']
        if mount_point == "login":
            mount_path = self.storage[storage_name].login_mount_point
        elif mount_point == "gpfs":
            mount_path = self.storage[storage_name].gpfs_mount_point
        else:
            logging.error("mount_point (%s) is not login or gpfs", mount_point)
            raise Exception("mount_point (%s) is not designated as gpfs or login" % (mount_point,))

        return os.path.join(mount_path, template[0], template[1](self.account.vsc_id))

    def _home_path(self, mount_point="gpfs"):
        """Return the path to the home dir."""
        return self._get_path(VSC_HOME, mount_point)

    def _data_path(self, mount_point="gpfs"):
        """Return the path to the data dir."""
        return self._get_path(VSC_DATA, mount_point)

    def _scratch_path(self, storage_name, mount_point="gpfs"):
        """Return the path to the scratch dir"""
        return self._get_path(storage_name, mount_point)

    def _grouping_home_path(self, mount_point="gpfs"):
        """Return the path to the grouping fileset for the users on data."""
        return self._get_grouping_path(VSC_HOME, mount_point)

    def _grouping_data_path(self, mount_point="gpfs"):
        """Return the path to the grouping fileset for the users on data."""
        return self._get_grouping_path(VSC_DATA, mount_point)

    def _grouping_scratch_path(self, storage_name, mount_point="gpfs"):
        """Return the path to the grouping fileset for the users on the given scratch filesystem."""
        return self._get_grouping_path(storage_name, mount_point)

    def create_home_dir(self):
        """Create all required files in the (future) user's home directory.

        Requires to be run on a system where the appropriate GPFS is mounted.
        Always set the quota.
        """
        try:
            path = self._grouping_home_path()
            self._create_grouping_fileset(self.storage[VSC_HOME].filesystem, path)

            path = self._home_path()
            self._create_user_dir(path)
        except Exception:
            logging.exception("Could not create home dir for user %s", self.account.vsc_id)
            raise

    def create_data_dir(self):
        """Create the user's directory on the HPC data filesystem.

        Required to be run on a system where the appropriate GPFS is mounted."""
        try:
            path = self._grouping_data_path()
            self._create_grouping_fileset(self.storage[VSC_DATA].filesystem, path)

            path = self._data_path()
            self._create_user_dir(path)
        except Exception:
            logging.exception("Could not create data dir for user %s", self.account.vsc_id)
            raise

    def create_scratch_dir(self, storage_name):
        """Create the user's directory on the given scratch filesystem

        @type storage_name: string
        @param storage_name: name of the storage system as defined in /etc/filesystem_info.conf
        """
        try:
            if self.storage[storage_name].user_grouping_fileset:
                path = self._grouping_scratch_path(storage_name)
                self._create_grouping_fileset(self.storage[storage_name].filesystem, path)

            path = self._scratch_path(storage_name)
            self._create_user_dir(path)
        except Exception:
            logging.exception("Could not create scratch dir for user %s", self.account.vsc_id)
            raise

    def _create_user_dir(self, path):
        """Create a user owned directory on the GPFS."""
        if self.gpfs.is_symlink(path):
            logging.warning("Trying to make a user dir, but a symlink already exists at %s", path)
            return

        create_stat_directory(
            path,
            0o700,
            int(self.account.vsc_id_number),
            int(self.usergroup.vsc_id_number),
            self.gpfs
        )

    def _set_quota(self, storage_name, path, hard):
        """Set the given quota on the target path.

        @type path: path into a GPFS mount
        @type hard: hard limit
        """
        if not hard:
            logging.error("No user quota set for %s", storage_name)
            return

        quota = hard * 1024 * self.storage[storage_name].data_replication_factor
        soft = int(self.vsc.quota_soft_fraction * quota)

        logging.info("Setting quota for %s on %s to %d", storage_name, path, quota)

        # LDAP information is expressed in KiB, GPFS wants bytes.
        self.gpfs.set_user_quota(soft, int(self.account.vsc_id_number), path, quota)
        self.gpfs.set_user_grace(path, self.vsc.user_storage_grace_time)  # 7 days

    def set_home_quota(self):
        """Set USR quota on the home FS in the user fileset."""
        path = self._home_path()
        hard = self.user_home_quota
        self._set_quota(VSC_HOME, path, hard)

    def set_data_quota(self):
        """Set USR quota on the data FS in the user fileset."""
        path = self._grouping_data_path()
        hard = self.user_data_quota
        self._set_quota(VSC_DATA, path, hard)

    def set_scratch_quota(self, storage_name):
        """Set USR quota on the scratch FS in the user fileset."""
        quota = filter(lambda q: q.storage['name'] in (storage_name,), self.user_scratch_quota)
        if not quota:
            logging.error("No scratch quota information available for %s", storage_name)
            return

        if self.storage[storage_name].user_grouping_fileset:
            path = self._grouping_scratch_path(storage_name)
        else:
            # Hack; this should actually become the link path of the fileset
            # that contains the path (the file, not the followed symlink)
            path = os.path.normpath(os.path.join(self._scratch_path(storage_name), '..'))

        self._set_quota(storage_name, path, quota[0].hard)

    def populate_home_dir(self):
        """Store the required files in the user's home directory.

        Does not overwrite files that may contain user defined content.
        """
        path = self._home_path()
        self.gpfs.populate_home_dir(int(self.account.vsc_id_number),
                                    int(self.usergroup.vsc_id_number),
                                    path,
                                    [p.pubkey for p in self.pubkeys])

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - otherwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(VscTier2AccountpageUser, self).__setattr__(name, value)


class MukAccountpageUser(VscAccountPageUser):
    """A VSC user who is allowed to execute on the Tier 1 machine(s).

    This class provides functionality for administrating users on the
    Tier 1 machine(s).

    - Provide a fileset for the user on scratch ($VSC_SCRATCH)
    - Set up quota (scratch)
    - Symlink the user's home ($VSC_HOME) to the real home
        - AFM cached mount (GPFS) of the NFS path
        - NFS mount of the home institute's directory for the user
        - Local scratch location
      This is more involved than it seems since Ghent has a different
      path compared to the other institutes.
      Also, /scratch needs to remain the real scratch.
    - All changes should be an idempotent operation, i.e., f . f = f.
    - All changes should be made based on the timestamp of the LDAP entry,
      i.e., only if the modification time is more recent, we update the
      deployed settings.
    """

    def __init__(self, user_id, storage=None, pickle_storage='VSC_SCRATCH_MUK', rest_client=None):
        """Initialisation.
        @type vsc_user_id: string representing the user's VSC ID (vsc[0-9]{5})
        """
        super(MukAccountpageUser, self).__init__(user_id, rest_client)

        if not storage:
            self.storage = VscStorage()
        else:
            self.storage = storage

        self.gpfs = GpfsOperations()  # Only used when needed
        self.posix = PosixOperations()
        self.ext = ExtOperations()

        self.pickle_storage = pickle_storage

        self.muk = Muk()

        try:
            all_quota = rest_client.account[self.user_id].quota.get()[1]
        except HTTPError:
            logging.exception("Unable to retrieve quota information from the accountpage")
            self.user_scratch_storage = 0
        else:
            muk_quota = filter(lambda q: q['storage']['name'] == self.muk.storage_name, all_quota)
            if muk_quota:
                self.user_scratch_quota = muk_quota[0]['hard'] * 1024
            else:
                self.user_scratch_quota = 250 * 1024 * 1024 * 1024

        self.scratch = self.gpfs.get_filesystem_info(self.muk.scratch_name)

    def pickle_path(self):
        return self._scratch_path()

    def _scratch_path(self):
        """Determines the path (relative to the scratch mount point)

        For a user with ID vscXYZUV this becomes users/vscXYZ/vscXYZUV. Note that the 'user' dir on scratch is
        different, that is there to ensure the home dir symlink tree can be present on all nodes.

        @returns: string representing the relative path for this user.
        """
        path = os.path.join(self.scratch['defaultMountPoint'], 'users', self.user_id[:-2], self.user_id)
        return path

    def create_scratch_fileset(self):
        """Create a fileset for the user on the scratch filesystem.

        - creates the fileset if it does not already exist
        - sets the (fixed) quota on this fileset
        - no user quota on scratch! only per-fileset quota
        """
        self.gpfs.list_filesets()

        fileset_name = self.user_id
        path = self._scratch_path()

        if not self.gpfs.get_fileset_info(self.muk.scratch_name, fileset_name):
            logging.info("Creating new fileset on Muk scratch with name %s and path %s" % (fileset_name, path))
            base_dir_hierarchy = os.path.dirname(path)
            self.gpfs.make_dir(base_dir_hierarchy)
            self.gpfs.make_fileset(path, fileset_name)
        else:
            logging.info("Fileset %s already exists for user %s ... not doing anything." % (fileset_name, self.user_id))

        self.gpfs.set_fileset_quota(self.user_scratch_quota, path, fileset_name)

        # We will always populate the scratch directory of the user as if it's his home directory
        # In this way, if the user moves to home on scratch, everything will be up to date and in place.

    def populate_scratch_fallback(self):
        """The scratch fileset is populated with the

        - ssh keys,
        - a clean .bashrc script,
        - a clean .bash_profile.

        The user can then always log in to the scratch, should the synchronisation fail to detect
        a valid NFS mount point and avoid setting home on Muk.
        """
        path = self._scratch_path()
        self.gpfs.populate_home_dir(int(self.account.vsc_id_number),
                                    int(self.usergroup.vsc_id_number),
                                    path,
                                    [p.pubkey for p in self.pubkeys])

    def create_home_dir(self):
        """Create the symlink to the real user's home dir that is

        - mounted somewhere over NFS
        - has an AFM cache covering the real NFS mount
        - sits on scratch (as indicated by the LDAP attribute).
        """
        source = self.account.home_directory
        base_home_dir_hierarchy = os.path.dirname(source.rstrip('/'))
        target = None

        if 'VSC_MUK_SCRATCH' in [s.storage.name for s in self.home_on_scratch]:
            logging.info("User %s has his home on Muk scratch" % (self.account.vsc_id))
            target = self._scratch_path()
        elif 'VSC_MUK_AFM' in [s.storage.name for s in self.home_on_scratch]:
            logging.info("User %s has his home on Muk AFM" % (self.user_id))
            target = self.muk.user_afm_home_mount(self.account.vsc_id, self.person.institute['site'])

        if target is None:
            # This is the default case
            target = self.muk.user_nfs_home_mount(self.account.vsc_id, self.person.institute['site'])

        self.gpfs.ignorerealpathmismatch = True
        self.gpfs.make_dir(base_home_dir_hierarchy)
        try:
            os.symlink(target, source)  # since it's just a link pointing to places that need not exist on the sync host
        except OSError as err:
            if err.errno not in [errno.EEXIST]:
                raise
            else:
                logging.info("Symlink from %s to %s already exists" % (source, target))
        self.gpfs.ignorerealpathmismatch = False

    def cleanup_home_dir(self):
        """Remove the symlink to the home dir for the user."""
        source = self.account.home_directory

        if self.gpfs.is_symlink(source):
            os.unlink(source)
            logging.info("Removed the symbolic link %s" % (source,))
        else:
            logging.error("Home dir cleanup wanted to remove a non-symlink %s")

    def __setattr__(self, name, value):
        """Override the setting of an attribute:

        - dry_run: set this here and in the gpfs and posix instance fields.
        - otherwise, call super's __setattr__()
        """

        if name == 'dry_run':
            self.gpfs.dry_run = value
            self.posix.dry_run = value

        super(MukAccountpageUser, self).__setattr__(name, value)


cluster_user_pickle_location_map = {
    'delcatty': VscTier2AccountpageUser,
    'muk': MukAccountpageUser,
}

cluster_user_pickle_store_map = {
    'delcatty': 'VSC_SCRATCH_DELCATTY',
    'muk': 'VSC_SCRATCH_MUK',
}


def update_user_status(user, client):
    """
    Change the status of the user's account in the account page to active.
    The usergroup status is always in sync with thte accounts status
    """
    if user.dry_run:
        log.info("User %s has account status %s. Dry-run, not changing anything", user.user_id, user.account.status)
        return

    if user.account.status not in (NEW, MODIFIED, MODIFY):
        log.info("Account %s has status %s, not changing" % (user.user_id, user.account.status))
        return

    payload = {"status": ACTIVE}
    try:
        response_account = client.account[user.user_id].patch(body=payload)
    except HTTPError as err:
        log.error("Account %s and UserGroup %s status were not changed", user.user_id, user.user_id)
        raise UserStatusUpdateError("Account %s status was not changed - received HTTP code %d" % err.code)
    else:
        account = mkVscAccount(response_account[1])
        if account.status == ACTIVE:
            log.info("Account %s status changed to %s" % (user.user_id, ACTIVE))
        else:
            log.error("Account %s status was not changed", user.user_id)
            raise UserStatusUpdateError("Account %s status was not changed, still at %s" %
                                        (user.user_id, account.status))


def process_users_quota(options, user_quota, storage_name, client):
    """
    Process the users' quota for the given storage.
    """
    error_quota = []
    ok_quota = []

    for quota in user_quota:
        user = VscTier2AccountpageUser(quota.user, rest_client=client)
        user.dry_run = options.dry_run

        try:
            if storage_name in ['VSC_HOME']:
                user.set_home_quota()

            if storage_name in ['VSC_DATA']:
                user.set_data_quota()

            if storage_name in ['VSC_SCRATCH_DELCATTY', 'VSC_SCRATCH_PHANPY']:
                user.set_scratch_quota(storage_name)

            ok_quota.append(quota)
        except Exception:
            log.exception("Cannot process user %s" % (user.user_id))
            error_quota.append(quota)

    return (ok_quota, error_quota)


def process_users(options, account_ids, storage_name, client):
    """
    Process the users.

    We make a distinction here between three types of filesystems.
        - home (unique)
            - create and populate the home directory
        - data (unique)
            - create the grouping fileset if needed
            - create the user data directory
        - scratch (multiple)
            - create the grouping fileset if needed
            - create the user scratch directory

    """
    error_users = []
    ok_users = []

    for vsc_id in sorted(account_ids):

        user = VscTier2AccountpageUser(vsc_id, rest_client=client)
        user.dry_run = options.dry_run

        try:
            if storage_name in ['VSC_HOME']:
                user.create_home_dir()
                user.populate_home_dir()
                update_user_status(user, client)

            if storage_name in ['VSC_DATA']:
                user.create_data_dir()

            if storage_name in ['VSC_SCRATCH_DELCATTY', 'VSC_SCRATCH_PHANPY']:
                user.create_scratch_dir(storage_name)

            ok_users.append(user)
        except Exception:
            log.exception("Cannot process user %s" % (user.user_id))
            error_users.append(user)

    return (ok_users, error_users)
