#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2013-2017 Ghent University
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
Get existing Django accountpage users and sync them to the VSC LDAP
"""
import grp
import os
import pwd
import sys

from datetime import datetime, timezone

from ldap import LDAPError
from vsc.config.base import GENT, ACTIVE, VSC_CONF_DEFAULT_FILENAME

from vsc.accountpage.client import AccountpageClient
from vsc.accountpage.wrappers import mkVscAutogroup, mkVscGroup, mkVscAccountPubkey, mkVscUserGroup

from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.entities import VscLdapUser, VscLdapGroup
from vsc.ldap.filters import CnFilter
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.nagios import NAGIOS_EXIT_CRITICAL
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = "sync_django_to_ldap"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes
SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)

ACCOUNT_WITHOUT_PUBLIC_KEYS_MAGIC_STRING = "THIS ACCOUNT HAS NO VALID PUBLIC KEYS"

fancylogger.setLogLevelInfo()
fancylogger.logToScreen(True)
_log = fancylogger.getLogger(NAGIOS_HEADER)

DONE = 'done'
NEW = 'new'
UPDATED = 'updated'
ERROR = 'error'


def class LdapSyncer(object):
    """
    This class implements a system for syncing changes from the accountpage api
    to the vsc ldap
    """
    def __init__(self, client):
        self.client = client

    def add_or_update(self, VscLdapKlass, cn, ldap_attributes, dry_run):
        """
        Perform the update in LDAP for the given vsc.ldap.entitities class, cn and the ldap attributes.

        @return: NEW, UPDATED or ERROR, depending on the operation and its result.
        """
        ldap_entries = self.vscldapclass.lookup(CnFilter(cn))
        if not ldap_entries:
            # add the entry
            _log.debug("add new entry %s %s with the attributes %s", VscLdapKlass.__name__, cn, ldap_attributes)

            if not dry_run:
                try:
                    entry = VscLdapKlass(cn)
                    entry.add(ldap_attributes)
                    _log.info("Added a new user %s to LDAP" % (cn,))
                except LDAPError:
                    _log.warning("Could not add %s %s to LDAP" % (VscLdapKlass.__name__, cn,))
                    return ERROR
            return NEW
        else:
            ldap_entries[0].status
            _log.debug("update existing entry %s %s with the attributes %s -- old entry: %s",
                       VscLdapKlass.__name__, cn, ldap_attributes, ldap_entries[0].ldap_info)

            if not dry_run:
                try:
                    ldap_entries[0].modify_ldap(ldap_attributes)
                    _log.info("Modified %s %s in LDAP" % (VscLdapKlass.__name__, cn,))
                except LDAPError:
                    _log.warning("Could not add %s %s to LDAP" % (VscLdapKlass.__name__, cn,))
                    return ERROR
            return UPDATED


    def get_public_keys(self, vsc_id):
        """Get a list of public keys for a given vsc id"""
        #TODO: check deleted syntax
        pks =  [mkVscAccountPubkey(p) for p in self.client.account[p.vsc_id].pubkey if not p['deleted']]
        if not pks:
            pks = [ACCOUNT_WITHOUT_PUBLIC_KEYS_MAGIC_STRING]
        return pks

    def sync_altered_accounts(self, last, dry_run=True):
        """
        Add new users to the LDAP and update altered users. This does not include usergroups.

        this does include pubkeys
        @type last: datetime
        @return: tuple (new, updated, error) that indicates what accounts were new, changed or could not be altered.
        """
        changed_accounts= [mkVscAccount(a) for a in self.client.account.modified[last].get()[1]]
        now = datetime.now()

        accounts = {
            NEW: set(),
            UPDATED: set(),
            ERROR: set(),
        }

        sync_accounts = list(changed_accounts)

        _log.info("Found %d modified accounts in the range %s until %s" % (len(sync_accounts),
                                                                           last.strftime("%Y%m%d%H%M%SZ"),
                                                                           now.strftime("%Y%m%d%H%M%SZ")))
        _log.debug("Modified accounts: %s", [a.vsc_id for a in sync_accounts])

        for account in sync_accounts:
            try:
                usergroup = mkVscUserGroup(client.account[account.vsc_id].usergroup.get()[1])
            except HTTPError:
                _log.error("No corresponding UserGroup for user %s" % (account.vsc_id,))
                continue
           try:
                gecos = str(account.user.person.gecos)
            except UnicodeEncodeError:
                gecos = account.person.gecos.encode('ascii', 'ignore')
                _log.warning("Converting unicode to ascii for gecos resulting in %s", gecos)

            public_keys = self.get_public_keys(account.vsc_id)

            ldap_attributes = {
                'cn': str(account.vsc_id),
                'uidNumber': ["%s" % (account.vsc_id_number,)],
                'gecos': [gecos],
                'mail': [str(account.email)],
                'institute': [str(account.person.institute)],
                'instituteLogin': [str(account.person.institute_login)],
                'uid': [str(account.vsc_id)],
                'homeDirectory': [str(account.home_directory)],
                'dataDirectory': [str(account.data_directory)],
                'scratchDirectory': [str(account.scratch_directory)],
                #TODO: fill in
                #'homeQuota': ["%d" % (home_quota,)],
                #'dataQuota': ["%d" % (data_quota,)],
                #'scratchQuota': ["%d" % (scratch_quota,)],
                'pubkey': public_keys,
                'gidNumber': [str(usergroup.vsc_id_number)],
                'loginShell': [str(account.login_shell)],
                # 'mukHomeOnScratch': ["FALSE"],  # FIXME, see #37
                'researchField': [account.research_field],
                'status': [str(account.status)],
            }
                        result = add_or_update(VscLdapUser, account.vsc_id, ldap_attributes, dry_run)
            accounts[result].add(account)

        return accounts

    def sync_altered_groups(self, last, now, dry_run=True):
        """
        Synchronise altered groups back to LDAP.
        This also includes usergroups
        """
        changed_groups= [mkGroup(a) for a in self.client.allgroups.modified[last].get()[1]]

        _log.info("Found %d modified groups in the range %s until %s" % (len(changed_groups),
                                                                         last.strftime("%Y%m%d%H%M%SZ"),
                                                                         now.strftime("%Y%m%d%H%M%SZ")))
        _log.debug("Modified groups: %s", [g.vsc_id for g in changed_groups])
        groups = {
            NEW: set(),
            UPDATED: set(),
            ERROR: set(),
       }

        for group in changed_groups:
            vo = False
            try:
                vo = mkVo(self.client.vo[group.vsc_id].get()[1])
                voquota = self.client.vo[group.vsc_id].quota.get()[1]
            except HTTPError as err:
                # if a 404 occured, the autogroup does not exist, otherwise something else went wrong.
                if err.code != 404:
                    raise

            ldap_attributes = {
                'cn': str(group.vsc_id),
                'institute': [str(group.institute)],
                'gidNumber': ["%d" % (group.vsc_id_number,)],
                'moderator': [str(m['vsc_id']) for m in group.moderators],
                'memberUid': [str(a['vsc_id') for a in group.members],
                'status': [str(group.status)],
            }
            if vo:
                ldap_attributes['fairshare'] = ["%d" % (vo.fairshare,)]
                ldap_attributes['description'] = [str(vo.description)]
                ldap_attributes['dataDirectory'] = [str(vo.data_path)]
                ldap_attributes['scratchDirectory'] = [str(vo.scratch_path)]
                #TODO: fix quota: have proper api documentation
                #ldap_attributes['dataQuota'] = [str(vo_quota[)],
                #ldap_attributes['scratchQuota'] = [str(vo_quota[)],
            }

            _log.debug("Proposed changes for group %s: %s", group.vsc_id, ldap_attributes)

            result = add_or_update(VscLdapGroup, group.vsc_id, ldap_attributes, dry_run)
            groups[result].add(group)

        return groups


def main():
    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'start-timestamp': ("The timestamp form which to start, otherwise use the cached value", None, "store", None),
        'access_token': ('OAuth2 token identifying the user with the accountpage', None, 'store', None),
        }
    opts = ExtendedSimpleOption(options)
    stats = {}

    # Creating this here because this is a singleton class
    _ = LdapQuery(VscConfiguration(VSC_CONF_DEFAULT_FILENAME))

    last_timestamp = opts.options.start_timestamp
    if not last_timestamp:
        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except Exception:
            _log.warning("Something broke reading the timestamp from %s" % SYNC_TIMESTAMP_FILENAME)
            last_timestamp = "201604230000Z"
            _log.warning("We will resync from a while back : %s" % (last_timestamp,))

    _log.info("Using timestamp %s" % (last_timestamp))

    try:
        parent_pid = os.fork()
        _log.info("Forked.")
    except OSError:
        _log.exception("Could not fork")
        parent_pid = 1
    except Exception:
        _log.exception("Oops")
        parent_pid = 1

    if parent_pid == 0:
        try:
            global _log
            _log = fancylogger.getLogger(NAGIOS_HEADER)
            # drop privileges in the child
            try:
                apache_uid = pwd.getpwnam('apache').pw_uid
                apache_gid = grp.getgrnam('apache').gr_gid

                os.setgroups([])
                os.setgid(apache_gid)
                os.setuid(apache_uid)

                _log.info("Now running as %s" % (os.geteuid(),))
            except OSError:
                _log.raiseException("Could not drop privileges")
            last = datetime.strptime(last_timestamp, "%Y%m%d%H%M%SZ").replace(tzinfo=timezone.utc)

            client = AccountpageClient(token=opts.options.access_token)
            syncer = LdapSyncer(client)
            altered_accounts = syncer.sync_altered_accounts(last, now, opts.options.dry_run)

            _log.debug("Altered accounts: %s",  syncer.processed_accounts)

            altered_groups = syncer.sync_altered_groups(last, now, opts.options.dry_run)

            _log.debug("Altered groups: %s" % (altered_groups,))

            if not altered_accounts[ERROR] \
                and not altered_groups[ERROR] \
                _log.info("Child process exiting correctly")
                sys.exit(0)
            else:
                _log.info("Child process exiting with status -1")
                _log.warning("Error occured in %s" % (
                    ["%s: %s\n" % (k, v) for (k, v) in [
                        ("altered accounts", altered_accounts[ERROR]),
                        ("altered groups", altered_groups[ERROR]),
                    ]]
                ))
                sys.exit(-1)
        except Exception:
            _log.exception("Child caught an exception")
            sys.exit(-1)

    else:
        # parent
        (_, result) = os.waitpid(parent_pid, 0)
        _log.info("Child exited with exit code %d" % (result,))

        if not result:
            if not opts.options.start_timestamp:
                (_, ldap_timestamp) = convert_timestamp(now)
                if not opts.options.dry_run:
                    write_timestamp(SYNC_TIMESTAMP_FILENAME, ldap_timestamp)
            else:
                _log.info("Not updating the timestamp, since one was provided on the command line")
            opts.epilogue("Synchronised LDAP users to the Django DB", stats)
        else:
            _log.info("Not updating the timestamp, since it was given on the command line for this run")
            sys.exit(NAGIOS_EXIT_CRITICAL)


if __name__ == '__main__':
    main()
