#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2013-2016 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
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
    def __init__(self):
        self.processed_accounts = None
        self.processed_pubkeys = None
        self.client = AccountpageClient() # TODO: things here
        pass

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
        pks =  [mkVscAccountPubkey(p) for p in self.client.account[p.vsc_id] if not p.deleted]
        if not pks:
            pks = [ACCOUNT_WITHOUT_PUBLIC_KEYS_MAGIC_STRING]
        return pks

    def sync_altered_pubkeys(last, dry_run=True):
        """
        Remove obsolete public keys from the LDAP and add new public keys to the LDAP.
        """
         changed_pubkeys = [mkVscAccountPubkey(p) for p in self.client.account.pubkey.modified.now.get()]

        pubkeys = {
            UPDATED: set(),
            DONE: set(),
            ERROR: set(),
            }

        new_pubkeys = [p for p in changed_pubkeys if not p.deleted]
        deleted_pubkeys = [p for p in changed_pubkeys if p.deleted]

        _log.warning("Deleted pubkeys %s" % (deleted_pubkeys,))
        _log.debug("New pubkeys %s", new_pubkeys)

        for p in changed_pubkeys:

            if not p.vsc_id:
                # This should NOT happen
                _log.error("Key %d had no associated user anymore",p)
                continue

            try:
                account = mkVscAccount(self.client.account[p.vsc_id])
            except HTTPError:
                _log.warning("No account for the user %s corresponding to the public key %d" % (p.vsc_id, p))
                continue

            if account in self.processed_accounts[NEW] or account in self.processed_accounts[UPDATED]:
                _log.info("Account %s was already processed and has the new set of public keys" % (account.vsc_id,))
                pubkeys[DONE].add(p)
                continue

            try:
                ldap_user = VscLdapUser(p.vsc_id)
                ldap_user.pubkey = [pk.pubkey for pk in pks]
                self.processed_accounts[UPDATED].add(account)
                pubkeys[UPDATED].add(p)
            except Exception:
                _log.warning("Cannot add pubkey for account %s to LDAP" % (account.vsc_id,))
                pubkeys[ERROR].add(p)

        self.processed_pubkeys = pubkeys


    def sync_altered_accounts(self, last, dry_run=True):
        """
        Add new users to the LDAP and update altered users. This does not include usergroups.

        @type last: datetime
        @type now: datetime
        @return: tuple (new, updated, error) that indicates what accounts were new, changed or could not be altered.
        """
         changed_accounts= [mkVscAccount(a) for a in self.client.account.modified.now.get()]
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
                #TODO: get usergroup here
                pass
            except UserGroup.DoesNotExist:
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
                #TODO: no longer needed?
                #'homeQuota': ["%d" % (home_quota,)],
                #'dataQuota': ["%d" % (data_quota,)],
                #'scratchQuota': ["%d" % (scratch_quota,)],
                'pubkey': public_keys,
                # TODO get usergroup with api
                'gidNumber': ["%s" % (account.usergroup.vsc_id_number,)],
                'loginShell': [str(account.login_shell)],
                # 'mukHomeOnScratch': ["FALSE"],  # FIXME, see #37
                'researchField': [account.research_field],
                'status': [str(account.status)],
            }

            result = add_or_update(VscLdapUser, account.vsc_id, ldap_attributes, dry_run)
            accounts[result].add(account)

        self.processed_accounts = accounts


    def sync_altered_user_groups(last, now, dry_run=True):
        """
        Add new usergroups to the LDAP and update altered usergroups.

        @type last: datetime
        @type now: datetime
        @return: tuple (new, updated, error) that indicates what usergroups were new, changed or could not be altered.
        """

        changed_usergroups = UserGroup.objects.filter(modify_timestamp__range=[last, now])

        _log.info("Found %d modified usergroups in the range %s until %s" % (len(changed_usergroups),
                                                                             last.strftime("%Y%m%d%H%M%SZ"),
                                                                             now.strftime("%Y%m%d%H%M%SZ")))

        _log.debug("Modified usergroups: %s", [g.vsc_id for g in changed_usergroups])

        groups = {
            NEW: set(),
            UPDATED: set(),
            ERROR: set(),
        }

        for usergroup in changed_usergroups:

            ldap_attributes = {
                'cn': str(usergroup.vsc_id),
                'institute': [str(usergroup.institute.site)],
                'gidNumber': ["%d" % (usergroup.vsc_id_number,)],
                'moderator': [str(usergroup.vsc_id)],  # a fixed single moderator!
                'memberUid': [str(usergroup.vsc_id)],  # a single member
                'status': [str(usergroup.status)],
            }

            result = add_or_update(VscLdapGroup, usergroup.vsc_id, ldap_attributes, dry_run)
            groups[result].add(usergroup)

        return groups


    def sync_altered_autogroups(dry_run=True):

        changed_autogroups = Autogroup.objects.all() # we always sync autogroups since we cannot know beforehand if their membership list changed

        _log.info("Found %d autogroups" % (len(changed_autogroups),))
        _log.debug("Autogroups: %s", [a.vsc_id for a in changed_autogroups])

        groups = {
            NEW: set(),
            UPDATED: set(),
            ERROR: set(),
        }

        for autogroup in changed_autogroups:

            ldap_attributes = {
                'cn': str(autogroup.vsc_id),
                'institute': [str(autogroup.institute.site)],
                'gidNumber': ["%d" % (autogroup.vsc_id_number,)],
                'moderator': [str(u.account.vsc_id) for u in DGroup.objects.get(name='administrator_%s' % (autogroup.institute.site,)).user_set.all()],
                'memberUid': [str(a.vsc_id) for a in autogroup.get_members()],
                'status': [str(autogroup.status)],
                'autogroup': [str(s.vsc_id) for s in autogroup.sources.all()],
            }

            _log.debug("Proposed changes for autogroup %s: %s", autogroup.vsc_id, ldap_attributes)

            result = add_or_update(VscLdapGroup, autogroup.vsc_id, ldap_attributes, dry_run)
            groups[result].add(autogroup)

        return groups


    def sync_altered_group_membership(last, now, processed_groups, dry_run=True):
        """
        Synchronise the memberships for groups when users are added/removed.
        """
        changed_members = Membership.objects.filter(modify_timestamp__range=[last, now])

        _log.info("Found %d modified members in the range %s until %s" % (len(changed_members),
                                                                          last.strftime("%Y%m%d%H%M%SZ"),
                                                                          now.strftime("%Y%m%d%H%M%SZ")))
        _log.debug("Modified members: %s", [m.account.vsc_id for m in changed_members])

        members = {
            NEW: set(),
            UPDATED: set(),
            ERROR: set(),
            DONE: set(),
        }

        newly_processed_groups = set()
        for member in changed_members:

            if member.group in processed_groups[NEW] \
                    or member.group in processed_groups[UPDATED] \
                    or member.group in newly_processed_groups:
                _log.info("Member %s was already processed with group %s" % (member.account.vsc_id, member.group))
                members[DONE].add(member)
                continue

            try:
                ldap_group = VscLdapGroup(member.group.vsc_id)
                ldap_group.status
                ldap_group.memberUid = [str(m.account.vsc_id) for m in Membership.objects.filter(group=member.group)]
            except Exception:
                _log.warning("Cannot add member %s to group %s" % (member.account.vsc_id, member.group.vsc_id))
                members[ERROR].add(member)
            else:
                newly_processed_groups.add(member.group)
                members[UPDATED].add(member)
                _log.info("Processed group %s member %s" % (member.group.vsc_id, member.account.vsc_id))

        return members


    def sync_altered_groups(last, now, dry_run=True):
        """
        Synchronise altered groups back to LDAP.
        """
        changed_groups = Group.objects.filter(modify_timestamp__range=[last, now])

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

            ldap_attributes = {
                'cn': str(group.vsc_id),
                'institute': [str(group.institute.site)],
                'gidNumber': ["%d" % (group.vsc_id_number,)],
                'moderator': [str(m.account.vsc_id) for m in Membership.objects.filter(moderator=True, group=group)],
                'memberUid': [str(a.vsc_id) for a in group.get_members()],
                'status': [str(group.status)],
            }

            _log.debug("Proposed changes for group %s: %s", group.vsc_id, ldap_attributes)

            result = add_or_update(VscLdapGroup, group.vsc_id, ldap_attributes, dry_run)
            groups[result].add(group)

        return groups


    def sync_altered_vo_membership(last, now, processed_vos, dry_run=True):
        """
        Synchronise the memberships for groups when users are added/removed.
        """
        changed_members = VoMembership.objects.filter(modify_timestamp__range=[last, now])

        _log.info("Found %d modified members in the range %s until %s" % (len(changed_members),
                                                                          last.strftime("%Y%m%d%H%M%SZ"),
                                                                          now.strftime("%Y%m%d%H%M%SZ")))
        _log.debug("Modified VO members: %s", [m.account.vsc_id for m in changed_members])
        members = {
            NEW: set(),
            UPDATED: set(),
            ERROR: set(),
            DONE: set(),
        }
        newly_processed_vos = set()

        for member in changed_members:

            if member.group in processed_vos[NEW] \
                    or member.group in processed_vos[UPDATED] \
                    or member.group in newly_processed_vos:
                _log.info("Member %s membership was already processed with group %s" % (member.account.vsc_id, member.group))
                members[DONE].add(member)
                continue

            try:
                ldap_group = VscLdapGroup(member.group.vsc_id)
                ldap_group.status
                ldap_group.memberUid = [str(m.account.vsc_id) for m in VoMembership.objects.filter(group=member.group)]
            except Exception:
                _log.warning("Cannot add member %s to group %s" % (member.account.vsc_id, member.group.vsc_id))
                members[ERROR].add(member)
            else:
                newly_processed_vos.add(member.group)
                members[UPDATED].add(member)
                _log.info("Processed group %s member %s" % (member.group.vsc_id, member.account.vsc_id))

        return members


    def sync_altered_VO(last, now, dry_run=True):
        """
        Synchronise altered VOs back to the LDAP.
        """
        changed_vos = VirtualOrganisation.objects.filter(modify_timestamp__range=[last, now])
        _log.info("Found %d modified vos in the range %s until %s" % (len(changed_vos),
                                                                      last.strftime("%Y%m%d%H%M%SZ"),
                                                                      now.strftime("%Y%m%d%H%M%SZ")))
        _log.debug("Modified VOs: %s", [v.vsc_id for v in changed_vos])

        vos = {
            NEW: set(),
            UPDATED: set(),
            ERROR: set(),
        }

        for vo in changed_vos:

            # Hack to deal with the anomaly that the VO admin actually 'belongs' to multiple VOs, only in the LDAP
            # the moderator need not be a member
            if vo.vsc_id in settings.VSC.institute_vos.values():
                moderators = ['vsc40024']
            else:
                moderators = [str(m.account.vsc_id) for m in VoMembership.objects.filter(moderator=True, group=vo)]

            ldap_attributes = {
                'cn': str(vo.vsc_id),
                'institute': [str(vo.institute.site)],
                'gidNumber': ["%d" % (vo.vsc_id_number,)],
                'moderator': moderators,
                'memberUid': [str(m.account.vsc_id) for m in VoMembership.objects.filter(group=vo)],
                'status': [str(vo.status)],
                'fairshare': ["%d" % (vo.fairshare,)],
                'description': [str(vo.description)],
                'dataDirectory': [str(vo.data_path)],
                'scratchDirectory': [str(vo.scratch_path)],
                #TODO: what do here?
                #'dataQuota': [str(data_quota)],
                #'scratchQuota': [str(scratch_quota)],
            }

            _log.debug("Proposed changes for VO %s: %s", vo.vsc_id, ldap_attributes)

            result = add_or_update(VscLdapGroup, vo.vsc_id, ldap_attributes, dry_run)
            vos[result].add(vo)

        return vos


def main():
    now = datetime.utcnow().replace(tzinfo=timezone.utc)

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'start-timestamp': ("The timestamp form which to start, otherwise use the cached value", None, "store", None),
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
            last_timestamp = "201404230000Z"
            _log.warning("We will resync from the beginning of the account page era, i.e. %s" % (last_timestamp,))

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
            syncer = LdapSyncer()
            syncer.sync_altered_accounts(last, now, opts.options.dry_run)
            syncer.sync_altered_pubkeys(last, now, opts.options.dry_run)
            # altered_users = sync_altered_users(last, now, altered_accounts)  # FIXME: no modification timestamps here :(

            _log.debug("Altered accounts: %s",  syncer.processed_accounts)
            _log.debug("Altered pubkeys: %s", syncer.altered_pubkeys)
            # _log.debug("Altered users: %s" % (altered_users,))

            altered_usergroups = sync_altered_user_groups(last, now, opts.options.dry_run)
            altered_autogroups = sync_altered_autogroups(opts.options.dry_run)

            altered_groups = sync_altered_groups(last, now, opts.options.dry_run)
            altered_members = sync_altered_group_membership(last, now, altered_groups, opts.options.dry_run)
            altered_vos = sync_altered_VO(last, now, opts.options.dry_run)
            altered_vo_members = sync_altered_vo_membership(last, now, altered_vos, opts.options.dry_run)


            _log.debug("Altered usergroups: %s" % (altered_usergroups,))
            _log.debug("Altered autogroups: %s" % (altered_autogroups,))
            _log.debug("Altered groups: %s" % (altered_groups,))
            _log.debug("Altered members: %s" % (altered_members,))
            _log.debug("Altered VOs: %s" % (altered_vos,))
            _log.debug("Altered VO members: %s" % (altered_vo_members,))

            if not altered_accounts[ERROR] \
                and not altered_groups[ERROR] \
                and not altered_vos[ERROR] \
                and not altered_members[ERROR] \
                and not altered_vo_members[ERROR] \
                and not altered_usergroups[ERROR] \
                _log.info("Child process exiting correctly")
                sys.exit(0)
            else:
                _log.info("Child process exiting with status -1")
                _log.warning("Error occured in %s" % (
                    ["%s: %s\n" % (k, v) for (k, v) in [
                        ("altered accounts", altered_accounts[ERROR]),
                        ("altered groups", altered_groups[ERROR]),
                        ("altered vos", altered_vos[ERROR]),
                        ("altered members", altered_members[ERROR]),
                        ("altered vo_members", altered_vo_members[ERROR]),
                        ("altered usergroups", altered_usergroups[ERROR]),
                    ]]
                ))
                sys.exit(-1)
        except Exception:
            _log.exception("Child caught an exception")
            sys.exit(-1)

    else:
        # parent
        (pid, result) = os.waitpid(parent_pid, 0)
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
