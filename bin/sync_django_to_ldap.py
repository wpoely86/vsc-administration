#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2013 Ghent University
#
# This file is part VSC-accountpage,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
Get existing Django accountpage users and sync them to the VSC LDAP
"""
import grp
import os
import pwd
import sys

from datetime import datetime

from ldap import LDAPError

from django.conf import settings
from django.contrib.auth.models import Group as DGroup
from django.contrib.auth.models import User
from django.utils.timezone import utc

from account.models import Account, Person, Pubkey, MailList
from group.models import Autogroup, Group, UserGroup, VirtualOrganisation, Membership, VoMembership
from host.models import Storage, Site
from quota.models import UserSizeQuota, VirtualOrganisationSizeQuota

from vsc.config.base import GENT

from vsc.ldap.configuration import VscConfiguration
from vsc.ldap.entities import VscLdapUser, VscLdapGroup
from vsc.ldap.filters import CnFilter
from vsc.ldap.timestamp import convert_timestamp, read_timestamp, write_timestamp
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.script_tools import ExtendedSimpleOption

NAGIOS_HEADER = "sync_django_to_ldap"
NAGIOS_CHECK_INTERVAL_THRESHOLD = 15 * 60  # 15 minutes
SYNC_TIMESTAMP_FILENAME = "/var/cache/%s.timestamp" % (NAGIOS_HEADER)


fancylogger.setLogLevelInfo()
fancylogger.logToScreen(True)

DONE = 'done'
NEW = 'new'
UPDATED = 'updated'
ERROR = 'error'


def add_or_update(VscLdapKlass, cn, ldap_attributes, dry_run):
    """
    Perform the update in LDAP for the given vsc.ldap.entitities class, cn and the ldap attributes.

    @return: NEW, UPDATED or ERROR, depending on the operation and its result.
    """
    ldap_entries = VscLdapKlass.lookup(CnFilter(cn))
    if not ldap_entries:
        # add the entry
        _log.info("add a new entry (%s) %s to LDAP with attributes %s" % (VscLdapKlass.__name__,
                                                                                         cn,
                                                                                         ldap_attributes))

        if not dry_run:
            try:
                entry = VscLdapKlass(cn)
                entry.add(ldap_attributes)
                _log.info("Added a new user %s to LDAP" % (cn,))
            except LDAPError:
                _log.exception("Could not add %s %s to LDAP" % (VscLdapKlass.__name__, cn,))
                return ERROR
        return NEW
    else:
        ldap_entries[0].status
        _log.info("update existing entry %s %s with the attributes %s -- old entry: %s" % (VscLdapKlass.__name__,
                                                                                           cn,
                                                                                           ldap_attributes,
                                                                                           ldap_entries[0].ldap_info))
        if not dry_run:
            try:
                ldap_entries[0].modify_ldap(ldap_attributes)
                _log.info("Modified %s %s in LDAP" % (VscLdapKlass.__name__, cn,))
            except LDAPError:
                _log.exception("Could not add %s %s to LDAP" % (VscLdapKlass.__name__, cn,))
                return ERROR
        return UPDATED


def sync_altered_pubkeys(last, now, processed_accounts=None, dry_run=True):
    """
    Remove obsolete public keys from the LDAP and add new public keys to the LDAP.
    """
    changed_pubkeys = Pubkey.objects.filter(modify_timestamp__range=[last, now])

    pubkeys = {
        UPDATED: set(),
        DONE: set(),
        ERROR: set(),
        }

    new_pubkeys = [p for p in changed_pubkeys if not p.deleted]
    deleted_pubkeys = [p for p in changed_pubkeys if p.deleted]

    _log.warning("Deleted pubkeys %s" % (deleted_pubkeys,))
    _log.debug("New pubkeys %s" % (new_pubkeys,))

    for p in changed_pubkeys:

        if not p.user:
            # This should NOT happen
            _log.error("Key %d had no associated user any more: %s" % (p.pk, p))
            continue

        try:
            account = p.user.account
        except User.DoesNotExist:
            _log.error("No user found for the given public key %d" % (p.pk,))
            continue
        except Account.DoesNotExist:
            _log.error("No account for the user %s corresponding to the public key %d" % (p.user.username, p.pk))
            continue

        if account in processed_accounts[NEW] or account in processed_accounts[UPDATED]:
            _log.info("Account %s was already processed and has the new set of public keys" % (account.vsc_id,))
            pubkeys[DONE].add(p)
            continue

        try:
            pks = Pubkey.objects.filter(user=p.user, deleted=False)
            ldap_user = VscLdapUser(account.vsc_id)
            ldap_user.pubkey = [p.pubkey for p in pks]
            processed_accounts[UPDATED].add(account)
            pubkeys[UPDATED].add(p)
        except Exception:
            _log.exception("Cannot add pubkey for account %s to LDAP" % (account.vsc_id,))
            pubkeys[ERROR].add(p)

    return pubkeys


def sync_altered_users(last, now, processed_accounts, dry_run=True):
    """
    The only thing that can be changed is the email address, but we should sync that too.
    """
    changed_users = User.objects.filter(modify_timestamp__range=[last, now])

    _log.info("Changed users: %s" % ([u.username for u in changed_users]))

    users = {
        UPDATED: set(),
        DONE: set(),
        ERROR: set(),
    }

    for u in changed_users:

        if u.account in processed_accounts[NEW] or u in processed_accounts[UPDATED]:
            _log.info("Account %s was already processed and has the new email address" % (u.account.vsc_id,))
            users[DONE].add(u)
            continue

        try:
            ldap_user = VscLdapUser(u.account.vsc_id)
            ldap_user.mail = u.email
            processed_accounts[UPDATED].add(u.account)
            users[UPDATED].add(u)
        except Exception:
            _log.exception("Cannot change email address to %s for %s in LDAP" % (u.email, u.account.vsc_id))
            users[ERROR].add(u)

    return users


def sync_altered_accounts(last, now, dry_run=True):
    """
    Add new users to the LDAP and update altered users. This does not include usergroups.

    @type last: datetime
    @type now: datetime
    @return: tuple (new, updated, error) that indicates what accounts were new, changed or could not be altered.
    """
    changed_accounts = Account.objects.filter(modify_timestamp__range=[last, now])

    accounts = {
        NEW: set(),
        UPDATED: set(),
        ERROR: set(),
    }

    sync_accounts = list(changed_accounts)

    _log.info("Found %d modified accounts in the range %s until %s" % (len(sync_accounts),
                                                                       last.strftime("%Y%m%d%H%M%SZ"),
                                                                       now.strftime("%Y%m%d%H%M%SZ")))
    _log.info("Modified accounts: %s" % ([a.vsc_id for a in sync_accounts],))


    for account in sync_accounts:

        try:
            home_storage = Storage.objects.get(storage_type=settings.HOME, institute=account.user.person.institute)
            home_quota = UserSizeQuota.objects.get(user=account, storage=home_storage).hard
        except UserSizeQuota.DoesNotExist:
            home_quota = 0
            _log.error("Could not find quota information for %s on %s, setting to 0" % (account.vsc_id, home_storage.name))
        except Storage.DoesNotExist:
            home_quota = 0
            _log.error("No home storage for institute %s defined, setting quota to 0" % (account.user.person.institute,))
        except User.DoesNotExist:
            _log.error("No corresponding User for account %s" % (account.vsc_id,))
            continue
        except Person.DoesNotExist:
            _log.error("No corresponding Person for account %s" % (account.vsc_id,))
            continue

        try:
            data_storage = Storage.objects.get(storage_type=settings.DATA, institute=account.user.person.institute)
            data_quota_ = UserSizeQuota.objects.filter(user=account, storage=data_storage)
            if len(list(data_quota_)) > 1:
                # this is the UGent case; we need to further distinguish between our various filesets, in
                # this case the vscxyz fileset
                data_quota = data_quota_.get(fileset=account.vsc_id[:6]).hard
            else:
                data_quota = data_quota_[0].hard
        except (UserSizeQuota.DoesNotExist, IndexError):
            data_quota = 0
            _log.error("Could not find quota information for %s on %s, setting to 0" % (account.vsc_id, data_storage.name))
        except Storage.DoesNotExist:
            data_quota = 0
            _log.error("No data storage for institute %s defined, setting quota to 0" % (account.user.person.institute,))

        try:
            scratch_storage = Storage.objects.filter(storage_type=settings.SCRATCH, institute=account.user.person.institute)
            if not scratch_storage:
                raise Storage.DoesNotExist("No scratch storage for institute %s" % (account.user.person.institute,))

            if account.user.person.institute in (Site.objects.get(site=GENT),):
                scratch_storage = scratch_storage.filter(name='VSC_SCRATCH_GENGAR')[0]  # TODO: This can be ignored once we go to sync from django, skipping the LDAP completely
            else:
                scratch_storage = scratch_storage[0]

            scratch_quota_ = UserSizeQuota.objects.filter(user=account, storage=scratch_storage)  # take the first one
            if len(list(scratch_quota_)) > 1:
                # this is the UGent case; we need to further distinguish between our various filesets, in
                # this case the vscxyz fileset
                scratch_quota = scratch_quota_.get(fileset=account.vsc_id[:6]).hard
            else:
                scratch_quota = scratch_quota_[0].hard
        except (UserSizeQuota.DoesNotExist, IndexError):
            scratch_quota = 0
            _log.error("Could not find quota information for %s on %s, setting to 0" % (account.vsc_id, scratch_storage.name))
        except Storage.DoesNotExist:
            scratch_quota = 0
            _log.error("No scratch storage for institute %s defined, setting quota to 0" % (account.user.person.institute,))

        try:
            try:
                gecos = str(account.user.person.gecos)
            except UnicodeEncodeError:
                gecos = account.user.person.gecos.encode('ascii', 'ignore')
                _log.warning("Converting unicode to ascii for gecos resulting in %s", gecos)

            ldap_attributes = {
                'cn': str(account.vsc_id),
                'uidNumber': ["%s" % (account.vsc_id_number,)],
                'gecos': [gecos],
                'mail': [str(account.user.email)],
                'institute': [str(account.user.person.institute.site)],
                'instituteLogin': [str(account.user.person.institute_login)],
                'uid': [str(account.vsc_id)],
                'homeDirectory': [str(account.home_directory)],
                'dataDirectory': [str(account.data_directory)],
                'scratchDirectory': [str(account.scratch_directory)],
                'homeQuota': ["%d" % (home_quota,)],
                'dataQuota': ["%d" % (data_quota,)],
                'scratchQuota': ["%d" % (scratch_quota,)],
                'pubkey': [str(p.pubkey) for p in Pubkey.objects.filter(user=account.user, deleted=False)],
                'gidNumber': ["%s" % (account.usergroup.vsc_id_number,)],
                'loginShell': [str(account.login_shell)],
                'mukHomeOnScratch': ["FALSE"],  # FIXME, see #37
                'researchField': ["unknown"],
                'status': [str(account.status)],
            }
        except UserGroup.DoesNotExist:
            _log.error("No corresponding UserGroup for user %s" % (account.vsc_id,))
            continue

        result = add_or_update(VscLdapUser, account.vsc_id, ldap_attributes, dry_run)
        accounts[result].add(account)

    return accounts


def sync_altered_user_quota(last, now, altered_accounts, dry_run=True):
    """
    Check for users who have altered quota and sync these to the LDAP.

    @type last: datetime
    @type now: datetime
    @return: tuple (new, updated, error) that indicates what accounts were new, changed or could not be altered.
    """

    changed_quota = UserSizeQuota.objects.filter(modify_timestamp__range=[last, now])

    _log.info("Found %d modified quota in the range %s until %s" % (len(changed_quota),
                                                                    last.strftime("%Y%m%d%H%M%SZ"),
                                                                    now.strftime("%Y%m%d%H%M%SZ")))
    quotas = {
        NEW: set(),
        UPDATED: set(),
        ERROR: set(),
        DONE: set(),
        }

    for quota in changed_quota:
        account = quota.user
        if account in altered_accounts[NEW] or account in altered_accounts[UPDATED]:
            _log.info("Quota %s was already processed with account %s" % (quota, account.vsc_id))
            quotas[DONE].add(quota)
            continue

        try:
            ldap_user = VscLdapUser(account.vsc_id)
            ldap_user.status
            if quota.storage.storage_type in (settings.HOME,):
                ldap_user.homeQuota = "%d" % (quota.hard,)
                quotas[UPDATED].add(quota)
            elif quota.storage.storage_type in (settings.DATA,):
                ldap_user.dataQuota = "%d" % (quota.hard,)
                quotas[UPDATED].add(quota)
            elif quota.storage.storage_type in (settings.SCRATCH,):
                ldap_user.scratchQuota = "%d" (quota.hard,)
                quotas[UPDATED].add(quota)
            else:
                _log.warning("Cannot sync quota to LDAP (storage type %s unknown)" % (quota.storage.storage_type,))

        except Exception:
            _log.exception("Cannot update quota %s" % (quota,))
            quotas[ERROR].add(quota)

    return quotas

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

    _log.info("Modified usergroups: %s" % ([g.vsc_id for g in changed_usergroups],))

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
    _log.info("Autogroups: %s" % ([a.vsc_id for a in changed_autogroups],))

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

        _log.info("Proposed changes for autogroup %s: %s" % (autogroup.vsc_id, ldap_attributes))

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
    _log.info("Modified members: %s" % ([m.account.vsc_id for m in changed_members],))

    members = {
        NEW: set(),
        UPDATED: set(),
        ERROR: set(),
        DONE: set(),
    }

    for member in changed_members:

        if member.group in processed_groups[NEW] or member.group in processed_groups[UPDATED]:
            _log.info("Member %s was already processed with group %s" % (member.account.vsc_id, member.group))
            members[DONE].add(member)
            continue

        try:
            ldap_group = VscLdapGroup(member.group.vsc_id)
            ldap_group.status
            ldap_group.memberUid = [str(m.account.vsc_id) for m in Membership.objects.filter(group=member.group)]
            processed_groups[UPDATED].add(member.group)
            members[UPDATED].add(member)
        except Exception:
            _log.exception("Cannot add member %s to group %s" % (member.account.vsc_id, member.group.vsc_id))
            members[ERROR].add(member)

    return members


def sync_altered_groups(last, now, dry_run=True):
    """
    Synchronise altered groups back to LDAP.
    """
    changed_groups = Group.objects.filter(modify_timestamp__range=[last, now])

    _log.info("Found %d modified groups in the range %s until %s" % (len(changed_groups),
                                                                     last.strftime("%Y%m%d%H%M%SZ"),
                                                                     now.strftime("%Y%m%d%H%M%SZ")))
    _log.info("Modified groups: %s" % ([g.vsc_id for g in changed_groups],))
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

        _log.info("Proposed changes for group %s: %s" % (group.vsc_id, ldap_attributes))

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
    _log.info("Modified VO members: %s" % ([m.account.vsc_id for m in changed_members],))
    members = {
        NEW: set(),
        UPDATED: set(),
        ERROR: set(),
        DONE: set(),
    }

    for member in changed_members:

        if member.group in processed_vos[NEW] or member.group in processed_vos[UPDATED]:
            _log.info("Member %s membership was already processed with group %s" % (member.account.vsc_id, member.group))
            members[DONE].add(member)
            continue

        try:
            ldap_group = VscLdapGroup(member.group.vsc_id)
            ldap_group.status
            ldap_group.memberUid = [str(m.account.vsc_id) for m in VoMembership.objects.filter(group=member.group)]
            processed_vos[UPDATED].add(member.group)
            members[UPDATED].add(member)
        except Exception:
            _log.exception("Cannot add member %s to group %s" % (member.account.vsc_id, member.group.vsc_id))
            members[ERROR].add(member)

    return members


def sync_altered_VO(last, now, dry_run=True):
    """
    Synchronise altered VOs back to the LDAP.
    """
    changed_vos = VirtualOrganisation.objects.filter(modify_timestamp__range=[last, now])
    _log.info("Found %d modified vos in the range %s until %s" % (len(changed_vos),
                                                                  last.strftime("%Y%m%d%H%M%SZ"),
                                                                  now.strftime("%Y%m%d%H%M%SZ")))
    _log.info("Modified VOs: %s" % ([v.vsc_id for v in changed_vos],))

    vos = {
        NEW: set(),
        UPDATED: set(),
        ERROR: set(),
    }

    for vo in changed_vos:



        try:
            data_storage = Storage.objects.get(storage_type=settings.DATA, institute=vo.institute)
            data_quota = VirtualOrganisationSizeQuota.objects.get(virtual_organisation=vo, storage=data_storage, fileset=vo.vsc_id).hard
        except (VirtualOrganisationSizeQuota.DoesNotExist, IndexError):
            data_quota = 0
            _log.error("Could not find VO quota information for %s on %s, setting to 0" % (vo.vsc_id, data_storage.name))
        except Storage.DoesNotExist:
            data_quota = 0
            _log.error("No VO data storage for institute %s defined, setting quota to 0" % (vo.institute,))

        try:
            scratch_storage = Storage.objects.filter(storage_type=settings.SCRATCH, institute=vo.institute)
            scratch_quota = VirtualOrganisationSizeQuota.objects.get(virtual_organisation=vo, storage=scratch_storage[0], fileset=vo.vsc_id).hard  # take the first one
        except (VirtualOrganisationSizeQuota.DoesNotExist, IndexError):
            scratch_quota = 0
            _log.error("Could not find VO quota information for %s on %s, setting to 0" % (vo.vsc_id, data_storage.name))
        except Storage.DoesNotExist:
            scratch_quota = 0
            _log.error("No VO scratch storage for institute %s defined, setting quota to 0" % (vo.institute,))

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
            'dataQuota': [str(data_quota)],
            'scratchQuota': [str(scratch_quota)],
        }

        _log.info("Proposed changes for VO %s: %s" % (vo.vsc_id, ldap_attributes))

        result = add_or_update(VscLdapGroup, vo.vsc_id, ldap_attributes, dry_run)
        vos[result].add(vo)

    return vos


def sync_altered_vo_quota(last, now, altered_vos, dry_run=True):
    """
    Sync the changed quota for the VO to the LDAP
    """
    changed_quota = VirtualOrganisationSizeQuota.objects.filter(modify_timestamp__range=[last, now])

    _log.info("Found %d modified quota in the range %s until %s" % (len(changed_quota),
                                                                    last.strftime("%Y%m%d%H%M%SZ"),
                                                                    now.strftime("%Y%m%d%H%M%SZ")))
    quotas = {
        NEW: set(),
        UPDATED: set(),
        ERROR: set(),
        DONE: set(),
        }

    for quota in changed_quota:
        virtual_organisation = quota.virtual_organisation
        if virtual_organisation in altered_vos[NEW] or virtual_organisation in altered_vos[UPDATED]:
            _log.info("Quota %s was already processed with VO %s" % (quota, virtual_organisation.vsc_id))
            quotas[DONE].add(quota)
            continue

        try:
            ldap_group = VscLdapGroup(virtual_organisation.vsc_id)
            ldap_group.status
            if quota.storage.storage_type in (settings.HOME,):
                ldap_group.homeQuota = "%d" % (quota.hard,)
                quotas[UPDATED].add(quota)
            elif quota.storage.storage_type in (settings.DATA,):
                ldap_group.dataQuota = "%d" % (quota.hard,)
                quotas[UPDATED].add(quota)
            elif quota.storage.storage_type in (settings.SCRATCH,):
                ldap_group.scratchQuota = "%d" (quota.hard,)
                quotas[UPDATED].add(quota)
            else:
                _log.warning("Cannot sync quota to LDAP (storage type %s unknown)" % (quota.storage.storage_type,))

        except Exception:
            _log.exception("Cannot update quota %s" % (quota,))
            quotas[ERROR].add(quota)

    return quotas

def main():
    _log = fancylogger.getLogger(NAGIOS_HEADER)
    _log.propagate = False

    options = {
        'nagios-check-interval-threshold': NAGIOS_CHECK_INTERVAL_THRESHOLD,
        'start-timestamp': ("The timestamp form which to start, otherwise use the cached value", None, "store", None),
        }
    opts = ExtendedSimpleOption(options)
    stats = {}

    l = LdapQuery(VscConfiguration('/etc/vsc.conf.new'))

    last_timestamp = opts.options.start_timestamp
    if not last_timestamp:
        try:
            last_timestamp = read_timestamp(SYNC_TIMESTAMP_FILENAME)
        except Exception:
            _log.exception("Something broke reading the timestamp from %s" % SYNC_TIMESTAMP_FILENAME)
            last_timestamp = "201404230000Z"
            _log.warning("We will resync from the beginning of the account page era, i.e. %s" % (last_timestamp,))

    _log.info("Using timestamp %s" % (last_timestamp))

    parent_pid = os.fork()

    if parent_pid == 0:
        try:
            _log = fancylogger.getLogger(NAGIOS_HEADER)
            _log.propagate = False
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

            last = datetime.strptime(last_timestamp, "%Y%m%d%H%M%SZ").replace(tzinfo=utc)
            now = datetime.utcnow().replace(tzinfo=utc)

            altered_accounts = sync_altered_accounts(last, now, opts.options.dry_run)
            altered_pubkeys = sync_altered_pubkeys(last, now, altered_accounts, opts.options.dry_run)
            # altered_users = sync_altered_users(last, now, altered_accounts)  # FIXME: no modification timestamps here :(

            _log.debug("Altered accounts: %s" % (altered_accounts,))
            _log.debug("Altered pubkeys: %s" % (altered_pubkeys,))
            # _log.debug("Altered users: %s" % (altered_users,))

            altered_usergroups = sync_altered_user_groups(last, now, opts.options.dry_run)
            altered_autogroups = sync_altered_autogroups(opts.options.dry_run)

            altered_groups = sync_altered_groups(last, now, opts.options.dry_run)
            altered_members = sync_altered_group_membership(last, now, altered_groups, opts.options.dry_run)
            altered_vos = sync_altered_VO(last, now, opts.options.dry_run)
            altered_vo_members = sync_altered_vo_membership(last, now, altered_vos, opts.options.dry_run)

            altered_user_quota = sync_altered_user_quota(last, now, altered_accounts, opts.options.dry_run)
            altered_vo_quota = sync_altered_vo_quota(last, now, altered_vos, opts.options.dry_run)

            _log.debug("Altered usergroups: %s" % (altered_usergroups,))
            _log.debug("Altered autogroups: %s" % (altered_autogroups,))
            _log.debug("Altered groups: %s" % (altered_groups,))
            _log.debug("Altered members: %s" % (altered_members,))
            _log.debug("Altered VOs: %s" % (altered_vos,))
            _log.debug("Altered VO members: %s" % (altered_vo_members,))
            _log.debug("Altered user quota: %s" % (altered_user_quota,))
            _log.debug("Altered VO quota: %s" % (altered_vo_quota,))

            if not altered_accounts[ERROR] \
                and not altered_groups[ERROR] \
                and not altered_vos[ERROR] \
                and not altered_members[ERROR] \
                and not altered_vo_members[ERROR] \
                and not altered_usergroups[ERROR] \
                and not altered_user_quota[ERROR] \
                and not altered_vo_quota[ERROR]:
                _log.info("Child process exiting correctly")
                sys.exit(0)
            else:
                _log.info("Child process exiting with status -1")
                sys.exit(-1)
        except Exception:
            _log.exception("Child caught an exception")
            sys.exit(-1)

    else:
        # parent
        (pid, result) = os.waitpid(parent_pid, 0)
        _log.info("Child exited with exit code %d" % (result,))

        if not result and not opts.options.dry_run and not opts.options.start_timestamp:
            (_, ldap_timestamp) = convert_timestamp()
            if not opts.options.dry_run:
                write_timestamp(SYNC_TIMESTAMP_FILENAME, ldap_timestamp)
            else:
                _log.info("Not updating the timestamp, since we had at least one error during sync")
        else:
            _log.info("Not updating the timestamp, since it was given on the command line for this run")

        opts.epilogue("Synchronised LDAP users to the Django DB", stats)


if __name__ == '__main__':
    main()
