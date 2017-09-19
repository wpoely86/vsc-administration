#
# Copyright 2015-2017 Ghent University
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
Tests for vsc.administration.user

@author: Andy Georges (Ghent University)
@author: Jens Timmerman (Ghent University)
"""
import mock

import vsc

from vsc.install.testing import TestCase
from vsc.accountpage.wrappers import mkVscAccountPubkey, mkVscAccount

from vsc.administration.ldapsync import LdapSyncer, UPDATED
from vsc.ldap.entities import VscLdapUser

from .user import test_account_1, test_usergroup_1, test_pubkeys_1

test_quota = [
    {
        "user": "vsc10018",
        "storage": {
            "institute": "brussel",
            "name": "VSC_HOME",
            "storage_type": "home"
        },
        "fileset": "None",
        "hard": 5242880
    }
]


class LDAPSyncerTest(TestCase):
    """
    Tests for the LDAP syncer that sync account page information to the vsc ldap.
    """

    @mock.patch.object(vsc.administration.ldapsync.LdapSyncer, 'add_or_update')
    def test_sync_altered_accounts(self, mock_add_or_update):
        """Test the sync_altered accounts function"""
        mock_client = mock.MagicMock()
        test_account = mkVscAccount(test_account_1)
        mock_client.account[test_account.vsc_id] = mock.MagicMock()
        mock_client.account.modified[1].get.return_value = (200, [test_account_1])
        mock_client.account[test_account.vsc_id].usergroup.get.return_value = (200, test_usergroup_1)
        mock_client.get_public_keys.return_value = [mkVscAccountPubkey(p) for p in test_pubkeys_1]
        mock_client.account[test_account.vsc_id].quota.get.return_value = (200, test_quota)

        mock_add_or_update.return_value = UPDATED
        ldapsyncer = LdapSyncer(mock_client)
        accounts = ldapsyncer.sync_altered_accounts(1)
        self.assertEqual(accounts, {'error': set([]), 'new': set([]), 'updated': set([test_account.vsc_id])})
        ldap_attrs =  {'status': ['active'], 'dataDirectory': ['/user/data/gent/vsc400/vsc40075'], 'cn': 'vsc40075', 'homeQuota': ['5242880'], 'loginShell': ['/bin/bash'], 'uidNumber': ['2540075'], 'gidNumber': ['2540075'], 'instituteLogin': ['foobar'], 'uid': ['vsc40075'], 'scratchDirectory': ['/user/scratch/gent/vsc400/vsc40075'], 'institute': ['gent'], 'researchField': ['Bollocks'], 'gecos': ['Foo Bar'], 'homeDirectory': ['/user/home/gent/vsc400/vsc40075'], 'mail': ['foobar@ugent.be'], 'pubkey': ['pubkey1', 'pubkey2']}
        mock_add_or_update.assert_called_with(VscLdapUser, test_account.vsc_id, ldap_attrs, True)
