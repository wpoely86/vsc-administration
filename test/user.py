#
# Copyright 2015-2016 Ghent University
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
Tests for vsc.administration.vo

@author: Andy Georges (Ghent University)
"""
import mock

from collections import namedtuple

import vsc.administration.user as user

from vsc.accountpage.wrappers import mkVscAccount
from vsc.config.base import VSC_DATA, VSC_HOME, VSC_SCRATCH_PHANPY, VSC_SCRATCH_DELCATTY
from vsc.install.testing import TestCase


class VscAccountPageUserTest(TestCase):
    """
    Tests for the base class of users derived from account page information.
    """

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @mock.patch('vsc.administration.user.VscAccount')
    @mock.patch('vsc.administration.user.VscAccountPerson')
    @mock.patch('vsc.administration.user.VscAccountPubkey')
    @mock.patch('vsc.administration.user.VscGroup')
    @mock.patch('vsc.administration.user.VscUserGroup')
    @mock.patch('vsc.administration.user.VscHomeOnScratch')
    def test_get_institute_prefix(self,
                                  mock_home_on_scratch,
                                  mock_usergroup,
                                  mock_group,
                                  mock_pubkey,
                                  mock_person,
                                  mock_account,
                                  mock_client):

        test_account = mkVscAccount({
            u'broken': False,
            u'create_timestamp': u'1970-01-01T00:00:00.197Z',
            u'data_directory': u'/user/data/gent/vsc400/vsc40075',
            u'email': u'foobar@ugent.be',
            u'home_directory': u'/user/home/gent/vsc400/vsc40075',
            u'login_shell': u'/bin/bash',
            u'person': {
                u'gecos': u'Foo Bar',
                u'institute': {u'site': u'gent'},
                u'institute_login': u'foobar'
            },
            u'research_field': [u'Bollocks', u'Pluto'],
            u'scratch_directory': u'/user/scratch/gent/vsc400/vsc40075',
            u'status': u'active',
            u'vsc_id': u'vsc40075',
            u'vsc_id_number': 2540075
        })

        mock_person.return_value = test_account.person
        mock_client = mock.MagicMock()
        accountpageuser = user.VscAccountPageUser(test_account.vsc_id, mock_client)

        self.assertEqual(accountpageuser.get_institute_prefix(), 'g')


class UserDeploymentTest(TestCase):
    """
    Tests for the User deployment code.
    """

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_process_regular_users(self, mock_client):

        test_account_ids = ['vsc40075', 'vsc40123', 'vsc40039']
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mock_client.return_value = mock.MagicMock()

        for storage_name in (VSC_HOME, VSC_DATA, VSC_SCRATCH_DELCATTY, VSC_SCRATCH_PHANPY):
            with mock.patch('vsc.administration.user.VscTier2AccountpageUser', autospec=True) as mock_user:
                with mock.patch('vsc.administration.user.update_user_status') as mock_update_user_status:

                        mock_user.return_value = mock.MagicMock()
                        mock_user_instance = mock_user.return_value

                        user.process_users(options, test_account_ids, storage_name, mock_client)

                        mock_user_instance.set_scratch_quota.assert_not_called()
                        mock_user_instance.set_home_quota.assert_not_called()
                        mock_user_instance.set_data_quota.assert_not_called()

                        if storage_name in (VSC_HOME):
                            mock_user_instance.create_scratch_dir.assert_not_called()
                            mock_user_instance.create_data_dir.assert_not_called()

                            self.assertEqual(mock_user_instance.create_home_dir.called, True)
                            self.assertEqual(mock_user_instance.populate_home_dir.called, True)
                            self.assertEqual(mock_update_user_status.called, True)

                        if storage_name in (VSC_DATA,):
                            mock_user_instance.create_home_dir.assert_not_called()
                            mock_user_instance.populate_home_dir.assert_not_called()
                            mock_update_user_status.assert_not_called()

                            mock_user_instance.create_scratch_dir.assert_not_called()

                            self.assertEqual(mock_user_instance.create_data_dir.called, True)

                        if storage_name not in (VSC_HOME, VSC_DATA):
                            mock_user_instance.create_home_dir.assert_not_called()
                            mock_user_instance.populate_home_dir.assert_not_called()
                            mock_update_user_status.assert_not_called()
                            mock_user_instance.create_data_dir.assert_not_called()

                            self.assertEqual(mock_user_instance.create_scratch_dir.called, True)

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_process_regular_users_quota(self, mock_client):

        TestQuota = namedtuple("TestQuota", ['user'])
        test_quota_account_ids = ['vsc40075', 'vsc40123', 'vsc40039']
        test_quota = [TestQuota(user=u) for u in test_quota_account_ids]
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mock_client.return_value = mock.MagicMock()

        for storage_name in (VSC_HOME, VSC_DATA, VSC_SCRATCH_DELCATTY, VSC_SCRATCH_PHANPY):
            with mock.patch('vsc.administration.user.VscTier2AccountpageUser', autospec=True) as mock_user:

                        mock_user.return_value = mock.MagicMock()
                        mock_user_instance = mock_user.return_value

                        user.process_users_quota(options, test_quota, storage_name, mock_client)

                        if storage_name in (VSC_HOME):
                            self.assertEqual(mock_user_instance.set_home_quota.called, True)

                            mock_user_instance.set_data_quota.assert_not_called()
                            mock_user_instance.set_scratch_quota.assert_not_called()

                        if storage_name in (VSC_DATA,):
                            self.assertEqual(mock_user_instance.set_data_quota.called, True)

                            mock_user_instance.set_home_quota.assert_not_called()
                            mock_user_instance.set_scratch_quota.assert_not_called()

                        if storage_name not in (VSC_HOME, VSC_DATA):
                            self.assertEqual(mock_user_instance.set_scratch_quota.called, True)

                            mock_user_instance.set_home_quota.assert_not_called()
                            mock_user_instance.set_data_quota.assert_not_called()
