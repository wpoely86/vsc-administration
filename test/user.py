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
import logging
import mock

from collections import namedtuple

import vsc.administration.vo as vo
import vsc.administration.user as user

from vsc.config.base import VSC_DATA, VSC_HOME, VSC_SCRATCH_PHANPY, VSC_SCRATCH_DELCATTY
from vsc.install.testing import TestCase


class UserDeploymentTest(TestCase):
    """
    Tests for the VO deployment code.
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
