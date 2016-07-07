#
# Copyright 2015-2016 Ghent University
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


class VoDeploymentTest(TestCase):
    """
    Tests for the VO deployment code.
    """

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_process_regular_vos(self, mock_client):

        test_vo_ids = ["gvo000%0d" % d for d in [1, 2, 20, 25, 33, 54]]
        TestVO = namedtuple("TestVO", ['members'])
        test_vo = TestVO(members=['vsc40001', 'vsc40002'])
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mock_client.return_value = mock.MagicMock()
        storage = None  # not used anymore, it seems

        for storage_name in (VSC_HOME, VSC_DATA, VSC_SCRATCH_DELCATTY, VSC_SCRATCH_PHANPY):
            with mock.patch('vsc.administration.vo.VscTier2AccountpageVo', autospec=True) as mock_vo:
                with mock.patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                    with mock.patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:

                        mock_vo.return_value = mock.MagicMock()
                        mock_vo_instance = mock_vo.return_value
                        mock_vo_instance.vo = test_vo
                        mock_user.return_value = mock.MagicMock()

                        vo.process_vos(options, test_vo_ids, storage, storage_name, mock_client)

                        if storage_name in (VSC_HOME, VSC_DATA):
                            mock_vo_instance.create_scratch_fileset.assert_not_called()
                            mock_vo_instance.set_scratch_quota.assert_not_called()

                        if storage_name in (VSC_DATA,):
                            self.assertEqual(mock_vo_instance.create_data_fileset.called, True)
                            self.assertEqual(mock_vo_instance.set_data_quota.called, True)
                            self.assertEqual(mock_update_vo_status.called, True)

                            self.assertEqual(mock_vo_instance.set_member_data_quota.called, True)
                            self.assertEqual(mock_vo_instance.create_member_data_dir.called, True)

                        else:
                            mock_vo_instance.create_data_fileset.assert_not_called()
                            mock_vo_instance.set_data_quota.assert_not_called()
                            mock_update_vo_status.assert_not_called()

                            if storage_name not in (VSC_HOME,):
                                self.assertEqual(mock_vo_instance.create_scratch_fileset.called, True)
                                self.assertEqual(mock_vo_instance.set_scratch_quota.called, True)

                                self.assertEqual(mock_vo_instance.set_member_scratch_quota.called, True)
                                self.assertEqual(mock_vo_instance.create_member_scratch_dir.called, True)


    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_process_non_gent_institute_vos(self, mock_client):

        test_vo_ids = ["gvo000%0d" % d for d in [16, 17, 18]]
        TestVO = namedtuple("TestVO", ['members'])
        test_vo = TestVO(members=['vsc40001', 'vsc40002'])
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mock_client.return_value = mock.MagicMock()
        storage = None  # not used anymore, it seems

        for storage_name in (VSC_HOME, VSC_DATA, VSC_SCRATCH_DELCATTY, VSC_SCRATCH_PHANPY):
            with mock.patch('vsc.administration.vo.VscTier2AccountpageVo', autospec=True) as mock_vo:
                with mock.patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                    with mock.patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:

                        mock_vo.return_value = mock.MagicMock()
                        mock_vo_instance = mock_vo.return_value
                        mock_vo_instance.vo = test_vo
                        mock_user.return_value = mock.MagicMock()

                        vo.process_vos(options, test_vo_ids, storage, storage_name, mock_client)

                        if storage_name in (VSC_HOME, VSC_DATA):
                            mock_vo_instance.create_scratch_fileset.assert_not_called()
                            mock_vo_instance.set_scratch_quota.assert_not_called()
                            mock_vo_instance.create_data_fileset.assert_not_called()
                            mock_vo_instance.set_data_quota.assert_not_called()
                            mock_update_vo_status.assert_not_called()

                            mock_vo_instance.set_member_data_quota.assert_not_called()
                            mock_vo_instance.create_member_data_dir.assert_not_called()

                        else:
                            mock_vo_instance.create_data_fileset.assert_not_called()
                            mock_vo_instance.set_data_quota.assert_not_called()
                            mock_update_vo_status.assert_not_called()

                            self.assertEqual(mock_vo_instance.create_scratch_fileset.called, True)
                            self.assertEqual(mock_vo_instance.set_scratch_quota.called, True)

                            self.assertEqual(mock_vo_instance.set_member_scratch_quota.called, True)
                            self.assertEqual(mock_vo_instance.create_member_scratch_dir.called, True)
