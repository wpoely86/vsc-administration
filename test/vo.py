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

        test_vo_id = "gvo00002"
        TestVO = namedtuple("TestVO", ['members'])
        test_vo = TestVO(members=['vsc40001', 'vsc40002'])
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mc = mock_client.return_value
        mc.vo = mock.MagicMock()
        date = "20321231"
        #mc.vo.__getitem__.return_value.members.modified.__getitem__.return_value.get.return_value = (
        mc.vo['gvo00002'].members.modified[date].get.return_value = (
            200, [{
                u'broken': False,
                u'create_timestamp': u'2014-04-23T09:11:22.460Z',
                u'data_directory': u'/user/data/gent/vsc400/vsc40075',
                u'email': u'andy.georges@ugent.be',
                u'home_directory': u'/user/home/gent/vsc400/vsc40075',
                u'login_shell': u'/bin/bash',
                u'person': {
                    u'gecos': u'Andy Georges',
                    u'institute': {u'site': u'gent'},
                    u'institute_login': u'ageorges'
                },
                u'research_field': [u'Computer systems, architectures, networks', u'nwo'],
                u'scratch_directory': u'/user/scratch/gent/vsc400/vsc40075',
                u'status': u'active',
                u'vsc_id': u'vsc40075',
                u'vsc_id_number': 2540075,
        }])

        for storage_name in (VSC_HOME, VSC_DATA, VSC_SCRATCH_DELCATTY, VSC_SCRATCH_PHANPY):
            with mock.patch('vsc.administration.vo.VscTier2AccountpageVo', autospec=True) as mock_vo:
                with mock.patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                    with mock.patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:

                        mock_vo.return_value = mock.MagicMock()
                        mock_vo_instance = mock_vo.return_value
                        mock_vo_instance.vo = test_vo
                        mock_vo_instance.vsc_id = test_vo_id
                        mock_user.return_value = mock.MagicMock()

                        vo.process_vos(options, [test_vo_id], storage_name, mc, date)

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

        test_vo_id = "gvo00018"
        TestVO = namedtuple("TestVO", ['members', 'vsc_id'])
        test_vo = TestVO(members=['vsc30001', 'vsc30002'], vsc_id=test_vo_id)
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mc = mock_client.return_value
        mc.vo = mock.MagicMock()
        date = "20321231"
        mc.vo['gvo00018'].members.modified[date].get.return_value = (
            200, [{
                u'broken': False,
                u'create_timestamp': u'2014-04-23T09:11:22.460Z',
                u'data_directory': u'/user/leuven/data/vsc400/vsc40075',
                u'email': u'andy.georges@kuleuven.be',
                u'home_directory': u'/user/leuven/home/vsc400/vsc40075',
                u'login_shell': u'/bin/bash',
                u'person': {
                    u'gecos': u'Andy Georges',
                    u'institute': {u'site': u'leuven'},
                    u'institute_login': u'ageorges'
                },
                u'research_field': [u'Computer systems, architectures, networks', u'nwo'],
                u'scratch_directory': u'/user/leuven/scratch/vsc400/vsc40075',
                u'status': u'active',
                u'vsc_id': u'vsc40075',
                u'vsc_id_number': 2540075,
        }])

        for storage_name in (VSC_HOME, VSC_DATA, VSC_SCRATCH_DELCATTY, VSC_SCRATCH_PHANPY):
            with mock.patch('vsc.administration.vo.VscTier2AccountpageVo', autospec=True) as mock_vo:
                with mock.patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                    with mock.patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:

                        mock_vo.return_value = mock.MagicMock()
                        mock_vo_instance = mock_vo.return_value
                        mock_vo_instance.vo = test_vo
                        mock_vo_instance.vsc_id = test_vo_id
                        mock_user.return_value = mock.MagicMock()

                        vo.process_vos(options, [test_vo_id], storage_name, mc, "99991231")

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
