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
import mock

from mock import patch

from collections import namedtuple

import vsc.administration.vo as vo

from vsc.config.base import VSC_DATA, VSC_HOME, VSC_SCRATCH_PHANPY, VSC_SCRATCH_DELCATTY
from vsc.install.testing import TestCase


class VoDeploymentTest(TestCase):
    """
    Tests for the VO deployment code.
    """

    @patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @patch('vsc.administration.vo.VscStorage', autospec=True)
    def test_process_regular_vos(self, mock_storage, mock_client):
        """Test to see if the VscTier2AccountpageVo class is used properly"""

        test_vo_id = "gvo00002"
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mc = mock_client.return_value
        mc.vo = mock.MagicMock()
        date = "20321231"
        mc.vo['gvo00002'].member.modified[date].get.return_value = (
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
            with mock.patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                with mock.patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:
                    with mock.patch.object(vo.VscTier2AccountpageVo, 'create_scratch_fileset') as mock_cr_s_fileset:
                        with mock.patch.object(vo.VscTier2AccountpageVo, 'set_scratch_quota') as mock_s_s_quota:
                            with mock.patch.object(vo.VscTier2AccountpageVo, 'create_data_fileset') as mock_cr_d_fileset:
                                with mock.patch.object(vo.VscTier2AccountpageVo, 'set_data_quota') as mock_s_d_quota:
                                    with mock.patch.object(vo.VscTier2AccountpageVo, 'set_member_data_quota') as mock_s_m_d_quota:
                                        with mock.patch.object(vo.VscTier2AccountpageVo, 'create_member_data_dir') as mock_cr_m_d_dir:
                                            with mock.patch.object(vo.VscTier2AccountpageVo, 'set_member_scratch_quota') as mock_s_m_s_quota:
                                                with mock.patch.object(vo.VscTier2AccountpageVo, 'create_member_scratch_dir') as mock_cr_m_s_dir:
                                                    mock_user.return_value = mock.MagicMock()
                                                    vo.process_vos(options, [test_vo_id], storage_name, mc, date)

                                                    if storage_name in (VSC_HOME, VSC_DATA):
                                                        mock_cr_s_fileset.assert_not_called()
                                                        mock_s_s_quota.assert_not_called()

                                                    if storage_name in (VSC_DATA,):
                                                        self.assertEqual(mock_cr_d_fileset.called, True)
                                                        self.assertEqual(mock_s_d_quota.called, True)
                                                        self.assertEqual(mock_update_vo_status.called, True)

                                                        self.assertEqual(mock_s_m_d_quota.called, True)
                                                        self.assertEqual(mock_cr_m_d_dir.called, True)

                                                    else:
                                                        mock_cr_d_fileset.assert_not_called()
                                                        mock_s_d_quota.assert_not_called()
                                                        mock_update_vo_status.assert_not_called()

                                                        if storage_name not in (VSC_HOME,):
                                                            self.assertEqual(mock_cr_s_fileset.called, True)
                                                            self.assertEqual(mock_s_s_quota.called, True)

                                                            self.assertEqual(mock_s_m_s_quota.called, True)
                                                            self.assertEqual(mock_cr_m_s_dir.called, True)

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @patch('vsc.administration.vo.VscStorage', autospec=True)
    def test_process_non_gent_institute_vos(self, mock_storage, mock_client):

        test_vo_id = "gvo00018"
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mc = mock_client.return_value
        mc.vo = mock.MagicMock()
        date = "20321231"
        mc.vo['gvo00018'].member.modified[date].get.return_value = (
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
            with mock.patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                with mock.patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:
                    with mock.patch.object(vo.VscTier2AccountpageVo, 'create_scratch_fileset') as mock_cr_s_fileset:
                        with mock.patch.object(vo.VscTier2AccountpageVo, 'set_scratch_quota') as mock_s_s_quota:
                            with mock.patch.object(vo.VscTier2AccountpageVo, 'create_data_fileset') as mock_cr_d_fileset:
                                with mock.patch.object(vo.VscTier2AccountpageVo, 'set_data_quota') as mock_s_d_quota:
                                    with mock.patch.object(vo.VscTier2AccountpageVo, 'set_member_data_quota') as mock_s_m_d_quota:
                                        with mock.patch.object(vo.VscTier2AccountpageVo, 'create_member_data_dir') as mock_cr_m_d_dir:
                                            with mock.patch.object(vo.VscTier2AccountpageVo, 'set_member_scratch_quota') as mock_s_m_s_quota:
                                                with mock.patch.object(vo.VscTier2AccountpageVo, 'create_member_scratch_dir') as mock_cr_m_s_dir:

                                                    mock_user.return_value = mock.MagicMock()
                                                    vo.process_vos(options, [test_vo_id], storage_name, mc, "99991231")

                                                    if storage_name in (VSC_HOME, VSC_DATA):
                                                        mock_cr_s_fileset.assert_not_called()
                                                        mock_s_s_quota.assert_not_called()
                                                        mock_cr_d_fileset.assert_not_called()
                                                        mock_s_d_quota.assert_not_called()
                                                        mock_update_vo_status.assert_not_called()

                                                        mock_s_m_d_quota.assert_not_called()
                                                        mock_cr_m_d_dir.assert_not_called()

                                                    else:
                                                        mock_cr_d_fileset.assert_not_called()
                                                        mock_s_d_quota.assert_not_called()
                                                        mock_update_vo_status.assert_not_called()

                                                        self.assertEqual(mock_cr_s_fileset.called, True)
                                                        self.assertEqual(mock_s_s_quota.called, True)

                                                        self.assertEqual(mock_s_m_s_quota.called, True)
                                                        self.assertEqual(mock_cr_m_s_dir.called, True)

    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    @patch('vsc.administration.vo.VscStorage', autospec=True)
    def test_process_gent_institute_vo(self, mock_storage, mock_client):

        test_vo_id = "gvo00012"
        Options = namedtuple("Options", ['dry_run'])
        options = Options(dry_run=False)

        mc = mock_client.return_value
        mc.vo = mock.MagicMock()
        date = "20321231"
        mc.vo['gvo00012'].member.modified[date].get.return_value = (
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
            with mock.patch('vsc.administration.vo.VscTier2AccountpageUser', autospec=True) as mock_user:
                with mock.patch('vsc.administration.vo.update_vo_status') as mock_update_vo_status:
                    with mock.patch.object(vo.VscTier2AccountpageVo, 'create_scratch_fileset') as mock_cr_s_fileset:
                        with mock.patch.object(vo.VscTier2AccountpageVo, 'set_scratch_quota') as mock_s_s_quota:
                            with mock.patch.object(vo.VscTier2AccountpageVo, 'create_data_fileset') as mock_cr_d_fileset:
                                with mock.patch.object(vo.VscTier2AccountpageVo, 'set_data_quota') as mock_s_d_quota:
                                    with mock.patch.object(vo.VscTier2AccountpageVo, 'set_member_data_quota') as mock_s_m_d_quota:
                                        with mock.patch.object(vo.VscTier2AccountpageVo, 'create_member_data_dir') as mock_cr_m_d_dir:
                                            with mock.patch.object(vo.VscTier2AccountpageVo, 'set_member_scratch_quota') as mock_s_m_s_quota:
                                                with mock.patch.object(vo.VscTier2AccountpageVo, 'create_member_scratch_dir') as mock_cr_m_s_dir:

                                                    mock_user.return_value = mock.MagicMock()
                                                    vo.process_vos(options, [test_vo_id], storage_name, mc, "99991231")

                                                    if storage_name in (VSC_HOME, VSC_DATA):
                                                        mock_cr_s_fileset.assert_not_called()
                                                        mock_s_s_quota.assert_not_called()
                                                        mock_cr_d_fileset.assert_not_called()
                                                        mock_s_d_quota.assert_not_called()
                                                        mock_update_vo_status.assert_not_called()

                                                        mock_s_m_d_quota.assert_not_called()
                                                        mock_cr_m_d_dir.assert_not_called()
