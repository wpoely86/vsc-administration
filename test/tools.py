#
# Copyright 2015-2019 Ghent University
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

from collections import namedtuple

from vsc.administration.tools import create_stat_directory
from vsc.install.testing import TestCase


class StatDirTest(TestCase):
    """
    Tests for the VO code.
    """
    @mock.patch('os.stat')
    @mock.patch('vsc.filesystem.posix')
    def test_create_stat_dir_new(self, mock_posix, mock_os_stat):
        """
        Test to see what happens if the dir already exists
        """
        mock_os_stat.side_effect = OSError('dir not found')
        mock_posix.make_dir.result_value = True
        mock_posix.chmod.result_value = None

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        create_stat_directory(test_path, test_permissions, test_uid, test_gid, mock_posix, False)

        mock_os_stat.assert_called_with(test_path)
        mock_posix.make_dir.assert_called_with(test_path)
        mock_posix.chmod.assert_called_with(test_permissions, test_path)
        mock_posix.chown.assert_called_with(test_uid, test_gid, test_path)

    @mock.patch('os.stat')
    @mock.patch('vsc.filesystem.posix')
    def test_create_stat_dir_existing_no_override_same_id(self, mock_posix, mock_os_stat):
        """
        Test to see what happens if the dir already exists
        """

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        Statinfo = namedtuple("Statinfo", ["st_uid", "st_gid"])
        mock_os_stat.result_value = Statinfo(test_uid, test_gid)

        create_stat_directory(test_path, test_permissions, test_uid, test_gid, mock_posix, False)

        mock_os_stat.assert_called_with(test_path)
        self.assertFalse(mock_posix.make_dir.called)
        self.assertFalse(mock_posix.chmod.called)

    @mock.patch('os.stat')
    @mock.patch('vsc.filesystem.posix')
    def test_create_stat_dir_existing_no_override_diff_uid(self, mock_posix, mock_os_stat):
        """
        Test to see what happens if the dir already exists
        """

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        Statinfo = namedtuple("Statinfo", ["st_uid", "st_gid"])
        mock_os_stat.result_value = Statinfo(test_uid+1, test_gid)

        create_stat_directory(test_path, test_permissions, test_uid, test_gid, mock_posix, False)

        mock_os_stat.assert_called_with(test_path)
        self.assertFalse(mock_posix.make_dir.called)
        mock_posix.chown.assert_called_with(test_uid, test_gid, test_path)

    @mock.patch('os.stat')
    @mock.patch('vsc.filesystem.posix')
    def test_create_stat_dir_existing_no_override_diff_gid(self, mock_posix, mock_os_stat):
        """
        Test to see what happens if the dir already exists
        """

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        Statinfo = namedtuple("Statinfo", ["st_uid", "st_gid"])
        mock_os_stat.result_value = Statinfo(test_uid, test_gid+1)

        create_stat_directory(test_path, test_permissions, test_uid, test_gid, mock_posix, False)

        mock_os_stat.assert_called_with(test_path)
        self.assertFalse(mock_posix.make_dir.called)
        mock_posix.chown.assert_called_with(test_uid, test_gid, test_path)

    @mock.patch('os.stat')
    @mock.patch('stat.S_IMODE')
    @mock.patch('vsc.filesystem.posix')
    def test_create_stat_dir_existing_override(self, mock_posix, mock_stat_s_imode, mock_os_stat):
        """
        Test to see what happens if the dir already exists
        """

        test_uid = 2048
        test_gid = 4096
        test_path = '/tmp/test'
        test_permissions = 0o711

        Statinfo = namedtuple("Statinfo", ["st_uid", "st_gid"])
        mock_os_stat.result_value = Statinfo(test_uid, test_gid)
        mock_stat_s_imode.return_value = 0o755

        create_stat_directory(test_path, test_permissions, test_uid, test_gid, mock_posix, True)

        mock_os_stat.assert_called_with(test_path)
        self.assertFalse(mock_posix.make_dir.called)
        mock_posix.chmod.assert_called_with(test_permissions, test_path)
