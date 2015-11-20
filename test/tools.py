#!/usr/bin/env python
#
# Copyright 2015-2015 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
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
import os
import stat

from collections import namedtuple

import vsc.filesystem.posix


from vsc.accountpage.wrappers import mkVscAccount
from vsc.administration import tools
from vsc.administration.tools import create_stat_directory, cleanup_purgees

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
        test_permissions = 0711

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
        test_permissions = 0711

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
        test_permissions = 0711

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
        test_permissions = 0711

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
        test_permissions = 0711

        Statinfo = namedtuple("Statinfo", ["st_uid", "st_gid"])
        mock_os_stat.result_value = Statinfo(test_uid, test_gid)
        mock_stat_s_imode.return_value = 0755

        create_stat_directory(test_path, test_permissions, test_uid, test_gid, mock_posix, True)

        mock_os_stat.assert_called_with(test_path)
        self.assertFalse(mock_posix.make_dir.called)
        mock_posix.chmod.assert_called_with(test_permissions, test_path)


class PurgeesTest(TestCase):
    """"
    Testcases for everything related to purged users.
    """

    @mock.patch('vsc.administration.user.MukAccountpageUser', autospec=True)
    @mock.patch('vsc.administration.tools.notify_reinstatement')
    @mock.patch('vsc.accountpage.client.AccountpageClient', autospec=True)
    def test_cleanup_purgees(self, mock_client, mock_notify_reinstatement, mock_accountpage_user):
        """
        Test that we're selecting the correct people to remove from the purgees list
        """
        test_current_users = [1, 2, 3, 4, 5]
        test_current_purgees = [8, 2, 4, 6, 7]
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

        mock_notify_reinstatement.return_value = None
        mock_accountpage_user.return_value = mock.MagicMock()
        mock_accountpage_user.person = test_account.person
        mock_accountpage_user.dry_run = False
        mock_client.return_value = mock.MagicMock()
        mock_client.group = mock.MagicMock()
        mock_client.group["gt1_mukgraceusers"] = mock.MagicMock()
        mock_client.group["gt1_mukgraceusers"].member = mock.MagicMock()

        purgees_undone = cleanup_purgees(test_current_users, test_current_purgees, mock_client, False)

        mock_notify_reinstatement.assert_called()
        self.assertTrue(purgees_undone == 2)
