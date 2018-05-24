#
# Copyright 2015-2018 Ghent University
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
Tests for vsc.administration.slurm.*

@author: Andy Georges (Ghent University)
"""

from collections import namedtuple

from vsc.install.testing import TestCase

from vsc.administration.slurm.sync import slurm_vo_accounts


VO = namedtuple("VO", ["vsc_id"])


class SlurmSyncTest(TestCase):
    """Test for the slurm account sync."""

    def test_slurm_vo_accounts(self):

        vos = [
            VO(vsc_id="gvo00001"),
            VO(vsc_id="gvo00002"),
            VO(vsc_id="gvo00012"),
            VO(vsc_id="gvo00016"),
            VO(vsc_id="gvo00017"),
            VO(vsc_id="gvo00018"),
        ]

        commands = slurm_vo_accounts(vos, [], ["mycluster"])

        self.assertEqual(commands, [
            "/usr/bin/sacctmgr add account gvo00001 Parent=gent Organization=ugent Cluster=mycluster",
            "/usr/bin/sacctmgr add account gvo00002 Parent=gent Organization=ugent Cluster=mycluster",
        ])
