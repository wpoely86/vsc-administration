#
# Copyright 2020-2020 Ghent University
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
Tests for vsc.administration.tools

@author Ward Poelmans (Vrije Universiteit Brussel)
"""
from vsc.install.testing import TestCase

from vsc.administration.tools import process_public_keys


test_pubkeys = [
    {
        "raw": u"ssh-rsa foobar \u201chuppelde@daar.com\u201d",
        "proccessed": "ssh-rsa foobar",
    },
    {
        "raw": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDjyWus19qi+dlmMfHAAqtc/fS6jsq14pQx4y8= user@somesystem.vub.ac.be",
        "proccessed": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQDjyWus19qi+dlmMfHAAqtc/fS6jsq14pQx4y8=",
    },
    {
        "raw": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINLxFcWXYe24BkW1fEfzaHU/wEryP4SuoUOcbXL9MQ4Z user@foo.bar.be",
        "proccessed": "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAINLxFcWXYe24BkW1fEfzaHU/wEryP4SuoUOcbXL9MQ4Z",
    },
]


class public_keysTest(TestCase):
    """
    Test the stripping of the comment in ssh public keys.
    """

    def test_keys(self):
        for key in test_pubkeys:
            test_key = process_public_keys([key["raw"]])
            self.assertEqual(test_key, [key["proccessed"]])
