#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2012-2013 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
vsc-administration distribution setup.py

@author: Andy Georges (Ghent University)
"""

from vsc.install import shared_setup
from vsc.install.shared_setup import ag

PACKAGE = {
    'version': '0.33',
    'author': [ag],
    'maintainer': [ag],
    'tests_require': ['mock'],
    'install_requires': [
        'vsc-accountpage-clients >= 0.2',
        'vsc-base >= 2.4.16',
        'vsc-config >= 1.20',
        'vsc-filesystems >= 0.19',
        'vsc-ldap >= 1.1',
        'vsc-ldap-extension >= 1.3',
        'vsc-utils >= 1.4.4',
        'lockfile >= 0.9.1',
        # following dependencies are intentionally not declared until #11 is addressed
        #'vsc-postgres',
        #'django',
    ],
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
