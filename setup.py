#!/usr/bin/env python
# -*- coding: latin-1 -*-
##
# Copyright 2012-2013 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# All rights reserved.
#
##
"""
vsc-administration distribution setup.py

@author: Andy Georges (Ghent University)
@author: Jens Timmerman (Ghent University)
"""
import sys

from vsc.install import shared_setup
from vsc.install.shared_setup import ag, jt

install_requires = [
    'vsc-accountpage-clients >= 2.0.0',
    'vsc-base >= 3.0.6',
    'vsc-config >= 3.3.3',
    'vsc-filesystems >= 1.0.1',
    'python-ldap',
    'vsc-ldap >= 2.0.0',
    'vsc-ldap-extension >= 2.0.0',
    'vsc-utils >= 2.0.0',
    'lockfile >= 0.9.1',
]

if sys.version_info < (3, 0):
    # enum34 is only required with Python 2
    install_requires.append('enum34')

PACKAGE = {
    'version': '2.4.4',
    'author': [ag, jt],
    'maintainer': [ag, jt],
    'tests_require': ['mock'],
    'setup_requires': [
        'vsc-install >= 0.15.3',
    ],
    'install_requires': install_requires,
}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
