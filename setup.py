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
"""

from vsc.install import shared_setup
from vsc.install.shared_setup import ag

PACKAGE = {
    'version': '0.36.2',
    'author': [ag],
    'maintainer': [ag],
    'tests_require': ['mock'],
    'install_requires': [
        'vsc-accountpage-clients >= 0.7',
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
    'dependency_links': [
        "git+https://github.com/hpcugent/vsc-utils.git#egg=vsc-utils-1.8.2",
        "git+https://github.com/hpcugent/vsc-filesystems.git#egg=vsc-filesystems-0.30.1",
        "git+ssh://github.com/hpcugent/vsc-accountpage-clients.git#egg=vsc-accountpage-clients-0.7",
        "git+ssh://github.com/hpcugent/vsc-ldap.git#egg=vsc-ldap-1.4.2",
        "git+ssh://github.com/hpcugent/vsc-ldap-extension.git#egg=vsc-ldap-extensions-1.10.2",
    ],



}


if __name__ == '__main__':
    shared_setup.action_target(PACKAGE)
