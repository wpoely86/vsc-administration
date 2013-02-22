#!/usr/bin/env python
# -*- coding: latin-1 -*-
#
# Copyright 2009-2013 Ghent University
#
# This file is part of vsc-base,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Hercules foundation (http://www.herculesstichting.be/in_English)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# http://github.com/hpcugent/vsc-base
#
# vsc-administration is free software: you can redistribute it and/or modify
# it under the terms of the GNU Library General Public License as
# published by the Free Software Foundation, either version 2 of
# the License, or (at your option) any later version.
#
# vsc-administration is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library General Public License for more details.
#
# You should have received a copy of the GNU Library General Public License
# along with vsc-administration. If not, see <http://www.gnu.org/licenses/>.
#
"""
vsc-administration distribution setup.py

@author: Andy Georges (Ghent University)
"""
from shared_setup import ag
from shared_setup import action_target

    'scripts': ['bin/logdaemon.py', 'bin/startlogdaemon.sh'],
}

PACKAGE = {
    'name': 'vsc-administration',
    'version': '0.3',
    'author': [ag],
    'maintainer': [ag],
    'packages': ['vsc.administration'],
    'namespace_packages': ['vsc'],
    'install_requires': [
        'vsc-base >= 0.90',
        'vsc-ldap >= 0.90',
        'vsc-ldap-extension >= 0.90',
        'vsc-config >= 0.90',
        'vsc-packages-lockfile >= 0.9.1',
        ],
    'provides': ['python-vsc-administration = 0.3'],
}


if __name__ == '__main__':
    action_target(PACKAGE)
