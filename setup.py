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
import shared_setup
from shared_setup import ag


def remove_bdist_rpm_source_file():
    """List of files to remove from the (source) RPM."""
    return ['lib/vsc/__init__.py']


shared_setup.remove_extra_bdist_rpm_files = remove_bdist_rpm_source_file
shared_setup.SHARED_TARGET.update({
    'url': 'https://github.ugent.be/hpcugent/vsc-administration',
    'download_url': 'https://github.ugent.be/hpcugent/vsc-administration'
})


PACKAGE = {
    'name': 'vsc-administration',
    'version': '0.4',
    'author': [ag],
    'maintainer': [ag],
    'packages': ['vsc', 'vsc.administration'],
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
    shared_setup.action_target(PACKAGE)
