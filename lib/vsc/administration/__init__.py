#!/usr/bin/env python
##
#
# Copyright 2012 Andy Georges
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
This module contains the utilities for dealing with users on the VSC.

@author Andy Georges

@created Apr 23, 2012
"""

__author__ = 'ageorges'
__date__ = 'Apr 24, 2012'


class VscAdminError(Exception):
    pass

class NoSuchUserError(VscAdminError):
    def __init__(self, message):
        pass
