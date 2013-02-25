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
#!/usr/bin/env python
##
#
# Copyright 2011-2012 Ghent University
# Copyright 2011-2012 Wouter De Pypere
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
##
"""
Test script for vsc.icingadb.icingadb.

TODO: rewrite into a genuis test.
"""


import os
import sys

from vsc import fancylogger
from vsc.icingadb.icingadb import icingadb


if __name__ == "__main__":

    sys.path.append('/root/globfs')

    fancylogger.getLogger("test.icingadb.log")
    fancylogger.setLogLevelDebug()

    icinga = icingadb()

    last_notification_id=None
    last_notification_id=1336389

    if last_notification_id:
        print icinga.getRealProblemHostStatus(last_notification_id)
        print icinga.getRealProblemServiceStatus(last_notification_id)
    else:
        print icingadb.getProblemHostStatus()
        print icingadb.getProblemServiceStatus()
    icinga.close()
