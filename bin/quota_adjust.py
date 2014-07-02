#!/usr/bin/env python
#
#
# Copyright 2014-2014 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
This script will adjust the quota for the given entity, be it a user or a VO
on the given storage.
"""



from vsc.accountpage.client import AccountPageClient
from vsc.utils.generaloption import SimpleOption
from vsc.utils.script_tools import ExtendedSimpleOption

def main():
    """
    Main script.
    - build the filter
    - fetches the users
    - process the users
    - write the new timestamp if everything went OK
    - write the nagios check file
    """

    options = {
        'storage': ("The storage system's name", None, 'store', None),
        'fileset': ("The fileset where you want to adjust the quota", None, 'store', None),
        'user': ('process users', None, 'store', None),
        'vo': ('process vos', None, 'store', None),
        'account_page_url': ('Base URL of the account page', None, 'store', None),  # Not used
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
    }

    opts = SimpleOption(options)
    client = AccountPageClient(token=opts.options.access_token)

    fileset = opts.options.filesett
    storage = opts.options.storage
    size = int(opts.options.size)

    if opts.options.user:
        # quota/user/%(vsc_id)s/storage/%(storage)s/fileset/%(fileset)s/size/$
        vsc_id = opts.options.user
        client.quota.user[vsc_id].storage[storage].fileset[fileset].size.put(hard=size)

    if opts.options.vo:
        # quota/vo/%(vsc_id)s/storage/%(storage)s/fileset/%(fileset)s/size/$
        vsc_id = opts.options.vo
        client.quota.vo[vsc_id].storage[storage].fileset[fileset].size.put(hard=size)









