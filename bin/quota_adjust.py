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

import sys

from vsc.accountpage.client import AccountpageClient
from vsc.utils.generaloption import SimpleOption



def main():
    """
    Main script. Sets the hard limit for either user or VO.
    """

    options = {
        'storage': ("The storage system's name", None, 'store', None),
        'fileset': ("The fileset where you want to adjust the quota", None, 'store', None),
        'user': ('process users', None, 'store', None),
        'vo': ('process vos', None, 'store', None),
        'size': ('the target quota (in KiB', int, 'store', None),
        'original': ('show the original quota values', None, 'store_true', False),
        'account_page_url': ('Base URL of the account page', None, 'store', None),  # Not used
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
    }

    opts = SimpleOption(options)
    client = AccountpageClient(token=opts.options.access_token)
    client.client.url = "http://localhost:8000/api/"

    fileset = opts.options.fileset
    storage = opts.options.storage

    if not opts.options.size:
        sys.exit()

    # TODO: could use some love in allowing a unit to be appended and converting to KiB prior to uploading
    size = int(opts.options.size)

    if opts.options.user:
        # quota/user/%(vsc_id)s/storage/%(storage)s/fileset/%(fileset)s/size/$
        vsc_id = opts.options.user
        original = client.account[vsc_id].quota
        upload = client.quota.user[vsc_id].storage[storage].fileset[fileset].size

    if opts.options.vo:

        vsc_id = opts.options.vo
        original = client.vo[vsc_id].quota
        upload = client.quota.vo[vsc_id].storage[storage].fileset[fileset].size

    current = original.get()
    if current[0] in (200,):
        if opts.options.original:
            print("Original values: %s" % (current[1],))
    else:
        print("Error, could not get original quota values for the given parameters")
        print("Issue: %s" % (current[1],))
        sys.exit(-1)

    result = upload.put(body={"hard": size})
    if result[0] in (200,):
        print("Request OK.")
        print("New values: %s" % (result[1],))
    else:
        print("Request failed")
        print("Issue: %s" % (result[1],))

if __name__ == '__main__':
    main()

