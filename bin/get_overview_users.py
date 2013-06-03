#!/usr/bin/env python
##
#
# Copyright 2012-2013 Ghent University
#
# This file is part of the tools originally by the HPC team of
# Ghent University (http://ugent.be/hpc).
#
# This is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation v2.
"""
This script produces an overview of the HPC users.

@author: Andy Georges
@author: Wouter Depypere
"""

from collections import namedtuple

from vsc.config.base import GENT
from vsc.pg import cCol
from vsc.ldap.configuration import VscConfiguration, UGentLdapConfiguration
from vsc.ldap.filters import InstituteFilter, CnFilter, LdapFilter
from vsc.ldap.utils import LdapQuery
from vsc.utils.generaloption import simple_option
from vsc.utils.missing import Monoid, MonoidDict


User = namedtuple('User',[
    'vscid',
    'ugentid',
    'active',
    'employee',
    'student'
])


def get_hpc_collector_users(opts):
    """Get the users from UGent in the HPC collector database."""
    c = cCol()
    c.debug = opts.options.debug
    users = c.getlist("member", "uid, inst, active")

    opts.log.debug("Found the following users in the HPC collector DB: %s" % (users))

    return [User(vscid=u[0], ugentid=None, active=u[2], employee=None, student=None) for u in users if u[1] == GENT]


def get_ugent_id(opts, ldap, vscuid):
    """Retrieve the UGent ID from the HPC LDAP."""

    ldap_filter = InstituteFilter(GENT) & CnFilter(vscuid)
    attrs = ['instituteLogin']

    users = ldap.user_filter_search(ldap_filter, attrs)
    if users:
        return users[0]['instituteLogin']
    else:
        opts.log.warning("No user found with VSC ID (cn) %s" % (vscuid))
        return None


def ugent_status(opts, ldap_query, ugentid):
    """Check the UGent object classes for this users and return a tuple."""
    ldap_filter = LdapFilter("uid=%s" % (ugentid))

    users = ldap_query.user_filter_search(ldap_filter, ['objectClass'])

    if users:
        objectClasses = users[0]['objectClass']
        employee = objectClasses.count('ugentEmployee') > 0
        student = objectClasses.count('ugentStudent') > 0
        opts.log.debug("User with UGent ID %s is employee: %s, student: %s" % (ugentid, employee, student))
        return (employee, student)
    else:
        return (False, False)


class HpcLdapQuery(LdapQuery):
    pass


class UGentLdapQuery(LdapQuery):
    pass


def main():

    opts = simple_option({})  # provides debug and logging

    l = HpcLdapQuery(VscConfiguration())  # Initialise the LDAP connection

    users = get_hpc_collector_users(opts)
    users = [u._replace(ugentid=get_ugent_id(opts, l, u.vscid)) for u in users]

    l = UGentLdapQuery(UGentLdapConfiguration("collector"))  # Initialise the LDAP connection
    users = [u._replace(employee=employee, student=student) for u in users for (employee, student) in
             [ugent_status(opts, l, u.ugentid)]]

    addm = Monoid(0, lambda x, y: x+y)

    student_type = (False, True)
    employee_type = (True, False)
    both_type = (True, True)
    none_type = (False, False)

    user_types = MonoidDict(addm)
    active_user_types = MonoidDict(addm)
    inactive_user_types = MonoidDict(addm)

    active_users = 0
    inactive_users = 0

    output = ["-" * 65]
    output += ["%8s - %8s - %6s - %8s - %7s" % ("vscID", "UGentID", "Active", "Employee", "Student")]
    output += ["-" * 65]

    for user in users:
        output += ["%8s - %8s - %6s - %8s - %7s" % (user.vscid, user.ugentid, user.active, user.employee, user.student)]

        user_type = (user.employee, user.student)
        user_types[user_type] = 1

        if user.active:
            active_users += 1
            active_user_types[user_type] = 1
        else:
            inactive_users += 1
            inactive_user_types[user_type] = 1

    output += ["-" * 65]

    template = "number of %s: %d"
    output += [template % ("users", len(users))]
    output += [template % ("active users", active_users)]
    output += [template % ("inactive users", inactive_users)]
    output += [""]
    output += [template % ("(only) students", user_types[student_type])]
    output += [template % ("(only) employees", user_types[employee_type])]
    output += [template % ("people who are both employee as student", user_types[both_type])]
    output += [template % ("people who are neither", user_types[none_type])]
    output += [""]
    output += [template % ("active students", active_user_types[student_type])]
    output += [template % ("active employees", active_user_types[employee_type])]
    output += [template % ("active people who are both employee as student", active_user_types[both_type])]
    output += [template % ("active people who are neither", active_user_types[none_type])]
    output += [""]
    output += [template % ("inactive students", inactive_user_types[student_type])]
    output += [template % ("inactive employees", inactive_user_types[employee_type])]
    output += [template % ("inactive people who are both employee as student", inactive_user_types[both_type])]
    output += [template % ("inactive people who are neither", inactive_user_types[none_type])]
    output += ["-" * 65]

    print "\n".join(output)

if __name__ == '__main__':
    main()
