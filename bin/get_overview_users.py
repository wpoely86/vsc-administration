#!/usr/bin/python
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

    opts.log.debug("Found the following users in the HPC collector DB")
    opts.log.debug("%s" % (users))

    return [User(vscid=u[0], ugentid=None, active=u[2], employee=None, student=None) for u in users if u[1] == GENT]


def get_ugent_id(opts, ldap, vscuid):
    """Retrieve the UGent ID from the HPC LDAP."""

    ldap_filter = InstituteFilter(GENT) & CnFilter(vscuid)

    attrs = ['instituteLogin']

    ugentid = ""
    for entry in ldap.user_filter_search(ldap_filter, attrs):
        ugentid = entry['instituteLogin']
    if ugentid == "":
        opts.log.warning("No LDAP info for %s. Wrong vsc ID?" % vscuid)
    return ugentid


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

    active_users = 0
    employees = 0
    student = 0
    student_employee = 0
    notstudent_nor_employee = 0

    employees_active = 0
    student_active = 0
    student_employee_active = 0
    notstudent_nor_employee_active = 0

    employees_inactive = 0
    student_inactive = 0
    student_employee_inactive = 0
    notstudent_nor_employee_inactive = 0

    print "-----------------------------------------------------------------"
    print "   vscID -  UGentID - Active - Employee - Student"
    print "-----------------------------------------------------------------"

    for user in users:
        print "%8s - %8s - %6s - %8s - %7s" % (user.vscid, user.ugentid, user.active, user.employee, user.student)
        if user.active:
            active_users = active_users + 1

        if user.employee and not user.student:
            employees = employees + 1
            if user.active:
                employees_active = employees_active + 1
            else:
                employees_inactive = employees_inactive + 1

        if not user.employee and user.student:
            student = student + 1
            if user.active:
                student_active = student_active + 1
            else:
                student_inactive = student_inactive + 1

        if not user.employee and not user.student:
            notstudent_nor_employee = notstudent_nor_employee + 1
            if user.active:
                notstudent_nor_employee_active = notstudent_nor_employee_active + 1
            else:
                notstudent_nor_employee_inactive = notstudent_nor_employee_inactive + 1

        if user.employee and user.student:
            student_employee = student_employee + 1
            if user.active:
                student_employee_active = student_employee_active + 1
            else:
                student_employee_inactive = student_employee_inactive + 1
    print "-----------------------------------------------------------------"
    print "number of users: %s" % len(users)
    print "number of active users: %s" % active_users
    print "number of inactive users: %s" % (len(users) - active_users)
    print ""
    print "number of (only) students: %s" % student
    print "number of (only) employees: %s" % employees
    print "number of people who are both employee as student: %s" % student_employee
    print "number of people who are neither: %s" % notstudent_nor_employee
    print ""
    print "number of active students: %s" % student_active
    print "number of active employees: %s" % employees_active
    print "number of active people who are both employee as student: %s" % student_employee_active
    print "number of active people who are neither: %s" % notstudent_nor_employee_active
    print ""
    print "number of inactive students: %s" % student_inactive
    print "number of inactive employees: %s" % employees_inactive
    print "number of inactive people who are both employee as student: %s" % student_employee_inactive
    print "number of inactive people who are neither: %s" % notstudent_nor_employee_inactive
    print ""
    print "-----------------------------------------------------------------"

if __name__ == '__main__':
    main()
