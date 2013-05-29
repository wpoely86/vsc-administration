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

import sys
from collections import namedtuple

from vsc.config.base import GENT
from vsc.pg import cCol
from vsc.ldap.configuration import VscConfiguration, UGentLdapConfiguration
from vsc.ldap.utils import LdapQuery
from vsc.utils.generaloption import simple_option

User = namedtuple('User',[
    'vscid',
    'ugentid',
    'active',
    'employee',
    'student'
])


def getAllDBusers(opts):
    """Get the users from UGent in the HPC collector database."""
    c = cCol()
    c.debug = opts.debug
    users = c.getlist("member", "uid, inst, active")

    return [User(vscid=u[0], ugentid=None, active=u[2], employee=None, student=None) for u in users if u[1] == GENT]


def getUGentID(vscuid):
    hpcldap = LdapQuery(VscConfiguration())
    filter = '(&(institute=gent)(uid=%s))' % vscuid
    attrs = ['instituteLogin']

    ugentid = ""
    for entry in hpcldap.user_filter_search(filter, attrs):
        ugentid = entry['instituteLogin']
    if ugentid == "":
        print "No LDAP info for %s. Wrong vsc ID?" % vscuid
        sys.exit(2)
    return ugentid


def getUGentSubcs(ugentid):
    ugentldap = ugent_ldap.ugent_ldap()
    ugentldap.connectUgentLdap()
    ugentldap.bindUgentLdap()

    attrs = ['objectClass', ]
    base = 'ou=people,dc=ugent,dc=be'
    filter = '(uid=%s)' % ugentid
    res = ugentldap.searchUgentLdap(base, filter, attrs)

    employee = None
    student = None

    if (len(res) > 0) and (len(res[0]) > 0) and (len(res[0][1]) > 0):
        objectClasses = res[0][1]['objectClass']
        if objectClasses.count('ugentEmployee') > 0:
            employee = True
        if objectClasses.count('ugentStudent') > 0:
            student = True

    return employee, student


def addAllUGID(users):
    updated_list = []
    for UGuser in users:
        UGuser.ugentid = getUGentID(UGuser.vscid)
        updated_list.append(UGuser)
    return updated_list


def addEmplStudent(users):
    updated_list = []
    for UGuser in users:
        UGuser.employee, UGuser.student = getUGentSubcs(UGuser.ugentid)
        updated_list.append(UGuser)
    return updated_list


def main():

    opts = simple_option({})  # provides debug and logging

    users = getAllDBusers(opts)
    users = addAllUGID(opts, users)
    users = addEmplStudent(opts, users)

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
    print "vscID - UGentID - Active - Employee - Student"
    print "-----------------------------------------------------------------"

    for user in users:
        print user
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
