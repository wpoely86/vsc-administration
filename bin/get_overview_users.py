#!/usr/bin/env python
#
# Copyright 2012-2016 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://vscentrum.be/nl/en),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
This script produces an overview of the HPC users.

@author: Andy Georges
@author: Wouter Depypere
"""

from collections import namedtuple
from sqlalchemy import MetaData, Table, create_engine, select
from urllib2 import HTTPError

from vsc.accountpage.client import AccountpageClient
from vsc.accountpage.wrappers import mkVscPerson
from vsc.config.base import GENT
from vsc.config.options import VscOptions
from vsc.ldap.configuration import UGentLdapConfiguration
from vsc.ldap.filters import LdapFilter
from vsc.ldap.utils import LdapQuery
from vsc.utils import fancylogger
from vsc.utils.missing import Monoid, MonoidDict
from vsc.utils.script_tools import ExtendedSimpleOption

User = namedtuple('User', ['vscid', 'ugentid', 'active', 'employee', 'student'])

log = fancylogger.getLogger(__name__)
fancylogger.logToScreen(True)
fancylogger.logLevelInfo()

CONFIG_FILE = '/etc/vsc_conf.cfg'
PASSWD_FILE = '/etc/vsc_passwd.cfg'
DATABASE_NAME = "hpccollector"
DATABASE_USER = "hpccollector"


def get_hpc_collector_users(db, members):
    """Get the users from UGent in the HPC collector database."""
    users = [
        User(vscid=u, ugentid=None, active=a, employee=False, student=False) for (u, i, a) in
        db.execute(select([members.c.uid, members.c.active]).where(members.c.inst == GENT)).fetchall()
    ]
    log.debug("Found the following users in the HPC collector DB: %s", users)
    return users


def get_ugent_id(opts, client, vscuid):
    """Retrieve the UGent login from the account page"""
    try:
        person = mkVscPerson(**client.account[vscuid].person.get()[1])
        return person.institute_login
    except HTTPError:
        log.warning("Cannot fetch information for user %s", vscuid)
        return None


def ugent_status(opts, ldap_query, ugentid):
    """Check the UGent object classes for this users and return a tuple."""
    ldap_filter = LdapFilter("uid=%s" % (ugentid))

    users = ldap_query.user_filter_search(ldap_filter, ['objectClass'])

    if users:
        object_classes = users[0]['objectClass']
        employee = object_classes.count('ugentEmployee') > 0
        student = object_classes.count('ugentStudent') > 0
        opts.log.debug("User with UGent ID %s is employee: %s, student: %s" % (ugentid, employee, student))
        return (employee, student)
    else:
        return (False, False)


class UGentLdapQuery(LdapQuery):
    pass


def main():
    options = {
        'account_page_url': ('Base URL of the account page', None, 'store', 'https://account.vscentrum.be/django'),
        'access_token': ('OAuth2 token to access the account page REST API', None, 'store', None),
    }
    opts = ExtendedSimpleOption(options)

    global log
    log = opts.log

    vsc_options = VscOptions(go_args=[], go_configfiles=[CONFIG_FILE, PASSWD_FILE])

    client = AccountpageClient(token=opts.options.access_token)

    db_password = getattr(vsc_options.options, 'hpccollector_hpccollector')
    db_engine = create_engine('postgresql://%s:%s@localhost/%s' % (DATABASE_USER, db_password, DATABASE_NAME))
    db_connection = db_engine.connect()
    meta = MetaData()

    member = Table('member', meta, autoload=True, autoload_with=db_engine)

    users = get_hpc_collector_users(db_connection, member)
    users = [u._replace(ugentid=get_ugent_id(opts, client, u.vscid)) for u in users]

    ugent_ldap_query = UGentLdapQuery(UGentLdapConfiguration("collector"))  # Initialise the LDAP connection
    users = [u._replace(employee=employee, student=student) for u in users for (employee, student) in
             [ugent_status(opts, ugent_ldap_query, u.ugentid)]]

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
