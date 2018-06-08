#
# Copyright 2013-2018 Ghent University
#
# This file is part of vsc-administration,
# originally created by the HPC team of Ghent University (http://ugent.be/hpc/en),
# with support of Ghent University (http://ugent.be/hpc),
# the Flemish Supercomputer Centre (VSC) (https://www.vscentrum.be),
# the Flemish Research Foundation (FWO) (http://www.fwo.be/en)
# and the Department of Economy, Science and Innovation (EWI) (http://www.ewi-vlaanderen.be/en).
#
# https://github.com/hpcugent/vsc-administration
#
# All rights reserved.
#
"""
Functions to deploy users to slurm.
"""
import logging
import subprocess
import tempfile

from vsc.accountpage.wrappers import mkNamedTupleInstance
from vsc.config.base import INSTITUTE_VOS, ANTWERPEN, BRUSSEL, GENT, LEUVEN
from vsc.utils.missing import namedtuple_with_defaults

SLURM_SACCT_MGR = "/usr/bin/sacctmgr"

SLURM_ORGANISATIONS = {
    ANTWERPEN: 'uantwerpen',
    BRUSSEL: 'vub',
    GENT: 'ugent',
    LEUVEN: 'kuleuven',
}


ACCOUNTS = "accounts"
USERS = "users"
IGNORE_USERS = ["root"]
IGNORE_ACCOUNTS = ["root"]

SacctUserFields = ["User", "Def_Acct", "Admin", "Cluster", "Account", "Partition", "Share",
                   "MaxJobs", "MaxNodes", "MaxCPUs", "MaxSubmit", "MaxWall", "MaxCPUMins",
                   "QOS", "Def_QOS"]
SacctAccountFields = ["Account", "Descr", "Org", "Cluster", "Par_Name", "User", "Share",
                      "GrpJobs", "GrpNodes", "GrpCPUs", "GrpMem", "GrpSubmit", "GrpWall", "GrpCPUMins",
                      "MaxJobs", "MaxNodes", "MaxCPUs", "MaxSubmit", "MaxWall", "MaxCPUMins",
                      "QOS", "Def_QOS"]

SlurmAccount = namedtuple_with_defaults('SlurmAccount', SacctAccountFields)
SlurmUser = namedtuple_with_defaults('SlurmUser', SacctUserFields)


def mkSlurmAccount(fields):
    """Make a named tuple from the given fields."""
    account = mkNamedTupleInstance(fields, SlurmAccount)
    if account.Account in IGNORE_ACCOUNTS:
        return None
    return account


def mkSlurmUser(fields):
    """Make a named tuple from the given fields."""
    user = mkNamedTupleInstance(fields, SlurmUser)
    if user.User in IGNORE_USERS:
        return None
    return user


def parse_slurm_acct_line(header, line, info_type, user_field_number):
    """Parse the line into the correct data type."""
    fields = line.split("|")

    if info_type == ACCOUNTS:
        if fields[user_field_number]:
            # association information for a user. Users are processed later.
            return None
        creator = mkSlurmAccount
    elif info_type == USERS:
        creator = mkSlurmUser
    else:
        return None

    return creator(dict(zip(header, fields)))


def parse_slurm_acct_dump(lines, info_type):
    """
    Parse the accounts from the listing.
    """
    acct_info = set()

    header = [w.replace(' ', '_') for w in lines[0].rstrip().split("|")]
    user_field_number = [h.lower() for h in header].index("user")

    for line in lines[1:]:
        line = line.rstrip()
        try:
            info = parse_slurm_acct_line(header, line, info_type, user_field_number)
            if info:
                acct_info.add(info)
        except Exception, err:
            logging.exception("Slurm acct sync: could not process line %s [%s]", line, err)

    return acct_info


def get_slurm_acct_info(info_type):
    """Get slurm account info for the given clusterself.

    @param info_type: this is either "accounts" or "users"
    """
    contents = None
    outputFile = tempfile.NamedTemporaryFile(delete=True)
    with open(outputFile.name, 'r+') as f:
        try:
            subprocess.check_call([
                SLURM_SACCT_MGR,
                "-s",
                "-P",
                "list",
                "%s" % info_type,
                ],
                stdout=f
            )
        except subprocess.CalledProcessError, err:
            logging.error("Could not get sacctmgr output: error %d", err.returncode)
            raise

        f.flush()
        f.seek(0)
        contents = f.readlines()
        logging.debug("read %d lines" % len(contents))

    info = parse_slurm_acct_dump(contents, info_type)

    return info


def create_add_account_command(account, parent, organisation, cluster):
    """
    Creates the command to add the given account.

    @param account: name of the account to add
    @param parent: name of the parent account. If None then parent will be "root".
    @param organisation: name of the organisation to which the account belongs.
    @param cluster: cluster to which the account must be added

    @returns: string comprising the command
    """
    CREATE_ACCOUNT_COMMAND = [
        SLURM_SACCT_MGR,
        "add", 
        "account", 
        account, 
        "Parent={parent}".format(parent=(parent or "root")),
        "Organization={organisation}".format(organisation=SLURM_ORGANISATIONS[organisation]),
        "Cluster={cluster}".format(cluster=cluster),
    ]
    logging.debug(
        "Adding account %s with Parent=%s Cluster=%s Organization=%s",
        account,
        parent,
        cluster,
        organisation,
        )

    return CREATE_ACCOUNT_COMMAND


def create_add_user_command(user, vo_id, cluster):
    """
    Creates the command to add the given account.

    @param account: name of the account to add
    @param parent: name of the parent account. If None then parent will be "root".
    @param organisation: name of the organisation to which the account belongs.
    @param cluster: cluster to which the account must be added

    @returns: string comprising the command
    """
    CREATE_USER_COMMAND = [
        SLURM_SACCT_MGR,
        "add",
        "user", 
        user,
        "Account={account}".format(account=vo_id),
        "Cluster={cluster}".format(cluster=cluster)
    ]
    logging.debug(
        "Adding user %s with Account=%s Cluster=%s",
        user,
        vo_id,
        cluster,
        )

    return CREATE_USER_COMMAND


def create_change_user_command(user, vo_id, cluster):
    CHANGE_USER_COMMAND = [
        SLURM_SACCT_MGR,
        "update",
        "user={user}".format(user=user),
        "where",
        "Cluster={cluster}".format(cluster=cluster),
        "set",
        "DefaultAccount={account}".format(account=vo_id),
        "Account={account}".format(account=vo_id)
    ]
    logging.debug(
        "Changing user %s on Cluster=%s to DefaultAccount=%s",
        user,
        cluster,
        vo_id,
        )

    return CHANGE_USER_COMMAND


def create_remove_user_command(user, cluster):
    REMOVE_USER_COMMAND = [
        SLURM_SACCT_MGR,
        "delete",
        "user",
        "name={user}".format(user=user),
        "Cluster={cluster}".format(cluster=cluster)
    ]
    logging.debug(
        "Removing user %s from Cluster=%s",
        user,
        cluster,
        )

    return REMOVE_USER_COMMAND


def slurm_institute_accounts(slurm_account_info, clusters):
    """Check for the presence of the institutes and their default VOs in the slurm account list.

    @returns: list of sacctmgr commands to add the accounts to the clusters if needed
    """
    commands = []
    for cluster in clusters:
        cluster_accounts = [acct.Account for acct in slurm_account_info if acct and acct.Cluster == cluster]
        for (inst, vo) in INSTITUTE_VOS.items():
            if inst not in cluster_accounts:
                commands.append(
                    create_add_account_command(account=inst, parent=None, cluster=cluster, organisation=inst)
                )
            if vo not in cluster_accounts:
                commands.append(
                    create_add_account_command(account=vo, parent=inst, cluster=cluster, organisation=inst)
                )

    return commands


def slurm_vo_accounts(account_page_vos, slurm_account_info, clusters):
    """Check for the presence of the new/changed VOs in the slurm account list.

    @returns: list of sacctmgr commands to add the accounts for VOs if needed
    """
    commands = []
    for cluster in clusters:
        cluster_accounts = [acct.Account for acct in slurm_account_info if acct and acct.Cluster == cluster]

        for vo in account_page_vos:
            if vo.vsc_id in INSTITUTE_VOS.values():
                continue

            if vo.vsc_id not in cluster_accounts:
                commands.append(create_add_account_command(
                    account=vo.vsc_id,
                    parent=vo.institute['site'],
                    cluster=cluster,
                    organisation=vo.institute['site']
                ))

    return commands


def slurm_user_accounts(vo_members, active_accounts, slurm_user_info, clusters):
    """Check for the presence of the user in his/her account.

    @returns: list of sacctmgr commands to add the users if needed.
    """
    commands = []

    active_vo_members = set()
    reverse_vo_mapping = dict()
    for (members, vo) in vo_members.values():
        active_vo_members |= members & active_accounts

        for m in members:
            reverse_vo_mapping[m] = (vo.vsc_id, vo.institute["site"])

    for cluster in clusters:
        cluster_users_acct = [
            (user.User, user.Def_Acct) for user in slurm_user_info if user and user.Cluster == cluster
        ]
        cluster_users = set([u[0] for u in cluster_users_acct])

        # these are the users that need to be removed as they are no longer an active user in any
        # (including the institute default) VO
        remove_users = cluster_users - active_vo_members

        new_users = set()
        changed_users = set()

        for (vo_id, (members, vo)) in vo_members.items():

            # these are users not yet in the Slurm DB for this cluster
            new_users |= set([
                (user, vo.vsc_id, vo.institute["site"])
                for user in (members & active_accounts) - cluster_users
            ])

            # these are the current Slurm users per Account, i.e., the VO currently being processed
            slurm_acct_users = set([user for (user, acct) in cluster_users_acct if acct == vo_id])

            # these are the users that should no longer be in this account, but should not be removed
            # we need to look up their new VO
            # TODO: verify that we have sufficient information with the user and do not need the current Def_Acct
            changed_users |= (slurm_acct_users - members) & active_accounts

        moved_users = set([(user, reverse_vo_mapping[user]) for user in changed_users])

        commands.extend([create_add_user_command(
            user=user,
            vo_id=vo_id,
            cluster=cluster) for (user, vo_id, _) in new_users
        ])
        commands.extend([create_remove_user_command(user=user, cluster=cluster) for user in remove_users])
        commands.extend([create_change_user_command(
            user=user,
            vo_id=vo_id,
            cluster=cluster) for (user, (vo_id, _)) in moved_users
        ])

    return commands
