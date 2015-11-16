# Due to #11
#from vsc.install.testing import VSCImportTest
import vsc.install.testing

class ImportTest(vsc.install.testing.VSCImportTest):
    EXCLUDE_SCRIPTS = [
        r'sync_django_ldap', # requires django
        r'get_overview_users', # requires vsc.pg
    ]
