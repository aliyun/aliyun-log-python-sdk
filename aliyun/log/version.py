__version__ = '0.6.45'

import sys
OS_VERSION = str(sys.platform)
PYTHON_VERSION = str(sys.version_info)
USER_AGENT = 'log-python-sdk-v-' + __version__ + ", {0}, {1}".format(PYTHON_VERSION, OS_VERSION)
LOGGING_HANDLER_USER_AGENT = 'logging-handler, ' + USER_AGENT
ES_MIGRATION_USER_AGENT = 'es-migration, ' + USER_AGENT
API_VERSION = '0.6.0'
