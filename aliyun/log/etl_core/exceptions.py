from ..logexception import LogException


class SettingError(LogException):
    def __init__(self, ex=None, settings="", msg=""):
        if msg and settings:
            msg += '\nInvalid Settings "{0}"'.format(settings)
        else:
            msg = msg or 'Invalid Settings "{0}"'.format(settings or 'unknown')

        super(SettingError, self).__init__('InvalidConfig', '{0}\nDetail: {1}'.format(msg, ex))
        self.settings = settings

