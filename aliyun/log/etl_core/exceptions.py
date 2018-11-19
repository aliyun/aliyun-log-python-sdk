from ..logexception import LogException


class SettingError(LogException):
    def __init__(self, ex=None, settings=""):
        super(SettingError, self).__init__('InvalidConfig', 'Invalid Settings "{0}"\nDetail: {1}'.format(settings, ex))
        self.settings = settings

