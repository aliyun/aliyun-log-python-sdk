

class CheckPointException(Exception):
    def __init__(self, message, cause=None):
        if cause is not None:
            super(CheckPointException, self).__init__(message, cause)
        else:
            super(CheckPointException, self).__init__(message)


class ClientWorkerException(Exception):

    def __init__(self, message, cause=None):
        if cause is not None:
            super(ClientWorkerException, self).__init__(message, cause)
        else:
            super(ClientWorkerException, self).__init__(message)
