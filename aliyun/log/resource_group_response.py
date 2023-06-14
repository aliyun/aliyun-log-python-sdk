from .logresponse import LogResponse

__all__ = [
    'ChangeResourceGroupResponse',
]

class ChangeResourceGroupResponse(LogResponse):
    """
    Response of change_resource_group
    """

    def __init__(self, header, resp=''):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('ChangeResourceGroupResponse:')
        print('headers:', self.get_all_headers())