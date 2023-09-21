from .logexception import LogException
from .logresponse import LogResponse
from .util import Util

__all__ = ['CreateScheduledSQLResponse', 'UpdateScheduledSQLResponse',
           'GetScheduledSQLResponse', 'ListScheduledSQLResponse', 'DeleteScheduledSQLResponse',
           "ListScheduledSqlJobInstancesResponse", "GetScheduledSqlJobInstanceResponse",
           "ModifyScheduledSqlJobStateResponse"]


class CreateScheduledSQLResponse(LogResponse):
    """
    Response of create_schedule_sql
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('CreateScheduleSqlResponse:')
        print('headers:', self.get_all_headers())


class UpdateScheduledSQLResponse(LogResponse):
    """ The response of the update_scheduled_sql API from log.
     :type resp: dict
     :param resp: UpdateScheduledSqlJobResponse HTTP response header
     :type header: dict
     :param header: UpdateScheduledSqlJobResponse HTTP response header
     """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('UpdateScheduledSqlJobResponse:')
        print('headers:', self.get_all_headers())


class GetScheduledSQLResponse(LogResponse):
    """ The response of the get_scheduled_sql API from log.
    :type resp: dict
    :param resp: GetScheduledSqlResponse HTTP response body
    :type header: dict
    :param header: GetScheduledSqlResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self.scheduled_sql_config = resp

    def get_scheduled_sql(self):
        """Get job from the response
        :return: dict, job
        """
        return self.scheduled_sql_config

    def log_print(self):
        print('GetScheduledSqlResponse:')
        print('headers:', self.get_all_headers())
        print('scheduled_sql: ', self.scheduled_sql_config)


class ListScheduledSQLResponse(LogResponse):
    """ The response of the list_scheduled_sql API from log.
      :type header: dict
      :param header: ListScheduledSqlJobResponse HTTP response header
      :type resp: dict
      :param resp: ListScheduledSqlJobResponse HTTP response body
      """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        try:
            self._count = int(resp["count"])
            self._total = int(resp["total"])
        except (KeyError, ValueError) as e:
            self._count = 0
            self._total = 0
            raise LogException('Failed to parse listScheduledSqlResponse count and total', '{}'.format(e))
        try:
            self._results = resp['results']
        except Exception as e:
            self._results = []
            raise LogException('Failed to parse jobs', '{}'.format(e))

    def get_scheduled_sql_jobs(self):
        """Get all the jobs from the response
        :return: list, all job
        """
        return self._results

    def get_count(self):
        """ Get count of jobs from the response
        :return: int, the number of count jobs from the response
        """
        return self._count

    def get_total(self):
        """Get total count of jobs from the response
        :return: int, the number of total jobs from the response
        """
        return self._total

    def log_print(self):
        print('ListScheduledSqlJobResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self._count))
        print('total:', str(self._total))
        print('jobs:', self._results)


class DeleteScheduledSQLResponse(LogResponse):
    """ The response of the delete_scheduledSql API from log.
    :type resp: dict
    :param resp: DeleteScheduledSqlResponse HTTP response body
    :type header: dict
    :param header: DeleteScheduledSqlResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('DeleteScheduledSqlResponse:')
        print('headers:', self.get_all_headers())


class ListScheduledSqlJobInstancesResponse(LogResponse):
    """ The response of the list_scheduled_sql_job_instance API from log.
    :type header: dict
    :param header: ListScheduledSqlJobInstanceResponse HTTP response header
    :type resp: dict
    :param resp: ListScheduledSqlJobInstanceResponse HTTP response body
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        try:
            self._count = int(resp["count"])
            self._total = int(resp["total"])
        except (KeyError, ValueError) as e:
            self._count = 0
            self._total = 0
            raise LogException('Failed to parse job count and total', '{}'.format(e))
        try:
            self._job_instances = resp.get("results")
        except Exception as e:
            self._job_instances = []
            raise LogException('Failed to parse job instances', '{}'.format(e))

    def get_scheduled_sql_jobs(self):
        """Get all the instances from the response
        :return: list, all job instance
        """
        return self._job_instances

    def get_count(self):
        """ Get count of instances from the response
        :return: int, the number of count instances from the response
        """
        return self._count

    def get_total(self):
        """Get total count of instances from the response
        :return: int, the number of total instances from the response
        """
        return self._total

    def log_print(self):
        print('ListScheduledSqlJobInstancesResponse:')
        print('headers:', self.get_all_headers())
        print('count:', str(self._count))
        print('total:', str(self._total))
        print('jobs:', self._job_instances)


class GetScheduledSqlJobInstanceResponse(LogResponse):
    """ The response of the get_scheduled_sql_job_instance API from log.
    :type resp: dict
    :param resp: GetScheduledSqlJobInstanceResponse HTTP response body
    :type header: dict
    :param header: GetScheduledSqlJobInstanceResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)
        self._instance = resp

    def get_instance(self):
        """Get instance from the response
        :return: dict, instance
        """
        return self._instance

    def log_print(self):
        print('GetScheduledSqlJobInstanceResponse:')
        print('headers:', self.get_all_headers())
        print('scheduled_sql: ', self._instance)


class ModifyScheduledSqlJobStateResponse(LogResponse):
    """ The response of the modify_scheduled_sql_instance_state API from log.
    :type resp: dict
    :param resp: ModifyScheduledSqlJobStateResponse HTTP response header
    :type header: dict
    :param header: ModifyScheduledSqlJobStateResponse HTTP response header
    """

    def __init__(self, header, resp):
        LogResponse.__init__(self, header, resp)

    def log_print(self):
        print('ModifyScheduledSqlJobStateResponse:')
        print('headers:', self.get_all_headers())
