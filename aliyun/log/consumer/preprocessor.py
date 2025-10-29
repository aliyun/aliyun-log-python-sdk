def to_log_group_list_pb(pull_log_response):
    return pull_log_response.get_loggroup_list()


def to_flattern_json_list(pull_log_response):
    return pull_log_response.get_flatten_logs_json()
