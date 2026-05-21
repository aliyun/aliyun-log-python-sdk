import time
from aliyun.log import LogClient


def main():
    client = LogClient("cn-hangzhou.log.aliyuncs.com",
                       "your_access_key_id", "your_access_key_secret")

    # replace with an unique job_name here
    job_name = 'test-rebuild-index-test-1'

    project = 'your_project'
    logstore = 'your_logstore'

    # to_time must <= now - 900
    to_time = int(time.time()) - 900
    from_time = to_time - 3600
    display_name = "this is create rebuild index test job"
    client.create_rebuild_index(project,
                                logstore,
                                job_name,
                                display_name,
                                from_time,
                                to_time)

    # wait a while util job complete
    while True:
        print('wait rebuild index done...')
        time.sleep(10)
        resp = client.get_rebuild_index(project, job_name)
        resp.log_print()

        if resp.get_status() == 'SUCCEEDED':
            print('rebuild index succeed')
            break


if __name__ == "__main__":
    main()
