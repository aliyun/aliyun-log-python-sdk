# encoding: utf-8
from __future__ import print_function

from aliyun.log import *
import time
import os

dashboard_detail = {
  "charts": [
    {
      "display": {
        "displayName": "",
        "height": 5,
        "width": 5,
        "xAxis": [
          "province"
        ],
        "xPos": 0,
        "yAxis": [
          "pv"
        ],
        "yPos": 0
      },
      "search": {
        "end": "now",
        "logstore": "access-log",
        "query": "method:  GET  | select  ip_to_province(remote_addr) as province , count(1) as pv group by province order by pv desc ",
        "start": "-86400s",
        "topic": ""
      },
      "title": "map",
      "type": "map"
    },
    {
      "display": {
        "displayName": "",
        "height": 5,
        "width": 5,
        "xAxis": [
          "province"
        ],
        "xPos": 5,
        "yAxis": [
          "pv"
        ],
        "yPos": 0
      },
      "search": {
        "end": "now",
        "logstore": "access-log",
        "query": "method:  POST  | select  ip_to_province(remote_addr) as province , count(1) as pv group by province order by pv desc ",
        "start": "-86400s",
        "topic": ""
      },
      "title": "post_map",
      "type": "map"
    }
  ],
  "dashboardName": 'alert_' + str(time.time()).replace('.', '-'),
  "description": ""
}

dashboard_for_alert = 'dashboard_' + str(time.time()).replace('.', '-')

alert_detail = {
    "name": 'alert_' + str(time.time()).replace('.', '-'),
    "displayName": "Alert for testing",
    "description": "",
    "type": "Alert",
    "state": "Enabled",
    "schedule": {
        "type": "FixedRate",
        "interval": "5m",
    },
    "configuration": {
        "condition": "total >= 100",
        "dashboard": dashboard_for_alert,
        "queryList": [
            {
                "logStore": "test-logstore",
                "start": "-120s",
                "end": "now",
                "timeSpanType": "Custom",
                "chartTitle": "chart-test",
                "query": "* | select count(1) as total",
            }
        ],
        "notificationList": [
            {
                "type": "DingTalk",
                "atMobiles": ['1867393xxxx'],
                "serviceUri": "https://oapi.dingtalk.com/robot/send?access_token=xxxx",
                "content": "Hi @1867393xxxx, your alert ${AlertDisplayName} is fired",
            },
            {
                "type": "MessageCenter",
                "content": "Message",
            },
            {
                "type": "Email",
                "subject": "Alerting",
                "emailList": ["abc@test.com"],
                "content": "Email Message",
            },
            {
                "type": "SMS",
                "mobileList": ["132373830xx"],
                "content": "Cellphone message"
            },
            {
                "type": "Voice",
                "mobileList": ["132373830xx"],
                "content": "Voice notification...",
            }
        ],
        "muteUntil": int(time.time()) + 300,
        "notifyThreshold": 1,
        "throttling": "5m",
    }
}

savedsearch_detail = {
    "logstore": "test2",
    "savedsearchName": 'search_' + str(time.time()).replace('.', '-'),
    "searchQuery": "boy | select sex, count() as Count group by sex",
    "topic": ""
}


def main():
    endpoint = os.environ.get('ALIYUN_LOG_SAMPLE_ENDPOINT', '')
    accessKeyId = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSID', '')
    accessKey = os.environ.get('ALIYUN_LOG_SAMPLE_ACCESSKEY', '')
    project = os.environ.get('ALIYUN_LOG_SAMPLE_PROJECT', '')

    dashboard = dashboard_detail.get('dashboardName')

    client = LogClient(endpoint, accessKeyId, accessKey, "")

    res = client.create_dashboard(project, dashboard_detail)
    res.log_print()

    res = client.list_dashboard(project)
    res.log_print()

    res = client.get_dashboard(project, dashboard)
    res.log_print()

    res = client.update_dashboard(project, dashboard_detail)
    res.log_print()

    res = client.delete_dashboard(project, dashboard)
    res.log_print()

    dashboard_detail['dashboardName'] = dashboard_for_alert
    res = client.create_dashboard(project, dashboard_detail)
    res.log_print()

    alert = alert_detail.get('name')

    res = client.create_alert(project, alert_detail)
    res.log_print()

    res = client.list_alert(project)
    res.log_print()
    print(res.get_alerts())

    res = client.get_alert(project, alert)
    res.log_print()

    res = client.update_alert(project, alert_detail)
    res.log_print()

    res = client.delete_alert(project, alert)
    res.log_print()
    res = client.delete_dashboard(project, dashboard_for_alert)
    res.log_print()

    savedsearch = savedsearch_detail.get('savedsearchName')

    res = client.create_savedsearch(project, savedsearch_detail)
    res.log_print()

    res = client.list_savedsearch(project)
    res.log_print()
    print(res.get_savedsearches())

    res = client.get_savedsearch(project, savedsearch)
    res.log_print()

    res = client.update_savedsearch(project, savedsearch_detail)
    res.log_print()

    res = client.delete_savedsearch(project, savedsearch)
    res.log_print()


if __name__ == '__main__':
    main()


