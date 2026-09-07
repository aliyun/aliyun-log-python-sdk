import json

from aliyun.log import LogClient


def test_create_metric_store_uses_labels(monkeypatch):
    client = LogClient('cn-mock.example.com', 'mock-id', 'mock-key')
    requests = []

    def send(method, project, body, resource, params, headers):
        requests.append((method, project, resource, json.loads(body.decode('utf-8'))))
        return {}, {}

    monkeypatch.setattr(client, '_send', send)
    monkeypatch.setattr('aliyun.log.logclient.time.sleep', lambda seconds: None)
    response = client.create_metric_store('my-project', 'my-metrics', ttl=18)

    assert [(method, project, resource) for method, project, resource, body in requests] == [
        ('POST', 'my-project', '/logstores'),
        ('POST', 'my-project', '/logstores/my-metrics/substores'),
    ]
    assert requests[0][3]['telemetryType'] == 'Metrics'
    assert requests[0][3]['logstoreName'] == 'my-metrics'
    assert requests[0][3]['ttl'] == 18
    assert requests[1][3] == {
        'name': 'prom', 'ttl': 18, 'sortedKeyCount': 2, 'timeIndex': 2,
        'keys': [
            {'name': '__name__', 'type': 'text'},
            {'name': '__labels__', 'type': 'labels'},
            {'name': '__time_nano__', 'type': 'long'},
            {'name': '__value__', 'type': 'double'},
        ],
    }
    assert response.get_logstore_response() is not None
    assert response.get_substore_response() is not None
