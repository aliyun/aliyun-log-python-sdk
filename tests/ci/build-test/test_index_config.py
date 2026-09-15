import json

from aliyun.log import IndexConfig, IndexLineConfig, LogClient


def test_auto_index_defaults_and_legacy_response():
    line = IndexLineConfig()
    assert line.auto_key_detect is False
    assert line.auto_text_keys == []
    assert line.to_json()['auto_key_detect'] is False
    assert line.to_json()['auto_text_keys'] == []
    line.auto_key_detect = True
    line.auto_text_keys.append('host')
    assert IndexLineConfig().auto_text_keys == []
    line.from_json({'token': [',', ' ']})
    assert line.auto_key_detect is False
    assert line.auto_text_keys == []


def test_auto_index_create_get_update(monkeypatch):
    client = LogClient('cn-mock.example.com', 'mock-id', 'mock-key')
    requests = []
    returned_line = {
        'token': [',', ' '], 'caseSensitive': False,
        'auto_key_detect': True, 'auto_text_keys': ['host', 'request_id', 'latency'],
    }

    def send(method, project, body, resource, params, headers):
        assert project == 'my-project'
        assert resource == '/logstores/my-logs/index'
        if method == 'GET':
            return {'line': returned_line}, {}
        requests.append((method, json.loads(body.decode('utf-8'))))
        return {}, {}

    monkeypatch.setattr(client, '_send', send)
    line = IndexLineConfig(token_list=[',', ' '], auto_key_detect=True,
                           auto_text_keys=['host', 'request_id', 'latency'])
    client.create_index('my-project', 'my-logs', IndexConfig(line_config=line))
    config = client.get_index_config('my-project', 'my-logs').get_index_config()
    assert config.line_config.auto_key_detect is True
    assert config.line_config.auto_text_keys == returned_line['auto_text_keys']
    config.line_config.case_sensitive = True
    client.update_index('my-project', 'my-logs', config)
    config.line_config.auto_text_keys = ['host']
    client.update_index('my-project', 'my-logs', config)
    config.line_config.auto_text_keys = []
    client.update_index('my-project', 'my-logs', config)

    assert [method for method, body in requests] == ['POST', 'PUT', 'PUT', 'PUT']
    assert requests[0][1]['line']['auto_key_detect'] is True
    assert requests[0][1]['line']['auto_text_keys'] == returned_line['auto_text_keys']
    assert requests[1][1]['line']['auto_text_keys'] == returned_line['auto_text_keys']
    assert requests[1][1]['line']['caseSensitive'] is True
    assert requests[2][1]['line']['auto_text_keys'] == ['host']
    assert requests[3][1]['line']['auto_text_keys'] == []


def test_auto_index_preserves_existing_optional_arguments():
    line = IndexLineConfig([], False, None, None, False, True, 100, ['host'])
    assert line.to_json()['auto_key_count_limit'] == 100
    assert line.to_json()['auto_text_keys'] == ['host']
    line.auto_key_detect = False
    assert 'auto_key_count_limit' not in line.to_json()
    assert line.to_json()['auto_text_keys'] == ['host']
    line.auto_key_detect = None
    assert 'auto_key_detect' not in line.to_json()
