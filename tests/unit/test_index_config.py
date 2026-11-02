# encoding: utf-8
"""Index configuration round trips without contacting an SLS endpoint."""

import json

import pytest

from aliyun.log import IndexConfig, IndexKeyConfig, IndexLineConfig, LogClient


@pytest.mark.parametrize('position_index', [None, True, False])
@pytest.mark.parametrize('default_phrase_query', [None, True, False])
def test_phrase_options_round_trip(position_index, default_phrase_query):
    config = IndexConfig(position_index=position_index,
                         default_phrase_query=default_phrase_query)
    body = config.to_json()
    for key, value in [('position_index', position_index),
                       ('default_phrase_query', default_phrase_query)]:
        if value is None:
            assert key not in body
        else:
            assert body[key] is value

    loaded = IndexConfig()
    loaded.from_json(json.loads(json.dumps(body)))
    assert loaded.position_index is position_index
    assert loaded.default_phrase_query is default_phrase_query
    assert loaded.to_json() == body


def test_legacy_response_clears_previous_phrase_options():
    config = IndexConfig(position_index=True, default_phrase_query=True)
    config.from_json({'line': {'token': [' '], 'caseSensitive': False}})
    assert config.position_index is None
    assert config.default_phrase_query is None
    assert 'position_index' not in config.to_json()
    assert 'default_phrase_query' not in config.to_json()


def test_existing_positional_arguments_are_unchanged():
    line = IndexLineConfig(token_list=[' '])
    keys = {'message': IndexKeyConfig(token_list=[' '])}
    config = IndexConfig(7, line, keys, None, True, False)
    assert config.ttl == 7
    assert config.to_json() == {
        'line': line.to_json(), 'keys': {'message': keys['message'].to_json()},
        'log_reduce': True, 'scan_index': False,
    }


@pytest.mark.parametrize('default_phrase_query', [True, False])
def test_create_get_update_preserves_and_disables_phrase_query(monkeypatch, default_phrase_query):
    client = LogClient('cn-mock.example.com', 'mock-id', 'mock-key')
    requests = []
    returned_config = {
        'line': {'token': [' '], 'caseSensitive': False},
        'position_index': True,
        'default_phrase_query': default_phrase_query,
    }

    def send(method, project, body, resource, params, headers):
        assert project == 'my-project'
        assert resource == '/logstores/my-logs/index'
        if method == 'GET':
            return returned_config, {}
        requests.append((method, json.loads(body.decode('utf-8'))))
        return {}, {}

    monkeypatch.setattr(client, '_send', send)
    config = IndexConfig(line_config=IndexLineConfig(token_list=[' ']),
                         position_index=True, default_phrase_query=default_phrase_query)
    client.create_index('my-project', 'my-logs', config)
    config = client.get_index_config('my-project', 'my-logs').get_index_config()
    assert config.position_index is True
    assert config.default_phrase_query is default_phrase_query

    config.line_config.case_sensitive = True
    client.update_index('my-project', 'my-logs', config)
    config.set_default_phrase_query(False)
    client.update_index('my-project', 'my-logs', config)
    config.set_default_phrase_query(None)
    config.set_position_index(None)
    client.update_index('my-project', 'my-logs', config)

    assert [method for method, body in requests] == ['POST', 'PUT', 'PUT', 'PUT']
    assert requests[0][1]['position_index'] is True
    assert requests[0][1]['default_phrase_query'] is default_phrase_query
    assert requests[1][1]['position_index'] is True
    assert requests[1][1]['default_phrase_query'] is default_phrase_query
    assert requests[1][1]['line']['caseSensitive'] is True
    assert requests[2][1]['position_index'] is True
    assert requests[2][1]['default_phrase_query'] is False
    assert 'position_index' not in requests[3][1]
    assert 'default_phrase_query' not in requests[3][1]
