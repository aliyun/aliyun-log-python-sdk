import requests

#: 每个Session连接池大小
connection_pool_size = 10


class Session(object):
    """属于同一个Session的请求共享一组连接池，如有可能也会重用HTTP连接。"""

    def __init__(self, pool_size=None, adapter=None):
        self.session = requests.Session()

        psize = pool_size or connection_pool_size
        if adapter is None:
            self.session.mount('http://', requests.adapters.HTTPAdapter(pool_connections=psize, pool_maxsize=psize))
            self.session.mount('https://', requests.adapters.HTTPAdapter(pool_connections=psize, pool_maxsize=psize))
        else:
            self.session.mount('http://', adapter)
            self.session.mount('https://', adapter)

    def do_request(self, method, url, params, data, headers, timeout):
        return self.session.request(method,
                                    url,
                                    data=data,
                                    params=params,
                                    headers=headers,
                                    stream=True,
                                    timeout=timeout)
