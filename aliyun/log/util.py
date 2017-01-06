#!/usr/bin/env python
#encoding: utf-8

import re
import hmac
import zlib
import base64
import urllib
import socket
import hashlib
import collections

try:
    import json
except ImportError:
    import simplejson as json

class Util:
    
    @staticmethod
    def is_row_ip(ip):
        iparray = ip.split('.')
        if len(iparray)!=4:
            return False
        for tmp in iparray:
            if not tmp.isdigit() or int(tmp)>=256:
                return False
        pattern = re.compile(r'^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$')
        if pattern.match(ip):
            return True
        return False
    
    @staticmethod
    def get_host_ip(logHost):
        """ If it is not match your local ip, you should fill the PutLogsRequest
        parameter source by yourself.
        """
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((logHost, 80))
            ip = s.getsockname()[0]
            return ip
        except:
            return '127.0.0.1'
        finally:
            s.close()
    
    @staticmethod
    def compress_data(data):
        return zlib.compress(data,6)
    
    @staticmethod
    def cal_md5(content):
        return hashlib.md5(content).hexdigest().upper()

    @staticmethod
    def hmac_sha1(content, key):
        if isinstance(content, unicode): # hmac.new accept 8-bit str
            content = content.encode('utf-8')
        hashed = hmac.new(key, content, hashlib.sha1).digest()
        return base64.encodestring(hashed).rstrip()

    @staticmethod
    def canonicalized_log_headers(headers):
        content = ''
        for key in sorted(headers.iterkeys()):
            if key[:6]=='x-log-' or key[:6] =='x-acs-':  # x-log- header
                content += key+':'+str(headers[key])+"\n";
        return content
    
    @staticmethod
    def url_encode(params):
        for key, value in params.items(): # urllib.urlencode accept 8-bit str
            if isinstance(value, unicode):
                params[key] = value.encode('utf-8')
        return urllib.urlencode(params, True)
    
    @staticmethod
    def canonicalized_resource(resource, params):
        if params:
            urlString = ''
            for key in sorted(params.iterkeys()):
                value = params[key]
                if not isinstance(value, unicode): # int, float, str to unicode 
                    value = unicode(str(value), 'utf-8')
                urlString += key+'='+value+'&'
            resource = resource+'?'+urlString[:-1] # strip the last &
        return resource
    
    @staticmethod
    def get_request_authorization(method, resource, key, params, headers):
        if not key:
            return ''
        content = method+"\n"
        if 'Content-MD5' in headers:
            content += headers['Content-MD5']
        content += '\n'
        if 'Content-Type' in headers:
            content += headers['Content-Type']
        content += "\n"
        content += headers['Date']+"\n"
        content += Util.canonicalized_log_headers(headers)
        content += Util.canonicalized_resource(resource, params)
        return Util.hmac_sha1(content, key)
    

    @staticmethod
    def convert_unicode_to_str(data):
        if isinstance(data, basestring):
            return str(data)
        elif isinstance(data, collections.Mapping):
            return dict(map(Util.convert_unicode_to_str, data.iteritems()))
        elif isinstance(data, collections.Iterable):
            return type(data)(map(Util.convert_unicode_to_str, data))
        else:
            return data
        
    @staticmethod
    def get_json_value(json_map, key, default_value = None) : 
        if key in json_map : 
            return json_map[key]
        return default_value


