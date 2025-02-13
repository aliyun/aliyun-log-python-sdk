"""
syslog RFC
https://datatracker.ietf.org/doc/rfc5424/
https://tools.ietf.org/html/rfc3164
"""
import socket
from datetime import datetime
import ssl
import six


def datetime2rfc3339(dt, is_utc=False):
    if not is_utc:
        # calculating timezone
        d1 = datetime.now()
        d2 = datetime.utcnow()
        diff_hr = round((d1 - d2).seconds / 60 / 60)
        tz = ""

        if diff_hr == 0:
            tz = "Z"
        else:
            if diff_hr > 0:
                tz = "+%s" % (tz)

            tz = "%s%.2d%.2d" % (tz, diff_hr, 0)

        return "%s%s" % (dt.strftime("%Y-%m-%dT%H:%M:%S.%f"), tz)

    else:
        return dt.isoformat() + 'Z'


FAC_KERNEL = 0
FAC_USER = 1
FAC_MAIL = 2
FAC_SYSTEM = 3
FAC_SECURITY = 4
FAC_SYSLOG = 5
FAC_PRINTER = 6
FAC_NETWORK = 7
FAC_UUCP = 8
FAC_CLOCK = 9
FAC_AUTH = 10
FAC_FTP = 11
FAC_NTP = 12
FAC_LOG_AUDIT = 13
FAC_LOG_ALERT = 14
FAC_CLOCK2 = 15
FAC_LOCAL0 = 16
FAC_LOCAL1 = 17
FAC_LOCAL2 = 18
FAC_LOCAL3 = 19
FAC_LOCAL4 = 20
FAC_LOCAL5 = 21
FAC_LOCAL6 = 22
FAC_LOCAL7 = 23

SEV_EMERGENCY = 0
SEV_ALERT = 1
SEV_CRITICAL = 2
SEV_ERROR = 3
SEV_WARNING = 4
SEV_NOTICE = 5
SEV_INFO = 6
SEV_DEBUG = 7


class SyslogClientBase(object):
    def __init__(self, server, port, proto='udp', clientname=None,
                 maxMessageLength=1024, timeout=120, cert_path=None):
        self.socket = None
        self.server = server
        self.port = port
        self.proto = socket.SOCK_DGRAM
        self.ssl_kwargs = None

        self.maxMessageLength = maxMessageLength
        self.timeout = timeout

        if proto is not None:
            if proto.upper() == 'UDP':
                self.proto = socket.SOCK_DGRAM
            elif proto.upper() == 'TCP':
                self.proto = socket.SOCK_STREAM
            elif proto.upper() == 'TLS':
                self.proto = socket.SOCK_STREAM
                self.ssl_kwargs ={
                                    'cert_reqs': ssl.CERT_REQUIRED,
                                    'ssl_version': ssl.PROTOCOL_TLS,
                                    'ca_certs': cert_path,
                                  }

        self.clientname = clientname or socket.getfqdn() or socket.gethostname()
        self.cert_path = cert_path

    def connect(self):
        if self.socket is None:
            r = socket.getaddrinfo(self.server, self.port, socket.AF_UNSPEC, self.proto)
            if r is None:
                return False
            for (addr_fam, sock_kind, proto, ca_name, sock_addr) in r:

                sock = socket.socket(addr_fam, self.proto)
                if six.PY3 and self.ssl_kwargs:
                    self.socket = ssl.wrap_socket(sock, **self.ssl_kwargs)
                else:
                    self.socket = sock

                if self.socket is None:
                    return False

                self.socket.settimeout(self.timeout)

                try:
                    self.socket.connect(sock_addr)
                    return True

                except socket.timeout as e:
                    if self.socket is not None:
                        self.socket.close()
                        self.socket = None
                    continue

                except socket.error as e:
                    if self.socket is not None:
                        self.socket.close()
                        self.socket = None
                    continue

            return False

        else:
            return True

    def __enter__(self):
        if not self.connect():
            raise ValueError("cannot connect to remote syslog server")

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        if self.socket is not None:
            self.socket.close()
            self.socket = None

    def log(self, message, timestamp=None, hostname=None, facility=None,
            severity=None):
        pass

    def send(self, messagedata):
        if self.socket is not None or self.connect():
            try:
                if self.maxMessageLength is not None:
                    self.socket.sendall(messagedata[:self.maxMessageLength])
                else:
                    self.socket.sendall(messagedata)
            except IOError as e:
                self.close()


class SyslogClientRFC5424(SyslogClientBase):
    def __init__(self, server, port, proto='udp',
                 clientname=None, timeout=120, cert_path=None):
        SyslogClientBase.__init__(self,
                                  server=server,
                                  port=port,
                                  proto=proto,
                                  clientname=clientname,
                                  maxMessageLength=None,
                                  timeout=timeout,
                                  cert_path=cert_path
                                  )

    def log(self, message, facility=None, severity=None, timestamp=None,
            hostname=None, version=1, program=None, pid=None, msgid=None):

        facility = FAC_USER if facility is None else facility
        severity = SEV_INFO if severity is None else severity
        pri = facility * 8 + severity
        hostname_s = hostname or self.clientname
        appname_s = program or "-"
        procid_s = pid or "-"
        msgid_s = msgid or "-"

        if timestamp is None:
            timestamp_s = datetime2rfc3339(datetime.utcnow(), is_utc=True)
        else:
            timestamp_s = datetime2rfc3339(timestamp, is_utc=False)

        d = "<%i>%i %s %s %s %s %s %s\n" % (
            pri,
            version,
            timestamp_s,
            hostname_s,
            appname_s,
            procid_s,
            msgid_s,
            message
        )

        self.send(d.encode('utf-8', errors='ignore'))


class SyslogClientRFC3164(SyslogClientBase):
    def __init__(self, server, port, proto='udp', clientname=None, timeout=120, cert_path=None):
        SyslogClientBase.__init__(self,
                                  server=server,
                                  port=port,
                                  proto=proto,
                                  clientname=clientname,
                                  maxMessageLength=1024,
                                  timeout=timeout,
                                  cert_path=cert_path
                                  )

    def log(self, message, facility=None, severity=None, timestamp=None,
            hostname=None, program="SyslogClient", pid=None):
        facility = FAC_USER if facility is None else facility
        severity = SEV_INFO if severity is None else severity

        pri = facility * 8 + severity

        t = timestamp or datetime.now()

        timestamp_s = t.strftime("%b %d %H:%M:%S")

        hostname_s = hostname or self.clientname

        tag_s = ""
        if program is None:
            tag_s += "SyslogClient"
        else:
            tag_s += program

        if pid is not None:
            tag_s += "[%i]" % (pid,)

        d = "<%i>%s %s %s: %s\n" % (
            pri,
            timestamp_s,
            hostname_s,
            tag_s,
            message
        )

        self.send(d.encode('ASCII', errors='ignore'))
