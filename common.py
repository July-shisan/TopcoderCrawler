from __future__ import print_function

import cookielib
import json
import random
import sys
import time
import urllib2
import zlib
from urllib import urlencode


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


token = ""


def prepare(use_proxy):
    ckp = urllib2.HTTPCookieProcessor(cookielib.CookieJar())

    if use_proxy:
        proxy_handler = urllib2.ProxyHandler({"http": "http://127.0.0.1:8087"})
        opener = urllib2.build_opener(proxy_handler, ckp)
    else:
        null_proxy_handler = urllib2.ProxyHandler({})
        opener = urllib2.build_opener(null_proxy_handler, ckp)

    urllib2.install_opener(opener)


def login():
    global token

    user = {
        "username": "vanxining",
        "password": "z20672067"
    }

    request = urllib2.Request("http://api.topcoder.com/v2/auth")
    response_body = urllib2.urlopen(request, urlencode(user)).read()

    resp = json.loads(response_body)
    token = resp["token"]


_std_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:52.0) Gecko/20100101 Firefox/52.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip, deflate",
    "Cache-Control": "no-cache",
    "Dnt": "1",
}


def open_request_and_read(request):
    response = urllib2.urlopen(request)
    content = response.read()
    encoding = response.info().get("Content-Encoding")

    if encoding == "gzip":
        content = zlib.decompress(content, zlib.MAX_WBITS | 16)
    elif encoding == "deflate":
        content = zlib.decompress(content, -zlib.MAX_WBITS)

    return content


def make_request(method):
    headers = {}

    if token:
        headers["Authorization"] = "Bearer " + token

    headers.update(_std_headers)

    url = "http://api.topcoder.com" + method.encode("utf-8")
    request = urllib2.Request(url, headers=headers)

    return request


def guarded_read(method):
    while True:
        try:
            return open_request_and_read(make_request(method))
        except urllib2.HTTPError, e:
            eprint("HTTP Error", e.code, e.msg)
            eprint(e.geturl())
            eprint(e.fp.read())
        except Exception, e:
            eprint(e)

        random_sleep(20)


def simple_read(url):
    return open_request_and_read(urllib2.Request(url, headers=_std_headers))


def to_json(raw):
    if not isinstance(raw, unicode):
        raw = raw.decode("utf-8")

    d = json.loads(raw)
    if u"requesterInformation" in d:
        del d[u"requesterInformation"]
        del d[u"serverInformation"]

    return d


def random_sleep(base=2):
    time.sleep(base + random.random())
