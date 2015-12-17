
import cookielib
from StringIO import StringIO
from urllib import urlencode
import urllib2
import gzip

import json
import time
import random


token = None


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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
    "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
    "Accept-Encoding": "gzip",
    "Cache-Control": "no-cache",
}


def open_request_and_read(request):
    response = urllib2.urlopen(request)
    if response.info().get("Content-Encoding") == "gzip":
        buf = StringIO(response.read())
        f = gzip.GzipFile(fileobj=buf)

        return f.read()
    else:
        return response.read()


def make_request(method):
    headers = {}

    if token is not None:
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
            print "HTTP Error", e.code, e.msg
            print e.geturl()
            print e.fp.read()
        except Exception, e:
            print e

        random_sleep(20)


def simple_read(url):
    return open_request_and_read(urllib2.Request(url, headers=_std_headers))


def to_json(raw):
    assert isinstance(raw, unicode)

    d = json.loads(raw)
    if u"requesterInformation" in d:
        del d[u"requesterInformation"]
        del d[u"serverInformation"]

    return d


def random_sleep(base=2):
    time.sleep(base + random.random())
