
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


def make_request(method):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
        "Accept-Language": "zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control": "no-cache",
    }

    if token is not None:
        headers["Authorization"] = "Bearer " + token

    url = "http://api.topcoder.com" + method.encode("utf-8")
    request = urllib2.Request(url, headers=headers)

    return request


def simple_read(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:41.0) Gecko/20100101 Firefox/41.0",
        "Accept-Encoding": "gzip",
        "Cache-Control": "no-cache",
    }

    request = urllib2.Request(url, headers=headers)
    response = urllib2.urlopen(request)

    if response.info().get("Content-Encoding") == "gzip":
        buf = StringIO(response.read())
        f = gzip.GzipFile(fileobj=buf)
        return f.read()
    else:
        return response.read()


def to_json(raw):
    d = json.loads(raw)
    if "requesterInformation" in d:
        del d["requesterInformation"]
        del d["serverInformation"]

    return d


def random_sleep(base=2):
    time.sleep(base + random.random())
