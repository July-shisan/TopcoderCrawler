#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import os
import traceback
import urllib2
from urllib import quote

import dateutil.parser
from pymongo import MongoClient

import common
import config.users as g_config


INVALID_HANDLES_FPATH = "config/invalid_handles"


def refine_user(d):
    d[u"handle"] = d[u"handle"].lower()

    for key in (u"createdAt", u"updatedAt",):
        d[key] = dateutil.parser.parse(d[key])


def user_skills(d):
    quoted = quote(d[u"handle"])
    request = common.make_request(u"/v3/members/%s/skills/" % quoted)
    raw = common.open_request_and_read(request).decode("utf-8")
    skills = common.to_json(raw)
    skills = skills[u"result"][u"content"][u"skills"]

    for dd in skills.values():
        del dd[u"hidden"]

    d[u"skills"] = skills


def user_stats(d):
    extra_info(d, u"stats")


def user_external_accounts(d):
    extra_info(d, u"externalAccounts")


def extra_info(d, category):
    quoted = quote(d[u"handle"])
    request = common.make_request(u"/v3/members/%s/%s/" % (quoted, category))
    raw = common.open_request_and_read(request).decode("utf-8")
    info = common.to_json(raw)[u"result"][u"content"]

    del info[u"handle"]
    del info[u"userId"]

    del info[u"createdBy"]
    del info[u"createdAt"]
    del info[u"updatedBy"]
    del info[u"updatedAt"]

    d[category] = info


def main():
    common.prepare(use_proxy=g_config.use_proxy)

    client = MongoClient()
    db = client.topcoder

    print "Crawling users..."
    print "Current:", db.users.count()

    if g_config.recrawl_all:
        print "Recrawl all users"

    if g_config.recheck_invalid_handles:
        print "Recheck invalid handles"

    invalid = set()

    def add_invalid_handle(hdl):
        invalid.add(hdl)

        with open(INVALID_HANDLES_FPATH, "w") as fp:
            for h in sorted(invalid):
                fp.write(h.encode("utf-8") + '\n')

    if os.path.exists(INVALID_HANDLES_FPATH):
        for line in open(INVALID_HANDLES_FPATH):
            line = line.strip()
            if line:
                invalid.add(line)

    handles = set()

    for challenge in db.challenges.find():
        for reg in challenge[u"registrants"]:
            handle = reg[u"handle"].lower()

            for ch in ur" \/":
                if ch in handle:
                    continue

            if handle in invalid:
                continue

            if handle in handles:
                continue

            if not g_config.recrawl_all:
                if db.users.find_one({u"handle": handle}, {u"_id": 1}):
                    continue

            handles.add(handle)

    if g_config.recheck_invalid_handles or g_config.recrawl_all:
        handles.update(invalid)
        invalid = set()

        if os.path.exists(INVALID_HANDLES_FPATH):
            os.rename(INVALID_HANDLES_FPATH, INVALID_HANDLES_FPATH + ".bak")

    print len(handles), "users to be crawled"
    print "-----"

    for index, handle in enumerate(handles):
        print "[%d/%d]" % (index + 1, len(handles)), handle

        while True:
            # noinspection PyBroadException
            try:
                try:
                    quoted = quote(handle)
                except KeyError:
                    add_invalid_handle(handle)

                    break

                request = common.make_request(u"/v3/members/" + quoted)
                s = common.open_request_and_read(request).decode("utf-8")
                d = common.to_json(s)[u"result"][u"content"]

                refine_user(d)
                user_skills(d)
                user_stats(d)
                user_external_accounts(d)

                db.users.insert_one(d)

                common.random_sleep(1)
                break
            except urllib2.HTTPError, e:
                if e.code in (404, 403,):
                    add_invalid_handle(handle)

                    common.random_sleep(1)
                    break
                else:
                    print "HTTP Error", e.code, e.msg
                    print e.geturl()
                    print e.fp.read()
            except KeyboardInterrupt:
                return
            except:
                traceback.print_exc()

            common.random_sleep(20)


if __name__ == "__main__":
    while True:
        # noinspection PyBroadException
        try:
            main()

            break
        except KeyboardInterrupt:
            break
        except:
            traceback.print_exc()
