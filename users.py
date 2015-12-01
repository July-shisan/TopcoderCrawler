# -*- coding: utf-8 -*-

import ConfigParser
import os
import urllib2

from pymongo import MongoClient
import dateutil.parser

import common


def refine_user(d):
    d[u"handle"] = d[u"handle"].lower()

    for key in (u"createdAt", u"updatedAt",):
        d[key] = dateutil.parser.parse(d[key])


def user_skills(d):
    request = common.make_request(u"/v3.0.0/members/%s/skills/" % d[u"handle"])
    skills = common.to_json(urllib2.urlopen(request).read())
    skills = skills[u"result"][u"content"][u"skills"]

    for dd in skills.values():
        del dd[u"hidden"]

    d[u"skills"] = skills


def user_stats(d):
    extra_info(d, u"stats")


def user_external_accounts(d):
    extra_info(d, u"externalAccounts")


def extra_info(d, category):
    request = common.make_request(u"/v3.0.0/members/%s/%s/" % (d[u"handle"], category))
    info = common.to_json(urllib2.urlopen(request).read())[u"result"][u"content"]

    del info[u"handle"]
    del info[u"userId"]

    del info[u"createdBy"]
    del info[u"createdAt"]
    del info[u"updatedBy"]
    del info[u"updatedAt"]

    d[category] = info


def main():
    config = ConfigParser.RawConfigParser()
    config.read("config/users.ini")

    use_proxy = config.getboolean("default", "proxy")
    common.prepare(use_proxy=use_proxy)

    client = MongoClient()
    db = client.topcoder

    print "Crawling users..."
    print "Current:", db.users.count()
    print "-----"

    invalid = set()

    if os.path.exists("config/invalid_handles"):
        for line in open("config/invalid_handles"):
            line = line.strip()
            if line:
                invalid.add(line)

    for challenge in db.challenges.find():
        for reg in challenge["registrants"]:
            handle = reg["handle"].lower()

            if u' ' in handle:
                continue

            if handle in invalid:
                continue

            if db.users.find_one({u"handle": handle}):
                continue

            print reg[u"handle"]

            while True:
                try:
                    request = common.make_request(u"/v3.0.0/members/" + handle)
                    s = urllib2.urlopen(request).read().decode("utf-8")

                    d = common.to_json(s)[u"result"][u"content"]
                    refine_user(d)

                    user_skills(d)

                    db.users.insert_one(d)

                    common.random_sleep(1)
                    break

                except urllib2.HTTPError, e:
                    if e.code == 404 or e.code == 403:
                        invalid.add(handle)

                        with open("config/invalid_handles", "w") as fp:
                            for handle in sorted(invalid):
                                fp.write(handle + '\n')

                        break
                    else:
                        print "HTTP Error", e.code, e.msg
                        print e.fp.read()
                except Exception, e:
                    print "An unknown exception occured."
                    print e

                common.random_sleep(20)


if __name__ == '__main__':
    while True:
        try:
            main()
            break
        except Exception, e:
            print e
