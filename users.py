import ConfigParser
import os
import urllib2

import pickle
from pymongo import MongoClient

import common


def crawl_user():
    pass


def user_skills(d):
    request = common.make_request("/v3.0.0/members/%s/skills/" % d["handle"])
    skills = common.to_json(urllib2.urlopen(request).read())
    skills = skills["result"]["content"]["skills"]

    for dd in skills.values():
        del dd["hidden"]

    d["skills"] = skills


def user_stats(d):
    extra_info(d, "stats")


def user_external_accounts(d):
    extra_info(d, "externalAccounts")


def extra_info(d, category):
    request = common.make_request("/v3.0.0/members/%s/%s/" % (d["handle"], category))
    info = common.to_json(urllib2.urlopen(request).read())["result"]["content"]

    del info["handle"]
    del info["userId"]

    del info["createdBy"]
    del info["createdAt"]
    del info["updatedBy"]
    del info["updatedAt"]

    d[category] = info


def main():
    config = ConfigParser.RawConfigParser()
    config.read("config/users.ini")

    use_proxy = config.getboolean("default", "proxy")
    common.prepare(use_proxy=use_proxy, https=True)

    client = MongoClient()
    db = client.topcoder

    if os.path.exists("config/invalid_handle"):
        with open("config/invalid_handle", "rb") as fp:
            invalid = pickle.load(fp)
    else:
        invalid = set()

    for challenge in db.challenges.find():
        for reg in challenge["registrants"]:
            uname = reg["handle"]

            if ' ' in uname:
                continue

            if uname in invalid:
                continue

            if db.users.find_one({"handle": uname}):
                continue

            print uname

            while True:
                try:
                    request = common.make_request("/v3.0.0/members/" + uname)
                    s = urllib2.urlopen(request).read()

                    d = common.to_json(s)["result"]["content"]
                    d["handle"] = d["handle"].lower()
                    user_skills(d)

                    db.users.insert_one(d)

                    common.random_sleep(1)
                    break

                except urllib2.HTTPError, e:
                    if e.code == 404 or e.code == 403:
                        with open("config/invalid_handle", "wb") as fp:
                            invalid.add(uname)
                            pickle.dump(invalid, fp)

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
