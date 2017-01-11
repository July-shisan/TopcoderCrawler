#!/usr/bin/env python2

import ConfigParser
import json

import dateutil.parser           # pip install python-dateutil
from pymongo import MongoClient  # pip install pymongo

import common


def format_challenge(challenge):
    datetime_keys = (
        "postingDate",
        "appealsEndDate",
        "registrationEndDate",
        "submissionEndDate",
        "currentPhaseEndDate",
    )

    for key in datetime_keys:
        if key in challenge:
            s = challenge[key]
            if not s:
                del challenge[key]
                continue

            dt = dateutil.parser.parse(s)
            challenge[key] = dt

    for reg in challenge["registrants"]:
        for attr in ("colorStyle", "rating", "reliability"):
            reg[attr] = None
            del reg[attr]

        dt = dateutil.parser.parse(reg["registrationDate"])
        reg["registrationDate"] = dt

        if reg["submissionDate"]:
            dt = dateutil.parser.parse(reg["submissionDate"])
            reg["submissionDate"] = dt
        else:
            reg["submissionDate"] = None

    for sub in challenge["finalSubmissions"]:
        dt = dateutil.parser.parse(sub["submissionDate"])
        sub["submissionDate"] = dt


def filter_out(cid):
    return cid in (30048087,)


def main():
    client = MongoClient()
    db = client.topcoder

    config = ConfigParser.RawConfigParser()
    config.read("config/challenges.ini")

    init = config.getboolean("default", "init")

    if init:
        index = config.getint("default", "page_index")
    else:
        index = 1

    use_proxy = config.getboolean("default", "use_proxy")
    common.prepare(use_proxy=use_proxy)

    while True:
        path = "/v2/challenges/past?type=develop&pageIndex=%d&pageSize=10" % index
        raw = common.guarded_read(path)

        if '"data": []' in raw:
            return

        print "Page", index

        lists = json.loads(raw)

        for challenge in lists["data"]:
            cid = challenge["challengeId"]

            if filter_out(cid):
                continue

            if db.challenges.find_one({"challengeId": cid}):
                if init:
                    continue
                else:
                    return

            common.random_sleep(1)

            print ' ', challenge["challengeName"]

            path = "/v2/challenges/" + str(cid)
            d = common.to_json(common.guarded_read(path))

            path = "/v2/challenges/registrants/" + str(cid)
            raw = '{"registrants": %s}' % common.guarded_read(path)
            registrants = common.to_json(raw)

            path = "/v2/challenges/submissions/" + str(cid)
            submissions = common.to_json(common.guarded_read(path))

            d.update(registrants)
            d.update(submissions)
            format_challenge(d)

            db.challenges.insert_one(d)

        index += 1

        if init:
            config.set("default", "page_index", index)
            with open("config/challenges.ini", "wb") as fp:
                config.write(fp)

        common.random_sleep(10)


if __name__ == "__main__":
    main()
