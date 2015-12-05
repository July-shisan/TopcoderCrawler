
import ConfigParser
import dateutil.parser
import json

from urllib2 import HTTPError, urlopen
from pymongo import MongoClient

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
        dt = dateutil.parser.parse(challenge[key])
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
        try:
            path = "/v2/challenges/past?type=develop&pageIndex=%d&pageSize=10" % index
            request = common.make_request(path)
            response_body = urlopen(request).read()

            if '"data": []' in response_body:
                return

            print "Page", index

            lists = json.loads(response_body)

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

                request = common.make_request("/v2/challenges/" + str(cid))
                d = common.to_json(urlopen(request).read())

                path = "/v2/challenges/registrants/" + str(cid)
                request = common.make_request(path)
                s = '{"registrants": %s}' % urlopen(request).read()
                registrants = common.to_json(s)

                path = "/v2/challenges/submissions/" + str(cid)
                request = common.make_request(path)
                submissions = common.to_json(urlopen(request).read())

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
            continue

        except HTTPError, e:
            print "HTTP Error", e.code, e.msg
            print e.geturl()
            print e.fp.read()
        except Exception, e:
            print e

        common.random_sleep(20)


if __name__ == "__main__":
    main()
