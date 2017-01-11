#!/usr/bin/env python2

from collections import defaultdict
from datetime import datetime

from pymongo import MongoClient


def main(win_times, year_from):
    client = MongoClient()
    db = client.topcoder

    counter = defaultdict(int)

    condition = {u"postingDate": {u"$gt": datetime(year_from, 1, 1)}}
    for challenge in db.challenges.find(condition):
        if u"finalSubmissions" not in challenge:
            continue

        for sub in challenge[u"finalSubmissions"]:
            handle = sub[u"handle"].lower()

            if isinstance(sub[u"placement"], int):
                counter[handle] += 1

    total = 0

    for user in counter:
        if counter[user] >= win_times:
            total += 1
            print user, counter[user]

    print "\n\nTotal:", total

if __name__ == "__main__":
    main(win_times=5, year_from=2014)
