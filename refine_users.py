#!/usr/bin/env python2

import dateutil.parser
from pymongo import MongoClient


keys = (u"createdAt", u"updatedAt",)


def main():
    client = MongoClient()
    db = client.topcoder

    s1 = set()

    for challenge in db.challenges.find():
        for reg in challenge[u"registrants"]:
            handle = reg[u"handle"].lower()

            if ' ' in handle:
                continue

            s1.add(handle)

    s2 = set()

    for user in db.users.find():
        s2.add(user[u"handle"])

        m = {}

        for key in keys:
            m[key] = dateutil.parser.parse(user[key])

        db.users.update({u"_id": user[u"_id"]}, {u"$set": m})

    for handle in s2.difference(s1):
        print handle

        db.users.remove({u"handle": handle})


if __name__ == "__main__":
    main()
