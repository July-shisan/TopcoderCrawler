
from pymongo import MongoClient
import dateutil.parser


client = MongoClient()
db = client.topcoder

datetime_keys = (
    u"postingDate",
    u"appealsEndDate",
    u"registrationEndDate",
    u"submissionEndDate",
    u"currentPhaseEndDate",
)

incompleted = []


def format_challenge(challenge):
    for key in datetime_keys:
        dt = dateutil.parser.parse(challenge[key])
        challenge[key] = dt

    for reg in challenge[u"registrants"]:
        for attr in (u"colorStyle", u"rating", u"reliability"):
            reg[attr] = None
            del reg[attr]

        dt = dateutil.parser.parse(reg[u"registrationDate"])
        reg[u"registrationDate"] = dt

        if reg[u"submissionDate"]:
            dt = dateutil.parser.parse(reg[u"submissionDate"])
            reg[u"submissionDate"] = dt
        else:
            reg[u"submissionDate"] = None

    for sub in challenge[u"finalSubmissions"]:
        dt = dateutil.parser.parse(sub[u"submissionDate"])
        sub[u"submissionDate"] = dt


def main():
    for index, challenge in enumerate(db.challenges.find()):
        try:
            if not isinstance(challenge[datetime_keys[0]], unicode):
                continue

            print index + 1, challenge[u"challengeName"]

            format_challenge(challenge)
            db.challenges.replace_one({u"_id": challenge[u"_id"]}, challenge)

        except KeyError, e:
            print e

            if u"challengeId" in challenge:
                cid = challenge[u"challengeId"]
            else:
                cid = challenge[u"_id"]

            incompleted.append(cid)
            print cid

    with open("config/incompleted.txt", "w") as outf:
        for cid in incompleted:
            outf.write(str(cid) + '\n')


if __name__ == "__main__":
    main()
