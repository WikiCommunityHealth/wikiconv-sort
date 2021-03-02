"""
Extract snapshots from list of revisions.

The output format is csv.
"""

from typing import Mapping
from datetime import datetime


# class WikiConvElement(TypedDict):
#     #  {
#     #     "id": "168283361.0.312",
#     #     "revId": "168283361",
#     #     "type": "DELETION",
#     #     "conversationId": "11458020.300.300",
#     #     "pageTitle": "Talk:The Case for Faith",
#     #     "content": "Moved page to reflect correctness in title. [...]
#     #     "cleanedContent": "Moved page to reflect correctness in title. [...]
#     #     "user": {
#     #         "id": "3939239",
#     #         "text": "Hrafn"
#     #     },
#     #     "timestamp": "2007-10-31T11:54:56Z",
#     #     "pageId": "1642201",
#     #     "parentId": "11458195.312.312",
#     #     "ancestorId": "11458020.312.300",
#     #     "authorList": [{
#     #         "id": "86737",
#     #         "text": "SocratesJedi"
#     #     }],
#     #     "comment": "Archived to merged article",
#     #     "score": {
#     #         "toxicity": 0.012741876766085625,
#     #         "severeToxicity": 0.0007551429443992674,
#     #         "profanity": 0.013319894671440125,
#     #         "threat": 0.019416864961385727,
#     #         "insult": 0.02139710634946823,
#     #         "identityAttack": 0.025092273950576782
#     #     },
#     #     "pageNamespace": 1
#     #   }
#     id: str
#     revId: int
#     type: str
#     conversationId: str
#     pageTitle: str
#     content: str
#     cleanedContent: str
#     user: Mapping
#     user['id']: int
#     user['text']: str
#     timestamp: str
#     pageId: int
#     parentId: str
#     ancestorId: str
#     authorList: List[Mapping]
#     authorList.id: int
#     authorList.text: str
#     comment: str
#     score: Mapping
#     score.toxicity: float
#     score.severeToxicity: float
#     score.profanity: float
#     score.threat: float
#     score.insult: float
#     score.identityAttack: float
#     pageNamespace: int

def __parse_user(userdct: Mapping) -> Mapping:
    if "id" in userdct:
        return {"id": int(userdct["id"]),
                "text": userdct["text"]
                }
    else:
        return userdct


def __parse_author(authordct: Mapping) -> Mapping:
    if "id" in authordct:
        return {"id": int(authordct["id"]),
                "text": authordct["text"]}
    else:
        return authordct


def cast_json(dct: Mapping) -> Mapping:
    res = {"id": dct["id"],
           "revId": int(dct["revId"]),
           "type": dct["type"],
           "conversationId": dct["conversationId"],
           "pageTitle": dct["pageTitle"],
           "content": dct["content"],
           "cleanedContent": dct["cleanedContent"],
           "user": __parse_user(dct.get("user", {})),
           # How do I parse an ISO 8601-formatted date?
           # https://stackoverflow.com/a/62769371/2377454
           "timestamp": datetime.fromisoformat(
                            dct["timestamp"].replace('Z', '+00:00')
                            ),
           "pageId": int(dct["pageId"]),
           "parentId": dct.get("parentId", None),
           "ancestorId": dct["ancestorId"],
           "authorList": [__parse_author(author)
                          for author in dct["authorList"]],
           "comment": dct.get("comment", None),
           "score": {
              "toxicity": float(dct["score"]["toxicity"]),
              "severeToxicity": float(dct["score"]["severeToxicity"]),
              "profanity": float(dct["score"]["profanity"]),
              "threat": float(dct["score"]["threat"]),
              "insult": float(dct["score"]["insult"]),
              "identityAttack": float(dct["score"]["identityAttack"]),
              },
           "pageNamespace": int(dct["pageNamespace"])
           }

    return res
