"""
Import sample data for lead scoring engine
"""

import predictionio
import argparse
import random
import uuid

SEED = 3

def import_events(client):
  random.seed(SEED)
  count = 0
  print client.get_status()
  print "Importing data..."

  # generate 10 users, with user ids u1,u2,....,u10
  user_ids = ["u%s" % i for i in range(1, 10+1)]

  # generate 50 items, with user ids u1,u2,....,u10
  item_ids = ["i%s" % i for i in range(1, 50+1)]

  # generate 20 pageId
  page_ids = ["example.com/page%s" % i for i in range(1, 20+1)]

  # generate 10 referrerId
  refferal_ids = ["referrer%s.com" % i for i in range(1, 10+1)]

  browsers = [ "Chrome", "Firefox", "Safari", "Internet Explorer" ]

  # for each session

  # simulate user session:
  # generate a session ID
  for loop in range(0, 50):
    session_id = uuid.uuid1().hex
    print "session", session_id
    referrer_id = random.choice(refferal_ids)
    browser = random.choice(browsers)
    uid = random.choice(user_ids)
    page_id = random.choice(page_ids)
    print "User", uid ,"lands on page", page_id, "referrer", referrer_id, \
      "browser", browser
    client.create_event(
      event = "view",
      entity_type = "user",
      entity_id = uid,
      target_entity_type = "page",
      target_entity_id = page_id,
      properties = {
        "sessionId": session_id,
        "referrerId": referrer_id,
        "browser": browser
      }
    )
    count += 1

    # 0 or more page view
    for i in range(0, random.randint(0,2)):
      page_id = random.choice(page_ids)
      print "User", uid ,"views page", page_id
      client.create_event(
        event = "view",
        entity_type = "user",
        entity_id = uid,
        target_entity_type = "page",
        target_entity_id = page_id,
        properties = {
          "sessionId": session_id
        }
      )
      count += 1

    if random.choice([True, False]):
      # 1 or more buy
      for i in range(0, random.randint(1,3)):
        item_id = random.choice(item_ids)
        print "User", uid ,"buys item", item_id
        client.create_event(
          event = "buy",
          entity_type = "user",
          entity_id = uid,
          target_entity_type = "item",
          target_entity_id = item_id,
          properties = {
            "sessionId": session_id,
          }
        )
        count += 1

  # pick a user id
  # random start time
  # view a random page, random referrer, random browser
  # random gap
  # more random visit
  # random gap
  # buy event
  # end session

  print "%s events are imported." % count

if __name__ == '__main__':
  parser = argparse.ArgumentParser(
    description="Import sample data for similar product engine")
  parser.add_argument('--access_key', default='invald_access_key')
  parser.add_argument('--url', default="http://localhost:7070")

  args = parser.parse_args()
  print args

  client = predictionio.EventClient(
    access_key=args.access_key,
    url=args.url,
    threads=4,
    qsize=100)
  import_events(client)
