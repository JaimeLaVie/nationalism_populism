# -*- coding: utf-8 -*-
import sys
import string
import time
import tweepy
import json
# from tweepy import Stream
# from tweepy.streaming import StreamListener
# from tweepy import OAuthHandler

bearer_token = ""

class CustomListener(tweepy.StreamingClient):
    """Custom StreamListener for streaming Twitter data."""

    """ def __init__(self):
        pass """

    def on_data(self, data):
        try:
            print ("In Writing")
            # print(data.decode("utf-8"))
            with open("/home/ubuntu/nationalism_populism/data/{}.jsonl".format(time.strftime('%Y-%m-%d-%H',time.localtime(time.time()))), 'a') as f:
                f.write(str(data.decode("utf-8"))+'\n')
                # f.write(json.dumps(data)+'\n')
            print (time.strftime('%Y-%m-%d-%H-%M',time.localtime(time.time())))
            return True
        except BaseException as e:
            print ("BaseException")
            sys.stderr.write("Error on_data: {}, sleep 5s\n".format(e))
            time.sleep(5)
            return True

    def on_error(self, status):
        if status == 420:
            sys.stderr.write("Rate limit exceeded: {}\n".format(status))
            return False
        else:
            sys.stderr.write("Error {}\n".format(status))
            return True

if __name__ == '__main__':
    twitter_stream = CustomListener(bearer_token)

    """ clean-up pre-existing rules """
    rule_ids = []
    result = twitter_stream.get_rules()
    for rule in result.data:
        print(f"Rule marked to delete: {rule.id} - {rule.value}")
        rule_ids.append(rule.id)
    
    if(len(rule_ids) > 0):
        twitter_stream.delete_rules(rule_ids)
        twitter_stream = CustomListener(bearer_token)
        print("Deletion completed")
    else:
        print("No rules to delete")
    
    # search_rule = "(アメリカ OR 日本) lang:ja place_country:JP" # 将日本作为推特发送地区的要求会使得返回推文数过少
    search_rule = "(形成外科 OR 美容 OR 美容手術 OR 日本) lang:ja"
    rule = tweepy.StreamRule(value=search_rule, tag=search_rule)
    twitter_stream.add_rules(rule)
    print(f"Rule added: {search_rule}")
    twitter_stream.filter(expansions="author_id",tweet_fields="created_at", backfill_minutes=5)
    