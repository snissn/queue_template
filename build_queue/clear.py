import argparse
from discord_webhook import DiscordWebhook

import boto
sqs = boto.connect_sqs()
parser = argparse.ArgumentParser(description='Load a Queue for a specified social network')
parser.add_argument('name', metavar='Network Name', type=str, help='worker name')
args = parser.parse_args()
version = open("/ebs/torch/indexer/build_queue/version").read().strip()
queue_name = "{}_index_{}".format(args.name, version)
q = sqs.get_queue(queue_name)
count = q.count()
q.clear()


if count > 0 :
  text = "Queue named {queue_name} had a count of {count} before clear".format(queue_name=queue_name,count=count)
  webhook = DiscordWebhook(url='https://discordapp.com/api/webhooks/blah, content=text)
  webhook.execute()
