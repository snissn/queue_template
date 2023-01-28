#!/usr/bin/python
import sys
import pika
import datetime
import argparse
import json

def myconverter(o):
    if isinstance(o, datetime.datetime):
        return o.__str__()
sys.path.append("/ebs/torch/indexer")
import boto
from boto.sqs.message import Message
from sql import SQL
class QueueWriter(object):
  def __init__(self, name):
    self.name = name
    self.N = 50

  # batch of N
  def write(self): 
    data = []
    for organization in self.read():
      data.append(organization)
      if len(data) == self.N:
        self.write_to_queue(data)
        data = []
    self.write_to_queue(data)
    data = []
  
  def read_report(self):
    sql = self.get_sql()

  def get_sql(self, where=''):
    sql = """
          SELECT   {name}_usernames.id                        AS {name}_username_id, 
                   {select_follower_count}
                   array_agg( DISTINCT organizations.id)      AS organization_ids, 
                   array_agg( DISTINCT organizations.irs_zip) AS zipcodes, 
                   (array_agg( organizations.name order by organizations.id asc))[1] AS organization_name, 
                   organization_type as organization_type,
                   {name}_usernames.name  as username
          FROM     vision2.{name}_usernames 
          join     vision2.{name}_usernames_organizations 
          ON       {name}_usernames_organizations.{name}_username_id = {name}_usernames.id 
          join     vision2.organizations 
          ON       {name}_usernames_organizations.organization_id = organizations.id 
          {join_follower_count}
          {where}
          GROUP BY {name}_usernames.id, 
                   {name}_usernames.name, organization_type

                   {group_by_follower_count}
    """
    result = sql.format(name=name_clean, where=where, group_by_follower_count=group_by_follower_count, select_follower_count=select_follower_count, join_follower_count=join_follower_count)
    return result

  def filter(self, row):
    return True

    #get list of work that needs to be done
  def read(self, where=''):
    sql_helper = SQL()
    sql = self.get_sql(where)
    for row in sql_helper.read_raw(sql):
      row = dict(row)
      if self.filter(row):
        yield row

  def write_to_queue(self, data):
    version = open("/ebs/torch/indexer/build_queue/version").read().strip()
    queue_name = '{name}_index_{version}'.format(name=self.name, version=version)
    sqs = boto.connect_sqs()
    sqs.create_queue(queue_name)
    q = sqs.get_queue(queue_name)
    m = Message()
    m.set_body(json.dumps(data, default=myconverter))
    q.write(m)

def main():
  parser = argparse.ArgumentParser(description='Load a Queue for a specified social network')
  parser.add_argument('name', metavar='Network Name', type=str, help='The worker name')
  args = parser.parse_args()
  f = QueueWriter(args.name)
  f.write()

if __name__ == "__main__":
  main()
