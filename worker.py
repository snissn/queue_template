import itertools
import sql_batch_write # creates batch writes
import boto

import traceback
import json

import sys

worker_name=sys.argv[1]

def process_organization(organization):
    output = do_work_organization(organization)
    sql_batch_write.queue_write(output)

    pass # actual code for the work goes here

def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(itertools.islice(it, n))
       if not chunk:
           return
       yield chunk

chunk=10


#while there is data in the queue, read from queue, 
#delete data from queue do work, send result async to sql
# once there s no longer work in the queue, send everything in the 
# sql queue and exit back to run.sh
def main():
  sqs = boto.connect_sqs()
  queue_version = 3
  q = sqs.get_queue('{worker_name}_{queue_version}'.format(queue_version=queue_version, worker_name = worker_name))


  message = q.read(100)
  while message:
    print 'goto1'
    sqs.delete_message(q,message)
    data = json.loads(message.get_body())
    for organizations in data:
        try:
          process_organization(organization)
        except Exception as e:
          traceback.print_exc()
          continue
    sql_batch_write.write_all()
    message = q.read(100)
    if not message:
      print 'no more message all done going to exit'
  sql_batch_write.write_all()
if __name__ == "__main__":
  main()
