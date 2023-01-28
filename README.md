# queue_template

Skeleton for how to make a linearly scalable worker queue


# run.sh
 
the main loop for the work
 - first make sure to clear the queue for idemptotence 
 - build the queue up - do an sql query to create a list of all the work as of now that needs to be done and insert it into the queue in batches of 50 work units at a time
 - choose your paralellism and launch $cpu_cores worth of workers that will chew on the work until all of the queue is emptied
 - repeat the loop - clear, build, work

# worker.py
  - reads work chunks from the queue system 
  - does the work
  - appends the work to a local sql queue and when that queue gets to 2000 it sends an update statement to update all of the sql from the work its done
  - when the worker has finished everything it sends anything in its sql update queue and then exits

# sql/sql_send_later..py

  - builds sql connections
  - has a method for write_later which appends data to a list until that list grows to N=2000
  - at N=2000 it builds the sql statement and executes it
  - has another method for write_all used when worker.py is finished

# build_queue/build_queue.py
  - queries sql to create a list of all the work that needs to be done then adds it to the queue in 50 item batches

# build_queue/clear.py
   - clears the queue
