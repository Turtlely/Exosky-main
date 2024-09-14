# This script is responsible for completing jobs

'''
For each job, we need to poll the database. 
There are about 1 billion rows in the database that need to be polled.
To finish the process in 10 seconds, we need to process 100 million rows per second.

We process the database in batches of around 1 million rows each.
When a batch is done, we send it off to a results "pile", and wait for it to get "picked up" by the client.

The first step is to pick a job out of the queue.
Next, we generate the indexes for each of 1000 batches we will perform. 
We pass this index to a function.

This function takes in the index and batch size, and first queries the database to retrieve all 1 million rows.
Next, it uses CuPy to process the massive array.
When the result is done, it returns the massive array.

When we get the result back from the function, we add each star in the result to a database. Each row is tagged with the job ID.

When a client requests data through their web socket using their job ID, we retrieve all rows from the database with their ID, and begin streaming it over in chunks of 100,000 stars at a time.

Whenever we send off a batch of data to the client, we modify a field "TO BE DELETED" to be "true". A separate background script goes through the database and deletes all entries that are marked for destruction.

If a client waits longer than 5 seconds to ping their web socket, the job is terminated and all entries are wiped from the database.
'''

from job_manager.db_manager import connect_db, get_job, add_results, create_table, remove_job, get_jobs
from star_db_manager.db_manager import get_min_max_ids
from computation_engine.main import worker
import multiprocessing as mp
import functools
import json
import time

# Connect to the job queue database
flag, conn = connect_db('exosky')

if flag == -1:
	print("Error: ", conn)
	quit()

# Lowest row ID in gaia database
flag, ids = get_min_max_ids()

if flag == -1:
	print("Error: ", ids)

min_id, max_id = ids

# Number of workers
n_workers = mp.cpu_count()

# Number of rows:
n_rows = max_id - min_id

# Batch size:
batch_size = 100000

# Number of batches
n_batches = n_rows // batch_size + (n_rows % batch_size > 0)

print(n_batches)

# Batch queue
batch_queue = [i * batch_size + min_id for i in range(n_batches)]

# Iterate through all rows of jobqueue.db
while True:
	# Get all rows
	_, _conn = connect_db('exosky')
	job_list = [x[0] for x in get_jobs(_conn)[1]]

	for job_id in job_list:
		# Timing the job execution
		start = time.time()
		print()
		job_id, ip_addr, timestamp, params = get_job(conn, job_id)

		params = json.loads(params)	

		print("--------------------------------------------------")
		print(f"Job ID: {job_id}")
		print(f"Client IP Address: {ip_addr}")
		print(f"Time requested: {timestamp}")
		print(f"Parameters: {params}")

		# Get the data from the database
		with mp.Pool(n_workers) as pool:
			# Each worker retrieves a batch of rows to process
			for _, result in pool.imap(functools.partial(worker, batch_size=batch_size, params = params, job_id = job_id), batch_queue):
				if _ == -1:
					print(result)
					break

				# Add to the results database
				flag, status = add_results(conn, job_id, result)

				# Check if it worked
				if flag == -1:
					print("Insert failed. ", status)
					continue

		# Remove the job from the queue database
		flag, status = remove_job(conn, job_id)
		if flag == -1:
			print("Could not remove job from queue. ", status)
			continue

		print(f"Job Complete! ({job_id}) Took {(time.time()-start) * 100 // 100} Seconds")
		print()