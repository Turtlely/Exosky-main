from job_manager.db_manager import connect_db, get_job, remove_job, get_jobs, remove_results, remove_delivered_results
import datetime
import time

while True:
	_, conn = connect_db('exosky')
	flag, rows = get_jobs(conn)
	for row in rows:
		job_id = row[0]
		timestamp = row[2]
		delta = int(datetime.datetime.now().timestamp()) - timestamp
		#print(job_id, delta)
		if delta > 10:
			# Remove the job from the queue
			remove_job(conn, job_id)
			time.sleep(0.1)
			# Delete all of the entries in the results table
			remove_results(conn, job_id)
			print(f"Terminated Job {job_id}")
		remove_delivered_results(conn)

