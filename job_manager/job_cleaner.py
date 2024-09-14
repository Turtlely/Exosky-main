from job_manager.db_manager import connect_db, get_job, remove_job, get_jobs
import datetime

while True:
	_, conn = connect_db('exosky')
	flag, rows = get_jobs(conn)
	for row in rows:
		job_id = row[0]
		timestamp = row[2]
		delta = int(datetime.datetime.now().timestamp()) - timestamp
		#print(job_id, delta)
		if delta > 3:
			remove_job(conn, job_id)
			print(f"Terminated Job {job_id}")