# Imports
import sqlite3
import uuid
import json
import mysql.connector
from star_db_manager.credentials import *

# Generate a unique job ID for a client
def generate_job_id():
	return str(uuid.uuid4())

# Connect to the gaia star database
def connect_db(db_name):
	connection = None
	try:
		connection = mysql.connector.connect(
			host=SERVER_HOST,
			user=ADMIN_USERNAME,
			password=ADMIN_PASSWORD,
			database=db_name
		)
		return 1, connection
	except Exception as e:
		return -1, e


# Create queue table if it does not exist
def create_table(conn, name):
	try:
		sql = f"CREATE TABLE IF NOT EXISTS {name} (job_id, ip_addr, timestamp, parameters)"
		conn.execute(sql)
		return 1, "Success"
	except Exception as e:
		return -1, e

# Add the job to the job queue database
def add_job(conn, job_id, ip_addr, ts, parameters):
	sql = """INSERT INTO jobqueue(job_id, ip_addr, timestamp, parameters)
		VALUES(%s,%s,%s,%s) """

	entry = (job_id, ip_addr, ts, parameters)
	try:
		cur = conn.cursor()
		cur.execute(sql, entry)
		conn.commit()
		return 1, "Success"
	except Exception as e:
		return -1, e

# Remove a job from the job queue database
def remove_job(conn, jid):
	sql = """DELETE FROM jobqueue WHERE job_id = %s"""

	try:
		cur = conn.cursor()
		cur.execute(sql, [jid])
		conn.commit()
		return 1, "Success"
	except Exception as e:
		return -1, e

# Get a list of all jobs in the job queue database
def get_jobs(conn):
	sql = """SELECT * FROM jobqueue"""
	try:
		cur = conn.cursor()
		cur.execute(sql)
		rows = cur.fetchall()
		return 1, rows
	except Exception as e:
		print("Error: ", e)
		return -1, e

# Get a single job from the job queue database
def get_job(conn, job_id):
	sql = """SELECT * FROM jobqueue WHERE job_id = %s"""
	try:
		cur = conn.cursor()
		cur.execute(sql, [job_id])
		row = cur.fetchone()
		return row
	except Exception as e:
		print("Error: ", e)
		return -1, e

def update_job(conn, job_id, row):
    try:
        cur = conn.cursor()

        # Prepare the SET clause by joining the column names and placeholders
        set_clause = ", ".join([f"{key} = %s" for key in row.keys()])
        # Create the SQL update statement
        query = f"UPDATE jobqueue SET {set_clause} WHERE job_id = %s"

        # Execute the query
        cur.execute(query, list(row.values())+[job_id])

        # Commit the changes
        conn.commit()

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()  # Rollback in case of error

    finally:
        cur.close()	

# Add results to the results database
def add_results(conn, job_id, results):
	sql = """INSERT INTO results(job_id, result)
		VALUES(%s, %s) """

	keys = ['GaiaID', 'x', 'y', 'z', 'mag', 'r','g','b']
	entries = [(job_id, json.dumps(dict(zip(keys, res)))) for res in results]
	try:
		cur = conn.cursor()
		cur.executemany(sql, entries)
		conn.commit()
		return 1, "Success"
	except Exception as e:
		return -1, e


# Get rows by column value in the results jobs database
def get_completed_rows(conn, job_id, chunksize, del_flag=True):
    try:
        cur = conn.cursor()

        # Begin a transaction
        cur.execute("START TRANSACTION")

        # Select rows to delete
        select_sql = f"SELECT result FROM results WHERE job_id = %s"

        cur.execute(select_sql, [job_id])
        rows = cur.fetchall()

        for i in range(0, len(rows), chunksize):
            yield [json.loads(r[0]) for r in rows[i:i+chunksize]]

        # Commit the deletion if delete flag is true
        if del_flag:
            delete_sql = f"DELETE FROM results WHERE job_id = %s"
            cur.execute(delete_sql, [job_id])
            conn.commit()

    except Exception as e:
        print("Error:", e)
        conn.rollback()
        raise

# Check if job ID exists in the results table or in the queue table
def does_job_exist(conn, job_id):
	try:
		sql = "SELECT COUNT(*) FROM results WHERE job_id = %s"
		cur = conn.cursor()
		cur.execute(sql, [job_id])
		count = cur.fetchone()[0]

		sql_queue = "SELECT COUNT(*) FROM jobqueue WHERE job_id = %s"
		cur.execute(sql_queue, [job_id])
		queue_count = cur.fetchone()[0]

		return 1, count > 0 or queue_count > 0
	except Exception as e:
		print("Error: ", e)
		return -1, e

# Drop all rows from a table and database
def drop_all(conn, table):
	sql = f"DELETE FROM `{table}`"

	try:
		cur = conn.cursor()
		cur.execute(sql)
		conn.commit()
	except Exception as e:
		print("Error: ", e)
		raise

if __name__ == "__main__":
	_, conn = connect_db("exosky")
	drop_all(conn, "results")