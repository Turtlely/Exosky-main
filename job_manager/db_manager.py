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
	sql = """INSERT INTO jobqueue(job_id, ip_addr, timestamp, parameters, inprogress)
		VALUES(%s,%s,%s,%s,%s) """

	entry = (job_id, ip_addr, ts, parameters, 0)
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

        # Prepare the values for the query
        # Use json.dumps() for JSON data (e.g., dict or list), else keep the value as is
        test = [json.dumps(x) if isinstance(x, (dict, list)) else x for x in row.values()] + [job_id]

        # Print the query and values for debugging (optional)
        #print(query)
        #print(test)

        # Execute the update query with the prepared values
        cur.execute(query, test)
        conn.commit()  # Commit the transaction

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        conn.rollback()  # Rollback in case of error
    #finally:
    #    cur.close()  # Close the cursor

# Add results to the results database
def add_results(conn, job_id, results):
	sql = """INSERT INTO results(job_id, result, deliver_flag)
		VALUES(%s, %s, %s) """

	keys = ['GaiaID', 'x', 'y', 'z', 'mag', 'r','g','b']
	entries = [(job_id, json.dumps(dict(zip(keys, res))), 0) for res in results] # 0 indicates the result hasn't been delivered yet
	try:
		cur = conn.cursor()
		cur.executemany(sql, entries)
		conn.commit()
		return 1, "Success"
	except Exception as e:
		return -1, e

# Delete results from the results table
def remove_results(conn, job_id):
	sql = "DELETE FROM results WHERE job_id = %s"
	try:
		cur = conn.cursor()
		cur.execute(sql, [job_id])
		conn.commit()
	except Exception as e:
		print(e)
		conn.rollback()

# Delete results from the results table
def remove_delivered_results(conn):
	sql = "DELETE FROM results WHERE deliver_flag = 1"
	try:
		cur = conn.cursor()
		cur.execute(sql)
		conn.commit()
	except Exception as e:
		print(e)
		conn.rollback()

'''
# Get rows by column value in the results jobs database
def get_completed_rows(conn, job_id):
    try:
        cur = conn.cursor()

        # Select rows
        select_sql = f"SELECT * FROM results WHERE job_id = %s AND deliver_flag = 0"

        cur.execute(select_sql, [job_id])
        rows = cur.fetchall()

        #print("Rows Prior: ", rows)

        return [json.loads(r[1]) for r in rows]

    except Exception as e:
        print("Error:", e)
    finally:
    	cur.close()
'''

def get_completed_rows(conn, job_id):
    try:
        cur = conn.cursor()

        # Begin a transaction to ensure atomicity
        conn.start_transaction()

        # Select rows where deliver_flag is 0
        select_sql = "SELECT * FROM results WHERE job_id = %s AND deliver_flag = 0"
        cur.execute(select_sql, [job_id])
        rows = cur.fetchall()

        # Update deliver_flag to 1 for the fetched rows
        #update_sql = "UPDATE results SET deliver_flag = 1 WHERE job_id = %s AND deliver_flag = 0"
        #cur.execute(update_sql, [job_id])

        # Commit the transaction
        conn.commit()

        # Return the fetched rows (assuming the 'result' is in column index 1)
        return [json.loads(r[1]) for r in rows]

    except Exception as e:
        print("Error:", e)
        conn.rollback()  # Rollback the transaction if an error occurs
        raise  # Reraise the exception to handle it further up

    finally:
        cur.close()  # Always close the cursor

# Get rows by column value in the results jobs database
def get_completed_rows_by_pl(conn, pl_name, chunksize, del_flag=True):
    try:
        cur = conn.cursor()

        # Begin a transaction
        cur.execute("START TRANSACTION")

        # Select rows to delete
        select_sql = f"SELECT result FROM results WHERE pl_name = %s"

        cur.execute(select_sql, [pl_name])
        rows = cur.fetchall()
        for i in range(0, len(rows), chunksize):
            yield [json.loads(r[0]) for r in rows[i:i+chunksize]]

        # Commit the deletion if delete flag is true
        if del_flag:
            delete_sql = f"DELETE FROM results WHERE pl_name = %s"
            cur.execute(delete_sql, [pl_name])
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
		print("QC: ", queue_count)

		return 1, queue_count > 0# or count > 0 
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