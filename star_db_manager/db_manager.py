import mysql.connector
from mysql.connector import Error
from star_db_manager.credentials import *
import numpy as np

# Connect to the gaia star database
def connect_db():
    connection = None
    try:
        connection = mysql.connector.connect(
            host=SERVER_HOST,
            user=ADMIN_USERNAME,
            password=ADMIN_PASSWORD,
            database=GAIA_DATABASE_NAME
        )
    except Error as e:
    	return -1, e
    return 1, connection

# Get the minimum and maximum row IDs
def get_min_max_ids():
	flag, conn = connect_db()
	
	if flag == -1:
		return -1, connection

	cur = conn.cursor()
	sql = f"SELECT MIN(id), MAX(id) FROM {GAIA_TABLE_NAME}"
	cur.execute(sql)

	min_id, max_id = cur.fetchone()
	conn.close()

	return 1, [min_id, max_id]

# Generate batches
def get_batch(start_id, batch_size):
	flag, conn = connect_db()

	if flag == -1:
		return -1, connection

	cur = conn.cursor()
	sql = f"SELECT GaiaID, x, y, z, temp, v_mag FROM {GAIA_TABLE_NAME} WHERE id > {start_id} AND temp IS NOT NULL LIMIT {batch_size}"
	cur.execute(sql)
	rows = cur.fetchmany(batch_size)
	conn.close()
	# If the query returns no rows, return a None
	if len(rows) == 0:
		return 1, None

	gaia_ids = np.array([row[0] for row in rows])
	data = np.array([row[1:] for row in rows], dtype=np.float32)

	# Replace all NaN with 0
	np.nan_to_num(data, copy=False)

	return 1, data, gaia_ids