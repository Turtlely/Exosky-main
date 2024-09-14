# Imports
from flask import Flask, request, Response, jsonify, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS
import datetime
import numpy as np
import json
import asyncio
from job_manager.db_manager import connect_db, add_job, generate_job_id, get_completed_rows, does_job_exist, update_job, get_job

# Initialize flask app
app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST"], "expose_headers": ["X-Is-Completed"]}})

# Allowed hosts for the API
ALLOWED_HOSTS = ['api.exosky.org']

limiter = Limiter(get_remote_address)
limiter.init_app(app)

# Ensure that requests are from allowed hosts
@app.before_request
def check_host():
	#if request.host not in ALLOWED_HOSTS:
	#	abort(403)
	pass

# Route to allow a client to create a job
@app.route('/create_job', methods=['POST'])
@limiter.limit("10 per minute")
def create_job():
	# Get the JSON data from the request
	data = request.get_json()

	# Ensure data exists
	if data is None:
		return jsonify({'error': 'No JSON data recieved'}), 400

	# Get client IP address and time of request
	ip_addr = request.remote_addr
	time_of_request = int(datetime.datetime.now().timestamp())

	# Extract specific fields from the JSON data
	lim_mag = data.get("limiting_magnitude")
	coord = data.get('coordinates')

	# Parameters
	params = json.dumps({"limiting_magnitude": lim_mag, "coordinates": coord})

	# Create a job ID for the client
	job_id = generate_job_id()

	print(f"Job recieved from {ip_addr}, at {time_of_request}. JOB_ID: {job_id}")

	# Add to the database
	_, conn = connect_db('exosky')
	flag, status = add_job(conn, job_id, ip_addr, time_of_request, params)

	if flag == -1:
		return jsonify({'error': f'Error inserting into database, {status}'}), 400

	print(f"JOB_ID {job_id} ADDED TO QUEUE")

	return jsonify({"job_id": job_id}), 200

@app.route('/get_job/<job_id>', methods=['GET'])
@limiter.exempt
def get_job_from_db(job_id):
	# Update the jobqueue database
	_, conn = connect_db('exosky')
	row = get_job(conn, job_id)
	if row is None:
		print("Completed or Cancelled")
		return jsonify("Job not found")

	update_job(conn, job_id, {"job_id": job_id, "ip_addr": row[1], "timestamp": int(datetime.datetime.now().timestamp()), "parameters": row[3]})

	chunksize = 100000
	reponse_data = [x for x in get_completed_rows(conn, job_id, chunksize)]
	response = jsonify(reponse_data)

	flag, exists = does_job_exist(conn, job_id)

	if (flag == 1) and not exists:
		response.headers['X-Is-Completed'] = 'true'
	elif (flag == 1) and exists:
		response.headers['X-Is-Completed'] = 'false'
	return response

@app.route('/')
def test():
	return "ping!"