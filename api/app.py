# Imports
from flask import Flask, request, Response, jsonify, abort, make_response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_cors import CORS
import datetime
import numpy as np
import json
import asyncio
from job_manager.db_manager import connect_db, add_job, generate_job_id, get_completed_rows, get_completed_rows_by_pl, does_job_exist, update_job, get_job, remove_results
from computation_engine.aux_data import getStarData
from computation_engine.exo_data import getExoData

# Initialize flask app
app = Flask(__name__)
cors = CORS(app, resources={r"/*": {"origins": "*", "methods": ["GET", "POST"], "expose_headers": ["X-Is-Completed"]}})

# Allowed hosts for the API
ALLOWED_HOSTS = ['api.exosky.org']

def get_ip():
	try:
		return request.headers.getlist("X-Forwarded-For")[0] 
	except Exception as e:
		return request.remote_addr

#limiter = Limiter(get_ip)
#limiter.init_app(app)

# Ensure that requests are from allowed hosts
@app.before_request
def check_host():
	print(f"RECIEVED FROM {request.host} AT {get_ip()}")
	#if request.host not in ALLOWED_HOSTS:
	#	abort(403)
	pass

@app.errorhandler(429)
def ratelimit_handler(e):
	print("Exceeded rate limit!")
	return make_response(
		jsonify(error=f"ratelimit exceeded {e.description}"), 429
	)

# Route to allow a client to create a job
@app.route('/create_job', methods=['POST'])
#@limiter.limit("4 per minute")
#@limiter.exempt
def create_job():
	# Get the JSON data from the request
	data = request.get_json()

	# Ensure data exists
	if data is None:
		return jsonify({'error': 'No JSON data recieved'}), 400

	# Get client IP address and time of request
	ip_addr = get_ip()
	time_of_request = int(datetime.datetime.now().timestamp())

	# Extract specific fields from the JSON data
	lim_mag = data.get("limiting_magnitude")
	coord = data.get('coordinates')
	pl_name = data.get('pl_name')

	# Parameters
	params = json.dumps({"limiting_magnitude": lim_mag, "coordinates": coord, "pl_name": pl_name})

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
#@limiter.exempt
def get_job_from_db(job_id):

	# Update the jobqueue database
	_, conn = connect_db('exosky')
	row = get_job(conn, job_id)

	if not row:
		print("None!")
		return jsonify([])

	update_job(conn, job_id, {"job_id": job_id, "ip_addr": row[1], "timestamp": int(datetime.datetime.now().timestamp()), "parameters": row[3], "inprogress": row[4]})
	response_data = [get_completed_rows(conn, job_id)]
	
	response = jsonify(response_data)

	_, _conn = connect_db('exosky')
	rcheck = get_job(_conn, job_id)

	if rcheck[4] == 2:
		response.headers['X-Is-Completed'] = 'true'
		print("Done!")
	else:
		response.headers['X-Is-Completed'] = 'false'
	
	return response

@app.route('/star_data/<star_id>', methods=['GET'])
#@limiter.exempt#limit("120 per minute")
def get_star_data(star_id):
	print(star_id)
	out = getStarData(str(star_id))
	response = jsonify(out)
	return response

@app.route('/exo_data/<pl_name>', methods=['GET'])
#@limiter.exempt#limit("30 per minute")
def get_exo_data(pl_name):
	out = getExoData(pl_name)
	response = jsonify(out)
	return response

@app.route('/')
#@limiter.exempt
def test():
	print("Pong")
	print("Ping")
	print(get_ip())
	return "ping!"