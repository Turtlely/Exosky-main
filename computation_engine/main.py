# Imports
from star_db_manager.db_manager import get_batch
from job_manager.db_manager import get_jobs, connect_db
import time
import numpy as np
import cupy as cp
from computation_engine.color import process_temperatures, load_color_lookup_table

# Import the lookup table
clookup = load_color_lookup_table('computation_engine/clookup.json')
COLOR_TEMPERATURE_COARSENESS = 10

# Worker function
def worker(start_id, batch_size, params, job_id):
	# Check if job is still active
	_, conn = connect_db('exosky')
	job_list = [x[0] for x in get_jobs(conn)[1]]
	if job_id not in job_list:
		return -1, "Terminated"

	exocoords = params['coordinates']
	lim_mag = params['limiting_magnitude']
	# Get batch of rows to process from the star database
	flag, rows, gaia_ids = get_batch(start_id, batch_size)
	if flag == -1:
		return -1, rows
	# Process rows
	flag, results = f(cp.array(rows), cp.array(exocoords), gaia_ids, lim_mag)
	#print(batch_size, len(rows),len(results))
	if flag == -1:
		return -1, results
	# Return results
	return 1, results

# Temporary dummy function
def f(x, new_ref, gaia_ids, lim_mag):
	'''
	x = [
		(GaiaID, x, y, z, temperature, earth_relative_magnitude, row ID"),
		... 
		('Gaia DR3 38655544960', 0.225233, 0.225272, 0.000110528, 4708.79, 14.1285, 24112785)
		]
	'''

	# Transpose the array
	x_T = x.T

	# First process positions
	positions = x_T[0:3, :].astype(cp.float32)
	delta = positions - new_ref[:, cp.newaxis]
	dist_ex = cp.linalg.norm(delta, axis=0)
	dist = cp.linalg.norm(positions, axis=0)
 
	# Calculate relative magnitudes
	visual_magnitudes = x_T[4, :]
	relative_magnitudes = visual_magnitudes + 5 * cp.log10(dist_ex / dist)
	
	# Get temperatures
	temperatures = x_T[3, :]

	# Magnitude mask
	mask = relative_magnitudes < lim_mag

	#print(dist_ex)
	#print(dist)
	#print(relative_magnitudes)
	#print(visual_magnitudes)
	#print(temperatures)
	#print(delta)
	#print(mask)
	
	# Filter all items
	fpositions = delta[:, mask]
	fmagnitudes = relative_magnitudes[mask]
	ftemperatures = temperatures[mask]
	fgaia_ids = gaia_ids[cp.asnumpy(mask)]

	# Convert temperatures to color, This uses the CPU
	# Round temperatures to nearest 10s

	rounded_temps = ftemperatures // COLOR_TEMPERATURE_COARSENESS * COLOR_TEMPERATURE_COARSENESS

	# Initialize RGB array
	rgb_array = np.zeros((len(rounded_temps), 3))
	
	rounded_temps = cp.asnumpy(rounded_temps) .astype(np.int32)
	#print("Rounded temps:", rounded_temps)
	#print("Actual temps: ", ftemperatures)

	for i, temp in enumerate(rounded_temps):
		#print(temp)
		if temp in clookup.keys():
			rgb_array[i] = clookup[temp]
		else:
			rgb_array[i] = [0, 1, 0]

	return 1, np.vstack((fgaia_ids, cp.asnumpy(cp.vstack((fpositions, fmagnitudes, cp.transpose((rgb_array))))))).T
	'''
	print(combined_output)

	# Artificial time delay
	#time.sleep(1)

	# Sample output:
	if np.random.rand() < 1:
		out = [{"GaiaID": "Gaia DR3 4295806720", "x": 0, "y": 0, "z": 0, "hex_color": "0x000000", "magnitude": 6.5}]
		return 1, out
	else:
		return 1, []
	'''

if __name__ == "__main__":
	out = f(cp.array([(1, 0, 0, 3000, 14.1285, 24112785),
		(0, 1, 0, 1000, 14.1285, 24112785),
		(0, 0, 1.5, 2000, 14.1285, 24112785)]),cp.array([0, 0.5, 0.5]), 14)
	print(out[0])