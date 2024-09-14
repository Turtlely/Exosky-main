# Path to the exoplanets csv
import csv
import json
import numpy as np

def spherical_to_cartesian(ra, dec, dist):
	# Convert ra, dec to radians
	ra = np.deg2rad(ra)
	dec = np.deg2rad(dec)

	z = dist * np.sin(dec)
	y = dist * np.cos(dec) * np.sin(ra)
	x = dist * np.cos(dec) * np.cos(ra)
	return x, y, z

final_json = {}

with open('./exoplanet_positions/exoplanets.csv') as f:
	csvFile = csv.DictReader(f)
	for lines in csvFile:
		if '' not in lines.values():
			pl_name = lines['pl_name']
			ra = float(lines['ra'])
			dec = float(lines['dec'])
			dist = float(lines['sy_dist'])

			# Convert spherical to cart
			x, y, z = spherical_to_cartesian(ra, dec, dist)
			final_json[pl_name] = {"x": x, "y": y, "z": z}

with open('exoplanets.json', 'w') as fp:
	json.dump(final_json, fp)
