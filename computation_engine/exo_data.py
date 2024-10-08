'''
discovery method
year discovered
habitable
RA
DEC
dist
mass
orbital period
name
temp
mass
type NO
mag
'''

from PyAstronomy import pyasl
from computation_engine.milky_map import plotSkyMap
#pl_name = "55 Cnc b"

def getExoData(pl_name):
	nexa = pyasl.NasaExoplanetArchive()
	dat = nexa.selectByPlanetName(pl_name)
	b64 = plotSkyMap(dat['ra'], dat['dec'], dat['sy_dist'])

	return {"RA": str(dat['ra']), 
			"DEC": str(dat['dec']),
			"DIST": str(dat['sy_dist']),
			"PLMASS": str(dat['pl_massj']), 
			"PLORBPER": str(dat['pl_orbper']),
			"HOSTNAME": str(dat['hostname']), 
			"TEMP": str(dat['st_teff']), 
			"HOSTMASS": str(dat['st_mass']),
			"MAG": str(dat['sy_vmag']),
			"IMAGE": b64.decode('utf-8')}
