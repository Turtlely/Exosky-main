from astroquery.gaia import Gaia
from astroquery.simbad import Simbad
import pandas as pd

def GetGAIAData(GaiaDR2SourceIDs):
    # gets the GAIA data for the provided GaiaDR2SourceIDs's
    # and writes to a local CSV
        
    dfGaia = pd.DataFrame()
    
    qry = f'SELECT teff_gspphot, distance_gspphot, phot_g_mean_mag  FROM gaiadr3.gaia_source gs WHERE gs.source_id = {GaiaDR2SourceIDs};'
    job = Gaia.launch_job_async( qry )
    tblGaia = job.get_results()       #Astropy table
    dfGaia = tblGaia.to_pandas()      #convert to Pandas dataframe
    return dfGaia

def GetSimbadData(ID):
    Simbad.add_votable_fields('sp_type')
    Simbad.add_votable_fields('main_id')
    result_table = Simbad.query_object(f"Gaia DR3 {ID}")
    return result_table['sp_type'][0], result_table['main_id'][0]

#ID = '4579157830215007488'

def getStarData(ID):
    out = GetGAIAData(ID)
    temp = int(out['teff_gspphot'][0])
    dist = int(out['distance_gspphot'][0])
    mag = out['phot_g_mean_mag'][0]
    out = GetSimbadData(ID)
    return {"Temperature (K)": temp,
            "Distance (pc)": dist,
            "Magnitude": str(mag.round(2)),
            "Stellar Type": out[0],
            "Primary Name": out[1]}

#print(getStarData(ID))