import numpy as np
import matplotlib.pyplot as plt
from astropy import units as u
from astropy.coordinates import SkyCoord
from io import BytesIO
import base64

'''
Example RA and Dec values
ra_value = 266.5
dec_value = -29.0
distance = 0
'''
def plotSkyMap (ra_value, dec_value, distance):
    img = plt.imread ("./computation_engine/image.jpg")
    img_height, img_width, _ = img.shape
    dist_offset=26095.89
    distance = distance/8000 * dist_offset
    plt.style.use('dark_background')
    coords = SkyCoord(ra=ra_value * u.degree, dec=dec_value * u.degree, frame='icrs')
    galactic_coords = coords.galactic

    plt.figure(figsize=(9, 9))
    ax = plt.subplot(111)

    l_rad = np.deg2rad(galactic_coords.l.deg)
    b_rad = np.deg2rad(galactic_coords.b.deg)
    #wiki image is offset; fix up plotting coords
    theta_r =0
    if(l_rad > np.pi):
        theta_r = 2*np.pi-l_rad
    else:
        theta_r = l_rad
    dist_new = np.sqrt(distance**2 + dist_offset**2 - 2*distance*dist_offset*np.cos(theta_r))
    beta = np.arccos((dist_offset**2+dist_new**2-distance**2)/(2*dist_offset*dist_new))
    l_radnew = 0
    if(l_rad > np.pi):
        l_radnew = np.pi+beta
    else:
        l_radnew = np.pi-beta

    x=-dist_new*np.sin(l_radnew)
    y=dist_new*np.cos(l_radnew)
    ax.scatter(0, -1 * dist_offset,s=150, color='g')
    scatter = ax.scatter(x, y, s=50, alpha=1, color='r', linewidth=0.5)
    #scatter = ax.scatter(l_radnew, dist_new, s=300, alpha=1, color='r', linewidth=0)
    
    ax.set_ylim(-66666.667, 66666.667)
    ax.set_xlim(-66666.667, 66666.667)
    ax.axes.xaxis.set_visible(False)
    ax.axes.yaxis.set_visible(False)
    ax.axes.spines[['right','bottom','top','left']].set_visible(False)
    plt.imshow(img, aspect="equal", extent=[-66666.667, 66666.667, -66666.667, 66666.667])


    buffer = BytesIO()
    plt.savefig(buffer, format="png")
    data = base64.b64encode(buffer.getbuffer())

    return data

    #plt.savefig("test.png")
    #plt.show()

#plotSkyMap(359.1915117,-22.1532316,436.098)
#plotSkyMap(359.1915117,-22.1532316,0)