# goal: each site, each 20m segment, each beam, return a Rg/Rv pair.
# return: rg, rv, atl03_name, atl_08_name, segment ID, lat,  lon, beam type, 
# photon_rate_can Float Photon	rate	of	canopy	photons	within	each	100	m	segment
# photon_rate_te Float Calculated	photon	rate	for	ground	photons	within	each segment
import os
import numpy
from pyproj import Transformer
from scipy import stats
from scipy import odr
import matplotlib.pyplot as plt
import ipywidgets
from gedipy import h5io # https://github.com/armstonj/gedipy
import pandas as pd
os.chdir('/gpfs/data1/vclgp/xiongl/ProjectIS2CalVal/tmp_test/')
atl03_filename = 'ATL03_20230711211853_03312008_006_01.h5'
atl08_filename = 'ATL08_20230711211853_03312008_006_01.h5'
# Open the ATL03 file
atl03_fid = h5io.ATL03H5File(atl03_filename)
atl03_fid.open_h5()
# Open the ATL08 file
atl08_fid = h5io.ATL08H5File(atl08_filename)
atl08_fid.open_h5()
# Which beams are power/weak depends on the orientation of the satellite
atlas_orientation = atl03_fid.get_atlas_orientation()
if atlas_orientation == 'backward':
    power_beams = [beam for beam in atl03_fid.beams if beam.endswith('l')]
elif atlas_orientation == 'forward':
    power_beams = [beam for beam in atl03_fid.beams if beam.endswith('r')]
else:
    print('ATLAS orientation in transition, do not use')
res=[]
for item in ['gt1', 'gt2', 'gt3']:
        # Select the RGT center power beam for this example
        beam = next(power_beam for power_beam in power_beams if item in power_beam)
        print(beam)
        # Is it night or day?
        night_flag = atl08_fid.get_dataset(beam, 'land_segments/night_flag')
        print('{:.2f}% of the land segments are night time'.format(sum(night_flag)/night_flag.size*100))
        longitude, latitude, elevation = atl03_fid.get_coordinates(beam, ht=True)
        ph_class = atl03_fid.get_photon_labels(beam, atl08_fid)
        print(ph_class)
        # ph_index_beg = atl03_fid.fid[beam+'/geolocation/ph_index_beg'][()]
        # segment_id = atl03_fid.fid[beam+'/geolocation/segment_id'][()]
        # valid_idx = ph_index_beg > 0
        # idx = numpy.searchsorted(segment_id[valid_idx],
        #       atl08_fid.get_photon_segment_id(beam), side='left')
        # atl08_ph_index_beg = ph_index_beg[valid_idx][idx] - 1
        # signal_segment_id = segment_id[valid_idx][idx]
        
        # ph_idx = atl08_ph_index_beg + atl08_fid.get_photon_index(beam)
        # ph_segment_id = numpy.zeros(atl03_fid.get_nrecords(beam), dtype=numpy.int32)
        # ph_segment_id[ph_idx] = signal_segment_id
        # valid = ~numpy.isnan(longitude)
        # valid &= ~numpy.isnan(latitude)
        
        # transformer = Transformer.from_crs(4326, 6933, always_xy=True)
        # x,y = transformer.transform(longitude[valid], latitude[valid])
        # quality = atl08_fid.get_quality_flag(beam = beam, night=False, power=False, 
        #                                      h_canopy_uncertainty=None, 
        #                                      n_canopy_photons=140)
        onebeam = pd.DataFrame()
        onebeam['rv'] = atl08_fid.get_dataset(beam, 'land_segments/canopy/photon_rate_can')
        onebeam['rg'] = atl08_fid.get_dataset(beam, 'land_segments/terrain/photon_rate_te')
        onebeam['seg_beg'] = atl08_fid.get_dataset(beam, 'land_segments/segment_id_beg')
        onebeam['seg_end'] = atl08_fid.get_dataset(beam, 'land_segments/segment_id_end')
        onebeam['lat'] = atl08_fid.get_dataset(beam, 'land_segments/latitude')
        onebeam['lon'] = atl08_fid.get_dataset(beam, 'land_segments/longitude')
        onebeam.insert(0, 'beam', beam)
        res.append(onebeam)
atl03_fid.close_h5()
atl08_fid.close_h5()
res = pd.concat(res, axis=0, ignore_index=True)
print(res)


