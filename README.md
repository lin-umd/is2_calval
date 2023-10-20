# is2_calval
co-locate is2 shots ?

# pulse shape information
tx_pulse_width_lower: difference between the lower thresholdcrossing times

tx_pulse_width_upper: difference between the upper thresholdcrossing times

tx_pulse_thresh_lower: lower threshold setting of the SPD volts ATL02, Section 5.2

tx_pulse_thresh_upper: upper threshold setting of the SPD volts ATL02, Section 5.2

tx_pulse_distribution: fraction of transmit pulse energy perbeam

tx_pulse_skew_est: difference between the means of thelower and upper threshold crossing times

# processing notes 

1.5 nanoseconds, light travels approximately 0.45 meters.

add slope --- [100m] --- 20m seg[000, 001,002,003,004]

100 m -- always have lat and lon, but not 20m segments. 

land_segments/segment_id_beg  ---

land_segments/segment_id_end

land_segments/rgt


################## Step 1 get all 20m segments in one las file ---

################## Step 2 convert to footprints coordinates 

################## Step 3 simulation ---> get 1 las file , get 1 waveform file.

################# later ---> merge h5 file ------------get all footprint wavefrom .

#################  each 20m --> find footprint ID --> get rh, ch. 

