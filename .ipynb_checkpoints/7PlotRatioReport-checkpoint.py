#!/usr/bin/env python
# coding: utf-8
# library 
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy
from pyproj import Transformer
from scipy import stats
from scipy import odr
# Orthogonal distance regression
# 正交方法能够同时考虑自变量和因变量的误差。
import matplotlib.pyplot as plt
import ipywidgets
from gedipy import h5io,rsmooth
from dask.distributed import Client, progress
import sys
import dask
# constants
VALID_SITES = ['amani','csir_agincourt', 'csir_dnyala', 'csir_ireagh', 'csir_justicia', 'csir_venetia', 'csir_welverdient', 'drc_ghent_field_32635', 
               'drc_ghent_field_32733', 'drc_ghent_field_32734', 'gsfc_mozambique', 'jpl_lope', 'jpl_rabi', 'tanzania_wwf_germany', 'khaoyai_thailand', 
               'chowilla', 'credo', 'karawatha', 'litchfield', 'rushworth_forests', 'tern_alice_mulga', 'tern_robson_whole', 'costarica_laselva2019', 
               'skidmore_bayerischer', 'zofin_180607', 'spain_exts1', 'spain_exts2', 'spain_exts3', 'spain_exts4', 'spain_leonposada', 'spain_leon1', 
               'spain_leon2', 'spain_leon3', 'jpl_borneo_004', 'jpl_borneo_013', 'jpl_borneo_040', 'jpl_borneo_119', 'jpl_borneo_144', 'chave_paracou', 
               'embrapa_brazil_2020_and_a01', 'embrapa_brazil_2020_bon_a01', 'embrapa_brazil_2020_cau_a01', 'embrapa_brazil_2020_duc_a01', 
               'embrapa_brazil_2020_hum_a01', 'embrapa_brazil_2020_par_a01', 'embrapa_brazil_2020_rib_a01', 'embrapa_brazil_2020_tal_a01',
               'embrapa_brazil_2020_tan_a01', 'embrapa_brazil_2020_tap_a01', 'embrapa_brazil_2020_tap_a04', 'walkerfire_20191007', 
               'neon_abby2018', 'neon_abby2019', 'neon_abby2021', 'neon_bart2018', 'neon_bart2019', 'neon_blan2019', 'neon_blan2021', 
               'neon_clbj2018', 'neon_clbj2019', 'neon_clbj2021', 'neon_clbj2021', 'neon_dela2018', 'neon_dela2019', 'neon_dela2021', 
               'neon_dsny2018', 'neon_dsny2021', 'neon_grsm2018', 'neon_grsm2021', 'neon_guan2018', 'neon_harv2018', 'neon_harv2019', 
               'neon_jerc2019', 'neon_jerc2021', 'neon_jorn2018', 'neon_jorn2019', 'neon_jorn2021', 'neon_konz2019', 'neon_konz2020', 
               'neon_leno2018', 'neon_leno2019', 'neon_leno2021', 'neon_mlbs2018', 'neon_mlbs2021', 'neon_moab2018', 'neon_moab2021', 
               'neon_niwo2019', 'neon_niwo2020', 'neon_nogp2021', 'neon_onaq2019', 'neon_onaq2021', 'neon_osbs2018', 'neon_osbs2019', 
               'neon_osbs2021', 'neon_puum2020', 'neon_rmnp2018', 'neon_rmnp2020', 'neon_scbi2019', 'neon_scbi2021', 'neon_serc2019', 
               'neon_serc2021', 'neon_sjer2019', 'neon_soap2018', 'neon_soap2019', 'neon_soap2021', 'neon_srer2019', 'neon_srer2021', 
               'neon_stei2019', 'neon_stei2020', 'neon_ster2021', 'neon_tall2018', 'neon_tall2019', 'neon_tall2021', 'neon_teak2021', 
               'neon_ukfs2018', 'neon_ukfs2019', 'neon_ukfs2020', 'neon_unde2019', 'neon_unde2020', 'neon_wood2021', 'neon_wref2019', 
               'neon_wref2021', 'neon_yell2018', 'neon_yell2019', 'neon_yell2020', 
               'neon_blan2022', 'neon_clbj2022', 'neon_grsm2022', 'neon_moab2022', 'neon_onaq2022', 'neon_rmnp2022', 'neon_serc2022', 
               'neon_stei2022', 'neon_steicheq2022', 'neon_ster2022', 'neon_unde2022', 'inpe_brazil31983', 'inpe_brazil31981', 
               'inpe_brazil31979', 'inpe_brazil31976', 'inpe_brazil31975', 'inpe_brazil31973', 'inpe_brazil31974', 'inpe_brazil31978',
                'jrsrp_ilcp2015_wholeq6', 'csir_limpopo']


# functions 


def fit_model(rv,rg):
    idx = (rv > 0) & (rg > 0)
    tmp = odr.Data(rv[idx], rg[idx])
    linear = odr.ODR(tmp, odr.unilinear, beta0=[-1.0,numpy.mean(rg[idx])])
    result = linear.run()
    rhog = result.beta[1]
    rhov = -rhog / result.beta[0]
    rhovg = -1 / result.beta[0]
    pgap = 1 - rv / (rv + rg * rhovg)
    p,p2 = stats.pearsonr(rv,rg)
    return result.beta,p,pgap,rhovg

def plot_model(rv,rg,beta,r,pgap,rhov_rhog,site,track,beam, lim=None):
    plt.rcParams.update({'font.size': 14})
    fig,ax = plt.subplots(1, 1, figsize=(10,8))
    # Define colors for each group
    colors = ['red', 'green', 'blue']
    for group, color in zip(['gt1' + beam.iloc[0][-1],  'gt2'+ beam.iloc[0][-1], 'gt3'+ beam.iloc[0][-1]], colors):
            if len(rg[beam == group]) > 2:
                ax.scatter(rv[beam == group], rg[beam == group], c=color, s=30, alpha=0.9, linewidth=0) # , vmin=0, vmax=1
    ax.set(xlabel=r'$R_{v}$', ylabel=r'$R_{g}$', xlim=lim, ylim=lim, title=site.upper())
    xtmp = numpy.linspace(0,lim[1],num=2)
    ax.plot(xtmp, beta[0]*xtmp + beta[1], color='black', linewidth=1)
    ax.text(1.2, 1.7, r'$\frac{\rho_v}{\rho_g}=%.02f}$'%rhov_rhog, fontsize=14)
    ax.text(1.2, 1.5, r'$r^{2}=%.02f}$'%r**2, fontsize=14)
    #cbar = ax.figure.colorbar(s, ax=ax)
    #cbar.ax.set_ylabel('Canopy Cover', rotation=-90, va='bottom')        
    #fig.canvas.draw()
    # Save the plot without displaying it
    # Save the plot to a PDF file
    #fig.savefig('../result/ratioPlot/' + site + '_' + track +  '.pdf', format='pdf', bbox_inches='tight')
    fig.savefig('../result/ratioPlot/' + site + '_' + track +  '.jpg', format='jpg', bbox_inches='tight')
    # Optionally, you can close the figure if you don't want to display it at all
    plt.close(fig)

def main(index):
    roi=sites.iloc[index]['geometry']
    site=sites.iloc[index]['name']
    if site not in VALID_SITES: return
    als_index = gdf_is2.sindex.query(roi) # super fast!!!!! # but only boundary box. 
    is2_in_als = gdf_is2.loc[als_index] 
    df_site = is2_in_als.clip(roi)  # get points inside polygon.
    df_site = df_site[df_site['land_segments/terrain/photon_rate_te'] < 140]
    if len(df_site) < 100: return
    # classify by tracks 
    grouped = df_site.groupby('root_file')
    for track, group in grouped:
        #print(site, track)
        if len(group) < 30: return
        rg = group['land_segments/terrain/photon_rate_te']
        rv = group['land_segments/canopy/photon_rate_can']
        beam = group['root_beam'] # this is a col. # beam array
        beta,r,pgap,rhov_rhog = fit_model(rv,rg)
        if r**2 > 0.5:
            plot_model(rv,rg,beta,r,pgap,rhov_rhog,site,track[:-3], beam, lim=(0,2)) #...  

# main function. 
if __name__ == "__main__":
        print('Loading is2 data in cal/val sites.')
        gdf_is2 = gpd.read_parquet('../result/ratio_100m.parquet')
        gdf_is2 = gdf_is2.reset_index(drop=True)
        sites   = gpd.read_parquet('../data/all_sites_20231218.parquet')
        for index, row in sites.iterrows():
                  #print(index)
                  main(index)
        # print('Starting client...')
        # client=Client(n_workers=10)
        # print('Dask link: ', client.dashboard_link)
        # cmds = [main(index) for index, row in sites.iterrows()]
        # _ = dask.persist(*cmds)
        # progress(_)
        # client.close()
        sys.exit('Done')