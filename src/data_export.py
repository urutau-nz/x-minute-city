'''
Export data for the d3 app
'''
import yaml
import main
import pandas as pd
import numpy as np
import json
import geopandas as gpd
import topojson as tp

config_filename = 'main'
# import config file
with open('./config/{}.yaml'.format(config_filename)) as file:
    config = yaml.load(file)

# connect to the psql database
db = main.init_db(config)

import code
code.interact(local=locals())

###
# distance as csv - only nonzero pop
###
# sql = "SELECT geoid, dest_type, distance FROM nearest_block WHERE population > 0"
# dist = pd.read_sql(sql, db['con'])
# dist.to_csv('./data/results/distances.csv')

###
# blocks - shpfile
###
sql = 'SELECT geoid as id, geometry FROM nearest_block WHERE population > 0 AND distance IS NOT NULL'
blocks = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
blocks.drop_duplicates(inplace=True)
blocks['centroids'] = blocks.centroid
zones = gpd.read_file('./data/raw/statsnzterritorial-authority-2021-generalised-SHP/territorial-authority-2021-generalised.shp')
zones = zones[['TA2021_V_1', 'TA2021_V1_','geometry']]
zones = zones.to_crs(blocks.crs)
blocks.set_geometry('centroids',inplace=True)
blocks = blocks.to_crs(zones.crs)
blocks = gpd.sjoin(blocks, zones, how='inner', op='within')
blocks.set_geometry('geometry',inplace=True)
blocks = blocks.to_crs(zones.crs)
blocks = blocks.loc[blocks['TA2021_V1_'] != 999]
blocks = blocks.loc[~blocks['id'].isin(['7022480', '7001121', '7001125', '7001123', '7023238', '7029871'])]
blocks.drop('centroids', axis=1, inplace=True)
blocks.to_file('./data/results/blocks.shp')


###
# topojson - or use https://mapshaper.org/
###
# sql = 'SELECT geoid as id, geometry FROM nearest_block WHERE population > 0'
# blocks = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
# blocks_topo = tp.Topology(blocks).topoquantize(1e6)
# blocks_topo.to_json('./data/results/blocks.topojson')

###
# destinations: dest_type, lat, lon
###
# sql = "SELECT dest_type, st_x(geom) as lon, st_y(geom) as lat FROM destinations"
# dist = pd.read_sql(sql, db['con'])
# dist.to_csv('./data/results/destinations.csv')

###
# histogram and cdf
###

# import data
sql = "SELECT geoid, dest_type, distance, population, geometry  FROM nearest_block WHERE population > 0 AND distance IS NOT NULL"
df = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
df['centroids'] = df.centroid
zones = gpd.read_file('./data/raw/statsnzterritorial-authority-2021-generalised-SHP/territorial-authority-2021-generalised.shp')
zones = zones[['TA2021_V_1','geometry']]
zones = zones.to_crs(df.crs)
df.set_geometry('centroids',inplace=True)
df = df.to_crs(zones.crs)
df = gpd.sjoin(df, zones, how='inner', op='within')
# # join with census data
df_census = pd.read_csv('./data/raw/Individual_part1_totalNZ-wide_format_updated_16-7-20.csv')
df_census['nz_euro'] = pd.to_numeric(df_census['Census_2018_Ethnicity_grouped_total_responses_level_1_1_European_CURP'].replace('C',0))
df_census['maori'] = pd.to_numeric(df_census['Census_2018_Ethnicity_grouped_total_responses_level_1_2_Māori_CURP'].replace('C',0))
df_census['pasifika'] = pd.to_numeric(df_census['Census_2018_Ethnicity_grouped_total_responses_level_1_3_Pacific_Peoples_CURP'].replace('C',0))
df_census['asian'] = pd.to_numeric(df_census['Census_2018_Ethnicity_grouped_total_responses_level_1_4_Asian_CURP'].replace('C',0))
df_census = df_census[['Area_code','nz_euro','maori','pasifika','asian']]
df_census['Area_code'] = df_census['Area_code'].map(str)
df = df.merge(df_census, how='inner', left_on='geoid',right_on='Area_code')
# set bins
bins = 100#list(range(0,21))
# create hist and cdf
groups = ['population','nz_euro','maori','pasifika','asian']#,'difficulty_walking']
hists = []
for group in groups:
    regions = df['TA2021_V_1'].unique()
    for service in df['dest_type'].unique():
        df_sub = df[df['dest_type']==service]
        # print(group)
        # print(service)
        # create the hist
        # import code
        # code.interact(local=locals())
        density, division = np.histogram(df_sub['distance']/60, bins = bins, weights=df_sub[group], density=True)
        unity_density = density / density.sum()
        unity_density = np.append(0, unity_density)
        division = np.append(0, division)
        df_new = pd.DataFrame({'pop_perc':unity_density, 'distance':division[:-1]})
        df_new['region'] = 'All'
        df_new['service']=service
        df_new['pop_perc'] = df_new['pop_perc']*100
        df_new['pop_perc_cum'] = df_new['pop_perc'].cumsum()
        df_new['group'] = group
        hists.append(df_new)
        for region in regions:
            df_sub = df[(df['dest_type']==service)&(df['TA2021_V_1']==region)]
            # create the hist
            density, division = np.histogram(df_sub['distance']/60, bins = bins, weights=df_sub[group], density=True)
            unity_density = density / density.sum()
            unity_density = np.append(0, unity_density)
            division = np.append(0, division)
            df_new = pd.DataFrame({'pop_perc':unity_density, 'distance':division[:-1]})
            df_new['service']=service
            df_new['pop_perc'] = df_new['pop_perc']*100
            df_new['pop_perc_cum'] = df_new['pop_perc'].cumsum()
            df_new['region'] = region
            df_new['group'] = group
            hists.append(df_new)

# concat
df_hists = pd.concat(hists)
df_hists = df_hists[df_hists['distance'] <= 100]
# export
df_hists.to_csv('./data/results/access_histogram.csv')

            # df_new['pop_perc'] = df_new['pop_perc']*100
            # df_new['pop_perc_cum'] = df_new['pop_perc'].cumsum()
            # df_new['region'] = region
            # df_new['group'] = group
            # hists.append(df_new)

            # # concat
            # df_hists = pd.concat(hists)
            # # export
            # df_hists.to_csv('./data/results/access_histogram.csv')
