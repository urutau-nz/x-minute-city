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

# import code
# code.interact(local=locals())

###
# distance as csv - only nonzero pop
###
sql = "SELECT geoid, dest_type, distance FROM nearest_block WHERE population > 0"
dist = pd.read_sql(sql, db['con'])
dist.to_csv('./data/results/distances.csv')

###
# topojson
###
sql = 'SELECT geoid as id, geometry FROM nearest_block WHERE population > 0'
blocks = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
blocks_topo = tp.Topology(blocks).topoquantize(1e6)
blocks_topo.to_json('./data/results/blocks.topojson')

###
# destinations: dest_type, lat, lon
###
sql = "SELECT dest_type, st_x(geom) as lon, st_y(geom) as lat FROM destinations"
dist = pd.read_sql(sql, db['con'])
dist.to_csv('./data/results/destinations.csv')

###
# histogram and cdf
###

# import data
sql = "SELECT geoid, dest_type, distance, population, geometry  FROM nearest_block WHERE population > 0"
df = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
zones = gpd.read_file('./data/raw/TransportZones.gdb', driver='FileGDB',layer='TransportZoneBoundaries')
zones = zones[['Location','geometry']]
zones = zones.to_crs(df.crs)
df = gpd.sjoin(df, zones, how='inner', op='within')
# # join with census data
# df_census = pd.read_csv('./data/raw/Individual_part2_totalNZ-wide_format_updated_16-7-20.csv')
# df_census['Census_2018_Difficulty_walking_02_Some_difficulty_CURP_5yrs_and_over'] = pd.to_numeric(df_census['Census_2018_Difficulty_walking_02_Some_difficulty_CURP_5yrs_and_over'].replace('C',0))
# df_census['Census_2018_Difficulty_walking_03_A_lot_of_difficulty_CURP_5yrs_and_over'] = pd.to_numeric(df_census['Census_2018_Difficulty_walking_03_A_lot_of_difficulty_CURP_5yrs_and_over'].replace('C',0))
# df_census['Census_2018_Difficulty_walking_04_Cannot_do_at_all_CURP_5yrs_and_over'] = pd.to_numeric(df_census['Census_2018_Difficulty_walking_04_Cannot_do_at_all_CURP_5yrs_and_over'].replace('C',0))
# df_census.eval("difficulty_walking = Census_2018_Difficulty_walking_02_Some_difficulty_CURP_5yrs_and_over + Census_2018_Difficulty_walking_03_A_lot_of_difficulty_CURP_5yrs_and_over + Census_2018_Difficulty_walking_04_Cannot_do_at_all_CURP_5yrs_and_over", inplace=True)
# df_census = df_census[['Area_code','difficulty_walking']]
# df_census['Area_code'] = df_census['Area_code'].map(str)
# df = df.merge(df_census, how='inner', left_on='geoid',right_on='Area_code')
# set bins
bins = 100#list(range(0,21))
# create hist and cdf
groups = ['population']#,'difficulty_walking']
hists = []
for group in groups:
    regions = df['Location'].unique()
    for service in df['dest_type'].unique():
        df_sub = df[df['dest_type']==service]
        # create the hist
        # import code
        # code.interact(local=locals())
        density, division = np.histogram(df_sub['distance']/1000, bins = bins, weights=df_sub[group], density=True)
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
            df_sub = df[(df['dest_type']==service)&(df['Location']==region)]
            # create the hist
            density, division = np.histogram(df_sub['distance']/1000, bins = bins, weights=df_sub[group], density=True)
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
# export
df_hists.to_csv('./data/results/access_histogram.csv')

            df_new['pop_perc'] = df_new['pop_perc']*100
            df_new['pop_perc_cum'] = df_new['pop_perc'].cumsum()
            df_new['region'] = region
            df_new['group'] = group
            hists.append(df_new)

# concat
df_hists = pd.concat(hists)
# export
df_hists.to_csv('./data/results/access_histogram.csv')
