'''
Export data for the d3 app
'''
import code
import yaml
from yaml import Loader, Dumper
import main
import pandas as pd
import numpy as np
import json
import geopandas as gpd
import topojson as tp
from tqdm import tqdm
import inequalipy as ineq
from geoalchemy2 import Geometry, WKTElement
import ray
import psutil


def main_export():
    config_filename = 'main'
    # import config file
    with open('./config/{}.yaml'.format(config_filename)) as file:
        config = yaml.load(file,Loader=Loader)

    # connect to the psql database
    db = main.init_db(config)
    con = db['con']
    engine = db['engine']
    projection = config['set_up']['projection']

    # import code
    # code.interact(local=locals())

    ###
    # duration as csv - only nonzero pop # block_results
    ###
    if config['data_export']['block_results']:
        sql = "SELECT * FROM nearest_{} WHERE population > 0".format(config['SQL']['table_name'])
        df = gpd.read_postgis(sql, engine, geom_col='geometry')
        df = df[df['dest_type'].isin(['pharmacy','oral_health','doctor','supermarket','ece','greenspace','primary_school'])]
        # dist.duration = dist.duration/60
        # dist.duration = dist.duration.astype(int)
        if config['alter_results']['alter_block_results']:
            df.loc[(df.geoid=='7026575')&(df.dest_type=='ece')&(df['mode']=='walking'),'duration'] = 40
            df.loc[(df.geoid=='7026575')&(df.dest_type=='ece')&(df['mode']=='cycling'),'duration'] = 14
            df.loc[(df.geoid=='7026575')&(df.dest_type=='ece')&(df['mode']=='driving'),'duration'] = 6

            df.loc[(df.geoid=='7026574')&(df.dest_type=='ece')&(df['mode']=='walking'),'duration'] = 40
            df.loc[(df.geoid=='7026574')&(df.dest_type=='ece')&(df['mode']=='cycling'),'duration'] = 14
            df.loc[(df.geoid=='7026574')&(df.dest_type=='ece')&(df['mode']=='driving'),'duration'] = 6

            df.loc[(df.geoid=='7026573')&(df.dest_type=='ece')&(df['mode']=='walking'),'duration'] = 38
            df.loc[(df.geoid=='7026573')&(df.dest_type=='ece')&(df['mode']=='cycling'),'duration'] = 13
            df.loc[(df.geoid=='7026573')&(df.dest_type=='ece')&(df['mode']=='driving'),'duration'] = 5

            df.loc[(df.geoid=='7026574')&(df.dest_type=='pharmacy')&(df['mode']=='walking'),'duration'] = 66
            df.loc[(df.geoid=='7026574')&(df.dest_type=='pharmacy')&(df['mode']=='cycling'),'duration'] = 25
            df.loc[(df.geoid=='7026574')&(df.dest_type=='pharmacy')&(df['mode']=='driving'),'duration'] = 9

            df.loc[(df.geoid=='7026575')&(df.dest_type=='pharmacy')&(df['mode']=='walking'),'duration'] = 66
            df.loc[(df.geoid=='7026575')&(df.dest_type=='pharmacy')&(df['mode']=='cycling'),'duration'] = 25
            df.loc[(df.geoid=='7026575')&(df.dest_type=='pharmacy')&(df['mode']=='driving'),'duration'] = 9

            df.loc[(df.geoid=='7026560')&(df.dest_type=='doctor')&(df['mode']=='walking'),'duration'] = 53
            df.loc[(df.geoid=='7026560')&(df.dest_type=='doctor')&(df['mode']=='cycling'),'duration'] = 20
            df.loc[(df.geoid=='7026560')&(df.dest_type=='doctor')&(df['mode']=='driving'),'duration'] = 7

            df.loc[(df.geoid=='7026572')&(df.dest_type=='doctor')&(df['mode']=='walking'),'duration'] = 57
            df.loc[(df.geoid=='7026572')&(df.dest_type=='doctor')&(df['mode']=='cycling'),'duration'] = 22
            df.loc[(df.geoid=='7026572')&(df.dest_type=='doctor')&(df['mode']=='driving'),'duration'] = 8

            df.loc[(df.geoid=='7026573')&(df.dest_type=='doctor')&(df['mode']=='walking'),'duration'] = 60
            df.loc[(df.geoid=='7026573')&(df.dest_type=='doctor')&(df['mode']=='cycling'),'duration'] = 23
            df.loc[(df.geoid=='7026573')&(df.dest_type=='doctor')&(df['mode']=='driving'),'duration'] = 8

            df.loc[(df.geoid=='7026574')&(df.dest_type=='doctor')&(df['mode']=='walking'),'duration'] = 65
            df.loc[(df.geoid=='7026574')&(df.dest_type=='doctor')&(df['mode']=='cycling'),'duration'] = 25
            df.loc[(df.geoid=='7026574')&(df.dest_type=='doctor')&(df['mode']=='driving'),'duration'] = 9

            df.loc[(df.geoid=='7026575')&(df.dest_type=='doctor')&(df['mode']=='walking'),'duration'] = 65
            df.loc[(df.geoid=='7026575')&(df.dest_type=='doctor')&(df['mode']=='cycling'),'duration'] = 25
            df.loc[(df.geoid=='7026575')&(df.dest_type=='doctor')&(df['mode']=='driving'),'duration'] = 9
        
        all_dist = df.groupby(['geoid','mode']).max()
        all_dist['dest_type']='all'
        all_dist.reset_index(inplace=True)
        geoms = []
        for idx in tqdm(list(all_dist['geoid'].unique())):
            geoms += [df[df['geoid']==idx].iloc[0]['geometry']]*3
        all_dist['geometry'] = geoms
        df = df.append(all_dist)

        df.to_postgis('updated_nearest_{}'.format(config['SQL']['table_name']), engine, if_exists='replace', dtype={'geometry': Geometry('POLYGON', srid= projection)})
        df = df[['geoid', 'dest_type', 'duration', 'mode']]
        df.to_csv('./data/results/duration.csv')
        print('Written: ./data/results/duration.csv')


    ###
    # blocks - shpfile # blocks_shapefile
    ###
    if config['data_export']['block_shapefile']:
        sql = 'SELECT geoid as id, geometry FROM updated_nearest_{} WHERE population > 0 AND duration IS NOT NULL'.format(config['SQL']['table_name'])
        blocks = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
        blocks.drop_duplicates(inplace=True)
        blocks['centroids'] = blocks.centroid
        urbans = gpd.read_file('./data/raw/new_urban_areas.shp')
        urbans = urbans[['UR2020_V_2', 'UR2020_V1_','geometry']]
        urbans = urbans.to_crs(blocks.crs)
        blocks.set_geometry('centroids',inplace=True)
        blocks = blocks.to_crs(urbans.crs)
        blocks = gpd.sjoin(blocks, urbans, how='inner', op='within')
        blocks.set_geometry('geometry',inplace=True)
        blocks = blocks.to_crs(urbans.crs)
        blocks = blocks.loc[blocks['UR2020_V1_'] != 999]
        blocks.drop('centroids', axis=1, inplace=True)
        fn = './data/results/blocks.shp'
        blocks.to_file(fn)
        print('Written: {}'.format(fn))

    ###
    # topojson - or use https://mapshaper.org/ # blocks_topojson
    ###
    if config['data_export']['block_topojson']:
        # sql = 'SELECT geoid as id, geometry FROM nearest_block WHERE population > 0'
        # blocks = gpd.read_postgis(sql, con=db['con'], geom_col='geometry')
        # blocks_topo = tp.Topology(blocks).topoquantize(1e6)
        # blocks_topo.to_json('./data/results/blocks.topojson')

        urbans = gpd.read_file('./data/raw/new_urban_areas.shp')
        urbans['region'] = urbans['UR2020_V_1']
        urbans=urbans[['UR2020_V1_','region']]
        blocks = gpd.read_file('./data/results/blocks.shp')
        blocks = blocks.merge(urbans, how='inner', left_on='UR2020_V1_',right_on='UR2020_V1_')
        regions = list(blocks['region'].unique())
        for region in tqdm(regions):
            sub_block = blocks[blocks['region']==region]
            blocks_topo = tp.Topology(sub_block).topoquantize(1e6)
            blocks_topo.to_json('./data/results/block_{}.topojson'.format(region))

        # Get centroids # region_centroids
    if config['data_export']['region_centroids']:
        urbans = gpd.read_file('./data/raw/new_urban_areas.shp')
        urbans = urbans[['UR2020_V_1','geometry']]
        urbans['centroid'] = urbans.centroid
        urbans['Y'] = urbans['centroid'].y
        urbans['X'] = urbans['centroid'].x
        urbans['zoom'] = 15
        urbans=urbans[['Y','X','UR2020_V_1','zoom']]
        fn = './data/results/urbans_centroids.csv'
        urbans.to_csv(fn)
        print('Written: {}'.format(fn))


    ###
    # destinations: dest_type, lat, lon # region_destinations
    ###
    if config['data_export']['region_destinations']:
        sql = "SELECT dest_type, st_x(geom) as lon, st_y(geom) as lat FROM destination_nz"
        df = pd.read_sql(sql, db['con'])
        gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat))
        urbans = gpd.read_file('./data/raw/new_urban_areas.shp')
        urbans = urbans[['UR2020_V_1','geometry']]
        gdf = gdf.set_crs(4326)
        # buffer
        # gdf = gdf.to_crs(3395)
        # gdf = gdf.buffer(5000, join_style=2)
        # gdf = gdf.to_crs(4326)
        df = gpd.sjoin(gdf, urbans, how='inner', op='within')
        df = df[['dest_type','lon','lat','UR2020_V_1']]
        # gdf = gpd.clip(zones, gdf)
        fn = './data/results/destinations_regions.csv'
        df.to_csv(fn)
        print('Written: {}'.format(fn))

        destinations = pd.Series(df.dest_type.unique())
        fn = './data/results/destinations_list.csv'
        destinations.to_csv(fn)
        print('Written: {}'.format(fn))

    ###
    # histogram and cdf # access_histogram
    ###
    if config['data_export']['access_histogram']:
        # import data
        sql = "SELECT geoid, dest_type, duration, population, geometry, mode  FROM updated_nearest_{} WHERE population > 0 AND duration IS NOT NULL".format(config['SQL']['table_name'])
        df = gpd.read_postgis(sql, con=db['engine'], geom_col='geometry')
        df['centroids'] = df.centroid
        urbans = gpd.read_file('./data/raw/new_urban_areas.shp')
        # zones = gpd.read_file('/homedirs/projects/x-minute-city/data/raw/statsnzterritorial-authority-2021-generalised-SHP/territorial-authority-2021-generalised.shp')
        urbans = urbans[['UR2020_V_1','geometry']]
        urbans = urbans.to_crs(df.crs)
        df.set_geometry('centroids',inplace=True)
        df = df.to_crs(urbans.crs)
        df = gpd.sjoin(df, urbans, how='inner', op='within')
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
        bins = list(range(1,60)) + [1000]
        # create hist and cdf
        groups = ['population','nz_euro','maori','pasifika','asian']#,'difficulty_walking']
        hists = []
        modes = df['mode'].unique()
        for mode in modes:
            for group in groups:
                regions = df['UR2020_V_1'].unique()
                for service in df['dest_type'].unique():
                    df_sub = df[(df['dest_type']==service)&(df['mode']==mode)]
                    # print(group)
                    # print(service)
                    # create the hist
                    # import code
                    # code.interact(local=locals())
                    density, division = np.histogram(df_sub['duration'], bins = bins, weights=df_sub[group], density=True)
                    unity_density = density / density.sum()
                    unity_density = np.append(0, unity_density)
                    division = np.append(0, division)
                    df_new = pd.DataFrame({'pop_perc':unity_density, 'duration':division[:-1]})
                    df_new['region'] = 'All'
                    df_new['service']=service
                    df_new['pop_perc'] = df_new['pop_perc']*100
                    df_new['pop_perc_cum'] = df_new['pop_perc'].cumsum()
                    df_new['group'] = group
                    df_new['mode'] = mode
                    hists.append(df_new)
                    for region in regions:
                        df_sub = df[(df['dest_type']==service)&(df['UR2020_V_1']==region)&(df['mode']==mode)]
                        # create the hist
                        density, division = np.histogram(df_sub['duration'], bins = bins, weights=df_sub[group], density=True)
                        unity_density = density / density.sum()
                        unity_density = np.append(0, unity_density)
                        division = np.append(0, division)
                        df_new = pd.DataFrame({'pop_perc':unity_density, 'duration':division[:-1]})
                        df_new['service']=service
                        df_new['pop_perc'] = df_new['pop_perc']*100
                        df_new['pop_perc_cum'] = df_new['pop_perc'].cumsum()
                        df_new['region'] = region
                        df_new['group'] = group
                        df_new['mode'] = mode
                        hists.append(df_new)

        # concat
        df_hists = pd.concat(hists)
        df_hists = df_hists[df_hists['duration'] <= 100]
        # export
        fn = './data/results/access_histogram.csv'
        df_hists.to_csv(fn)
        print('Written: {}'.format(fn))

                # df_new['pop_perc'] = df_new['pop_perc']*100
                # df_new['pop_perc_cum'] = df_new['pop_perc'].cumsum()
                # df_new['region'] = region
                # df_new['group'] = group
                # hists.append(df_new)

                # # concat
                # df_hists = pd.concat(hists)
                # # export
                # df_hists.to_csv('./data/results/access_histogram.csv')


    # Population weighted average
    sql = "SELECT geoid, dest_type, duration, population, geometry, mode  FROM updated_nearest_{} WHERE population > 0 AND duration IS NOT NULL".format(config['SQL']['table_name'])
    df = gpd.read_postgis(sql, con=db['engine'], geom_col='geometry')
    df['centroids'] = df.centroid
    urbans = gpd.read_file('./data/raw/new_urban_areas.shp')
    # zones = gpd.read_file('/homedirs/projects/x-minute-city/data/raw/statsnzterritorial-authority-2021-generalised-SHP/territorial-authority-2021-generalised.shp')
    urbans = urbans[['UR2020_V_1','geometry']]
    urbans = urbans.to_crs(df.crs)
    df.set_geometry('centroids',inplace=True)
    df = df.to_crs(urbans.crs)
    df = gpd.sjoin(df, urbans, how='inner', op='within')
    df['duration_weighted'] = df['duration'] * df.population
    df_group = df.groupby(['UR2020_V_1','dest_type','mode']).sum()
    df_group['duration_weighted'] = df_group['duration_weighted']/df_group['population']
    df_group = df_group[['duration_weighted']]
    df_group.reset_index(inplace=True)
    # import code
    # code.interact(local=locals())
    # calculate the maximum
    df_max = df_group.groupby(['UR2020_V_1','mode']).max()
    # df_max = df_group.loc[df_group.groupby(['UR2020_V_1','mode'],as_index=False).idxmax()]
    df_max = df_group.copy()
    df_max = df_max[df_max.dest_type != 'all']
    df_max = df_max.sort_values('duration_weighted',ascending=False).groupby(['UR2020_V_1','mode'], as_index=False).first()

    least_accessible = df_max.copy()
    least_accessible = least_accessible[['UR2020_V_1','mode','dest_type']]
    fn = './data/results/least_accessible.csv'
    least_accessible.to_csv(fn)
    print('Written: {}'.format(fn))

    df_max['dest_type'] = 'all'
    df_group = df_group[df_group.dest_type != 'all']
    df_group = df_group.append(df_max)
    fn = './data/results/statistics.csv'
    df_group.to_csv(fn)
    print('Written: {}'.format(fn))


    # EDEs #edes
    if config['data_export']['edes']:
        sql = "SELECT geoid, dest_type, duration, population, geometry, mode  FROM updated_nearest_{} WHERE population > 0 AND duration IS NOT NULL".format(config['SQL']['table_name'])
        df = gpd.read_postgis(sql, con=db['engine'], geom_col='geometry')
        df['centroids'] = df.centroid
        urbans = gpd.read_file('./data/raw/new_urban_areas.shp')
        # zones = gpd.read_file('/homedirs/projects/x-minute-city/data/raw/statsnzterritorial-authority-2021-generalised-SHP/territorial-authority-2021-generalised.shp')
        urbans = urbans[['UR2020_V_1','geometry']]
        urbans = urbans.to_crs(df.crs)
        df.set_geometry('centroids',inplace=True)
        df = df.to_crs(urbans.crs)
        df = gpd.sjoin(df, urbans, how='inner', op='within')


        modes = df['mode'].unique()
        regions = df['UR2020_V_1'].unique()
        dests = df['dest_type'].unique()

        results = []
        for mode in modes:
            for service in dests:
                for region in regions:
                    df_sub = df[(df['dest_type']==service)&(df['mode']==mode)&(df['UR2020_V_1']==region)]
                    ede = ineq.kolmpollak.ede(a = df_sub.duration.values, epsilon = -0.5, weights = df_sub.population.values)
                    result = [mode, service, region,ede]
                    results.append(result)

        results = pd.DataFrame(results, columns=['mode','dest_type','UR2020_V_1','duration_weighted'])

        df_max = results.copy()
        df_max = df_max[df_max.dest_type != 'all']
        df_max = df_max.sort_values('duration_weighted',ascending=False).groupby(['UR2020_V_1','mode'], as_index=False).first()

        least_accessible = df_max.copy()
        least_accessible = least_accessible[['UR2020_V_1','mode','dest_type']]
        fn = './data/results/least_accessible.csv'
        least_accessible.to_csv(fn)
        print('Written: {}'.format(fn))

        df_max['dest_type'] = 'all'
        results = results[results.dest_type != 'all']
        results = results.append(df_max)
        fn = './data/results/statistics.csv'
        results.to_csv(fn)
        print('Written: {}'.format(fn))

    # percent live within #percent_stats
    if config['data_export']['percent_stats']:
        # import csv for histagram
        df = pd.read_csv(r"./data/results/access_histogram.csv")
        df = df[df['group']=='population']
        regions = list(df['region'].unique())
        amenities = list(df['service'].unique())
        modes = list(df['mode'].unique())

        res_regions = []
        res_amenities = []
        res_modes = []
        res_percent = []

        for region in regions[1:]:
            df_region = df[df['region']==region]
            for amenity in amenities:
                df_amenity = df_region[df_region['service']==amenity]
                for mode in modes:
                    df_mode = df_amenity[df_amenity['mode']==mode]
                    result = list(df_mode[df_mode['duration']==10].pop_perc_cum)[0]
                    result = np.round(result,0)
                    res_regions.append(region)
                    res_amenities.append(amenity)
                    res_modes.append(mode)
                    res_percent.append(result)


        df_results = pd.DataFrame()

        df_results['region'] = res_regions
        df_results['service'] = res_amenities
        df_results['mode'] = res_modes
        df_results['percent'] = res_percent

        df_results.to_csv(r'./data/results/percent_statistics.csv')

    ###
    # Make CSV with rankings for each city, suburb, amenity, and mode
    ###
    # RAY
    num_cpus = np.int(psutil.cpu_count(logical=False) * 1)
    print('Using {} cores.'.format(num_cpus))
    ray.init(num_cpus=num_cpus)
    if config['data_export']['rankings']:
        # logger
        print('Making city and suburb rankings')
        # get duration data at block level
        sql = "SELECT geoid, dest_type, duration, population, geometry, mode  FROM updated_nearest_{} WHERE population > 0 AND duration IS NOT NULL".format(config['SQL']['table_name'])
        df = gpd.read_postgis(sql, con=db['engine'], geom_col='geometry')
        # set centroids
        df['centroids'] = df.centroid
        # import urban areas to clip on
        urbans = gpd.read_file('./data/raw/new_urban_areas.shp')
        # zones = gpd.read_file('/homedirs/projects/x-minute-city/data/raw/statsnzterritorial-authority-2021-generalised-SHP/territorial-authority-2021-generalised.shp')
        urbans = urbans[['UR2020_V_1','geometry']]
        urbans = urbans.to_crs(df.crs)
        df.set_geometry('centroids',inplace=True)
        df = df.to_crs(urbans.crs)
        df = gpd.sjoin(df, urbans, how='inner', op='within')
        df = df.drop(columns=['index_right'])

        # import suburban areas to clip on
        suburbans = gpd.read_file('./data/raw/statistical-area-2-2020-generalised.shp')
        suburbans = suburbans[['SA22020__1','geometry']]
        suburbans = suburbans.to_crs(df.crs)
        df = gpd.sjoin(df, suburbans, how='inner', op='within')

        # init lists to loop through
        modes = list(df['mode'].unique())
        regions = list(df['UR2020_V_1'].unique())
        suburbs = list(df['SA22020__1'].unique()) + ['all']
        dests = list(df['dest_type'].unique())


        # init results list
        results = pd.DataFrame(columns = ['mode', 'dest_type', 'region', 'duration', 'suburb'])
        # loop through modes
        for mode in tqdm(modes):
            for region in tqdm(regions):
                suburbs = list(df[df['UR2020_V_1']==region]['SA22020__1'].unique()) + ['all']
                region_dfs = [loop_suburb.remote(df, mode, region, suburb, dests) for suburb in tqdm(suburbs)]
                region_dfs = ray.get(region_dfs)
                region_dfs = pd.concat(region_dfs)
                results = results.append(region_dfs)
        results.to_csv(r'data/results/rankings.csv')


@ray.remote
def loop_suburb(df, mode, region, suburb, dests):
    region_df = pd.DataFrame(columns = ['mode', 'dest_type', 'region', 'duration', 'suburb'])
    for service in dests:
        if suburb == 'all':
            df_sub = df[(df['dest_type']==service)&(df['mode']==mode)&(df['UR2020_V_1']==region)]
        else:
            df_sub = df[(df['dest_type']==service)&(df['mode']==mode)&(df['UR2020_V_1']==region)&(df['SA22020__1']==suburb)]
        mean = np.average(df_sub['duration'], weights=df_sub['population'])
        result = {'mode':[mode], 'dest_type':[service], 'region':[region], 'duration':[mean], 'suburb':[suburb]}
        region_df = region_df.append(pd.DataFrame(data=result))
    return(region_df)
