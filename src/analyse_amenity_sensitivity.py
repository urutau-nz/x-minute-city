# import functions
from sqlalchemy import create_engine
import geopandas as gpd
import inequalipy as ineq
import pandas as pd
import numpy as np
from tqdm import tqdm
from itertools import combinations
import matplotlib.pyplot as plt
from geoalchemy2 import Geometry


dests = ['park','pharmacy','primary_school','supermarket'] #'doctor',
# import data
# need id_orig, city, destination, duration, population
###
# NZ
###
passw = open('pass.txt', 'r').read().strip('\n')
port = '5002'
db_host = '132.181.102.2'
db_name = 'x-minute-city'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)

sql = "SELECT geoid, dest_type, duration, population, geometry FROM nearest_durations WHERE population > 0 AND duration IS NOT NULL AND mode='walking'"
df = gpd.read_postgis(sql, con=engine, geom_col='geometry')
df = df.replace(['oral_health','greenspace','emergency_medical_service'],['dentist','park','doctor'])

df['centroids'] = df.centroid
urbans = gpd.read_file('/homedirs/projects/access_nz/data/raw/new_urban_areas.shp')
urbans = urbans[['UR2020_V_1','geometry']]
urbans = urbans.to_crs(df.crs)
df.set_geometry('centroids',inplace=True)
df = df.to_crs(urbans.crs)
df = gpd.sjoin(df, urbans, how='inner', op='within')
df.rename(columns={'UR2020_V_1':'city'}, inplace=True)
df = df[['geoid','city','dest_type','duration','population']]
engine.dispose()

print('read NZ')

###
# USA
###
db_name = 'cities_500'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)
sql = """select nearest_block20.*, blocks20.id_city as id_city, blocks20."U7B001" as population, cities.name as city, cities.state, blocks20.geometry from nearest_block20 inner join blocks20 using (geoid) inner join cities using (id_city);"""
df2 = pd.read_sql(sql, con=engine)
df2 = df2.replace(['oral_health','greenspace','emergency_medical_service'],['dentist','park','doctor'])
df2['city'] = df2['city'] + ', ' + df2['state']
df2['duration'] = df2['distance']/800*10

df2 = df2[['geoid','city','dest_type','duration','population']]
df = pd.concat([df,df2])
engine.dispose()
print('read USA')

import code
code.interact(local=locals())


# loop through combination of dest types
# dests = df['dest_type'].unique()
dests = ['supermarket','park','bank','dentist','doctor','pharmacy','primary_school']

xmin = []
for L in range(0, len(dests)+1):
    for subset in combinations(dests, L):
        df_sub = df[df.dest_type.isin(subset)]
        df_sub = df_sub[['geoid','city','duration']].groupby('geoid').max()
        df_sub.reset_index(inplace=True)
        df_sub = df_sub[['city','duration']].groupby('city').mean()
        df_sub['dest_type'] = ' '.join(subset)
        xmin.append(df_sub)
        # print(subset)

xmin = pd.concat(xmin)
xmin.to_csv('./data/analysis/leave_out_amenity.csv')
