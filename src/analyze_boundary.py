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
from pathlib import Path
import boundary_query
import yaml

dests = ['park','pharmacy','primary_school','supermarket'] #'doctor',
# import data
# need id_orig, city, destination, duration, population
passw = open('/media/CivilSystems/admin/pass.txt', 'r').read().strip('\n')
port = '5002'
db_host = 'encivmu-tml62'

# id_city = 202 
# id_city = 477 # Portland - 20min
# id_city = 179
# id_city = 26 # NOLA
# id_city=359 # Phoenix  -20 min
# id_city = 324 # San Diego
# id_city = 304 # Boulder
# id_city = 460 # San Jose
id_city = 30 # Jacksonville, FL
id_city = 105 # Phily

# get the origins and destinations

# Get boundaries
db_name = 'cities_500'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)
crs = 4326
# import the block groups
sql = 'SELECT id_city, geometry FROM block_groups where id_city = {}'.format(id_city)
df_bg = gpd.read_postgis(sql, con=engine, geom_col='geometry')
df_bg = df_bg.to_crs(crs)
boundary_urban = df_bg.dissolve()

# # import the cities
sql = 'SELECT id_city, geometry, state FROM cities where id_city = {}'.format(id_city)
df_cities = gpd.read_postgis(sql, con=engine, geom_col='geometry')
df_cities = df_cities.to_crs(crs)
boundary_city = df_cities.dissolve()

# save the boundaries
boundary_urban.to_file('/home/tml/CivilSystems/projects/x_minute_measuring/results/boundary/boundary_urban_{}.shp'.format(id_city))
boundary_city.to_file('/home/tml/CivilSystems/projects/x_minute_measuring/results/boundary/boundary_city{}.shp'.format(id_city))

# get the origins
state = df_cities.state.iloc[0]
shp = "/media/CivilSystems/data/usa/2020_census/nhgis0086_shape/{}_block_2020.shp".format(state)
df_block = gpd.read_file(shp, mask=boundary_city)
df_block = df_block[['GEOID20', 'geometry','GISJOIN']]
df_block = df_block.to_crs(crs)
df_block = df_block.rename(columns={'GEOID20': 'geoid'})

# determine the centroid of the blocks
df_block['centroid'] = df_block.representative_point()
df_block.set_geometry('centroid', inplace=True)

# assign whether within city and urban boundaries
boundary_urban['urban'] = True
df_block = gpd.sjoin(df_block, boundary_urban, how='left', predicate='within')
df_block['urban'][df_block.urban.isnull()] = False

# add population to origins
df_info = pd.read_csv(
    '/media/CivilSystems/data/usa/2020_census/nhgis0086_csv/nhgis0086_ds248_2020_block.csv', encoding="ISO-8859-1",
    usecols=['GISJOIN', 'U7B001'],
    dtype={x: 'str' for x in ['GISJOIN']})
df_block = df_block.merge(df_info, on='GISJOIN')

# write to sql
db_name = 'x-minute-city'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)

df_write = df_block[['geoid','centroid','U7B001','urban']]
df_write.to_postgis('origin_{}'.format(id_city), engine, if_exists='replace', dtype={
                     'centroid': Geometry('POINT', srid=crs)}
                     )
engine.dispose()                     

# destinations
db_name = 'cities_500'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)
crs = 4326
sql = 'SELECT * FROM destinations where id_city = {}'.format(id_city)
destinations = gpd.read_postgis(sql, con=engine, geom_col='geometry')
engine.dispose()    
destinations = destinations.to_crs(crs)
destinations = destinations[destinations.dest_type.isin(dests)]
db_name = 'x-minute-city'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)

destinations.to_postgis('destination_{}'.format(id_city), engine, if_exists='replace', dtype={
                     'geometry': Geometry('POINT', srid=crs)}
                     )
engine.dispose()                    


###
# need to query here
with open('./src/boundary_main.yaml') as file:
    config = yaml.safe_load(file)

boundary_query.main(config, id_city)

###
# calculate statistics
###
db_name = 'x-minute-city'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)

sql = "SELECT * from distance_boundary LEFT JOIN origin_{0} ON origin_{0}.geoid=distance_boundary.id_orig".format(id_city)
df = pd.read_sql(sql, con=engine)
df = df.replace(['oral_health','greenspace','emergency_medical_service'],['dentist','park','doctor'])
engine.dispose()

# calculate the nearest
df = df[['id_orig','duration','dest_type','U7B001','urban']]
df.duration = df.duration/60
nearest = df.groupby(['id_orig','dest_type']).min()
nearest.reset_index(inplace=True)

###
# City
###
nearest = nearest[nearest.dest_type.isin(dests)]
# calculate the x-minute statistic
df_max = nearest.groupby(['id_orig']).max()

# calculate the mean
mean_minute_city = np.average(df_max.duration, weights=df_max.U7B001)

# calculate the percentage of people within 10 minutes
df_max.sort_values(by='duration',inplace=True)
df_max['cumulative_pop'] = df_max.U7B001.cumsum()/df_max.U7B001.sum()
min10_city = df_max[df_max.duration>10].iloc[0]['cumulative_pop']

p90_city = df_max[df_max.cumulative_pop>0.9].iloc[0]['duration']
min20_city = df_max[df_max.duration>20].iloc[0]['cumulative_pop']


###
# Urban
###
# calculate the x-minute statistic
nearest = nearest[nearest.urban]
df_max = nearest.groupby(['id_orig']).max()

# calculate the mean
mean_minute = np.average(df_max.duration, weights=df_max.U7B001)

# calculate the percentage of people within 10 minutes
df_max.sort_values(by='duration',inplace=True)
df_max['cumulative_pop'] = df_max.U7B001.cumsum()/df_max.U7B001.sum()
min10 = df_max[df_max.duration>10].iloc[0]['cumulative_pop']
min20 = df_max[df_max.duration>20].iloc[0]['cumulative_pop']

p90 = df_max[df_max.cumulative_pop>0.9].iloc[0]['duration']


print('Xminute')
print(mean_minute_city)
print(mean_minute)

print('10 min %')
print(min10_city*100)
print(min10*100)

print('20 min %')
print(min20_city*100)
print(min20*100)

print('90%')
print(p90_city)
print(p90)

import code
code.interact(local=locals())