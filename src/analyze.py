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
db_host = 'encivmu-tml62'
db_name = 'x-minute-city'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)

sql = "SELECT geoid, dest_type, duration, population, geometry FROM nearest_durations WHERE population > 0 AND duration IS NOT NULL AND mode='walking'"
df = gpd.read_postgis(sql, con=engine, geom_col='geometry')
df = df.replace(['oral_health','greenspace','emergency_medical_service'],['dentist','park','doctor'])

# calculate the x-minute statistic
df = df[df.dest_type.isin(dests)]
df_max = df.groupby(['geoid']).max()
df_max['dest_type']='all'
df_max.reset_index(inplace=True)
df2 = df_max.merge(df[['geoid','geometry']], how='left', on='geoid')
df2.drop_duplicates(inplace=True)
df2.to_postgis('x_minute', engine, if_exists='replace', dtype={
                     'geometry': Geometry('MULTIPOLYGON', srid=4326)}
                     )
print('x-minute NZ written to PSQL')

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

###
# USA
###
db_name = 'cities_500'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)
sql = """select nearest_block20.*, blocks20.id_city as id_city, blocks20."U7B001" as population, cities.name as city, cities.state, blocks20.geometry from nearest_block20 inner join blocks20 using (geoid) inner join cities using (id_city);"""
df2 = gpd.read_postgis(sql, con=engine, geom_col='geometry')
df2 = df2.replace(['oral_health','greenspace','emergency_medical_service'],['dentist','park','doctor'])
df2['city'] = df2['city'] + ', ' + df2['state']
df2['duration'] = df2['distance']/800*10

# calculate the x-minute statistic
df2 = df2[df2.dest_type.isin(dests)]
df_max = df2.groupby(['geoid']).max()
df_max['dest_type']='all'
df_max.reset_index(inplace=True)
df3 = df_max.merge(df2[['geoid','geometry']], how='left', on='geoid')
df3.drop_duplicates(inplace=True)
df3.to_postgis('x_minute', engine, if_exists='replace', dtype={
                     'geometry': Geometry('MULTIPOLYGON', srid=4326)}
                     )
print('x-minute USA written to PSQL')

df2 = df2[['geoid','city','dest_type','duration','population']]
df = pd.concat([df,df2])
engine.dispose()

# import code
# code.interact(local=locals())


#######################################################
# Calculate the different city metrics
# For Mean -> Max

cities = df['city'].unique()
dests = df['dest_type'].unique()
# dests = np.setdiff1d(dests, np.array(['all','bank','fire_station']))


results = []
for city in tqdm(cities):
    for service in dests:
        df_sub = df[(df['dest_type']==service)&(df['city']==city)].copy()
        if len(df_sub) > 0:
            # Population weighted mean
            pwm = np.average(a = df_sub.duration.values, weights = df_sub.population.values)
            # Inequality penalised mean
            ede = ineq.kolmpollak.ede(a = df_sub.duration.values, epsilon = -1, weights = df_sub.population.values)
            # Max
            mx = df_sub.duration.values.max()
            # median
            df_sub.sort_values('duration', inplace=True)
            cumsum = df_sub.population.cumsum()
            cutoff = df_sub.population.sum() * 0.5
            p50 = df_sub.duration[cumsum >= cutoff].iloc[0]
            # 90th percentile
            cutoff = df_sub.population.sum() * 0.9
            p90 = df_sub.duration[cumsum >= cutoff].iloc[0]
            # average of top 10%
            top10 = np.average(a = df_sub.duration.values[cumsum >= cutoff], weights = df_sub.population.values[cumsum >= cutoff])
            # 10 min
            # total_population = df_sub.population.sum()
            # greaterthan = df_sub.duration >= 10
            # if greaterthan.sum() > 0:
            #     min10 = cumsum[greaterthan].iloc[0]/total_population
            # else:
            #     min10 = 1
            # # 15 min
            # greaterthan = df_sub.duration >= 15
            # if greaterthan.sum() > 0:
            #     min15 = cumsum[greaterthan].iloc[0]/total_population
            # else:
            #     min15 = 1
            # # 20 min
            # greaterthan = df_sub.duration >= 20
            # if greaterthan.sum() > 0:
            #     min20 = cumsum[greaterthan].iloc[0]/total_population
            # else:
            #     min20 = 1
            result = [service, city, pwm, ede, mx, p50, p90, top10]#, min10, min15, min20]
            results.append(result)
        else:
            print(city, service)
    

results = pd.DataFrame(results, columns=['destination','city','mean','ede','max','median','90th','top10_mean'])

results_max = []
for city in cities:
    df_sub = results[results.city==city]
    result = ['all', city] + list(df_sub[['mean','ede','max','median','90th','top10_mean']].max())
    results_max.append(result)

results_max = pd.DataFrame(results_max, columns=['destination','city','mean','ede','max','median','90th','top10_mean'])
results = results.append(results_max)
fn = './data/analysis/measures_amenities.csv'
results.to_csv(fn)
print('Written: {}'.format(fn))

# plot
comb = list(combinations(['mean','ede','max','median','90th'], 2))
for x, y in comb:
    results_max.plot.scatter(x=x,y=y)
    plt.savefig('./data/analysis/aggmax_{}_{}.pdf'.format(x,y),
            dpi=500, format='pdf', transparent=True, bbox_inches='tight', facecolor='w')
    plt.close()


#######################################################
# Calculate the different city metrics
# For Max -> Aggregate

cities = df['city'].unique()
dests = df['dest_type'].unique()
dests = ['all']#np.setdiff1d(dests, np.array(['all','bank','fire_station']))
service = 'all'
# df['geoid'] = df['geoid'].astype('category')
dest_subset = ['park','pharmacy','primary_school','supermarket'] #'doctor',

results = []
for city in tqdm(cities):
    # for service in dests:
        df_sub = df[(df['city']==city)].copy()
        df_sub = df_sub[df_sub.dest_type.isin(dest_subset)]
        df_sub = df_sub.groupby(['geoid']).max()
        df_sub['dest_type']='all'
        df_sub.reset_index(inplace=True)
        if len(df_sub) > 0:
            # Population weighted mean
            pwm = np.average(a = df_sub.duration.values, weights = df_sub.population.values)
            # Inequality penalised mean
            ede = ineq.kolmpollak.ede(a = df_sub.duration.values, epsilon = -1, weights = df_sub.population.values)
            # Max
            mx = df_sub.duration.values.max()
            # median
            df_sub.sort_values('duration', inplace=True)
            cumsum = df_sub.population.cumsum()
            cutoff = df_sub.population.sum() * 0.5
            p50 = df_sub.duration[cumsum >= cutoff].iloc[0]
            # 90th percentile
            cutoff = df_sub.population.sum() * 0.9
            p90 = df_sub.duration[cumsum >= cutoff].iloc[0]
            # average of top 10%
            top10 = np.average(a = df_sub.duration.values[cumsum >= cutoff], weights = df_sub.population.values[cumsum >= cutoff])
            # 10 min
            total_population = df_sub.population.sum()
            greaterthan = df_sub.duration >= 10
            if greaterthan.sum() > 0:
                min10 = cumsum[greaterthan].iloc[0]/total_population
            else:
                min10 = 1
            # 15 min
            greaterthan = df_sub.duration >= 15
            if greaterthan.sum() > 0:
                min15 = cumsum[greaterthan].iloc[0]/total_population
            else:
                min15 = 1
            # 20 min
            greaterthan = df_sub.duration >= 20
            if greaterthan.sum() > 0:
                min20 = cumsum[greaterthan].iloc[0]/total_population
            else:
                min20 = 1
            result = [service, city, pwm, ede, mx, p50, p90, top10, min10, min15, min20]
            results.append(result)
        else:
            print(city, service)
    

results = pd.DataFrame(results, columns=['destination','city','mean','ede','max','median','90th','top10_mean', 'min10', 'min15', 'min20'])
results['country'] = 'USA'
results.loc[:len(urbans),'country'] = 'NZ'
results['color_country'] = 'blue'
results.loc[:len(urbans),'color_country'] = 'red'

fn = './data/analysis/measures_xmin.csv'
results.to_csv(fn)
print('Written: {}'.format(fn))

# results = results[results.city!='Ashburton']
# plot
comb = list(combinations(['mean','ede','max','median','90th', 'min10', 'min15', 'min20'], 2))
for x, y in comb:
    results.plot.scatter(x=x,y=y,c='color_country')
    plt.savefig('./data/analysis/maxagg_{}_{}.pdf'.format(x,y),
            dpi=500, format='pdf', transparent=True, bbox_inches='tight', facecolor='w')
    plt.close()


###
# Evaluate the mean of maxs vs max of means

momean = pd.read_csv('./data/analysis/measures_amenities.csv')
momax = pd.read_csv('./data/analysis/measures_xmin.csv')

dest_subset = ['park','pharmacy','primary_school','supermarket'] #'doctor',
momean = momean[momean.destination.isin(dest_subset)]
results_max = []
for city in momean.city.unique():
    df_sub = momean[momean.city==city]
    result = ['all', city] + list(df_sub[['mean','ede','max','median','90th','top10_mean']].max())
    results_max.append(result)

results_max = pd.DataFrame(results_max, columns=['destination','city','mean','ede','max','median','90th','top10_mean'])


momean = results_max[['city','mean']]
momean.rename(columns={'mean':'momean'}, inplace=True)
momax.rename(columns={'mean':'momax'}, inplace=True)
momax = momax[['city','momax','country','color_country']]
momean['city']=momean['city'].astype(str)
momax['city']=momax['city'].astype(str)
df = momax.merge(momean, on='city')
# df = df[df.city!='Ashburton']
df.plot.scatter(x='momax', y='momean', c='color_country')
plt.xlim([10,60])
plt.ylim([10,60])
plt.plot([10,60],[10,60])
plt.savefig('./data/analysis/momax_v_momean.pdf',
        dpi=500, format='pdf', transparent=True, bbox_inches='tight', facecolor='w')
plt.close()

fn = './data/analysis/momax_momean.csv'
df.to_csv(fn)
print('Written: {}'.format(fn))


####
# Get distribution of Jacksonville, NC #381
df_sub = df[df.city=='Salt Lake City, UT']
dests = ['doctor','supermarket','park','pharmacy','primary_school','all']
df_sub = df_sub[df_sub.dest_type.isin(dests)]

dfd = df_sub.groupby(['geoid']).max()
dfd['dest_type']='all'
dfd.reset_index(inplace=True)

df_sub = pd.concat([df_sub, dfd])

import seaborn as sns
ax = sns.kdeplot(data=df_sub, x="duration", hue="dest_type")
# plot a histogram with each amenity colored differently

for d in dests:
    dfd = df_sub[df_sub.dest_type==d]
    # color = next(ax._get_lines.prop_cycler)['color']
    # dfd['duration'].plot(kind='kde', weights=dfd.population, label=d, alpha=0.5, color=color)
    d_mean = np.average(dfd.duration, weights=dfd.population)
    ax.axvline(d_mean, label=d)



# plt.legend()
plt.savefig('./data/analysis/kde_saltlake.pdf',
        dpi=500, format='pdf', transparent=True, bbox_inches='tight', facecolor='w')
plt.close()
plt.clf()


####
# Get city characteristics
db_name = 'x-minute-city'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)
sql = 'SELECT "SA12018_V1" as geoid, "C18_CURPop" as population, geometry FROM blocks'
df = gpd.read_postgis(sql, con=engine, geom_col='geometry')
engine.dispose()
df['centroids'] = df.centroid
urbans = gpd.read_file('/homedirs/projects/access_nz/data/raw/new_urban_areas.shp')
urbans = urbans[['UR2020_V_1','geometry']]
urbans = urbans.to_crs(df.crs)
df.set_geometry('centroids',inplace=True)
df = df.to_crs(urbans.crs)
df = gpd.sjoin(df, urbans, how='inner', op='within')
df.rename(columns={'UR2020_V_1':'city'}, inplace=True)
df = df[['geoid','city','population','geometry']]
df = df.set_geometry('geometry')
df = df.to_crs(3395)
df['area'] = df.geometry.area
df['country'] = 'NZ'
df['color'] = 'red'

###
# USA
###
db_name = 'cities_500'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)
sql = """select blocks20.geoid, blocks20."U7B001" as population, blocks20.geometry, cities.name, cities.state from blocks20 inner join cities using (id_city);"""
df2 = gpd.read_postgis(sql, con=engine, geom_col='geometry')
engine.dispose()
df2['city'] = df2['name'] + ', ' + df2['state']
# df2['duration'] = df2['distance']/800*10
df2 = df2[['geoid','city','population','geometry']]
df2 = df2.to_crs(3395)
df2['area'] = df2.geometry.area
df2['country'] = 'USA'
df2['color'] = 'blue'

df = pd.concat([df,df2])

df.area = df.area * 1e-6
df_color = df[['city','color']]
df_color.drop_duplicates(inplace=True)
cities = df.groupby('city').sum()
cities['popdens'] = cities.population/cities.area
cities = pd.merge(cities, df_color, left_index=True, right_on='city',how='left')
cities.plot.scatter(x='population', y='popdens', c=cities.color)

plt.savefig('./data/analysis/city_characteristics.jpg',
        dpi=500, format='jpg', transparent=True, bbox_inches='tight', facecolor='w')
plt.close()
plt.clf()

#######################################################
# Get city populations
fn = './data/analysis/city_characteristics.csv'
cities.to_csv(fn)
print('Written: {}'.format(fn))