# import functions
from sqlalchemy import create_engine
import geopandas as gpd
import inequalipy as ineq
import pandas as pd
import numpy as np
from tqdm import tqdm
from itertools import combinations
import matplotlib.pyplot as plt




# import data
# need id_orig, city, destination, duration, population
###
# NZ
###
passw = open('pass.txt', 'r').read().strip('\n')
port = '5002'
db_host = '132.181.102.2'
db_name = 'nz_access_equity'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)

sql = "SELECT geoid, dest_type, duration, population, geometry FROM nearest_block WHERE population > 0 AND duration IS NOT NULL AND mode='walking'"
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
df = df[['geoid','city','dest_type','duration','population']]

###
# USA
###
db_name = 'cities_500'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)
sql = """select nearest_block20.*, blocks20.id_city as city, blocks20."U7B001" as population from nearest_block20 inner join blocks20 using (geoid);"""
df2 = pd.read_sql(sql, con=engine)
engine.dispose()
df2['duration'] = df2['distance']/800*10
df2 = df2[['geoid','city','dest_type','duration','population']]

df = pd.concat([df,df2])
df = df.replace(['oral_health','greenspace','emergency_medical_service'],['dentist','park','doctor'])

#######################################################
# Calculate the different city metrics
# For Mean -> Max

cities = df['city'].unique()
dests = df['dest_type'].unique()
dests = np.setdiff1d(dests, np.array(['all','bank','fire_station']))

results = []
for city in tqdm(cities):
    for service in dests:
        df_sub = df[(df['dest_type']==service)&(df['city']==city)].copy()
        if len(df_sub) > 0:
            # Population weighted mean
            pwm = np.average(a = df_sub.duration.values, weights = df_sub.population.values)
            # Inequality penalised mean
            ede = ineq.kolmpollak.ede(a = df_sub.duration.values, epsilon = -0.5, weights = df_sub.population.values)
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
fn = './data/analysis/measures.csv'
results.to_csv(fn)
print('Written: {}'.format(fn))

# plot
comb = list(combinations(['mean','ede','max','median','90th'], 2))
for x, y in comb:
    results_max.plot.scatter(x=x,y=y)
    plt.savefig('./data/analysis/aggmax_{}_{}.jpg'.format(x,y),
            dpi=500, format='jpg', transparent=True, bbox_inches='tight', facecolor='w')
    plt.close()


#######################################################
# Calculate the different city metrics
# For Max -> Aggregate

cities = df['city'].unique()
dests = df['dest_type'].unique()
dests = ['all']#np.setdiff1d(dests, np.array(['all','bank','fire_station']))
service = 'all'
# df['geoid'] = df['geoid'].astype('category')

results = []
for city in tqdm(cities):
    # for service in dests:
        df_sub = df[(df['city']==city)].copy()
        df_sub = df_sub[df_sub.dest_type.isin(['doctor','park','pharmacy','primary_school','supermarket'])]
        df_sub = df_sub.groupby(['geoid']).max()
        df_sub['dest_type']='all'
        df_sub.reset_index(inplace=True)
        if len(df_sub) > 0:
            # Population weighted mean
            pwm = np.average(a = df_sub.duration.values, weights = df_sub.population.values)
            # Inequality penalised mean
            ede = ineq.kolmpollak.ede(a = df_sub.duration.values, epsilon = -0.5, weights = df_sub.population.values)
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

fn = './data/analysis/measures_maxagg.csv'
results.to_csv(fn)
print('Written: {}'.format(fn))

# plot
comb = list(combinations(['mean','ede','max','median','90th', 'min10', 'min15', 'min20'], 2))
for x, y in comb:
    results.plot.scatter(x=x,y=y,c='color_country')
    plt.savefig('./data/analysis/maxagg_{}_{}.jpg'.format(x,y),
            dpi=500, format='jpg', transparent=True, bbox_inches='tight', facecolor='w')
    plt.close()
