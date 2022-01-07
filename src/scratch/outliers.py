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
import matplotlib as mpl
mpl.rcParams['pdf.fonttype'] = 42


city = 213


dests = ['park','pharmacy','primary_school','supermarket'] #'doctor',

passw = open('/media/CivilSystems/admin/pass.txt', 'r').read().strip('\n')
port = '5002'
db_host = 'encivmu-tml62'
db_name = 'cities_500'
engine = create_engine('postgresql+psycopg2://postgres:' + passw + '@' + db_host + '/' + db_name + '?port=' + port)
sql = """select nearest_block20_duration.*, blocks20.id_city as id_city, blocks20."U7B001" as population, cities.name as city, cities.state, blocks20.geometry from nearest_block20_duration inner join blocks20 using (geoid) inner join cities using (id_city) where blocks20.id_city={};""".format(city)
# df2 = gpd.read_postgis(sql, con=engine, geom_col='geometry')
df2 = pd.read_sql(sql, con=engine)
df2 = df2.replace(['oral_health','greenspace','emergency_medical_service'],['dentist','park','doctor'])
df2['city'] = df2['city'] + ', ' + df2['state']
df2['duration'] = df2['duration']/60

# calculate the x-minute statistic
df2 = df2[df2.dest_type.isin(dests)]
df2 = df2[~df2.duration.isnull()]
df_max = df2.groupby(['geoid']).max()
df_max['dest_type']='all'
df_max.reset_index(inplace=True)
df3 = df_max.merge(df2[['geoid','geometry']], how='left', on='geoid')
df3.drop_duplicates(inplace=True)


engine.dispose()


plt.plot(df3.population, df3.duration,'x',color='black')
plt.ylabel('duration')
plt.xlabel('pop')

# could add iqr as horizontal line


# could add stdev as horizontal line
dist_mean = df3.duration.mean()
dist_std1 = dist_mean + df3.duration.std() * 1
dist_std2 = dist_mean + df3.duration.std() * 2
dist_std3 = dist_mean + df3.duration.std() * 3
dist_std4 = dist_mean + df3.duration.std() * 4
dist_std5 = dist_mean + df3.duration.std() * 5
dist_std10 = dist_mean + df3.duration.std() * 10
plt.axhline(y=dist_mean, linestyle='--', color='red', linewidth=1)
plt.axhline(y=dist_std1, linestyle='--', color='red', linewidth=1)
plt.axhline(y=dist_std2, linestyle='--', color='red', linewidth=1)
plt.axhline(y=dist_std3, linestyle='--', color='red', linewidth=1)
plt.axhline(y=dist_std4, linestyle='--', color='red', linewidth=1)
plt.axhline(y=dist_std5, linestyle='--', color='red', linewidth=1)
plt.axhline(y=dist_std10, linestyle='--', color='red', linewidth=1)

plt.savefig('{}_pop_v_dist.jpg'.format(city),
            dpi=500, format='jpg', transparent=True, bbox_inches='tight', facecolor='w')
plt.close()