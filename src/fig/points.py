import geopandas as gpd 
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import glob
import seaborn as sns

import matplotlib as mpl
mpl.rcParams['pdf.fonttype'] = 42
data = pd.read_csv('/media/CivilSystems/projects/x_minute_measuring/results/leave_out_amenity.csv')
cities = ['New York, NY', 'San Francisco, CA', 'Wellington', 'Philadelphia, PA', 
        'Washington, DC', 'Chicago, IL', 'Seattle, WA', 'Hamilton', 'Auckland', 
        'Los Angeles, CA', 'Portland, OR']
data = data[data['city'].isin(cities)]
data = data[~data.dest_type.str.contains('bank')]
ammen_lst = []
lst_len = []
for ammen in data['dest_type']:
        ammen = ammen.split()
        ammen_lst.append(ammen)
        lst_len.append(len(ammen))
data['ammen_lists'] = ammen_lst
data['list_len'] = lst_len
for name in cities:
        city_data = data[data['city'] == name]
        city_data = city_data.sort_values(by='duration', ascending=True)
        df = city_data[:1]
        for i in range(2,8):
                ammen = city_data[city_data['list_len'] == i].sort_values(by='duration', ascending=True)
                df_add = ammen[0:1]
                df = df.append(df_add)
        df.to_csv('{}_xmin.csv'.format(name))
df = pd.concat(map(pd.read_csv, ['Auckland_xmin.csv', 'Chicago, IL_xmin.csv','Hamilton_xmin.csv', 
                                'Los Angeles, CA_xmin.csv', 'New York, NY_xmin.csv', 'Philadelphia, PA_xmin.csv',
                                'Portland, OR_xmin.csv', 'San Francisco, CA_xmin.csv', 'Seattle, WA_xmin.csv', 
                                'Washington, DC_xmin.csv', 'Wellington_xmin.csv']))
c_list = [1, 2, 3, 4, 5, 6, 
1, 3, 2, 5, 4, 6,
1, 2, 3, 4, 5, 6,
1, 3, 2, 5, 6, 4,
1, 5, 2, 3, 6, 4,
1, 5, 3, 2, 6, 4,
1, 3, 2, 6, 5, 4, 
1, 3, 2, 5, 6, 4,
1, 3, 2, 5, 6, 4,
1, 3, 5, 2, 6, 4, 
1, 3, 2, 4, 5, 6]
for i in range(len(c_list)):
        if c_list[i] == 1:
                c_list[i] = 'park'
        if c_list[i] == 2:
                c_list[i] = 'dentist'
        if c_list[i] == 3:
                c_list[i] = 'primary school'
        if c_list[i] == 4:
                c_list[i] = 'doctor'
        if c_list[i] == 5:
                c_list[i] = 'pharmacy'
        if c_list[i] == 6:
                c_list[i] = 'supermarket'
df['colours'] = c_list

# import code
# code.interact(local=locals())

df = df[df.city.isin(['New York, NY','San Francisco, CA','Philadelphia, PA','Washington, DC','Chicago, IL','Wellington'])]

df_max = df.groupby('city').max('duration')
df_max.reset_index(inplace=True)
df = df.merge(df_max, on='city')
df.sort_values(by='duration_y', inplace=True)

x1 = df[df['list_len_x']==1].drop_duplicates(subset='city')#.sort_values(by='city')
x1 = x1['duration_x']
x6 = df[df['list_len_x']==6].drop_duplicates(subset='city')#.sort_values(by='city')
x6 = x6['duration_x']



# plotting 
y = df.drop_duplicates(subset='city')
y = y['city']
fig, ax = plt.subplots()
fig.set_size_inches(18.3/2.54, 10/2.54)

ax.hlines(y=y, xmin=x1, xmax=x6, color='black')
sns.scatterplot(data=df, x='duration_x', y='city', hue='colours',s=80)
ax.set_xlabel('mean x-min time as amenities added')
ax.set_xlim([0,50])
plt.legend(prop={'size': 8})
plt.grid(True)
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/x_minute_measuring/results/exclude_destination.pdf',
            dpi=500, format='pdf', transparent=True, bbox_inches='tight', facecolor='w')