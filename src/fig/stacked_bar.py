import geopandas as gpd 
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

import matplotlib as mpl
mpl.rcParams['pdf.fonttype'] = 42
xmin_stats = pd.read_csv('/media/CivilSystems/projects/x_minute_measuring/results/measures_xmin.csv')
xmin_stats = xmin_stats.sort_values(by='city')
city_pops = pd.read_csv('/media/CivilSystems/projects/x_minute_measuring/results/city_characteristics.csv')
city_pops = city_pops.sort_values(by='city')
city_pops = list(city_pops['population'])
xmin_stats['population'] = city_pops
nz_xmin = xmin_stats[(xmin_stats['country'] == 'NZ')] 
usa_xmin = xmin_stats[(xmin_stats['country'] == 'USA')]
nz_top5_pop = nz_xmin.sort_values(by= 'population', ascending=False).head(5)
usa_top20_pop = usa_xmin.sort_values(by= 'population', ascending=False).head(20)
usa_top20_pop = usa_top20_pop.sort_values(by= 'mean')
usa_cities = usa_top20_pop['city'].to_list()
usa_cities = usa_cities[0:-5]
extras = ['Portland, OR', 'Baltimore, MD', 'Detroit, MI', 'Atlanta, GA', 'Tempe, AZ']
usa_cities = usa_cities + extras
usa_top20 = usa_xmin[usa_xmin['city'].isin(usa_cities)]
data = nz_top5_pop.append(usa_top20).sort_values(by= 'mean', ascending=False)
#prepping data for plotting
names = data['city'].tolist()
mean = data['mean'].tolist()
min10 = (data['min10']*100).tolist()
min15 = (data['min15']*100).tolist()
min20 = (data['min20']*100).tolist()
# plotting horizontal stacked bar chart 
fig, ax = plt.subplots()
ax.barh(names, min20, 0.5, label='<20 mins', color='#a5afc3')
ax.barh(names, min15, 0.5, left=0, label='<15 mins', color='#627397')
ax.barh(names, min10, 0.5, left=0, label='<10 mins', color='#1f386b')
ax2 = ax.twiny() #set a second axis
ax2.plot(mean, names, color='#BC3D40', zorder=2)
ax.set_yticks(np.arange(len(names)), labels=names)
# ax.tick_params(labelsize=2)
ax.set_xlabel('Population percentage')
ax2.set_xlabel('Mean time to ammenities (x-minutes)')
ax.legend(loc='right')
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/x_minute_measuring/results/stacked_bar.pdf',
            dpi=500, format='pdf', transparent=True, bbox_inches='tight', facecolor='w')