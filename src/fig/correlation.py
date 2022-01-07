import geopandas as gpd 
import pandas as pd 
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import matplotlib as mpl
mpl.rcParams['pdf.fonttype'] = 42

xmin_stats = pd.read_csv('/media/CivilSystems/projects/x_minute_measuring/results/measures_xmin.csv')
xmin_stats = xmin_stats.sort_values(by='city')
xmin_stats = xmin_stats[['min10','min15','min20','max','mean','median','90th','ede']]
dict = {'mean':'Mean', 'min10':'10 minutes', 
        'min15':'15 minutes', 'min20':'20 minutes', 'median':'Median','max':'Max','ede':'EDE','min':'Min'}
xmin_stats.rename(columns=dict, inplace=True)
# xmin_stats = xmin_stats.drop(columns='top10_mean')

corr = xmin_stats.corr()
mask = np.triu(np.ones_like(corr, dtype=bool))
mask = np.rot90(mask,k=-1)
corr = corr[corr.columns[::-1]]
# corr.drop(index=corr.index[0], axis=0, inplace=True)
# corr.drop(columns=corr.columns[0], axis=1, inplace=True)
corr_text = (corr*100).round().astype(int).astype(str) + '%'
corr = corr.abs()

# corr = np.flip(corr, axis=1)

corr = corr * 100

# import code
# code.interact(local=locals())


ticks = list(corr.index)
ticks.reverse()
fig, ax = plt.subplots()
sns.heatmap(corr, mask=mask, cmap='Blues', vmin=25, vmax=100, annot=corr_text.to_numpy(), fmt='',
            square=True, linewidths=.5, cbar_kws={"shrink": .5})
ax.tick_params(left=False, bottom=False)
ax.set_xticklabels(ticks, rotation=45)#, fontdict={'horizontalalignment':'right'})
# ax.xaxis.tick_top() # x axis on top
# ax.xaxis.set_label_position('top')
plt.tick_params(axis='both', which='major', labelsize=10, labelbottom = False, bottom=False, top = False, labeltop=True)

# plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('/home/tml/CivilSystems/projects/x_minute_measuring/results/measure_correlation.pdf',
            dpi=500, format='pdf', transparent=True, bbox_inches='tight', facecolor='w')