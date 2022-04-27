""" clip and edit shapefile data from stats aus website
"""
import numpy as np
import pandas as pd
# functions - geospatial
import geopandas as gpd
import shapely
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement

### significant urban areas SUA (population over 10,000)
sua_gdf = gpd.read_file('./src/australia/data/SUA_2016_AUST.shp')
sua_gdf = sua_gdf[~sua_gdf['SUA_NAME16'].str.startswith('Not ')]
sua_gdf.to_file('./src/australia/data/clipped_SUA_2016_AUST.shp')

sua_gdf = gpd.read_file('./src/australia/data/clipped_SUA_2016_AUST.shp')
### clip meshblocks to only SUAs
### faster to just clip in QGIS for future refernce 
def clip_meshblocks(sua_gdf, meshblock_filepath):
    meshblock_gdf = gpd.read_file(meshblock_filepath)
    meshblock_gdf = meshblock_gdf.dropna()
    clipped_mesh_gdf = gpd.clip(meshblock_gdf, sua_gdf)
    return clipped_mesh_gdf

act_clipped = clip_meshblocks(sua_gdf, './src/australia/data/MB_2016_ACT.shp')
nsw_clipped = clip_meshblocks(sua_gdf, './src/australia/data/MB_2016_NSW.shp')
nt_clipped = clip_meshblocks(sua_gdf, './src/australia/data/MB_2016_NT.shp')
ot_clipped = clip_meshblocks(sua_gdf, './src/australia/data/MB_2016_OT.shp')
qld_clipped = clip_meshblocks(sua_gdf, './src/australia/data/MB_2016_QLD.shp')
sa_clipped = clip_meshblocks(sua_gdf, './src/australia/data/MB_2016_SA.shp')
tas_clipped = clip_meshblocks(sua_gdf, './src/australia/data/MB_2016_TAS.shp')
vic_clipped = clip_meshblocks(sua_gdf, './src/australia/data/MB_2016_VIC.shp')
wa_clipped = clip_meshblocks(sua_gdf, './src/australia/data/MB_2016_WA.shp')

### uploading the QGIS clipped layers 
# act_clipped = gpd.read_file('./src/australia/data/clipped/ACT_clipped.shp')
# nsw_clipped = gpd.read_file('./src/australia/data/clipped/NSW_clipped.shp')
# nt_clipped = gpd.read_file('./src/australia/data/clipped/NT_clipped.shp')
# ot_clipped = gpd.read_file('./src/australia/data/clipped/OT_clipped.shp')
# qld_clipped = gpd.read_file('./src/australia/data/clipped/QLD_clipped.shp')
# sa_clipped = gpd.read_file('./src/australia/data/clipped/SA_clipped.shp')
# tas_clipped = gpd.read_file('./src/australia/data/clipped/TAS_clipped.shp')
# vic_clipped = gpd.read_file('./src/australia/data/clipped/VIC_clipped.shp')
# wa_clipped = gpd.read_file('./src/australia/data/clipped/WA_clipped.shp')

clip_list = [act_clipped, nsw_clipped, nt_clipped, ot_clipped, qld_clipped, sa_clipped, tas_clipped, vic_clipped, wa_clipped]
meshblocks_sua = pd.concat(clip_list)
meshblocks_sua.to_file('./src/australia/data/clipped/SUA_meshblocks.shp')