import init_osrm
import determine_nearest
import query
import yaml
import subprocess
import numpy as np
# functions - data management
import psycopg2
from sqlalchemy.types import Float, Integer
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
# functions - geospatial
import osgeo.ogr
import geopandas as gpd
import shapely
from shapely.geometry import Point
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from geoalchemy2 import Geometry, WKTElement
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


def main(config_filename=None):
    # establish config filename
    config_filename = 'main'
    # if config_filename is None:
    #     config_filename = input('Insert Config Filename (filename.yaml): ')
    #     if ('yaml' in config_filename) == True:
    #         config_filename = config_filename[:-5]

    # import config file
    with open('./config/{}.yaml'.format(config_filename)) as file:
        config = yaml.load(file)

    # initialize the OSRM server
    init_osrm.main(config, logger)

    # initialize and connect to the server
    db = init_db(config)

    # add origins and destinations
    init_destinations(db, config)
    init_origins(db, config)
    
    # query
    query.main(config)

    # calculate nearest
    determine_nearest.main(db)

    # shutdown the OSRM server
    db['con'].close()
    shutdown_db(config)


def init_db(config):
    '''create the database and then connect to it'''
    # SQL connection
    db = config['SQL'].copy()
    db['passw'] = open('pass.txt', 'r').read().strip('\n')
    db['engine'] = create_engine('postgresql+psycopg2://postgres:' + db['passw'] + '@' + db['host'] + '/' + db['database_name'] + '?port=' + db['port'])
    db['address'] = "host=" + db['host'] + " dbname=" + db['database_name'] + " user=postgres password='"+ db['passw'] + "' port=" + db['port']

    # Create the database
    exists = database_exists(db['engine'].url)
    if not exists:
        create_database(db['engine'].url)

    # connect to database
    db['con'] = psycopg2.connect(db['address'])

    # enable postgis
    if not exists:
        db['con'].cursor().execute("CREATE EXTENSION postgis;")
        db['con'].commit()

    logger.info('Database connection established')
    return(db)


def init_origins(db, config):
    # variables
    con = db['con']
    engine = db['engine']
    projection = config['set_up']['projection']
    # import and project
    origin = gpd.read_file(r'{}'.format(config['set_up']['origin_file_directory']))
    origin = origin.to_crs("EPSG:{}".format(projection))
    # remove MultiPolygons by taking the largest
    is_mp = [type(origin.loc[i].geometry)==MultiPolygon for i in origin.index]
    # origin = origin[[not i for i in is_mp]]
    count_mp = sum(is_mp)
    print("There were ", count_mp, "Multipolygons found and largest kept")
    for o in origin.index:
        p = origin.loc[o].geometry
        if p.geom_type == 'MultiPolygon':
            # print(o)
            # mp = p
            origin.loc[o,'geometry'] = max(p, key=lambda a: a.area)
            # print(origin.loc[o].geometry)
    blocks = origin.copy()
    blocks = origin[[config['orig_id'],'geometry','C18_CURPop']]
    blocks['geometry'] = blocks['geometry'].apply(lambda x: WKTElement(x.wkt, srid=projection))
    blocks.to_sql('blocks', engine, if_exists='replace', dtype={'geometry': Geometry('POLYGON', srid= projection)})
    # origins
    origin['centroid'] = origin.centroid
    origin['geom'] = origin['centroid'].apply(lambda x: WKTElement(x.wkt, srid=projection))
    origin = origin[[config['orig_id'],'geom','C18_CURPop']]
    # # origin.set_index(config['orig_id'])
    origin.to_sql('origin', engine, if_exists='replace', dtype={'geom': Geometry('POINT', srid= projection)})
    logger.info('Successfully exported origin shapefile to SQL')




def init_destinations(db, config):
    '''
    create the table of destinations
    '''
    if config['set_up']['destination_file_directory'] is not False:
        # db connections
        con = db['con']
        engine = db['engine']
        # destinations and locations
        types = config['services']
        # projection
        projection = config['set_up']['projection']
        # import the csv's
        gdf = gpd.GeoDataFrame()
        count = 0
        id_dest = []
        for dest_type in types:
            file = config['set_up']['destination_file_directory'][count]
            df_type = gpd.read_file(r'{}'.format(file))
            if dest_type == 'supermarket':
                df_type['lon'] = list(df_type['X'].astype(float))
                df_type['lat'] = list(df_type['Y'].astype(float))
                geometry = [Point(xy) for xy in zip(df_type['lon'], df_type['lat'])]
                crs = {'init': 'epsg:{}'.format(projection)}
                df_type = gpd.GeoDataFrame(df_type, crs=crs, geometry=geometry)
            if dest_type == 'health_services':
                df_type['lon'] = list(df_type['X'].astype(float))
                df_type['lat'] = list(df_type['Y'].astype(float))
                geometry = [Point(xy) for xy in zip(df_type['lon'], df_type['lat'])]
                crs = {'init': 'epsg:{}'.format(projection)}
                df_type = gpd.GeoDataFrame(df_type, crs=crs, geometry=geometry)
            # df_type = pd.read_csv('data/destinations/' + dest_type + '_FL.csv', encoding = "ISO-8859-1", usecols = ['id','name','lat','lon'])
            df_type['dest_type'] = dest_type
            df_type = df_type.to_crs("EPSG:{}".format(projection))
            gdf = gdf.append(df_type)
            # import code
            # code.interact(local=locals())
            id_dest = np.append(id_dest, df_type[config['set_up']['dest_id_colname'][count]].values, axis=0)
            count += 1
            logger.info('{} loaded'.format(dest_type))
        # set a unique id for each destination
        gdf['id_dest'] = range(len(gdf))
        # retrieve id from file
        gdf['id_type'] = id_dest
        # prepare for sql
        gdf['geom'] = gdf['geometry'].apply(lambda x: WKTElement(x.wkt, srid=projection))
        #drop all columns except id, dest_type, and geom
        gdf = gdf[['id_dest','id_type','dest_type','geom']]
        # set index
        gdf.set_index(['id_dest','dest_type'])
        # export to sql
        gdf.to_sql('destinations', engine, if_exists='replace', dtype={'geom': Geometry('POINT', srid= projection)})
        # update indices
        cursor = con.cursor()
        queries = ['CREATE INDEX "destinations_id" ON destinations ("id_dest");',
                'CREATE INDEX "destinations_type" ON destinations ("dest_type");']
        for q in queries:
            cursor.execute(q)
        # commit to db
        con.commit()
        logger.info('Successfully exported destination shapefile to SQL')


def shutdown_db(config):
    if config['OSRM']['shutdown']:
        shell_commands = [
                            'docker stop osrm-{}'.format(config['location']['state']),
                            'docker rm osrm-{}'.format(config['location']['state']),
                            ]
        for com in shell_commands:
            com = com.split()
            subprocess.run(com)
    logger.info('OSRM server shutdown and removed')

def multi_regions():
    # establish config filenames
    states = ['il','md','fl', 'co', 'mi', 'la', 'ga', 'or', 'wa', 'tx']
    for state in states:
        config_filename = state
        # run
        main(config_filename)


if __name__ == '__main__':
    main()
