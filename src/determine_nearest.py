'''
Create a table with the nearest distance, grouped by destination type for each of the blocks
'''
from sqlalchemy_utils.functions.orm import table_name
import yaml 
import psycopg2
from sqlalchemy.types import Float, Integer
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
# functions - logging
import logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)



config_filename = 'main'
# import config file
with open('./config/{}.yaml'.format(config_filename)) as file:
    config = yaml.load(file)


def nearest(db):

    conn = db['engine'].raw_connection()
    cur = conn.cursor()

    config_table_name = config['SQL']['table_name']

    dest_types = pd.read_sql('Select distinct(dest_type) from {}'.format(config_table_name), db['con'])['dest_type'].values

    modes = pd.read_sql('Select distinct(mode) from {}'.format(config_table_name), db['con'])['mode'].values

    # get the nearest distance for each block by each destination type
    queries_1 = ['DROP TABLE IF EXISTS nearest_{};'.format(config_table_name),
        'CREATE TABLE IF NOT EXISTS nearest_{}(geoid TEXT, dest_type TEXT, duration INT, population INT, geometry geometry, mode TEXT)'.format(config_table_name)
    ]
    queries_2 = [''' INSERT INTO nearest_{} (geoid, dest_type, duration, population, geometry, mode)
            SELECT dist.id_orig as geoid, destinations.dest_type, MIN(dist.duration) as duration, blocks."C18_CURPop" as population, blocks.geometry, dist.mode
            FROM {} as dist
            INNER JOIN destinations ON dist.id_dest = destinations.id_dest
            INNER JOIN blocks ON  dist.id_orig = blocks."SA12018_V1"
            WHERE destinations.dest_type='{}' AND dist.mode = '{}'
            GROUP BY dist.id_orig, destinations.dest_type, blocks.geometry, blocks."C18_CURPop", dist.mode;
        '''.format(config_table_name, config_table_name, dest_type, mode)
        for mode in modes 
        for dest_type in dest_types]
    queries_3 = ['CREATE INDEX nearest_geoid_{} ON nearest_{} (geoid)'.format(config_table_name,config_table_name)]

    queries = queries_1 + queries_2 + queries_3

    # import code
    # code.interact(local=locals())
    logger.error('Creating table')
    for q in queries:
        cur.execute(q)
    conn.commit()
    logger.error('Table created')

    db['con'].close()
    logger.error('Database connection closed')
