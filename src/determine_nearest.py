'''
Create a table with the nearest distance, grouped by destination type for each of the blocks
'''
import main
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

def main(db):

    conn = db['engine'].raw_connection()
    cur = conn.cursor()

    dest_types = pd.read_sql('Select distinct(dest_type) from destinations', db['con'])['dest_type'].values

    # get the nearest distance for each block by each destination type
    queries_1 = ['DROP TABLE IF EXISTS nearest_block;',
        # """CREATE TABLE nearest_block
        # AS
        # SELECT distances.id_orig AS geoid, destinations.dest_type, MIN(distances.distance) AS distance
        # FROM distances
        # INNER JOIN destinations ON distances.id_dest = destinations.id_dest
        # INNER JOIN blocks ON  distances.id_orig = blocks.geoid
        # GROUP BY distances.id_orig, destinations.dest_type;
        # """,
        'CREATE TABLE IF NOT EXISTS nearest_block(geoid TEXT, dest_type TEXT, distance INT, population INT, geometry geometry, mode TEXT)'
    ]
    queries_2 = [''' INSERT INTO nearest_block (geoid, dest_type, distance, population, geometry, mode)
            SELECT dist.id_orig as geoid, destinations.dest_type, MIN(dist.duration) as duration, blocks."C18_CURPop" as population, blocks.geometry, dist.mode
            FROM duration as dist
            INNER JOIN destinations ON dist.id_dest = destinations.id_dest
            INNER JOIN blocks ON  dist.id_orig = blocks."SA12018_V1"
            WHERE destinations.dest_type='{}' AND dist.mode='driving'
            GROUP BY dist.id_orig, destinations.dest_type, blocks.geometry, blocks."C18_CURPop", dist.mode;
        '''.format(dest_type)
        for dest_type in dest_types]
    queries_3 = [''' INSERT INTO nearest_block (geoid, dest_type, distance, population, geometry, mode)
            SELECT dist.id_orig as geoid, destinations.dest_type, MIN(dist.duration) as duration, blocks."C18_CURPop" as population, blocks.geometry, dist.mode
            FROM duration as dist
            INNER JOIN destinations ON dist.id_dest = destinations.id_dest
            INNER JOIN blocks ON  dist.id_orig = blocks."SA12018_V1"
            WHERE destinations.dest_type='{}' AND dist.mode='cycling'
            GROUP BY dist.id_orig, destinations.dest_type, blocks.geometry, blocks."C18_CURPop", dist.mode;
        '''.format(dest_type)
        for dest_type in dest_types]
    queries_4 = [''' INSERT INTO nearest_block (geoid, dest_type, distance, population, geometry, mode)
            SELECT dist.id_orig as geoid, destinations.dest_type, MIN(dist.duration) as duration, blocks."C18_CURPop" as population, blocks.geometry, dist.mode
            FROM duration as dist
            INNER JOIN destinations ON dist.id_dest = destinations.id_dest
            INNER JOIN blocks ON  dist.id_orig = blocks."SA12018_V1"
            WHERE destinations.dest_type='{}' AND dist.mode='walking'
            GROUP BY dist.id_orig, destinations.dest_type, blocks.geometry, blocks."C18_CURPop", dist.mode;
        '''.format(dest_type)
        for dest_type in dest_types]
    queries_5 = ['CREATE INDEX nearest_geoid ON nearest_block (geoid)']

    queries = queries_1 + queries_2 + queries_3 + queries_4 + queries_5

    # import code
    # code.interact(local=locals())
    logger.error('Creating table')
    for q in queries:
        cur.execute(q)
    conn.commit()
    logger.error('Table created')

    db['con'].close()
    logger.error('Database connection closed')
