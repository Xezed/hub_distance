from math import radians, cos, sin, asin, sqrt
import pandas as pd
import multiprocessing
import utm
import sqlite3
from contextlib import closing
import os
import sys
import unicodecsv as csv
from simpledbf import Dbf5

reload(sys)  
sys.setdefaultencoding('utf8')

def main():
    try:
        name = sys.argv[1]
    except:
        name = False

    nodes = pd.DataFrame()

    def id_lon_lat(row):
        new_id = for_id + '--' + unicode(int(row[0]))
        lat, lon = utm.to_latlon(row[1], row[2], 18, 'N')
        return new_id, lat, lon

    if not name:
        for dir_root, dirs, files in os.walk('Nodes'):
            for file in files:
                file_path = os.path.join(dir_root, file)
                print file_path
                if file.endswith('xlsx'):

                    for_id = file[0]

                    if file.startswith('Troncal'):
                        df_troncal = pd.read_excel(file_path, parse_cols='G,P,Q', names=['stop_id', 'stop_lat', 'stop_lon'], index_col=False)
                        df_troncal = df_troncal.drop_duplicates(subset='stop_id')
                        df_troncal = df_troncal.apply(id_lon_lat, axis=1)
                        df_troncal = pd.DataFrame(df_troncal.tolist(), columns=['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref'], index=df_troncal.index)
                        df_troncal['stop_type_ref'] = 'T'
                        continue

                    if file.startswith('Dual'):
                        df = pd.read_excel(file_path, parse_cols='N,Q,R', names=['stop_id', 'stop_lat', 'stop_lon'])

                    else:
                        df = pd.read_excel(file_path, parse_cols='F,H,I', names=['stop_id', 'stop_lat', 'stop_lon'])

                    df = df.drop_duplicates(subset='stop_id')
                    df = df.apply(id_lon_lat, axis=1)
                    df = pd.DataFrame(df.tolist(), columns=['stop_id', 'stop_lat', 'stop_lon'], index=df.index)
                    nodes = nodes.append(df)

    else:
        df = pd.read_csv(name, usecols=[0, 4, 5])
        nodes = df
    # nodes.to_csv('Nodes.csv', index=False)

    dbf = Dbf5('Ref Layers/Cenefas/Cenefas/Cenefas.dbf')
    df = dbf.to_dataframe()
    df.to_csv('cenefas.csv', columns=['CENEFA', 'Latitud', 'Longitud'], index=False)
    ref_stops = pd.read_csv('cenefas.csv', header=0, names=['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref'], index_col=False)
    ref_stops['stop_type_ref'] = 'Z'
    if not name:
        ref_stops = ref_stops.append(df_troncal, ignore_index=True)
    else:
        df = pd.read_csv(name, usecols=[0, 4, 5], header=0, names=['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref'], index_col=False)
        df = df[df.stop_id_ref.str[0] == 'T']
        df['stop_type_ref'] = 'T'
        global ref_stops
        ref_stops = ref_stops.append(df)

    print len(nodes), len(ref_stops)

    fields = ['stop_id', 'stop_lat', 'stop_lon', 'stop_id_ref', 'stop_lat_ref', 'stop_lon_ref', 'stop_type_ref',
              'distance']

    with open('NodesRefStop.csv', 'w') as f:
        with open('UnmatchedNodes.csv', 'w') as un:
            writer = csv.writer(f)
            writer.writerow(fields)
            writer = csv.writer(un)
            writer.writerow(fields)

    with closing(multiprocessing.Pool()) as pool:
        # joined_rows will contain lists of joined rows in arbitrary order.
        # use name=None so we get proper tuples, pandas named tuples cannot be pickled, see https://github.com/pandas-dev/pandas/issues/11791
        joined_rows = pool.imap_unordered(join_rows, nodes.itertuples(name=None))

        # open file and write out all rows from incoming lists of rows
        with open('NodesRefStop.csv', 'a') as f:
            with open('UnmatchedNodes.csv', 'a') as un:
                writer = csv.writer(f)
                un_writer = csv.writer(un)
                for row_list in joined_rows:
                    min_dist = sys.maxint
                    min_row = ''
                    for row in row_list:
                        if int(row[7]) < min_dist:
                            min_dist = int(row[7])
                            min_row = row
                    if int(min_row[7]) > 20:
                        un_writer.writerow(min_row)
                    else:
                        writer.writerow(min_row)

    nodes = pd.read_csv('NodesRefStop.csv', usecols=[3, 4, 5], header=0,
                     names=['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref'])
    nodes = nodes[nodes.stop_id_ref.str[0] == 'T']
    print nodes

    dbf = Dbf5('Ref Layers/Estaciones/Estaciones/Estaciones.dbf')
    dbf = dbf.to_dataframe()
    dbf.to_csv('estaciones.csv', columns=['ID', 'latitude', 'longitude'], index=False)
    global ref_stops
    ref_stops = pd.read_csv('estaciones.csv', header=0, names=['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref'],
                            index_col=False)
    ref_stops['stop_type_ref'] = 'T'

    with closing(multiprocessing.Pool()) as pool:
        # joined_rows will contain lists of joined rows in arbitrary order.
        # use name=None so we get proper tuples, pandas named tuples cannot be pickled, see https://github.com/pandas-dev/pandas/issues/11791
        joined_rows = pool.imap_unordered(join_rows, nodes.itertuples(name=None))

        # open file and write out all rows from incoming lists of rows
        with open('RefStopParent.csv', 'w') as f:
            with open('RefStopOrphans.csv', 'w') as un:
                writer = csv.writer(f)
                un_writer = csv.writer(un)
                for row_list in joined_rows:
                    min_dist = sys.maxint
                    min_row = ''
                    for row in row_list:
                        if int(row[7]) < min_dist:
                            min_dist = int(row[7])
                            min_row = row
                    if int(min_row[7]) > 100:
                        un_writer.writerow(min_row)
                    else:
                        writer.writerow(min_row)

    with open('stops.txt', 'w') as f:
        writer = csv.writer(f, encoding='utf-8', delimiter=',')
        dbf.to_csv('estaciones.csv', columns=['NOMBRE', 'UBICACION', 'latitude', 'longitude'], encoding='utf-8')
        estaciones = pd.read_csv('estaciones.csv', header=0, names=['stop_id', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon'])
        for row in estaciones.itertuples(name=None):
            writer.writerow([unicode(row[1]), row[2], row[3], unicode(row[4]), unicode(row[5]), '1', ' '])


def join_rows(row):
    # for row in nodes.itertuples():
    l = []
    for row2 in ref_stops.itertuples(name=None):
        l.append((row[1], row[2], row[3], row2[1],
                  row2[2], row2[3], row2[4], haversine(row[3], row[2], row2[3], row2[2])))
    return l


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6378137 # Radius of earth in meters. Use 3956 for miles
    return c * r


if __name__ == '__main__':
    main()