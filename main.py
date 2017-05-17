# -*- coding: utf-8 -*-
from math import radians, cos, sin, asin, sqrt
import pandas as pd
import multiprocessing
import utm
from contextlib import closing
import os
import sys
import time
import unicodecsv as csv
from simpledbf import Dbf5

reload(sys)  
sys.setdefaultencoding('utf8')

columns_nodes_refstop = ['a_nodo', 'nodo_lat', 'nodo_lon', 'stop_id', 'stop_lat', 'stop_lon', 'stop_type', 'distance']
columns_trunkstop_parent = ['stop_id', 'stop_lat', 'stop_lon', 'station_id', 'station_lat', 'station_lon', 'stop_type', 'distance']


def main():
    try:
        input_stops = sys.argv[1]
    except:
        input_stops = False

    nodes = pd.DataFrame()

    def id_lon_lat(row):
        a_nodo = agency_id + '--' + unicode(int(row[0]))
        lat, lon = utm.to_latlon(row[1], row[2], 18, 'N')
        return a_nodo, lat, lon

    # create dataframes from distance matrices
    if not input_stops:
        for dir_root, dirs, files in os.walk('Nodes'):
            for file in files:
                file_path = os.path.join(dir_root, file)
                print file_path
                if file.endswith('xlsx'):

                    agency_id = file[0]

                    # create dataframe of trunk stops
                    if agency_id == 'T':
                        df = pd.read_excel(file_path, parse_cols='M,P,Q', names=['stop_id', 'stop_lat', 'stop_lon'])
                        df = df.drop_duplicates(subset='stop_id')
                        df = df.apply(id_lon_lat, axis=1)
                        trunk_stops = pd.DataFrame(df.tolist(), columns=['stop_id', 'stop_lat', 'stop_lon'], index=df.index)
                        trunk_stops['stop_type'] = 'T'
                        continue

                    # create dataframe of non-trunk nodes
                    elif agency_id == 'D':
                        df = pd.read_excel(file_path, parse_cols='N,Q,R', names=['a_nodo', 'nodo_lat', 'nodo_lat'])

                    else:
                        df = pd.read_excel(file_path, parse_cols='F,H,I', names=['a_nodo', 'nodo_lat', 'nodo_lat'])

                    df = df.drop_duplicates(subset='a_nodo')
                    df = df.apply(id_lon_lat, axis=1)
                    df = pd.DataFrame(df.tolist(), columns=['a_nodo', 'nodo_lat', 'nodo_lat'], index=df.index)
                    nodes = nodes.append(df)

    # create dataframe of nodes from stops.txt input
    else:
        df = pd.read_csv(input_stops, usecols=[0, 4, 5], names=['a_nodo', 'nodo_lat', 'nodo_lat'], index_col=False)
        nodes = df[df.a_nodo.str[0] != 'T']

        trunk_stops = df[df.a_nodo.str[0] == 'T']
        trunk_stops = pd.DataFrame(trunk_stops.tolist(), columns=['stop_id', 'stop_lat', 'stop_lon'], index=trunk_stops.index)
        trunk_stops['stop_type'] = 'T'

    # collate road side ref stops from Cenefas input
    dbf = Dbf5('Ref Layers/Cenefas/Cenefas.dbf')
    df = dbf.to_dataframe()
    df.to_csv('cenefas.csv', columns=['CENEFA', 'NOMBRE', 'DIRECCION', 'LOCALIDAD', 'Longitud', 'Latitud'], encoding='utf-8', index=False)
    
    global ref_stops
    ref_stops = pd.read_csv('cenefas.csv', header=0, usecols=[0, 4, 5], names=['stop_id', 'stop_lon', 'stop_lat'], index_col=False)
    #ref_stops = pd.DataFrame(cenefas.tolist(), columns=['stop_id', 'stop_lat', 'stop_lon'])
    ref_stops['stop_type'] = 'Z'

    # append trunk stops from nodes
    ref_stops = ref_stops.append(trunk_stops, ignore_index=True)

    with closing(multiprocessing.Pool()) as pool:
        # joined_rows will contain lists of joined rows in arbitrary order.
        # use name=None so we get proper tuples, pandas named tuples cannot be pickled, see https://github.com/pandas-dev/pandas/issues/11791
        joined_rows = pool.imap_unordered(join_rows_unify, nodes.itertuples(name=None))

        # open file and write out all rows from incoming lists of rows
        with open('nodes_refstop.csv', 'a') as f:
            with open('unmatched_nodes.csv', 'a') as un:
                writer = csv.writer(f)
                un_writer = csv.writer(un)
                writer.writerow(columns_nodes_refstop)
                un_writer.writerow(columns_nodes_refstop)
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

    dbf = Dbf5('Ref Layers/Estaciones/Estaciones.dbf')
    df = dbf.to_dataframe()
    df.to_csv('estaciones.csv', columns=['NOMBRE', 'UBICACION', 'TRONCAL', 'ID', 'latitude', 'longitude'], encoding='utf-8', index=False)

    global stations
    stations = pd.read_csv('estaciones.csv', header=0, usecols=[3, 4, 5], names=['station_id', 'station_lat', 'station_lon'], index_col=False)
    stations['stop_type_ref'] = 'T'

    with closing(multiprocessing.Pool()) as pool:
        # joined_rows will contain lists of joined rows in arbitrary order.
        # use name=None so we get proper tuples, pandas named tuples cannot be pickled, see https://github.com/pandas-dev/pandas/issues/11791
        joined_rows = pool.imap_unordered(join_rows_parent, trunk_stops.itertuples(name=None))

        # open file and write out all rows from incoming lists of rows
        with open('trunkstop_parent.csv', 'w') as f:
            with open('trunkstop_orphans.csv', 'w') as un:
                writer = csv.writer(f)
                un_writer = csv.writer(un)
                writer.writerow(columns_trunkstop_parent)
                un_writer.writerow(columns_trunkstop_parent)
                for row_list in joined_rows:
                    min_dist = sys.maxint
                    min_row = ''
                    for row in row_list:
                        if int(row[7]) < min_dist:
                            min_dist = int(row[7])
                            min_row = row
                    if int(min_row[7]) > 500:
                        un_writer.writerow(min_row)
                    else:
                        writer.writerow(min_row)

    # with open('stops.txt', 'w') as f:
    #     writer = csv.writer(f, encoding='utf-8', delimiter=',')
    #     stations = pd.read_csv('estaciones.csv', header=0, names=['stop_id', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon'], index_col=False)
    #     for row in stations.itertuples(name=None):
    #         writer.writerow([unicode(row[1]), row[2], row[3], unicode(row[4]), unicode(row[5]), '1', ' '])

    # print counts of points
    print 'Nodos de paraderos: ' + str(len(nodes))
    print 'Nodos de estaciones BRT: ' + str(len(trunk_stops))
    print 'Cenefas: ' + str(len(ref_stops))
    print 'Estaciones BRT: ' + str(len(stations))

def join_rows_unify(row):
    # for row in nodes.itertuples():
    l = []
    for row2 in ref_stops.itertuples(name=None):
        l.append((row[1], row[2], row[3], row2[1],
                  row2[2], row2[3], row2[4], haversine(row[3], row[2], row2[3], row2[2])))
    return l

def join_rows_parent(row):
    # for row in nodes.itertuples():
    l = []
    for row2 in stations.itertuples(name=None):
        l.append((row[1], row[2], row[3], int(row2[1]),
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
    r = 6378137 # Equatorial radius of earth in meters.
    return c * r

time_start = time.time()
if __name__ == '__main__':
    main()
print 'Nodos unificados en ' + str(common.seconds_readable(int(time.time() - time_start)))    