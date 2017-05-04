from math import radians, cos, sin, asin, sqrt
import pandas as pd
import utm
import os
import sys
from simpledbf import Dbf5


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
                    nodes.append(df)

    else:
        df = pd.read_csv(name, usecols=[0, 4, 5])
        nodes = df

    nodes.to_csv('Nodes.csv', index=False)

    dbf = Dbf5('Ref Layers/Cenefas/Cenefas/Cenefas.dbf')
    df = dbf.to_dataframe()
    df.to_csv('road_side.csv', columns=['CENEFA', 'Longitud', 'Latitud'], index=False)
    ref_stops = pd.read_csv('road_side.csv', header=0, names=['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref'], index_col=False)
    ref_stops['stop_type_ref'] = 'Z'
    if not name:
        ref_stops = ref_stops.append(df_troncal, ignore_index=True)
    else:
        df = pd.read_csv(name, usecols=[0, 4, 5], header=0, names=['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref'], index_col=False)
        cols = ['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref']
        df = df[df.stop_id_ref.str[0] == 'T']
        df['stop_type_ref'] = 'T'
        ref_stops = ref_stops.append(df)

    nodes_ref_stop = pd.DataFrame(columns=['stop_id', 'stop_lat', 'stop_lon', 'stop_id_ref',
                                           'stop_lat_ref', 'stop_lon_ref', 'stop_type_ref', 'distance'])

    for row in nodes.itertuples():
        for row2 in ref_stops.itertuples():
            l = [row[1], row[2], row[3], row2[1], row2[2], row2[3], row2[4], haversine(row[2], row[3], row2[2], row2[3])]
            nodes_ref_stop = nodes_ref_stop.append(pd.Series(l, index=['stop_id', 'stop_lat', 'stop_lon', 'stop_id_ref',
                                                                       'stop_lat_ref', 'stop_lon_ref', 'stop_type_ref', 'distance']), ignore_index=True)


    nodes_ref_stop.to_csv('bigfile.csv')

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