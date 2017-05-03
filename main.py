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
        print 'doing'
        ref_stops = ref_stops.append(df_troncal, ignore_index=True)
    else:
        df = pd.read_csv(name, usecols=[0, 4, 5], header=0, names=['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref'], index_col=False)
        cols = ['stop_id_ref', 'stop_lat_ref', 'stop_lon_ref']
        df = df[df.stop_id_ref.str[0] == 'T']
        df['stop_type_ref'] = 'T'
        ref_stops = ref_stops.append(df)

    ref_stops.to_csv('test.csv', index=False)


if __name__ == '__main__':
    main()