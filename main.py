import pandas as pd
import utm
import os


def main():
    nodes = pd.DataFrame()

    for dir_root, dirs, files in os.walk('Nodes'):
        for file in files:
            file_path = os.path.join(dir_root, file)
            print file_path
            if file.endswith('xlsx') and not file.startswith('Troncal'):
                for_id = file[0]

                def id_lon_lat(row):
                    new_id = for_id + '--' + unicode(int(row[0]))
                    lat, lon = utm.to_latlon(row[1], row[2], 18, 'N')
                    return new_id, lat, lon
                if file.startswith('Dual'):
                    df = pd.read_excel(file_path, parse_cols='N,Q,R', names=['stop_id', 'stop_lat', 'stop_lon'])
                else:
                    df = pd.read_excel(file_path, parse_cols='F,H,I', names=['stop_id', 'stop_lat', 'stop_lon'])
                df = df.drop_duplicates(subset='stop_id')
                df = df.apply(id_lon_lat, axis=1)
                df = pd.DataFrame(df.tolist(), columns=['stop_id', 'stop_lat', 'stop_lon'], index=df.index)
                nodes.append(df)

            elif file.endswith('txt'):
                df = pd.read_csv(file_path, usecols=[0, 4, 5])
                nodes = df

    nodes.to_csv('Nodes.csv', index=False)


if __name__ == '__main__':
    main()