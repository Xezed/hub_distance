import pandas as pd
import utm
import sys


def main():
    input_file = sys.argv[1]
    if input_file.endswith('xlsx'):
        for_id = input_file[0]

        def id_lon_lat(row):
            new_id = for_id + '--' + unicode(int(row[0]))
            lat, lon = utm.to_latlon(row[1], row[2], 18, 'N')
            return new_id, lat, lon

        df = pd.read_excel(input_file, parse_cols='F,H,I', names=['stop_id', 'stop_lat', 'stop_lon'])
        df = df.drop_duplicates(subset='stop_id')
        df = df.apply(id_lon_lat, axis=1)
        df = pd.DataFrame(df.tolist(), columns=['stop_id', 'stop_lat', 'stop_lon'], index=df.index)

        df.to_csv('from_excel.txt', index=False)

    else:
        Nodes = pd.read_csv(input_file, usecols=[0, 4, 5])
        Nodes.to_csv('Nodes.csv', index=False)


if __name__ == '__main__':
    main()