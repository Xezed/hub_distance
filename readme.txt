Consolidation of transit stops

The 9 operators of our transit system each draw their routes separately, using different names and IDs to refer to the same stop. As a result, our database contains up to 9 “nodes” for each stop.

The transit agency maintains shapefiles with the precise coordinates and data of all the stops and stations without duplications. Cenefas.shp contains all of the road-side stops. Estaciones.shp contains all of the stations, which each contain multiple stops. 

I need a Python script that associates the nodes drawn by the operators with reference layers maintained by the transit agency.

There are four inputs:
	stops.txt (unique list of all nodes drawn by operators, Lat/Lon coordinates)
	Matriz de Distancia (non-unique list of all nodes drawn by operators, UTM 18N coordinates)
	Cenefas.shp (reference shapefile of road-side stops)
	Estaciones.shp (reference shapefile of stations)

The output file should have the following columns:

stop_id | stop_lat | stop_lon | stop_id2


——Step 1: Create Nodes table

-With stops.txt:

Select only the following columns:

stop_id | stop_lat | stop_lon

and call the table Nodes


-With Matriz de Distancia:

The following are the relevant fields from the Excel files:

agency_id is first letter of the filename (T, A, D or Z)
PosX and PosY are the coordinates of the Node in UTM 18N.
Punto/Nodo is the Node ID. 
The other fields can be ignored.

Convert PosX and PosY to Lat/Lon using the ‘utm’ library:

	Lat, Lon = utm.to_latlon(PosX, PosY, 18, 'N')

Create a table called Nodes with the following fields using only the Alimentador, Dual and Zonal Matriz de Distancia files:

stop_id | stop_lat | stop_lon

where:

	stop_id is a concatenation of agency_id and Nodo (Ex. A—-2205, matching the format of stop_id in the stops.txt file)

	stop_lat and stop_lon are the converted coordinates in WSG84

Since the Excel file includes all the routes there will be duplicate rows in the Nodes table. Filter them out so that Nodes is a unique list.


—-Step 2: Create reference table

Convert Cenefas.shp into a csv file that includes the Lat/Lon coordinates. This includes all the road-side stops.


Create the reference table for stops within stations. (The inputs for troncal do not have duplicity, and thus can serve as a reference):

with stops.txt:
	Select stop_id, stop_lat, stop_lon from stops.txt where stop_id begins with ’T’.

with Matriz de Distancia:
	Use the process in Step 1 except this time with the Troncal file.

Append the two tables into a table called RefStops with the following fields:

stop_id_ref | stop_lat_ref | stop_lon_ref | stop_type_ref

where stop_id = Cenefa from the Cenefa.shp input
		stop_id from the stops.txt input
		stop_id = Puntos/Nodos from the Matriz de Distancia input

stop_lat and stop_lon are the coordinates of the stop

stop_type is ‘Z’ for road-side stops in the Cenefa layer and ’T’ for station stops from the stops.txt or Matriz de Distancia inputs.


—-Step 3: Calculate distance matrix

Using the haversine function with a radius of 6,378,137 meters, calculate the distance in meters between every record in Nodes and every record in RefStops in a table called “NodesRefStop” with the following columns:

stop_id | stop_lat | stop_lon | stop_id_ref | stop_lat_ref | stop_lon_ref | stop_type_ref | distance

—-Step 4: Filter distance matrix to identify nearest reference stop

For each Node, identify the nearest RefStop (minimum distance) and keep only these records. NodesRefStop should now include the same number of records as Nodes.

Identify records in NodesRefStop where distance > 20 meters. Remove these records from NodesRefStop, and save them in a table called UnmatchedNodes.

Export NodesRefStop.csv, UnmatchedNodes.csv with encoding that permits Spanish characters.

I’m open to alternative methods for Steps 3 - 4 that are faster.

—-Step 6: Identify parent station of troncal stops

Select stop_id_ref, stop_lat_ref, stop_lon_ref from NodesRefStop where stop_type_ref = ’T’.

Repeat steps 3 and 4 using the Estaciones.shp input to get the following output table called RefStopParent:

stop_id_ref | stop_lat_ref | stop_lon_ref | station_id | station_lat | station_lon | distance

In this case, filter the table using a radius of 100 meters. Save records more than 100 meters from the matched parents in a table called RefStopOrphans, and removed them from RefStopParent.

Export RefStopParent and RefStopOrphans as a CSV file with encoding that permits Spanish characters.

—-Step 7: Create stops.txt for parent stations

With the csv created from Estaciones.shp (with Lat/Lon coords), create the following a csv with the filename parent_stations.txt and the following columns:

stop_id | stop_name | stop_desc | stop_lat | stop_lon | location_type | parent_station | wheelchair_boarding

where:

stop_id is an arbitrary primary key
stop_name = NOMBRE
stop_desc = UBICACION
stop_lat and stop_lon are the coordinates in WSG84
location_type = 1
parent_station is left blank

