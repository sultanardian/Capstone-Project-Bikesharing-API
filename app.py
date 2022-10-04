import sqlite3
import requests
from tqdm import tqdm

from flask import Flask, request
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

@app.route('/')
def home():
    return 'Hello World'

# task 1
@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/stations/<station_id>')
def route_station_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()
# end of task 1

# task 2
@app.route('/stations/add', methods = ['POST'])
def route_add_stations():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, 'stations', conn)
    return result
# end of task 2

# task 3
@app.route('/trips/add', methods = ['POST'])
def route_add_trips():
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_table(data, 'trips', conn)
    return result
# end of task 3

# task 4
@app.route('/trips/average_duration') 
def route_average_duration():
    query = f'''
            SELECT bikeid, duration_minutes
            FROM trips
            '''
    conn = make_connection()
    trips_data = pd.read_sql_query(query, conn)
    mean_data = pd.crosstab(
                                index = trips_data['bikeid'],
                                columns = 'average',
                                values = trips_data['duration_minutes'],
                                aggfunc = 'mean'
                            )
    return mean_data.to_json()
# end of task 4

# task 5
@app.route('/trips/average_duration/<bike_id>')
def route_average_duration_by_bikeid(bike_id):
    query = f'''
            SELECT duration_minutes
            FROM trips
            WHERE bikeid = {bike_id}
            '''
    conn = make_connection()
    trips_data_by_bikeid = pd.read_sql_query(query, conn)

    mean_data = trips_data_by_bikeid.mean().values
    return str(mean_data)
# end of task 5

# task 6
@app.route('/trips/trip_route', methods = ['POST'])
def route_trip_route():
    data = pd.Series(eval(request.get_json(force=True)))

    conn = make_connection()
    query = f'''
             SELECT * FROM trips
             WHERE start_station_id = {data['start_station_id']} AND end_station_id = {data['end_station_id']}
             '''

    query_result = pd.read_sql_query(query, conn)

    agg_result = pd.crosstab(
                                    index = query_result['bikeid'],
                                    columns = 'result',
                                    values = query_result['duration_minutes'],
                                    aggfunc = ['count', 'mean']
                                 )

    return agg_result.to_json()
# end of task 6

def insert_into_table(data, table, conn):
    query = f"""INSERT INTO {table} values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection

def get_all_stations(conn):
    query = f'''SELECT * FROM stations LIMIT 20'''
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f'''SELECT * FROM stations WHERE station_id = {station_id}'''
    result = pd.read_sql_query(query, conn)
    return result

def get_all_trips(conn):
    query = f'''SELECT * FROM trips'''
    result = pd.read_sql_query(query, conn)
    return result

if __name__ == '__main__':
    app.run(debug=True, port=5000)