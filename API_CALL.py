from ast import parse
from doctest import debug
import string
from flask import Flask, jsonify , request
from flask_mysqldb import MySQL
from flask_restx import Api, Resource, fields

app = Flask (__name__)



# MySQL configuration
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'Root@123'
app.config['MYSQL_DB'] = 'corteva_database'

mysql = MySQL(app)

@app.route('/api/weather', methods = ['GET'])
def Getting_data_api():
    try:
        query = "SELECT * FROM weather_data WHERE 1=1 limit 10"
       
        cur = mysql.connection.cursor()  # This is how you get a cursor with Flask-MySQLdb
        cur.execute(query)  # Execute the query with parameters
        weather_data = cur.fetchall()  # Fecthing the result of the query.
        result = [{"id": w[0],"station_id": w[1],"date": w[2],"max_temp": w[3],"min_temp": w[4],"precipitation": w[5]}for w in weather_data]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/weather', methods = ['POST'])
def Getting_data_api_paramnets():
    try:
        date = str (request.args.get('date'))    # Parsing the data to get the date
        station_id = str(request.args.get('station_id'))   # Parsing the data to get the date Station_id
        query = "SELECT * FROM weather_data WHERE 1=1"
        query_params = []
        if date:
            query += " AND date = %s"  # %s is a placeholder for MySQL query parameterization
            query_params.append(date)  # Add year to query parameters list

        if station_id:
            query += " AND station_id = %s" # %s is a placeholder for MySQL query parameterization
            query_params.append(station_id) # Add station id to query parameters list


        print("Final Query:", query)
        print("Query Parameters:", query_params)

        cur = mysql.connection.cursor()  # This is how you get a cursor with Flask-MySQLdb
        cur.execute(query, query_params)  # Execute the query with parameters
        weather_data = cur.fetchall()     # Fecthing the result of the query.
        result = [{"id": w[0],"station_id": w[1],"date": w[2],"max_temp": w[3],"min_temp": w[4],"precipitation": w[5]}for w in weather_data]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/api/weather/stats', methods = ['GET'])
def Getting_data_stats():
    try:
        query = "SELECT * FROM weather_statistics WHERE 1=1 limit 10"
       
        cur = mysql.connection.cursor()  # This is how you get a cursor with Flask-MySQLdb
        cur.execute(query)  # Execute the query with parameters
        weather_data = cur.fetchall()    # Fecthing the result of the query.
        result = [{"id": w[0],"station_id": w[1],"year": w[2],"avg_max_temp": w[3],"avg_min_temp": w[4],"total_precipitation": w[5]}for w in weather_data]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/api/weather/stats', methods=['POST'])
def Getting_data_stats_paramnets():
    try:
        year = int (request.args.get('year'))    # Parsing the data to get the date
        station_id = str (request.args.get('station_id')) # Parsing the data to get the date Station_id
        query = "SELECT * FROM weather_statistics WHERE 1=1 "
        query_params = []

        if station_id:
            query += " AND station_id = %s"  # %s is a placeholder for MySQL query parameterization
            query_params.append(station_id)  # Add station id to query parameters list

        if year:
            query += " AND year = %s"  # %s is a placeholder for MySQL query parameterization
            query_params.append(year)  # Add year to query parameters list


        cur = mysql.connection.cursor()  # This is how you get a cursor with Flask-MySQLdb
        cur.execute(query, query_params)  # Execute the query with parameters
        weather_data = cur.fetchall()    # Fecthing the result of the query.

        result = [{"id": w[0],"station_id": w[1],"year": w[2],"avg_max_temp": w[3],"avg_min_temp": w[4],"total_precipitation": w[5]}for w in weather_data]
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__' :
    app.run(debug = True )