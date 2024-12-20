"""
  Project: SmartTraffic_Lakehouse_for_HCMC
  Author: Nguyen Trung Nghia (ren294)
  Contact: trungnghia294@gmail.com
  GitHub: Ren294
"""
import streamlit as st
import redis
import random
import json
import time
import psycopg2
from datetime import datetime
import pandas as pd
from connection import get_redis_client, get_postgres_connection
st.set_page_config(
    page_title="HCMC Traffic Monitor",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .big-font {
        font-size: 50px !important;
        font-weight: bold;
        text-align: center;
        display: flex;
        justify-content: center;
        align-items: center;
    }
    .medium-font {
        font-size:20px !important;
        font-weight: bold;
    }
    .info-box {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    .metric-card {
        background-color: white;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
        margin: 5px;
    }
    .status-critical {
        color: #ff4b4b;
        font-weight: bold;
    }
    .status-warning {
        color: #ffa726;
        font-weight: bold;
    }
    .status-good {
        color: #00c853;
        font-weight: bold;
    }
    .author-info {
            text-align: left;
            font-size: 16px;
            margin-bottom: 30px;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 8px;
            background-color: #f9f9f9;
        }
    .author-info p {
        margin: 5px 0;
        line-height: 1.6;
    }
    .author-info a {
        color: #007BFF;
        text-decoration: none;
    }
    .author-info a:hover {
        text-decoration: underline;
    }
    .medium-font {
        font-size: 20px;
        font-weight: bold;
        text-align: center;
        color: #333;
        margin-top: 20px;
    }
    </style>
    """, unsafe_allow_html=True)


# def get_redis_connection():
#     return redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)


# def get_postgres_connection():
#     return psycopg2.connect(
#         dbname="traffic",
#         user="postgres",
#         password="postgres",
#         host="localhost",
#         port="5431"
#     )


ROADS_DATA = pd.read_csv("./info/roads_tp_ho_chi_minh.csv")


def display_weather(weather_data):
    if weather_data:
        st.markdown('<p class="medium-font">🌤️ Weather Conditions</p>',
                    unsafe_allow_html=True)
        cols = st.columns(4)

        with cols[0]:
            temp = float(weather_data.get('feelslike', 0))
            temp_color = '#ff4b4b' if temp > 35 else '#00c853' if temp < 25 else '#ffa726'
            st.markdown(f"""
                <div class="metric-card">
                    <h3>🌡️ Temperature</h3>
                    <p style="color: {temp_color}; font-size: 24px;">{temp}°C</p>
                </div>
            """, unsafe_allow_html=True)

        with cols[1]:
            wind = float(weather_data.get('windspeed', 0))
            wind_color = '#ff4b4b' if wind > 40 else '#00c853' if wind < 20 else '#ffa726'
            st.markdown(f"""
                <div class="metric-card">
                    <h3>💨 Wind Speed</h3>
                    <p style="color: {wind_color}; font-size: 24px;">{wind} km/h</p>
                </div>
            """, unsafe_allow_html=True)

        with cols[2]:
            st.markdown(f"""
                <div class="metric-card">
                    <h3>🌅 Conditions</h3>
                    <p style="font-size: 24px;">{weather_data.get('conditions', 'N/A')}</p>
                </div>
            """, unsafe_allow_html=True)


def display_traffic(traffic_data, road_name):
    if traffic_data:
        st.markdown(
            f'<p class="medium-font">🚦 Traffic on {road_name}</p>', unsafe_allow_html=True)
        cols = st.columns(5)
        vehicles = {
            'Motorbike': '🛵',
            'Car': '🚗',
            'Bus': '🚌',
            'Truck': '🚛',
            'Bicycle': '🚲'
        }

        for col, (vehicle, icon) in zip(cols, vehicles.items()):
            count = int(traffic_data.get(vehicle, 0))
            congestion_level = 'good' if count < 50 else 'warning' if count < 100 else 'critical'
            with col:
                st.markdown(f"""
                    <div class="metric-card">
                        <h3>{icon} {vehicle}</h3>
                        <p class="status-{congestion_level}">{count}</p>
                    </div>
                """, unsafe_allow_html=True)


def display_accidents(accident_data, road_name):
    if accident_data:
        st.markdown(
            f'<p class="medium-font">⚠️ Accident Information - {road_name}</p>', unsafe_allow_html=True)

        accident_time = datetime.strptime(accident_data.get(
            'accident_time', ''), '%Y-%m-%dT%H:%M:%S')
        recovery_time = datetime.strptime(accident_data.get(
            'estimated_recovery_time', ''), '%Y-%m-%dT%H:%M:%S')

        severity = int(accident_data.get('accident_severity', 0))
        severity_color = '#ff4b4b' if severity > 3 else '#ffa726' if severity > 1 else '#00c853'

        st.markdown(f"""
            <div class="info-box">
                <div style="display: grid; grid-template-columns: 1fr 1fr;">
                    <div>
                        <h3 style="color: {severity_color}">Severity Level: {severity}/5</h3>
                        <p>🚗 Vehicles Involved: {accident_data.get('number_of_vehicles', 'N/A')}</p>
                        <p>🌐 Congestion: {accident_data.get('congestion_km', 'N/A')} km</p>
                    </div>
                    <div>
                        <p>⏰ Time: {accident_time.strftime('%H:%M:%S')}</p>
                        <p>🔄 Est. Recovery: {recovery_time.strftime('%H:%M:%S')}</p>
                        <p>📝 {accident_data.get('description', 'N/A')}</p>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)


def display_parking(parking_data, road_name):
    if parking_data:
        st.markdown(
            f'<p class="medium-font">🅿️ Parking Information - {road_name}</p>', unsafe_allow_html=True)
        cols = st.columns(3)

        available = int(parking_data.get('availablespaces', 0))
        total = int(parking_data.get('totalspaces', 1))
        occupancy_rate = (total - available) / total * 100
        status_color = '#00c853' if occupancy_rate < 70 else '#ffa726' if occupancy_rate < 90 else '#ff4b4b'

        with cols[0]:
            st.markdown(f"""
                <div class="metric-card">
                    <h3>🚗 Available Spaces</h3>
                    <p style="color: {status_color}; font-size: 24px;">{available}</p>
                </div>
            """, unsafe_allow_html=True)

        with cols[1]:
            st.markdown(f"""
                <div class="metric-card">
                    <h3>📊 Total Spaces</h3>
                    <p style="font-size: 24px;">{total}</p>
                </div>
            """, unsafe_allow_html=True)

        with cols[2]:
            st.markdown(f"""
                <div class="metric-card">
                    <h3>💰 Hourly Rate</h3>
                    <p style="font-size: 24px;">{parking_data.get('hourlyrate', 'N/A')} VND</p>
                </div>
            """, unsafe_allow_html=True)


def display_gas_station(storage_data, station_info):
    if storage_data or station_info:
        st.markdown(
            '<p class="medium-font">⛽ Gas Station Information</p>', unsafe_allow_html=True)

        for station in station_info:
            st.markdown(f"""
                <div class="info-box">
                    <h3>🏪 {station[1]}</h3>
                    <p>📍 Address: {station[2]}</p>
                    <p>📞 Contact: {station[3]}</p>
                </div>
            """, unsafe_allow_html=True)

            if storage_data:
                cols = st.columns(len(storage_data))
                for idx, (tank_key, tank_data) in enumerate(storage_data.items()):
                    tank_info = json.loads(tank_data)
                    capacity = float(tank_info['capacity'])
                    current = random.uniform(0, float(capacity))
                    percentage = (current / capacity) * 100
                    status_color = '#00c853' if percentage > 70 else '#ffa726' if percentage > 30 else '#ff4b4b'

                    with cols[idx]:
                        st.markdown(f"""
                            <div class="metric-card">
                                <h3>🛢️ {tank_info['tankname']}</h3>
                                <p style="color: {status_color}; font-size: 20px;">
                                    {current}/{capacity} L
                                </p>
                                <p>({percentage:.1f}%)</p>
                            </div>
                        """, unsafe_allow_html=True)


def get_nearby_roads(current_road):
    other_roads = ROADS_DATA[ROADS_DATA['Road'] != current_road[0]]
    nearby_roads = other_roads.sample(n=min(3, len(other_roads)))
    return [(row['Road'], row['District'], row['City']) for _, row in nearby_roads.iterrows()]


def get_random_road():
    random_index = random.randint(0, len(ROADS_DATA) - 1)
    road_row = ROADS_DATA.iloc[random_index]
    return (road_row['Road'], road_row['District'], road_row['City'])


def get_gasstation_info(conn, road_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT gasstationid, gasstationname, address, phonenumber, email 
        FROM gasstation.gasstation 
        WHERE address LIKE %s
    """, (f'%{road_name}%',))
    return cur.fetchall()


def normalize_road_name(road_name):

    normalized = road_name
    prefixes_to_remove = ['Duong', 'duong',
                          'đường', 'street', 'st.', 'road', 'rd.']
    for prefix in prefixes_to_remove:
        if normalized.startswith(prefix + ' '):
            normalized = normalized[len(prefix) + 1:]
        elif normalized.startswith(prefix):
            normalized = normalized[len(prefix):]

    normalized = ' '.join(normalized.split())
    return normalized


def format_road_key(road, district, city):

    normalized_road = road.replace(' ', '_')
    normalized_district = district.replace(' ', '_')
    normalized_city = city.replace(' ', '_')
    return f"{normalized_road.replace(' ', '_')}__{normalized_district}__Ho_Chi_Minh"


def get_parking_lot_info(redis_conn, road, district, city):
    key = f"parking_lot_{format_road_key(road, district, city)}"
    data = redis_conn.hgetall(key)

    if not data:
        key_alt = f"parking_lot_{normalize_road_name(road).replace(' ', '_')}"
        data = redis_conn.hgetall(key_alt)

        if not data:
            pattern = f"parking_lot_*{normalize_road_name(road).replace(' ', '_')}*"
            matching_keys = redis_conn.scan_iter(match=pattern)
            for matching_key in matching_keys:
                data = redis_conn.hgetall(matching_key)
                if data:
                    break

    if not data:
        st.warning(f"No parking data found for {road}. Keys tried: {key}")

    return data


def get_accident_info(redis_conn, road_name):
    key = f"accident_{road_name.replace(' ', '')}"
    return redis_conn.hgetall(key)


def get_traffic_info(redis_conn, road_name, district):
    normalized_road = normalize_road_name(road_name).replace(' ', '')
    normalized_district = district

    keys_to_try = [
        f"traffic_{normalized_road}_{normalized_district}",
        f"traffic_{normalized_road}",
        f"traffic__{normalized_road}_{normalized_district}"
    ]

    for key in keys_to_try:
        data = redis_conn.hgetall(key)
        if data:
            return data

    pattern = f"traffic*{normalized_road}*"
    matching_keys = redis_conn.scan_iter(match=pattern)
    for matching_key in matching_keys:
        data = redis_conn.hgetall(matching_key)
        if data:
            return data

    if not data:
        st.warning(
            f"No traffic data found for {road_name}. Keys tried: {keys_to_try}")

    return {}


def get_weather_info(redis_conn):
    return redis_conn.hgetall("weather")


def main():
    st.markdown('<h1 class="big-font">🌆 Ho Chi Minh City Traffic Monitor</h1>',
                unsafe_allow_html=True)

    if 'current_location' not in st.session_state:
        st.session_state.current_location = get_random_road()
        st.session_state.nearby_roads = get_nearby_roads(
            st.session_state.current_location)

    redis_conn = get_redis_client()
    pg_conn = get_postgres_connection()
    st.markdown(
        """
    <div class="author-info">
        <p><strong>📘 Project:</strong> SmartTraffic_Lakehouse_for_HCMC</p>
        <p><strong>👤 Author:</strong> Nguyen Trung Nghia (ren294)</p>
        <p><strong>📧 Contact:</strong> <a href="mailto:trungnghia294@gmail.com">trungnghia294@gmail.com</a></p>
        <p><strong>💻 GitHub:</strong> <a href="https://github.com/Ren294" target="_blank">Ren294</a></p>
    </div>
    """,
        unsafe_allow_html=True
    )

    current_road, current_district, current_city = st.session_state.current_location
    st.markdown(
        f'<p class="medium-font">📍 Current Location: {current_road}</p>', unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["📊 Current Location", "🗺️ Nearby Roads"])

    with tab1:
        weather_data = get_weather_info(redis_conn)
        display_weather(weather_data)

        st.markdown("---")

        traffic_data = get_traffic_info(
            redis_conn, current_road, current_district)
        display_traffic(traffic_data, current_road)

        accident_data = get_accident_info(redis_conn, current_road)
        if accident_data:
            st.markdown("---")
            display_accidents(accident_data, current_road)

        st.markdown("---")

        parking_data = get_parking_lot_info(
            redis_conn, current_road, current_district, current_city)
        display_parking(parking_data, current_road)

        station_info = get_gasstation_info(pg_conn, current_road)
        if station_info:
            st.markdown("---")
            for station in station_info:
                storage_data = redis_conn.hgetall(f"storage_tank_{station[0]}")
                display_gas_station(storage_data, [station])

    with tab2:
        for nearby_road, nearby_district, nearby_city in st.session_state.nearby_roads:
            with st.expander(f"🛣️ Information for {nearby_road}"):
                traffic_data = get_traffic_info(
                    redis_conn, nearby_road, nearby_district)
                display_traffic(traffic_data, nearby_road)

                accident_data = get_accident_info(redis_conn, nearby_road)
                if accident_data:
                    st.markdown("---")
                    display_accidents(accident_data, nearby_road)

                st.markdown("---")

                parking_data = get_parking_lot_info(
                    redis_conn, nearby_road, nearby_district, nearby_city)
                display_parking(parking_data, nearby_road)

                station_info = get_gasstation_info(pg_conn, nearby_road)
                if station_info:
                    st.markdown("---")
                    for station in station_info:
                        storage_data = redis_conn.hgetall(
                            f"storage_tank_{station[0]}")
                        display_gas_station(storage_data, [station])


if __name__ == "__main__":
    main()
