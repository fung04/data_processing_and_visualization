from math import sin, cos, sqrt, atan2, radians
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def haversine(lat1, lon1, lat2, lon2):
    # approximate radius of earth in km
    R = 6373.0

    # convert latitude and longitude to radians
    lat1 = radians(lat1)
    lon1 = radians(lon1)
    lat2 = radians(lat2)
    lon2 = radians(lon2)

    # calculate the difference between the two longitudes and latitudes
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # apply the Haversine formula to calculate the distance
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # calculate the distance in meters
    distance = R * c * 1000

    return distance

def calculate_total_distance(latitude, longitude, rssi):
    # create a list of tuples containing the latitude and longitude values
    gps_data = list(zip(latitude, longitude))

    # iterate through the list and calculate the distance between each pair of consecutive points
    total_distance = 0
    previous_distance = 0
    data_per_meter = []
    data_per_meter.append((previous_distance, rssi[0]))

    for i in range(len(gps_data) - 1):
        lat1, lon1 = gps_data[i]
        lat2, lon2 = gps_data[i+1]
        d = haversine(lat1, lon1, lat2, lon2)
        total_distance += d
        
        if int(total_distance) > int(previous_distance):
            previous_distance = total_distance
            data_per_meter.append((int(previous_distance), rssi[i]))

    return total_distance, data_per_meter

def visualize_data_from_csv(rx_csv_dir, type):
    
    if type == 'RSSI':
        unit = 'dBm'
        df = pd.read_csv(f'{rx_csv_dir}rx.csv', sep=',', skiprows=1, names=[
                        'epoch', 'latitude', 'longitude', 'RSSI'])
    elif type == 'Latency':
        unit = 'ms'
        df = pd.read_csv(f'{rx_csv_dir}rx.csv', sep=',', skiprows=1, names=[
                        'seq', 'latitude', 'longitude', 'Latency'])
        df['Latency'] = round(df['Latency'].astype(float) * 1000, 3)

    total_distance, data_per_meter = calculate_total_distance(df['latitude'], df['longitude'], df[f'{type}'])
    print("Total distance: ", round(total_distance, 2), "m")

    # plot the data
    plt.plot(*zip(*data_per_meter))
    plt.xlabel('Distance (m)')
    plt.ylabel(f'{type} ({unit})')


    data_min = np.min(df[f'{type}'])
    data_max = np.max(df[f'{type}'])
    data_avg = round(np.mean(df[f'{type}']), 3)

    # Annotate plot with statistics
    plt.text(0.1, 0.9, f"Min {type}: {data_min} {unit}", transform=plt.gca().transAxes)
    plt.text(0.1, 0.85, f"Max {type}: {data_max} {unit}", transform=plt.gca().transAxes)
    plt.text(0.1, 0.8, f"Avg {type}: {data_avg} {unit}", transform=plt.gca().transAxes)

    plt.savefig(f'{rx_csv_dir}{type}.png', dpi=300)
    
    plt.show()

if __name__ == '__main__':
    visualize_data_from_csv()

