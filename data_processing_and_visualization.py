import pyshark
import pandas as pd
import re
import plotly.graph_objs as go
import data_per_meter

# GPS_PCAP_DIR = f'maximun_range/OBU_2/test_6/'
# RX_PCAP_DIR = f'maximun_range/OBU_1/test_6/'
GPS_PCAP_DIR = f'test2/obu_2/'
RX_PCAP_DIR = f'test2/obu_1/'

FIELDTEST_TX_DIR = f'fieldtest/transmit/rsu_6/'

FIELDTEST_RX_DIR = f'fieldtest/receive/rsu_6/'


def plot_data_on_map(latitude, longitude, data, type):
    
    # Create a map
    fig = go.Figure()

    # Add markers
    fig.add_trace(go.Scattermapbox(
        mode="markers",
        lon=longitude,
        lat=latitude,
        showlegend=False,
        marker=dict(
            size=30,
            color=data,
            colorscale='Viridis',
            reversescale=True,
            opacity=0.7,
            colorbar=dict(
                title=type,
                titleside="bottom",
                titlefont=dict(size=25),
                tickfont=dict(size=30),
            )
        ),))

    # Add polyline
    fig.add_trace(go.Scattermapbox(
        mode="lines",
        showlegend=False,
        lon=longitude,
        lat=latitude,
        line={'color': 'red', 'width': 4},
    ))

    # Set map layout
    fig.update_layout(
        title={
            'text': f'{type} vs. Distance',
            'font': {'size': 35}
        },
        mapbox={
            'center': {'lon': longitude.mean(), 'lat': latitude.mean()},
            'style': "open-street-map",
            'zoom': 18,
        },
        margin=dict(t=60, b=10, l=10, r=5)  # Adjust the margin
    )
    fig.show()


def plot_trace_map(latitude, longitude):
    # Create a map
    fig = go.Figure()

    # Add markers
    fig.add_trace(go.Scattermapbox(
        mode="markers",
        lon=longitude,
        lat=latitude,
    ))

    # Add polyline
    fig.add_trace(go.Scattermapbox(
        mode="lines",
        lon=longitude,
        lat=latitude,
        line={'color': 'red', 'width': 4},
    ))

    # Set map layout
    fig.update_layout(
        mapbox={
            'center': {'lon': longitude.mean(), 'lat': latitude.mean()},
            'style': "open-street-map",
            'zoom': 18,
        },
    )
    fig.show()


def process_rx_pcap():
    print("Processing rx.pcap...")
    lat_list = []
    lon_list = []
    rssi_list = []
    epoch_list = []

    lon_lat_pattern = re.compile(r"(\d+\.\d+)")

    capture = pyshark.FileCapture(f'{RX_PCAP_DIR}rx.pcap')
    # get the field names of layer
    # print(capture[3].j2735_2016.field_names)
    # print(capture[3].COHDA.field_names)

    i = 0  # packet counter
    while True:
        try:
            packet = capture.next()
            i += 1

            try:
                if (packet["j2735_2016"]):
                    lat_list.append(packet["j2735_2016"].lat.showname)
                    lon_list.append(packet["j2735_2016"].long.showname)
                    rssi_list.append(packet["COHDA"].wsm_rssi)
                    epoch_list.append(packet.frame_info.time_epoch)
            except (KeyError, AttributeError):
                pass

            # Print a progress message every 1000 packets
            if i % 1000 == 0:
                print(f"Processed {i} packets.")
        except StopIteration:
            break

    print(f"Finished processing {i} packets.")

    print(
        f"Total lat : {len(lat_list)}, Total lon : {len(lon_list)}, Total rssi : {len(rssi_list)}, Total epoch : {len(epoch_list)}")

    # apply regex to get the lon and lat values
    lat_list = [float(re.findall(lon_lat_pattern, lat)[0]) for lat in lat_list]
    lon_list = [float(re.findall(lon_lat_pattern, lon)[0]) for lon in lon_list]

    df = pd.DataFrame({'lat': lat_list, 'lon': lon_list,
                       'rssi': rssi_list, 'epoch': epoch_list})

    # convert the epoch column to a datetime object and set it as the index
    df['epoch'] = pd.to_datetime(df['epoch'].astype(float), unit='s')
    df = df.set_index('epoch')

    # group the data by the 1 second
    # df_grouped = df.resample("1S").last()
    # df_grouped.to_csv('rx.csv')
    df.to_csv(f'{RX_PCAP_DIR}rx.csv')
    print("rx.csv created.")


def visualize_rx_data_from_csv():
    df = pd.read_csv(f'{RX_PCAP_DIR}rx.csv', sep=',', skiprows=1, names=[
        'epoch', 'latitude', 'longitude', 'Rssi'])
    df = df.dropna()

    print("Plotting rx.csv to map...")
    plot_data_on_map(df['latitude'], df['longitude'], df['Rssi'], 'RSSI (dBm)')


def process_gps_pcap():
    print("Processing gps.pcap...")
    lat_list = []
    lon_list = []

    capture = pyshark.FileCapture(f'{GPS_PCAP_DIR}gps.pcap', display_filter="frame.len == 360")
    # get the field names of layer
    # print(capture[3].j2735_2016.field_names)
    # print(capture[3].COHDA.field_names)


    i = 0  # packet counter
    while True:
        try:
            packet = capture.next()
            i += 1

            try:
                lat_list.append(packet.COHDA.gps_lat)
                lon_list.append(packet.COHDA.gps_lon)
            except KeyError:
                pass

            # Print a progress message every 1000 packets
            if i % 1000 == 0:
                print(f"Processed {i} packets.")
        except StopIteration:
            break

    print(f"Finished processing {i} packets.")

    print(f"Total lat : {len(lat_list)}, Total lon : {len(lon_list)}")

    df = pd.DataFrame({'lat': lat_list, 'lon': lon_list})
    df.to_csv(f'{GPS_PCAP_DIR}gps.csv')
    print("gps.csv created")


def visualize_gps_data_from_csv():
    # Read the CSV file
    df = pd.read_csv(f'{GPS_PCAP_DIR}gps.csv', sep=',', skiprows=1, names=[
                     'index', 'latitude', 'longitude'])
    df = df.dropna()
    print("Plotting gps.csv to map...")
    plot_trace_map(df['latitude'], df['longitude'])


def per_lat_calculator():
    # read data from file
    txlog_df = pd.read_csv(f'{FIELDTEST_TX_DIR}fieldtest_log.cw14tx', sep=',', names=[
        'transmit_time_s', 'test_start', 'sequence', 'lat_degs', 'long_degs', 'head_degs', 'speed', 'size', 'coding', 'antenna'])
    rxlog_df = pd.read_csv(f'{FIELDTEST_RX_DIR}fieldtest_log.cw14rx', sep=',', names=['Rx_time_s', 'Rx_tsf_s', 'lat_degs', 'long_degs', 'head_degs', 'speed',
                        'size', 'Src_start', 'sequence', 'Src_lat', 'Src_long', 'Src_head', 'Src_speed', 'rssi_a', 'noise_a', 'rssi_b', 'noise_b', 'coding', 'MAC_address'])

    # make the txlog_df have the size of last sequence number of rxlog_df
    txlog_df = txlog_df[:rxlog_df['sequence'].max()+1]
    print(len(txlog_df), len(rxlog_df))

    # compare the number of packets transmitted and received by comparing the sequence numbers
    tx_seqnums = set(txlog_df['sequence'].astype(int))
    rx_seqnums = set(rxlog_df['sequence'].astype(int))
    missing_seqnums = tx_seqnums - rx_seqnums
    print(f"Missing sequence numbers: {missing_seqnums}")

    # packet error rate
    print(f"Total receive packet: {len(rx_seqnums)}, lost packet: {len(missing_seqnums)} ")
    print(f"Packet error rate: {round(len(missing_seqnums) / len(tx_seqnums) * 100, 2)}%")

    # find the latency of each packet
    # convert the transmit_time_s and Rx_time_s columns to float
    txlog_df['transmit_time_s'] = txlog_df['transmit_time_s'].astype(float)
    rxlog_df['Rx_time_s'] = rxlog_df['Rx_time_s'].astype(float)
    txlog_df['sequence'] = txlog_df['sequence'].astype(int)
    rxlog_df['sequence'] = rxlog_df['sequence'].astype(int)

    # merge the two dataframes
    merged_df = pd.merge(txlog_df, rxlog_df, on='sequence')
    # find the latency of each packet
    merged_df['latency'] = merged_df['Rx_time_s'] - merged_df['transmit_time_s']
  

    # find the min and max, and average latency
    print(f"Min latency: {round(merged_df['latency'].min() * 1000,3)} ms")
    print(f"Max latency: {round(merged_df['latency'].max() * 1000,3)} ms")
    print(f"Avg latency: {round(merged_df['latency'].mean() * 1000,3)} ms")
    print(f"Latency of last packet: {round(merged_df['latency'].iloc[-1] * 1000, 3)} ms")

    # plot latency to map
    # plot_data_on_map(merged_df['lat_degs_y'], merged_df['long_degs_y'], merged_df['latency']*1000, 'Latency (ms)')

    # export the merged dataframe to csv withe sequence, lat, long, and latency columns
    merged_df[['sequence', 'lat_degs_y', 'long_degs_y', 'latency']].to_csv(f'{FIELDTEST_RX_DIR}rx.csv', index=False)
    

if __name__ == '__main__':

    try:
        per_lat_calculator()
        data_per_meter.visualize_data_from_csv(FIELDTEST_RX_DIR, 'Latency')

    except FileNotFoundError:
        print("fieldtest_log.cw14tx or fieldtest_log.cw14rx not found. Skipping PER and latency calculation.")

    # try:
    #     process_rx_pcap()
    #     visualize_rx_data_from_csv()
    # except FileNotFoundError:
    #     print("rx.pcap not found. Skipping rx.pcap processing.")

    # try:
    #     process_gps_pcap()
    #     visualize_gps_data_from_csv()
    # except FileNotFoundError:
    #     print("gps.pcap not found. Skipping gps.pcap processing.")

    # data_per_meter.visualize_rssi_data_from_csv(RX_PCAP_DIR, 'RSSI')
