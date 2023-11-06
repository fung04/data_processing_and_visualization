import pyshark
import pandas as pd
import re
import plotly.graph_objs as go
import data_per_meter

# example1609 log file directory
TX_PCAP_DIR = f'maximun_range_3/transmit_obu/obu_3/'
RX_PCAP_DIR = f'maximun_range_3/receive_obu/obu_3/'

# fieldtest log file directory
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
        title={
            'text': 'GPS trace on map',
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


def process_rx_pcap():
    print("Processing rx.pcap...")
    lat_list = []
    lon_list = []
    rssi_list = []
    latency_list = []
    message_count = []
    shift_list = []

    lon_lat_pattern = re.compile(r"(\d+\.\d+)")

    tx_cap = pyshark.FileCapture(f'{TX_PCAP_DIR}tx.pcap')
    rx_cap = pyshark.FileCapture(f'{RX_PCAP_DIR}rx.pcap')
    # get the field names of layer
    # print(capture[3].j2735_2016.field_names)
    # print(capture[3].COHDA.field_names)

    # find the device ID of the transmitter and receiver by shfiting through the packets
    while True:
        tx_pkt = tx_cap.next()
        rx_pkt = rx_cap.next()

        try:
            if (tx_pkt['j2735_2016'] and rx_pkt['j2735_2016']):
                tx_id = tx_pkt.J2735_2016.id
                rx_id = rx_pkt.J2735_2016.id

                if tx_id == rx_id:
                    print("Device ID matches.")

                    # sync both packet that has the same message count and epoch time
                    tx_msg_cnt = int(tx_pkt.J2735_2016.msgCnt)
                    rx_msg_cnt = int(rx_pkt.J2735_2016.msgCnt)

                    tx_epoch_time = float(tx_pkt.frame_info.time_epoch)
                    rx_epoch_time = float(rx_pkt.frame_info.time_epoch)

                    if rx_msg_cnt == tx_msg_cnt and abs(rx_epoch_time - tx_epoch_time) < 0.01:
                        print(
                            f"Packet synced at {tx_pkt.frame_info.number} frame.")

                        # validate by checking message count
                        print(f"tx_msg_cnt: {int(tx_pkt.J2735_2016.msgCnt)}")
                        print(f"rx_msg_cnt: {int(rx_pkt.J2735_2016.msgCnt)}")

                        # log the data to list of first packet
                        lat_list.append(rx_pkt.J2735_2016.lat.showname)
                        lon_list.append(rx_pkt.J2735_2016.long.showname)
                        rssi_list.append(rx_pkt.COHDA.wsm_rssi)
                        latency_list.append(rx_epoch_time - tx_epoch_time)
                        message_count.append(rx_msg_cnt)
                        shift_list.append(0)

                        break
                    else:
                        # shift the tx packet until the message count and epoch time matches
                        tx_pkt = tx_cap.next()
                        tx_epoch_time = float(tx_pkt.frame_info.time_epoch)
                        tx_msg_cnt = int(tx_pkt.J2735_2016.msgCnt)
                else:
                    print("Device ID does not match.")
        except (KeyError, AttributeError):
            pass

    i = 0  # packet counter
    packet_lost = 0
    shift_count = 0
    total_rx_packet = 1

    while True:
        try:
            i += 1

            rx_pkt = rx_cap.next()
            tx_pkt = tx_cap.next()

            tx_msg_cnt = int(tx_pkt.J2735_2016.msgCnt)
            rx_msg_cnt = int(rx_pkt.J2735_2016.msgCnt)

            tx_epoch_time = float(tx_pkt.frame_info.time_epoch)
            rx_epoch_time = float(rx_pkt.frame_info.time_epoch)

            # same message count indicates paceket is well received
            if rx_msg_cnt == tx_msg_cnt:

                # log the following data to list after first packet
                lat_list.append(rx_pkt.J2735_2016.lat.showname)
                lon_list.append(rx_pkt.J2735_2016.long.showname)
                rssi_list.append(rx_pkt.COHDA.wsm_rssi)
                latency_list.append(rx_epoch_time - tx_epoch_time)
                message_count.append(rx_msg_cnt)
                shift_list.append(0)
                total_rx_packet += 1

            # if message count does not match, indicates packet loss in transmission
            # Thus need to shift the tx packet until it matches
            elif rx_msg_cnt != tx_msg_cnt:
                while rx_msg_cnt != tx_msg_cnt:
                    packet_lost += 1
                    tx_pkt = tx_cap.next()
                    tx_msg_cnt = int(tx_pkt.J2735_2016.msgCnt)
                    shift_count += 1

                    if tx_msg_cnt == rx_msg_cnt:
                        # if message count matches, indicates tx and rx packet is synced
                        tx_epoch_time = float(tx_pkt.frame_info.time_epoch)
                        latency_list.append(rx_epoch_time - tx_epoch_time)
                        lat_list.append(rx_pkt.J2735_2016.lat.showname)
                        lon_list.append(rx_pkt.J2735_2016.long.showname)
                        rssi_list.append(rx_pkt.COHDA.wsm_rssi)
                        message_count.append(rx_msg_cnt)
                        shift_list.append(shift_count)

                        shift_count = 0
                        total_rx_packet += 1
                        break

            if i % 100 == 0:
                print(f"Processed {i} packets.")

        except StopIteration:
            print(f"Total packet lost: {packet_lost}")
            print(f"Total received packet: {total_rx_packet}")
            print(f"Total transmit packet: {packet_lost + total_rx_packet}")
            break

    # apply regex to get the lon and lat values
    lat_list = [float(re.findall(lon_lat_pattern, lat)[0]) for lat in lat_list]
    lon_list = [float(re.findall(lon_lat_pattern, lon)[0]) for lon in lon_list]

    # convert rssi to int, and latency to float, message count to int
    rssi_list = [int(rssi) for rssi in rssi_list]
    latency_list = [float(latency) for latency in latency_list]
    message_count = [int(msgcnt) for msgcnt in message_count]

    print(f"Total lat : {len(lat_list)}, Total lon : {len(lon_list)}, Total rssi : {len(rssi_list)}, Total latency : {len(latency_list)}, shift_list : {len(shift_list)}")

    df = pd.DataFrame({'msgcnt': message_count, 'lat': lat_list, 'lon': lon_list,
                       'rssi': rssi_list, 'latency': latency_list, 'shift': shift_list})

    df = df.set_index('msgcnt')
    df.to_csv(f'{RX_PCAP_DIR}rx.csv')

    print("rx.csv created.")


def visualize_rx_data_from_csv(rx_dir):
    df = pd.read_csv(f'{rx_dir}rx.csv', sep=',', skiprows=1, names=[
        'mscnt', 'latitude', 'longitude', 'RSSI', 'latency', 'shift'])
    df = df.dropna()

    print("Plotting rx.csv to map...")
    plot_data_on_map(df['latitude'], df['longitude'], df['RSSI'], 'RSSI (dBm)')
    plot_data_on_map(df['latitude'], df['longitude'],
                     df['latency']*1000, 'Latency (ms)')


def process_gps_pcap():
    print("Processing gps.pcap...")
    lat_list = []
    lon_list = []

    capture = pyshark.FileCapture(
        f'{TX_PCAP_DIR}gps.pcap', display_filter="frame.len == 360")
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
    df.to_csv(f'{tx_dir}gps.csv')
    print("gps.csv created")


def visualize_gps_data_from_csv():
    # Read the CSV file
    df = pd.read_csv(f'{TX_PCAP_DIR}gps.csv', sep=',', skiprows=1, names=[
                     'index', 'latitude', 'longitude'])
    df = df.dropna()
    print("Plotting gps.csv to map...")
    plot_trace_map(df['latitude'], df['longitude'])


def fieldtest_per_lat_calculator():
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
    print(
        f"Total receive packet: {len(rx_seqnums)}, lost packet: {len(missing_seqnums)} ")
    print(
        f"Packet error rate: {round(len(missing_seqnums) / len(tx_seqnums) * 100, 2)}%")

    # convert the transmit_time_s and Rx_time_s columns to float
    txlog_df['transmit_time_s'] = txlog_df['transmit_time_s'].astype(float)
    rxlog_df['Rx_time_s'] = rxlog_df['Rx_time_s'].astype(float)
    txlog_df['sequence'] = txlog_df['sequence'].astype(int)
    rxlog_df['sequence'] = rxlog_df['sequence'].astype(int)

    # merge the two dataframe by sequence number to get the latency
    merged_df = pd.merge(txlog_df, rxlog_df, on='sequence')
    merged_df['latency'] = merged_df['Rx_time_s'] - \
        merged_df['transmit_time_s']

    # find the min and max, and average latency
    print(f"Min latency: {round(merged_df['latency'].min() * 1000,3)} ms")
    print(f"Max latency: {round(merged_df['latency'].max() * 1000,3)} ms")
    print(f"Avg latency: {round(merged_df['latency'].mean() * 1000,3)} ms")
    print(
        f"Latency of last packet: {round(merged_df['latency'].iloc[-1] * 1000, 3)} ms")

    # find the min and max, and average RSSI
    # rssi_a for RSU mode, rssi_b for OBU mode
    print(f"Min RSSI: {merged_df['rssi_a'].max()}")
    print(f"Max RSSI: {merged_df['rssi_a'].min()}")
    print(f"Avg RSSI: {round(merged_df['rssi_a'].mean(), 3)}")
    print(f"RSSI of last packet: {merged_df['rssi_a'].iloc[-1],3}")

    # export the merged dataframe to csv withe sequence, lat, long, and latency columns
    merged_df[['sequence', 'lat_degs_y', 'long_degs_y', 'rssi_a', 'latency']].to_csv(
        f'{FIELDTEST_RX_DIR}rx.csv', index=False)


def example1609_per_lat_calculator():
    # open csv file in RX_PCAP_DIR
    rx_df = pd.read_csv(f'{RX_PCAP_DIR}rx.csv', sep=',', skiprows=1, names=[
                        'sequence', 'lat_degs', 'long_degs', 'rssi', 'latency', 'shift'])
    rx_df.index = rx_df.index + 1

    # find the number of packet lost
    packet_lost_list = []
    missing_packets = rx_df[rx_df['shift'] != 0]
    for index in missing_packets.index:
        for num_shift in range(missing_packets['shift'][index]):
            packet_lost_list.append(index - num_shift)
    print(f"\nTotal receive packet: {len(rx_df)}")
    print(f"Packet lost: {len(packet_lost_list)}")
    print(f"Packet lost list: {packet_lost_list}")
    print(
        f"Packet lost rate: {round(len(packet_lost_list)/len(rx_df)*100,2)}%")

    # convert the rssi, latency, and sequence columns to float
    rx_df['rssi'] = rx_df['rssi'].astype(float)
    rx_df['latency'] = rx_df['latency'].astype(float)
    rx_df['sequence'] = rx_df['sequence'].astype(int)

    # find the min and max, and average latency
    print(f"\nMin latency: {round(rx_df['latency'].min() * 1000,3)} ms")
    print(f"Max latency: {round(rx_df['latency'].max() * 1000,3)} ms")
    print(f"Avg latency: {round(rx_df['latency'].mean() * 1000,3)} ms")
    print(
        f"Latency of last packet: {round(rx_df['latency'].iloc[-1] * 1000, 3)} ms")

    # find the min and max, and average rssi
    print(f"\nMin rssi: {rx_df['rssi'].min()} dBm")
    print(f"Max rssi: {rx_df['rssi'].max()} dBm")
    print(f"Avg rssi: {round(rx_df['rssi'].mean(),3)} dBm")
    print(f"rssi of last packet: {rx_df['rssi'].iloc[-1]} dBm")


if __name__ == '__main__':

    try:
        fieldtest_per_lat_calculator()
        visualize_rx_data_from_csv(FIELDTEST_RX_DIR)

        data_per_meter.visualize_data_from_csv(FIELDTEST_RX_DIR, 'Latency')
        data_per_meter.visualize_data_from_csv(FIELDTEST_RX_DIR, 'RSSI')
    except FileNotFoundError:
        print("fieldtest_log.cw14tx or fieldtest_log.cw14rx not found")

    try:
        process_rx_pcap()
        example1609_per_lat_calculator()
        visualize_rx_data_from_csv(RX_PCAP_DIR)

        data_per_meter.visualize_data_from_csv(RX_PCAP_DIR, 'RSSI')
        data_per_meter.visualize_data_from_csv(RX_PCAP_DIR, 'Latency')
    except FileNotFoundError:
        print("rx.pcap not found")

    try:
        # only for example1609, fieldtest does not have gps.pcap
        process_gps_pcap()
        visualize_gps_data_from_csv()
    except FileNotFoundError:
        print("gps.pcap not found")
