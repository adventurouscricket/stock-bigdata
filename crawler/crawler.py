from confluent_kafka import Producer
import json
from vnstock import *
from datetime import datetime, timedelta
from time import sleep
import threading

# Hàm gửi dữ liệu JSON vào Kafka với mã chứng khoán
def produce_kafka_json(bootstrap_servers, topic_name, symbol, json_message):
    print("-----------------------------------------------")
    print("Start produce_kafka_json")
    print("-----------------------------------------------")
    producer = Producer({'bootstrap.servers': bootstrap_servers})
    producer.produce(topic_name, value=json_message.encode('utf-8'), key=symbol.encode('utf-8'), callback=delivery_report)
    producer.flush()

# Hàm callback cho việc gửi tin nhắn
def delivery_report(err, msg):
    """Callback được gọi khi tin nhắn được gửi thành công hoặc gặp lỗi."""
    if err is not None:
        print('Fail to send: {}'.format(err))
    else:
        print('Successful to send: {}'.format(msg.key().decode('utf-8')))

# Hàm lấy dữ liệu chứng khoán cho một mã cụ thể
def get_stock_data(symbol):
    today = datetime.now()
    start_date_this_week = today - timedelta(days=today.weekday())
    start_date_last_week = start_date_this_week - timedelta(days=7)
    end_date_last_week = start_date_last_week + timedelta(days=6)
    start_date_last_week_str = start_date_last_week.strftime('%Y-%m-%d')
    end_date_last_week_str = end_date_last_week.strftime('%Y-%m-%d')

    info_df = company_overview(symbol)
    
    df = stock_historical_data(symbol=symbol,
                               start_date=start_date_last_week_str,
                               end_date=end_date_last_week_str,
                               resolution='1',
                               type='stock',
                               beautify=True)
    # Chuyển dữ liệu thành JSON với thêm thông tin về mã chứng khoán
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(x) else None)    
    df = df.assign(companyType=info_df['companyType'].iloc[0])  
    json_data = df.to_json(date_format='iso', orient='records')
    return json_data

def get_stock_data_intraday(symbol): 
    df = stock_intraday_data(
            symbol=symbol, page_size=1, investor_segment=True
        )
    df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')
    df['total_minutes'] = df['time'].dt.hour * 60 + df['time'].dt.minute - 9*60 -15
    df['time'] = df['time'].dt.strftime('%H:%M:%S') #1
    df = df.drop_duplicates(subset=['total_minutes'])
    json_data = df.to_json(date_format='iso', orient='records')
    return json_data

def jobCrawlVn30Data(kafka_topic, bootstrap_servers):
    print("-----------------------------------------------")
    print("Start jobCrawlVn30Data in crawler.py")
    stock_array = ["ACB","BCM","BID","BVH","CTG","FPT","GAS","GVR","HDB","HPG","MBB","MSN","MWG","PLX","POW","SAB","SHB","SSB","SSI","STB","TCB","TPB","VCB","VHM","VIB","VIC","VJC","VNM","VPB","VRE"]
    print("Run jobCrawlVn30Data: ")
    print("-----------------------------------------------")

    while True:
        for symbol in stock_array:
            stock_data = get_stock_data(symbol)  # Lấy dữ liệu cho mã chứng khoán hiện tại
            produce_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)  # Gửi dữ liệu vào Kafka với mã chứng khoán
            time.sleep(2)  # Chờ 2 giây trước khi lấy dữ liệu cho mã chứng khoán tiếp theo
        break

def jobCrawlStockDataRealtime(symbol, kafka_topic, bootstrap_servers):
    index = 1
    print("-----------------------------------------------")
    print("Start jobCrawlStockDataRealtime in crawler.py")
    print("-----------------------------------------------")
    while True: 
        stock_data = get_stock_data_intraday(symbol)
        # print(str(index) +  ": \n")
        print("Run jobCrawlStockDataRealtime: ", stock_data)
        produce_kafka_json(bootstrap_servers, kafka_topic, symbol, stock_data)
        sleep(60)
        # index = index+1

if __name__ == "__main__":
    print("-----------------------------------------------")
    print("Start crawler")
    print("-----------------------------------------------")
    bootstrap_servers = 'kafka:9092'
    kafka_topic_vn30 = 'vn30'
    kafka_topic_realtime = 'stock_realtime4'

    t1 = threading.Thread(target=jobCrawlVn30Data, args=(kafka_topic_vn30, bootstrap_servers))
    t2 = threading.Thread(target=jobCrawlStockDataRealtime, args=('FPT', kafka_topic_realtime, bootstrap_servers))

    t1.start()
    t2.start()

    t1.join()
    t2.join()