import json
import csv
import fastavro
from fastavro.schema import load_schema  # Add this import
from datetime import datetime, timedelta
import random
import logging
import os
import io


def generate_dummy_timestamps(start_date, end_date, num_timestamps):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    delta = end - start
    timestamps = []

    for _ in range(num_timestamps):
        random_days = random.randint(0, delta.days)
        random_time = timedelta(days=random_days)
        random_timestamp = start + random_time
        timestamps.append(random_timestamp)

    return timestamps

def generate_dummy_ad_impressions(num_records, dummy_timestamps):
    ad_creative_ids = [f'AD{id}' for id in range(9000,10000)]
    user_ids = [f'U{id}' for id in range(0,1000)]
    websites = ['example.com', 'website.com', 'sample.org', 'test.net', 'demo.io']

    impressions_data = []

    for _ in range(num_records):
        ad_creative_id = random.choice(ad_creative_ids)
        user_id = random.choice(user_ids)
        timestamp = random.choice(dummy_timestamps)
        website_link = f"http://{random.choice(websites)}/page/{random.randint(1, 100)}"
        
        impressions_data.append({
            'ad_creative_id': ad_creative_id,
            'user_id': user_id,
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'website_link': website_link
        })
    
    return impressions_data

def generate_dummy_clicks_conversions(num_records, dummy_timestamps):
    user_ids = [f'U{id}' for id in range(0,1000)]
    ad_campaign_ids = [f'CAMP{id}' for id in range(1000000, 10000000)]
    conversion_types = ['Purchase', 'SignUp', 'Subscriptions', 'Download', 'View']

    click_conversion_data = []
    
    for _ in range(num_records):
        user_id = random.choice(user_ids)
        ad_campaign_id = random.choice(ad_campaign_ids)
        timestamp = random.choice(dummy_timestamps)
        conversion_type = random.choice(conversion_types)

        click_conversion_data.append({
            'event_timestamp' : timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'user_id' : user_id,
            'campaign_id' : ad_campaign_id,  # Corrected field name here
            'conversion_type' : conversion_type
        })
    
    return click_conversion_data

def generate_user_info():
    user_id = random.choice([f'U{id}' for id in range(0,1000)])
    user_location = random.choice(['India', 'USA', 'UK', 'Canada', 'Australia', 'Spain', 'Russia', 'Germany', 'France', 'Belgium'])
    user_age = random.randint(22,65)
    gender = random.choice(['Male', 'Female', 'Transgender'])
    
    return user_id, user_location, user_age, gender

def generate_auction_details():
    auction_id = random.choice([f'AUCtion_{id}' for id in range(0,1000)])
    bid_amount = round(random.uniform(0.1, 10), 2)
    ad_size = random.choice(['300 X 250', '728 X 90', '160 X 600', '468 X 60'])
    ad_type = random.choice(['Banner', 'Video'])

    return auction_id, bid_amount, ad_size, ad_type

def generate_ip_address():
    ip_address = f"{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}"
    return ip_address


def generate_dummy_rtb_auctions(num_records):
    rtb_auction_data = []
    for _ in range(num_records):
        user_id, user_location, user_age, gender = generate_user_info()
        auction_id, bid_amount, ad_size, ad_type = generate_auction_details()
        ip_address = generate_ip_address()

        rtb_auction_data.append({
            'user_id': user_id,
            'user_location': user_location,
            'user_age': user_age,
            'gender': gender,
            'auction_id': auction_id,
            'bid_amount($)': bid_amount,
            'ad_size': ad_size,
            'ad_type': ad_type,
            'ip_address': ip_address
        })

    return rtb_auction_data


# Example usage:
start_date = '2024-01-01'
end_date = '2024-05-31'
num_timestamps = 10000
num_records = 10000

dummy_timestamps = generate_dummy_timestamps(start_date, end_date, num_timestamps)

dummy_ad_impressions = generate_dummy_ad_impressions(num_records, dummy_timestamps)
json_filename = 'ad_impressions.json'
with open(json_filename, 'w') as jsonfiles:
    json.dump(dummy_ad_impressions, jsonfiles, indent=6)

dummy_click_conversions = generate_dummy_clicks_conversions(num_records, dummy_timestamps)
csv_filename = 'click_conversions.csv'
with open(csv_filename, 'w', newline='') as csvfiles:
    column_names = ['event_timestamp', 'user_id', 'campaign_id', 'conversion_type']
    writer = csv.DictWriter(csvfiles, fieldnames=column_names)
    writer.writeheader()
    for event in dummy_click_conversions:
        writer.writerow(event)

dummy_rtb_auctions = generate_dummy_rtb_auctions(num_records)
avro_schema = {
        'type' : 'record',
        'name' : 'Real_Time_Bid_Auction',
        'fields' : [
            {'name' : 'user_id', 'type' : 'string'},
            {'name' : 'user_location', 'type' : 'string'},
            {'name' : 'user_age', 'type' : 'int'},
            {'name' : 'gender', 'type' : 'string'},
            {'name' : 'auction_id', 'type' : 'string'},
            {'name' : 'bid_amount($)', 'type' : 'float'},
            {'name' : 'ad_size', 'type' : 'string'},
            {'name' : 'ad_type', 'type' : 'string'},
            {'name' : 'ip_address', 'type' : 'string'}
        ]
    }
avro_filename = 'bid_requests.avro'
with io.BytesIO() as avrofiles:
    fastavro.writer(avrofiles, avro_schema, dummy_rtb_auctions)
    avrofiles.seek(0)
    with open(avro_filename, 'wb') as fp:
        fp.write(avrofiles.read())
