from load import load_ad_impressions, load_click_conversions, load_rtb_auctions
from transformation import transform_ad_impressions, transform_click_conversions, transform_rtb_auctions
from extraction import extract_ad_impressons, extract_click_conversions, extract_rtb_auctions

json_filename = 'ad_impressions.json'
csv_filename = 'click_conversions.csv'
avro_filename = 'bid_requests.avro'
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

# Extraction
ad_impressions = extract_ad_impressons(json_filename)
click_conversions = extract_click_conversions(csv_filename)
rtb_auctions = extract_rtb_auctions(avro_filename, avro_schema)

# Transformations
ad_impressions = transform_ad_impressions(ad_impressions)
click_conversions = transform_click_conversions(click_conversions)
rtb_auctions = transform_rtb_auctions(rtb_auctions)

# Loading
load_ad_impressions(ad_impressions, json_filename)
load_click_conversions(click_conversions, csv_filename)
load_rtb_auctions(rtb_auctions, avro_filename, avro_schema)

print("Data pipeline ETL process completed Successfully ... ")