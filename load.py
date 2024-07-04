import json
import csv
import fastavro
import io

def load_ad_impressions(ad_impressions, json_output_adImpresions):
    with open(json_output_adImpresions, 'w') as json_file:
        json.dump(ad_impressions, json_file, indent=5)

def load_click_conversions(click_conversions, csv_output_clickConversions):
    with open(csv_output_clickConversions, 'w', newline='') as csv_file:
        column_names = ['event_timestamp', 'user_id', 'campaign_id', 'conversion_type']
        writer = csv.DictWriter(csv_file, fieldnames = column_names)
        writer.writeheader()
        for conversion in click_conversions:
            writer.writerow(conversion)

def load_rtb_auctions(rtb_auctions, avro_output_rtbAuction, avro_schema):
    with open('avro_output_rtbAuction', 'wb') as avro_file:
        fastavro.writer(avro_file, avro_schema, rtb_auctions)