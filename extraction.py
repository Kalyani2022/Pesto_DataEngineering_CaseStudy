import json
import csv
import fastavro
import io

def extract_ad_impressons(json_filename):
    with open(json_filename, 'r') as json_file:
        ad_impressions = json.load(json_file)
    return ad_impressions

def extract_click_conversions(csv_filename):
    click_conversions = []
    with open(csv_filename, 'r') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            click_conversions.append(row)
    return click_conversions

def extract_rtb_auctions(avro_filename, avro_schema):
    rtb_auctions = []
    with open(avro_filename, 'rb') as avro_file:
        reader = fastavro.reader(avro_file, avro_schema)
        for data in reader:
            rtb_auctions.append(data)
    return rtb_auctions