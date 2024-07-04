def transform_ad_impressions(ad_impressions):
    for impression in ad_impressions:
        impression['timestamp'] = impression['timestamp'].replace(' ', 'T')
    return ad_impressions

def transform_click_conversions(click_conversions):
    for conversion in click_conversions:
        conversion['event_timestamp'] = conversion['event_timestamp'].replace(' ', 'T')
    return click_conversions

def transform_rtb_auctions(rtb_auctions):
    for auction in rtb_auctions:
        auction['user_age_group'] = 'Senior' if auction['user_age'] > 50 else 'Adult'
    return rtb_auctions