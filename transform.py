import pandas as pd
import numpy as np

db_clean = pd.DataFrame()
db_rejected = pd.DataFrame()
def transform(orders_raw, ref_raw):
    # Transform
    # Clean null customer ratings to default to 2.5 (half of max 5)
    orders_raw['customer_rating'] = orders_raw['customer_rating'].fillna(2.5)

    # inner join with reference table
    db = pd.merge(orders_raw, ref_raw, on='restaurant_code', how='inner')

    # Encode 'own_driver' column to numeric
    db["own_driver"] = db["own_driver"].map({"No" : 0, "Yes" : 1})

    # Derive delivered on time field
    db["delivered_on_time"] = np.where(db['time_to_deliver'] > db['adv_delivery_time'], "No", "Yes")

    # Derive total price from order price, discount, and delivery fee
    db["total_price"] = db['order_price'] - db['discount_applied'] + (db['own_driver'] * db['delivery_fee'])

    # Check for soft errors and add warning flag if found
    db['data_quality_flag'] = ''
    # Soft error 1: order price higher than 3000
    db.loc[db['order_price'] > 3000, 'data_quality_flag'] += 'HIGH PRICE ALERT,'
    # Soft error 2: restaurant category unknown
    db.loc[db['category'].isnull(), 'data_quality_flag'] += 'UNKNOWN RESTAURANT CATEGORY,'
    db['data_quality_flag'] = db['data_quality_flag'].str.rstrip(',')
    db['category'] = db['category'].fillna("Unknown")
    db.loc[db['data_quality_flag'] == '', 'data_quality_flag'] = 'OK'

    # Check for hard errors
    def reject_reason(row):
        reasons = []
        #Hard error 1: order price less than 0
        if row['order_price'] < 0:
            reasons.append('INVALID ORDER PRICE')
        #Hard error 2: quantity of food items 0 or less
        if row['quantity'] <= 0:
            reasons.append('INVALID QUANTITY')
        
        return ','.join(reasons)

    db['reject_reason'] = db.apply(reject_reason, axis=1)
    return db

def reject(db):
    # Reject rows
    db_rejected = db[db['reject_reason'] != '']
    db_clean = db[db['reject_reason'] == '']

    db_rejected = db_rejected.drop(columns=['time_to_deliver', 'discount_applied', 'own_driver', 'adv_delivery_time'])
    db_clean = db_clean.drop(columns=['order_price', 'time_to_deliver', 'discount_applied', 'own_driver', 'adv_delivery_time', 'reject_reason'])
    return db_clean, db_rejected