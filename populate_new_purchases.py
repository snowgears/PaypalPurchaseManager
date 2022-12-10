from __future__ import print_function
import configparser
import requests
import mariadb
import json
from datetime import datetime, timedelta, timezone
import sched, time
import logging
from pathlib import Path

# --- variables ---

DB_HOST = None
DB_PORT = None
DB_DATABASE = None
DB_USER = None
DB_PASSWORD = None

PAYPAL_CLIENT_ID = None
PAYPAL_SECRET = None
PAYPAL_TOKEN = None
PAYPAL_ENDPOINT = "https://api-m.paypal.com"

CHECK_INTERVAL = 20
scheduler = sched.scheduler(time.time, time.sleep)
ts_token_expires = None

# --- functions ---

# formats a datetime object into the string format needed for paypal api
def format_datetime(time):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ')

# load the last time transaction history was fetched
def load_lastrun():
    try:
        lastrun = None
        with open ('lastrun.json', 'rb') as fp:
            lastrun = json.load(fp)
            fp.close
    except FileNotFoundError:
        return None
    lastrun['time'] = datetime.strptime(lastrun['time'], '%Y-%m-%dT%H:%M:%SZ')
    return lastrun

# save the last time transaction history was fetched
def save_lastrun(current_time):
    lastrun = {}
    lastrun['time'] = format_datetime(current_time)
    with open("lastrun.json", "w") as outfile:
        json.dump(lastrun, outfile)
        outfile.close
    return lastrun

# gets all transaction history from paypal between start_date and end_date
def get_transactions(start_date, end_date):

    url = PAYPAL_ENDPOINT + f"/v1/reporting/transactions"

    payload={
        "start_date": format_datetime(start_date),
        "end_date": format_datetime(end_date),
        'fields': 'all',
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {PAYPAL_TOKEN['access_token']}",
    }
    
    response = requests.get(url, headers=headers, params=payload)
    return response.json()

# get an oauth token from paypal api
def get_paypal_token():
    url = PAYPAL_ENDPOINT + f"/v1/oauth2/token"

    payload={
        "grant_type": "client_credentials",
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    
    response = requests.post(url, headers=headers, params=payload, auth=(PAYPAL_CLIENT_ID, PAYPAL_SECRET))

    global PAYPAL_TOKEN, ts_token_expires
    PAYPAL_TOKEN = response.json()

    # calculate the timestamp at which the oauth token expires
    ts_token_expires = datetime.now() + timedelta(seconds=(PAYPAL_TOKEN['expires_in'] - 60))


# load all of the variables from the config.ini
def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    global DB_HOST, DB_PORT, DB_DATABASE, DB_USER, DB_PASSWORD, PAYPAL_CLIENT_ID, PAYPAL_SECRET
    
    DB_HOST = config['database']['HOST']
    DB_PORT = config['database']['PORT']
    DB_DATABASE = config['database']['DATABASE']
    DB_USER = config['database']['USERNAME']
    DB_PASSWORD = config['database']['PASSWORD']

    PAYPAL_CLIENT_ID = config['paypal']['CLIENT_ID']
    PAYPAL_SECRET = config['paypal']['SECRET']

# parses a purchase object from the paypal transaction (return None if not a resource purchase)
def parse_resource_purchase(transaction):

    purchase = {}
    transaction_info = transaction['transaction_info']
    try:
        custom_field = transaction_info['custom_field']
        if custom_field.startswith('resource_purchase'):
            resource_id = custom_field[custom_field.rindex('|')+1:]
            purchase['resource_id'] = resource_id
        else:
            return None
        
        purchase['price'] = transaction_info['transaction_amount']['value']
        purchase['ts_purchased'] = transaction_info['transaction_initiation_date']
        
    except KeyError:
        return None
    
    payer_info = transaction['payer_info']
    try:
        purchase['email'] = payer_info['email_address']
    except KeyError:
        return None

    return purchase

# get a connection to the mariadb database
def get_database_connection():
    try:
        conn = mariadb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=int(DB_PORT),
            database=DB_DATABASE
        )
        return conn
    except mariadb.Error as e:
        logging.error(f"Could not connect to the database.")
        return None

# insert all purchases in a list to the database we are connected to
def insert_purchases_to_database(conn, purchases):
    cur = conn.cursor()

    for purchase in purchases:
        try:
            cur.execute(
            "insert into resource_purchase (ts_purchased, resource_id, price, email, server_id) values (?, ?, ?, ?, ?)", 
            (datetime.strptime(purchase['ts_purchased'], '%Y-%m-%dT%H:%M:%S%z'), purchase['resource_id'], purchase['price'], purchase['email'], 868241175688151051)
            )
        except mariadb.Error as e:
            print(f"Error: {e}")
    conn.commit() 

# the main function of the program that runs based on a scheduled task
def main_loop(sc): 

    # if current token is expired, get a new token
    if datetime.now() > ts_token_expires:
      get_paypal_token()

    # check the last time the main_loop was run
    lastrun = load_lastrun()
    if lastrun == None or lastrun['time'] == None:
        lastrun = {}
        lastrun['time'] = datetime.strptime('2019-12-02T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')

    # set initial start date to be the last time the program was run
    # set initial end date to be lastrun + 31 days (max of paypal api for range)
    start_date = lastrun['time']
    end_date = lastrun['time'] + timedelta(days=31)

    current_time = datetime.now()
    purchases = []

    # iterate by 31 day intervals from last run until we get to current time
    # store all purchases in objects as we go
    while end_date < current_time:

        print(start_date)
        print(end_date)

        transactions = get_transactions(start_date, end_date)
        for transaction in transactions['transaction_details']:
            purchase = parse_resource_purchase(transaction)
            if purchase != None:
                purchases.append(purchase)


        start_date = end_date
        end_date = start_date + timedelta(days=31)
    
    # after (potentially) looping, make sure end_date is current time if we overshot it and are now in the future
    if end_date > current_time:
        end_date = current_time

    # get purchases of the final, most recent time interval
    transactions = get_transactions(start_date, end_date)
    for transaction in transactions['transaction_details']:
        purchase = parse_resource_purchase(transaction)
        if purchase != None:
            purchases.append(purchase)

    # get a database connection, and if we cannot get one, exit without saving lastrun variable
    conn = get_database_connection()
    if conn == None:
        scheduler.enter((CHECK_INTERVAL*60), 1, main_loop, (sc,))
        return

    # save lastrun so that we can pick up from here the next time we run a check
    save_lastrun(current_time)

    # reschedule main_loop to run again if no new purchases were found
    if len(purchases) == 0:
        conn.close()
        scheduler.enter((CHECK_INTERVAL*60), 1, main_loop, (sc,))
        return
    else:
        logging.info(f"{len(purchases)} new purchases found.")

    # insert all purchases to database
    insert_purchases_to_database(conn, purchases)
    conn.close()

    # reschedule main_loop to run again at next interval
    scheduler.enter((CHECK_INTERVAL*60), 1, main_loop, (sc,))

# --- main thread ---

if __name__ == '__main__':

    # load all variables from the config.ini file
    load_config()

    # get an initial oauth token from paypal api
    get_paypal_token()

    #set the logging config
    logging.basicConfig(handlers=[logging.FileHandler('log_purchases.log', 'a+', 'utf-8')], level=logging.INFO, format='%(asctime)s: %(message)s')

    # create a new scheduler to run the main loop task every X minutes
    scheduler.enter(0, 1, main_loop, (scheduler,))
    scheduler.run()