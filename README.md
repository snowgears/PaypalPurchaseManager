# Paypal Purchase Manager
This is a python utility that gets recent purchases from the Paypal API and adds them to a defined database.

Because the Paypal API only allows 3 years of history, I setup a database to store all of the plugin purchases so that I can verify them later in discord.

# Steps for getting started:
- Install the libraries in requirements.txt
   - ```pip install --upgrade -r requirements.txt```
- Change the **config_example.ini** to **config.ini** and fill in variables

## Now just run the script:
``` 
python populate_new_purchases.py
```

The script is set to run every 60 minutes by default. When new purchases are found, the data will be logged and inserted into the database.
