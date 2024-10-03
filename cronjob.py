from playconsole_sales_parser import sales_parser
from apscheduler.schedulers.blocking import BlockingScheduler
from Google_Ads_Api import ads_data_parser
from Google_Analytics_Api import analytics_data_parser
from Facebook_Api import facebook_data_parser
from Google_Admob_Api import admob_data_parser
import combine_by_app
import combine_by_country
from datetime import date, datetime

import pandas as pd
import time
import os

# Generate today's date in YYYYMMDD format
today_date = datetime.today().strftime("%Y-%m-%d")
date = today_date[0:4]+today_date[5:7]+today_date[8:10]

# Function to fetch Google Ads data
def fetch_ads_data():
    print("Fetching Ads job")
    try:
        os.remove("C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Ads_Api/Google_Ads_All_Data.csv")
    except FileNotFoundError:
        print("Google Ads data file not found. Skipping deletion.")
    
    df = ads_data_parser.fetch_data()
    df.to_csv(
        f"C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Ads_Api/{date}_Google_Ads_All_Data.csv", index=False)
    df.to_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Ads_Api/Google_Ads_All_Data.csv", index=False)

# Function to fetch Google Analytics data
def fetch_analytics_data():
    print("Fetching Analytics Job")
    appended = False

    app_ids = {
        "AnalyticsAppName": "Appid",
    }

    for app_name, app_id in app_ids.items():
        df = analytics_data_parser.fetch_data(
            app_name, app_id, "C:/revenue/Apps-Revenue-DataPipeline/Google_Analytics_Api/service_account.json")
        
        # Write data to CSV (appending on subsequent runs)
        if not appended:
            df.to_csv(
                f"C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Analytics_Api/{date}_Google_Analytics_All_Data.csv", index=False)
            df.to_csv(
                "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Analytics_Api/Google_Analytics_All_Data.csv", index=False)
            appended = True
        else:
            df.to_csv(
                f"C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Analytics_Api/{date}_Google_Analytics_All_Data.csv", mode="a", index=False, header=False)
            df.to_csv(
                "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Analytics_Api/Google_Analytics_All_Data.csv", mode="a", index=False, header=False)
    
    print("Fetching Analytics Job Completed")

# Function to fetch Facebook Ads data
def fetch_facebook_data():
    print("Fetching Facebook Job")
    df = facebook_data_parser.fetch_data()
    df.to_csv(
        f"C:/revenue/Apps-Revenue-DataPipeline/datasheets/Facebook_Ads_Api/{date}_Facebook_Ads_All_Data.csv", index=False)
    df.to_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Facebook_Ads_Api/Facebook_Ads_All_Data.csv", index=False)

# Function to fetch AdMob data
def fetch_admob_data():
    print("Fetching Admob Job")
    df = admob_data_parser.fetch_data(
        'C:/revenue/Apps-Revenue-DataPipeline/Google_Admob_Api/token.pickle', 
        'C:/revenue/Apps-Revenue-DataPipeline/Google_Admob_Api/credentials.json')
    
    df.to_csv(
        f"C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Admob_Api/{date}_Google_Admob_All_Data.csv", index=False)
    df.to_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Admob_Api/Google_Admob_All_Data.csv", index=False)

# Function to combine data by app
def data_joiner_by_app():
    print("Joining data by App")
    facebook = pd.read_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Facebook_Ads_Api/Facebook_Ads_All_Data.csv")
    google = pd.read_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Ads_Api/Google_Ads_All_Data.csv")
    admob = pd.read_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Admob_Api/Google_Admob_All_Data.csv")
    playconsole = pd.read_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/PlayConsole_Api/Play_Console_All_Data.csv")

    # Combine data by app
    by_app = combine_by_app.combine_by_app(google, admob, facebook, playconsole)
    by_app.to_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Combined_Data_By_App/combined_data_by_app.csv")
    by_app.to_csv(
        f"C:/revenue/Apps-Revenue-DataPipeline/datasheets/Combined_Data_By_App/{date}_combined_data_by_app.csv")

# Function to combine data by country
def data_joiner_by_country():
    print("Joining data by Country")
    facebook = pd.read_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Facebook_Ads_Api/Facebook_Ads_All_Data.csv")
    google = pd.read_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Ads_Api/Google_Ads_All_Data.csv")
    admob = pd.read_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Google_Admob_Api/Google_Admob_All_Data.csv")
    playconsole = pd.read_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/PlayConsole_Api/Play_Console_All_Data.csv")

    # Combine data by country
    by_country = combine_by_country.combine_by_country(google, admob, facebook, playconsole)
    by_country.to_csv(
        "C:/revenue/Apps-Revenue-DataPipeline/datasheets/Combined_Data_By_Country/combined_data_by_country.csv")
    by_country.to_csv(
        f"C:/revenue/Apps-Revenue-DataPipeline/datasheets/Combined_Data_By_Country/{date}_combined_data_by_country.csv")

# Function to fetch Play Console data
def playconsole_data():
    print("Fetching PlayConsole Data")
    df = sales_parser()
    if not df.empty:
        df.to_csv(
            f"C:/revenue/Apps-Revenue-DataPipeline/datasheets/PlayConsole_Api/{date}_Play_Console_All_Data.csv", index=False)
        df.to_csv(
            "C:/revenue/Apps-Revenue-DataPipeline/datasheets/PlayConsole_Api/Play_Console_All_Data.csv", index=False)

# Function to fetch all data and run all the jobs
def fetch_all():
    fetch_ads_data()
    fetch_facebook_data()
    fetch_admob_data()
    playconsole_data()
    data_joiner_by_app()
    data_joiner_by_country()
    fetch_analytics_data()
    print("Cronjob Completed\n")

# Run all jobs
fetch_all()