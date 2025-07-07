#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple, Optional, Union

# 配置日志
logger = logging.getLogger(__name__)

# --- Constants ---
API_ENDPOINT = "https://history.openweathermap.org/data/2.5/history/city"
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5

class APIRequestException(Exception):
    """Custom exception for API request errors."""
    pass

def get_weather_data_for_month(lat: float, lon: float, year: int, month: int, api_key: str) -> Dict[str, Any]:
    """
    Fetches historical weather data for a specific month from the OpenWeather API.
    
    Args:
        lat: Latitude of the location.
        lon: Longitude of the location.
        year: The year for which to fetch data.
        month: The month (1-12) for which to fetch data.
        api_key: Your OpenWeather API key.
        
    Returns:
        A dictionary containing the API response data.
        
    Raises:
        APIRequestException: If the API request fails after multiple retries.
    """
    # Calculate start and end timestamps for the month
    start_dt = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end_dt = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end_dt = datetime(year, month + 1, 1, tzinfo=timezone.utc)
        
    start_timestamp = int(start_dt.timestamp())
    end_timestamp = int(end_dt.timestamp()) - 1 # End of the last day of the month

    params = {
        'lat': lat,
        'lon': lon,
        'type': 'hour',
        'start': start_timestamp,
        'end': end_timestamp,
        'appid': api_key,
        'units': 'metric'  # Request temperature in Celsius
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(API_ENDPOINT, params=params, timeout=20)
            response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
            logger.info(f"Successfully fetched data for {year}-{month:02d} for location ({lat}, {lon})")
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                logger.warning(f"Rate limit exceeded. Retrying in {RETRY_DELAY_SECONDS}s... (Attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY_SECONDS)
            else:
                raise APIRequestException(f"HTTP Error: {e.response.status_code} {e.response.text}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request failed: {e}. Retrying in {RETRY_DELAY_SECONDS}s... (Attempt {attempt + 1}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY_SECONDS)

    raise APIRequestException(f"API request failed for {year}-{month:02d} after {MAX_RETRIES} retries.")

def get_weather_data(city: str, lat: float, lon: float, year: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    Fetches and combines historical weather data for an entire year by iterating through months.
    """
    full_year_data = {'list': []}
    logger.info(f"Starting to fetch yearly data for {city} ({year})...")
    
    for month in range(1, 13):
        try:
            monthly_data = get_weather_data_for_month(lat, lon, year, month, api_key)
            if 'list' in monthly_data:
                full_year_data['list'].extend(monthly_data['list'])
            # A small delay to be respectful to the API
            time.sleep(1)
        except APIRequestException as e:
            logger.error(f"Failed to fetch data for {year}-{month:02d}. Aborting for this year. Reason: {e}")
            return None # If one month fails, we cannot calculate accurate yearly averages
            
    return full_year_data

def process_weather_data(data: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Processes the raw hourly data for a year to calculate annual averages.
    
    Args:
        data: A dictionary containing the raw 'list' of hourly data points.
        
    Returns:
        A tuple containing:
        - Average daily temperature for the year.
        - Average daily precipitation for the year.
    """
    if not data or 'list' not in data or not data['list']:
        logger.warning("No data provided to process.")
        return None, None

    daily_data = {} # Key: 'YYYY-MM-DD', Value: {'temps': [], 'precip': 0.0}

    for entry in data['list']:
        dt_object = datetime.fromtimestamp(entry['dt'], tz=timezone.utc)
        day_key = dt_object.strftime('%Y-%m-%d')

        if day_key not in daily_data:
            daily_data[day_key] = {'temps': [], 'precip': 0.0}
        
        # Aggregate temperature
        if 'main' in entry and 'temp' in entry['main']:
            daily_data[day_key]['temps'].append(entry['main']['temp'])
        
        # Aggregate precipitation (rain + snow)
        precip_mm = 0.0
        if 'rain' in entry and '1h' in entry['rain']:
            precip_mm += entry['rain']['1h']
        if 'snow' in entry and '1h' in entry['snow']:
            precip_mm += entry['snow']['1h']
        daily_data[day_key]['precip'] += precip_mm

    if not daily_data:
        logger.warning("Could not aggregate any daily data.")
        return None, None

    # Calculate daily averages and totals
    yearly_avg_temps = []
    yearly_total_precip = 0.0

    for day_key, values in daily_data.items():
        if values['temps']:
            avg_temp_day = sum(values['temps']) / len(values['temps'])
            yearly_avg_temps.append(avg_temp_day)
        
        yearly_total_precip += values['precip']

    if not yearly_avg_temps:
        logger.warning("No valid temperature data to calculate annual average.")
        return None, None

    # Calculate final annual averages
    avg_temp_year = sum(yearly_avg_temps) / len(yearly_avg_temps)
    # Average daily precipitation over the number of days for which we have data
    avg_precip_day_year = yearly_total_precip / len(daily_data)

    logger.info(f"Data processed: Avg Temp={avg_temp_year:.2f}°C, Avg Daily Precip={avg_precip_day_year:.2f}mm")
    
    return avg_temp_year, avg_precip_day_year

def get_city_weather(province: str, city: str, year: int, api_key: str, lat: float, lon: float) -> Optional[List[Union[str, int, float]]]:
    """
    Main function for the module. Fetches and processes weather data for a city.
    
    This function is called by the central dispatcher.
    """
    logger.info(f"Using OpenWeather module for {province} - {city} ({year})")
    
    raw_data = get_weather_data(city, lat, lon, year, api_key)
    if not raw_data:
        return None
        
    avg_temp, avg_precip = process_weather_data(raw_data)
    
    if avg_temp is not None and avg_precip is not None:
        # The result format [city, year, avg_temp, avg_value] must be consistent
        return [city, year, avg_temp, avg_precip]
    
    return None