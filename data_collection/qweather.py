#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional, Union

# 配置日志
logger = logging.getLogger(__name__)

# --- Constants ---
# 注意：这些是基于和风天气API风格的假设值，您需要根据您的实际API文档进行修改。
LOCATION_SEARCH_ENDPOINT = "https://geoapi.qweather.com/v2/city/lookup"
# 假设的历史天气API端点
HISTORICAL_WEATHER_ENDPOINT = "https://api.qweather.com/v7/historical/weather" 

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2

class APIKeyError(Exception):
    """Custom exception for missing API key."""
    pass

class APIRequestException(Exception):
    """Custom exception for API request errors."""
    pass

def get_location_id(city: str, api_key: str) -> Optional[str]:
    """
    (模拟/假设功能) 使用和风天气的GeoAPI查询城市的Location ID。
    在实际使用中，建议将查询到的ID缓存起来，避免重复查询。
    """
    params = {
        'location': city,
        'key': api_key,
        'number': 1 # 只���回最匹配的结果
    }
    try:
        response = requests.get(LOCATION_SEARCH_ENDPOINT, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if data.get('code') == '200' and data.get('location'):
            location_id = data['location'][0]['id']
            logger.info(f"成功获取城市 '{city}' 的 Location ID: {location_id}")
            return location_id
        else:
            logger.error(f"查询 Location ID 失败: {data.get('code')}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"查询 Location ID 时发生网络错误: {e}")
        return None

def get_daily_weather_data(location_id: str, date: str, api_key: str) -> Optional[Dict[str, Any]]:
    """
    (假设功能) 获取指定地点和单日的天气数据。
    """
    params = {
        'location': location_id,
        'date': date, # 格式: YYYYMMDD
        'key': api_key
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            # **重要提示**: 您需要将此处的 URL 替换为您的真实历史天气API端点
            response = requests.get(HISTORICAL_WEATHER_ENDPOINT, params=params, timeout=15)
            
            # 检查是否因为API不存在而返回404
            if response.status_code == 404:
                logger.error("API端点返回 404 Not Found。请在 qweather.py 中更新为您的真实历史天气API端点。")
                raise APIRequestException("Invalid API Endpoint")

            response.raise_for_status()
            data = response.json()

            # 和风天气的成功码通常是 '200'
            if data.get('code') == '200':
                return data
            else:
                logger.warning(f"API返回错误码: {data.get('code')}, 重试中... (尝试 {attempt + 1}/{MAX_RETRIES})")
                time.sleep(RETRY_DELAY_SECONDS)

        except requests.exceptions.RequestException as e:
            logger.warning(f"请求失败: {e}. 重试中... (尝试 {attempt + 1}/{MAX_RETRIES})")
            time.sleep(RETRY_DELAY_SECONDS)
            
    return None


def get_weather_data(city: str, lat: float, lon: float, year: int, api_key: str) -> Optional[Dict[str, Any]]:
    """
    通过迭代指定年份的每一天来获取全年的天气数据。
    """
    logger.warning("和风天气(QWeather)模块功能基于假设的API端点，可能需要您根据实际情况进行调整。")
    
    location_id = get_location_id(city, api_key)
    if not location_id:
        logger.error(f"无法获取 '{city}' 的 Location ID，跳过此城市。")
        return None

    full_year_data = {'daily': []}
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime('%Y%m%d')
        logger.debug(f"正在获取 {city} {date_str} 的数据...")
        
        daily_data = get_daily_weather_data(location_id, date_str, api_key)
        
        if daily_data and 'weatherDaily' in daily_data:
            full_year_data['daily'].append(daily_data['weatherDaily'])
        else:
            logger.error(f"获取 {city} {date_str} 的数据失败。将中止今年的数据采集以保证数据完整性。")
            return None # 如果一天失败，则全年数据不完整

        current_date += timedelta(days=1)
        time.sleep(0.1) # 遵守API请求频率限制

    return full_year_data


def process_weather_data(data: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    处理从API获取的原始数据，计算年度平均值。
    """
    if not data or 'daily' not in data or not data['daily']:
        logger.warning("没有提供有效数据进行处理。")
        return None, None

    total_avg_temp = 0
    total_precip = 0
    day_count = 0

    for daily_entry in data['daily']:
        try:
            # 假设���回的字段名为 tempMax, tempMin, precip
            temp_max = float(daily_entry['tempMax'])
            temp_min = float(daily_entry['tempMin'])
            precip = float(daily_entry.get('precip', 0.0))

            total_avg_temp += (temp_max + temp_min) / 2
            total_precip += precip
            day_count += 1
        except (ValueError, KeyError) as e:
            logger.warning(f"处理某天数据时出错，跳过该天: {e} - 数据: {daily_entry}")
            continue

    if day_count == 0:
        logger.error("无法从数据中处理任何有效的天数。")
        return None, None

    # 计算年度平均每日温度和年度平均日降水量
    avg_temp_year = total_avg_temp / day_count
    avg_precip_day_year = total_precip / day_count

    logger.info(f"数据处理完成: 年均气温={avg_temp_year:.2f}°C, 年均日降水量={avg_precip_day_year:.2f}mm")
    
    return avg_temp_year, avg_precip_day_year


def get_city_weather(province: str, city: str, year: int, api_key: str, lat: float, lon: float) -> Optional[List[Union[str, int, float]]]:
    """
    模块主函数，由中央调度器调用。
    """
    logger.info(f"使用和风天气(QWeather)模块处理 {province} - {city} ({year})")
    
    raw_data = get_weather_data(city, lat, lon, year, api_key)
    if not raw_data:
        return None
        
    avg_temp, avg_precip = process_weather_data(raw_data)
    
    if avg_temp is not None and avg_precip is not None:
        # 返回统一格式: [城市, 年份, 平均温度, 平均值]
        return [city, year, avg_temp, avg_precip]
    
    return None