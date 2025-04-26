#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import time
import requests
import csv
import logging
import threading
import queue
import pickle
import random
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any, Set, Union

# 导入断点管理器
from checkpoint_manager import CheckpointManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'qweather.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 全局变量
MAX_WORKERS = 5  # 最大并发线程数
MAX_RETRIES = 5  # 最大重试次数
BASE_WAIT_TIME = 1  # 基础等待时间（秒）
MAX_WAIT_TIME = 60  # 最大等待时间（秒）
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'cache')
STORAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'storage')
TARGET_YEARS = [2010, 2012, 2014, 2016, 2018, 2020, 2022]  # 目标年份
DATA_SOURCE = "qweather"  # 数据源名称

# 异常类定义
class APIRateLimitException(Exception):
    """API请求频率限制异常"""
    pass


class APIRequestException(Exception):
    """API请求异常"""
    pass


# 确保缓存和存储目录存在
def ensure_dirs():
    """
    确保必要的目录结构存在
    """
    for directory in [CACHE_DIR, STORAGE_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)


def load_config() -> Optional[str]:
    """
    从config.json加载和风天气API配置
    
    返回:
        str: API密钥
    """
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            return config['qweather']['apikey']
    except Exception as e:
        logger.error(f"加载配置文件时出错: {str(e)}")
        return None


def load_city_list() -> Dict[str, Dict[str, Dict[str, float]]]:
    """
    从city_list.json加载城市列表
    
    返回:
        Dict: 城市列表字典
    """
    try:
        city_list_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'city_list.json')
        with open(city_list_path, 'r', encoding='utf-8') as f:
            return json.load(f)['city']
    except Exception as e:
        logger.error(f"加载城市列表时出错: {str(e)}")
        return {}


def get_cache_key(city: str, year: int) -> str:
    """
    生成缓存键
    
    参数:
        city (str): 城市名
        year (int): 年份
        
    返回:
        str: 缓存键
    """
    return f"{city}_{year}"


def get_cache_path(cache_key: str) -> str:
    """
    获取缓存文件路径
    
    参数:
        cache_key (str): 缓存键
        
    返回:
        str: 缓存文件路径
    """
    return os.path.join(CACHE_DIR, f"{cache_key}.pkl")


def save_to_cache(cache_key: str, data: Any):
    """
    将数据保存到缓存
    
    参数:
        cache_key (str): 缓存键
        data (Any): 要缓存的数据
    """
    try:
        cache_path = get_cache_path(cache_key)
        with open(cache_path, 'wb') as f:
            pickle.dump(data, f)
        logger.debug(f"数据已缓存: {cache_key}")
    except Exception as e:
        logger.error(f"缓存数据时出错: {str(e)}")


def load_from_cache(cache_key: str) -> Optional[Any]:
    """
    从缓存加载数据
    
    参数:
        cache_key (str): 缓存键
        
    返回:
        Any: 缓存的数据，如果不存在则返回None
    """
    try:
        cache_path = get_cache_path(cache_key)
        if os.path.exists(cache_path):
            with open(cache_path, 'rb') as f:
                data = pickle.load(f)
            logger.debug(f"从缓存加载数据: {cache_key}")
            return data
        return None
    except Exception as e:
        logger.error(f"从缓存加载数据时出错: {str(e)}")
        return None


def calculate_exponential_backoff(retry_number: int, base_delay: float = 1.0, jitter: float = 0.5) -> float:
    """
    计算指数退避延迟时间
    
    参数:
        retry_number (int): 当前重试次数
        base_delay (float): 基础延迟时间（秒）
        jitter (float): 随机抖动因子，范围0-1
        
    返回:
        float: 计算后的延迟时间（秒）
    """
    # 计算指数退避时间: base_delay * 2^retry_number
    delay = base_delay * (2 ** retry_number)
    # 添加随机抖动，避免多个请求同时重试
    max_jitter = delay * jitter
    actual_jitter = random.uniform(0, max_jitter)
    
    return delay + actual_jitter


def format_date(date: datetime) -> str:
    """
    将日期格式化为和风天气API所需的格式 (YYYYMMDD)
    
    参数:
        date (datetime): 日期对象
        
    返回:
        str: 格式化后的日期字符串
    """
    return date.strftime("%Y%m%d")


def get_weather_data(city: str, lat: float, lon: float, year: int, api_key: str) -> Optional[Dict]:
    """
    从和风天气API获取指定城市和年份的历史天气数据，使用指数退避策略
    
    参数:
        city (str): 城市名
        lat (float): 纬度
        lon (float): 经度
        year (int): 年份
        api_key (str): API密钥
        
    返回:
        Dict: 包含天气数据的字典
    """
    # 检查缓存
    cache_key = get_cache_key(city, year)
    cached_data = load_from_cache(cache_key)
    if cached_data is not None:
        logger.info(f"使用缓存数据: {city} {year}年")
        return cached_data
    
    # 和风天气API需要按日期查询，所以我们需要遍历整年的每一天
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    current_date = start_date
    
    # 存储整年的天气数据
    yearly_data = []
    
    # 遍历整年的每一天
    while current_date <= end_date:
        date_str = format_date(current_date)
        
        # 构建API请求URL - 使用历史天气数据API
        url = f"https://api.qweather.com/v7/historical/weather"
        
        # 构建请求参数
        params = {
            'location': f"{lon},{lat}",  # 和风天气API使用经度,纬度格式
            'date': date_str,
            'key': api_key
        }
        
        # 使用指数退避策略进行重试
        for retry in range(MAX_RETRIES):
            try:
                response = requests.get(url, params=params, timeout=10)
                
                # 检查响应状态码
                if response.status_code == 200:
                    data = response.json()
                    
                    # 检查API返回的状态码
                    if data.get('code') == '200':
                        # 成功获取数据
                        daily_data = data.get('weatherDaily', [])
                        if daily_data:
                            # 添加日期信息
                            for item in daily_data:
                                item['date'] = date_str
                            yearly_data.extend(daily_data)
                        break
                    elif data.get('code') == '429':
                        # API请求频率限制
                        logger.warning(f"API请求频率限制，等待后重试: {city} {date_str}")
                        raise APIRateLimitException("API请求频率限制")
                    else:
                        # 其他API错误
                        logger.error(f"API错误: {data.get('code')} - {data.get('message')}")
                        raise APIRequestException(f"API错误: {data.get('code')} - {data.get('message')}")
                elif response.status_code == 429:
                    # HTTP 429 Too Many Requests
                    logger.warning(f"HTTP 429: 请求过多，等待后重试: {city} {date_str}")
                    raise APIRateLimitException("请求过多")
                else:
                    # 其他HTTP错误
                    logger.error(f"HTTP错误: {response.status_code} - {response.text}")
                    raise APIRequestException(f"HTTP错误: {response.status_code}")
                
            except (APIRateLimitException, requests.exceptions.RequestException) as e:
                # 计算等待时间
                wait_time = calculate_exponential_backoff(retry)
                wait_time = min(wait_time, MAX_WAIT_TIME)  # 限制最大等待时间
                
                logger.warning(f"请求失败，等待 {wait_time:.2f} 秒后重试 ({retry+1}/{MAX_RETRIES}): {city} {date_str} - {str(e)}")
                time.sleep(wait_time)
                
                # 最后一次重试失败
                if retry == MAX_RETRIES - 1:
                    logger.error(f"达到最大重试次数，放弃请求: {city} {date_str}")
                    return None
            
            except Exception as e:
                logger.error(f"未知错误: {str(e)}")
                return None
        
        # 移动到下一天
        current_date += timedelta(days=1)
        
        # 每次成功请求后短暂暂停，避免触发API限制
        time.sleep(random.uniform(0.5, 1.5))
    
    # 构建结果数据结构
    result = {
        "city": city,
        "year": year,
        "lat": lat,
        "lon": lon,
        "data": yearly_data
    }
    
    # 缓存结果
    save_to_cache(cache_key, result)
    
    return result


def process_city_year(city: str, province: str, city_info: Dict[str, float], year: int, api_key: str, checkpoint_manager: CheckpointManager) -> bool:
    """
    处理单个城市的单年数据
    
    参数:
        city (str): 城市名
        province (str): 省份名
        city_info (Dict[str, float]): 城市信息，包含经纬度
        year (int): 年份
        api_key (str): API密钥
        checkpoint_manager (CheckpointManager): 断点管理器
        
    返回:
        bool: 是否成功处理
    """
    try:
        # 检查该城市年份是否已处理
        if checkpoint_manager.is_completed(city, year, province):
            logger.info(f"跳过已处理的城市年份: {province} - {city} {year}年")
            return True
        
        # 标记为处理中
        checkpoint_manager.mark_in_progress(city, year, province)
        
        # 获取城市经纬度
        lat = city_info.get('lat')
        lon = city_info.get('lon')
        
        if lat is None or lon is None:
            logger.error(f"城市经纬度信息缺失: {city}")
            checkpoint_manager.mark_failed(city, year, province, "经纬度信息缺失")
            return False
        
        # 获取天气数据
        logger.info(f"开始获取天气数据: {province} - {city} {year}年")
        weather_data = get_weather_data(city, lat, lon, year, api_key)
        
        if weather_data is None:
            logger.error(f"获取天气数据失败: {province} - {city} {year}年")
            checkpoint_manager.mark_failed(city, year, province, "获取天气数据失败")
            return False
        
        # 保存数据到CSV文件
        output_dir = os.path.join(STORAGE_DIR, DATA_SOURCE, province, str(year))
        os.makedirs(output_dir, exist_ok=True)
        
        output_file = os.path.join(output_dir, f"{city}.csv")
        save_to_csv(weather_data, output_file)
        
        # 标记为已完成
        checkpoint_manager.mark_completed(city, year, province)
        logger.info(f"成功处理城市年份: {province} - {city} {year}年")
        
        return True
    
    except Exception as e:
        logger.error(f"处理城市年份时出错: {province} - {city} {year}年 - {str(e)}")
        checkpoint_manager.mark_failed(city, year, province, str(e))
        return False


def save_to_csv(weather_data: Dict[str, Any], output_file: str) -> None:
    """
    将天气数据保存为CSV文件
    
    参数:
        weather_data (Dict[str, Any]): 天气数据
        output_file (str): 输出文件路径
    """
    try:
        # 提取数据
        city = weather_data.get('city', '')
        year = weather_data.get('year', '')
        data = weather_data.get('data', [])
        
        if not data:
            logger.warning(f"没有数据可保存: {city} {year}年")
            return
        
        # 确定CSV列头
        # 注意：根据实际API返回的数据结构调整列头
        fieldnames = [
            'date', 'tempMax', 'tempMin', 'tempAvg',
            'humidity', 'precip', 'pressure', 'windSpeed',
            'windDir', 'cloud', 'uvIndex', 'vis'
        ]
        
        # 写入CSV文件
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for daily_data in data:
                # 提取需要的字段
                row = {field: daily_data.get(field, '') for field in fieldnames}
                writer.writerow(row)
        
        logger.info(f"数据已保存到: {output_file}")
    
    except Exception as e:
        logger.error(f"保存CSV文件时出错: {str(e)}")


def process_province_year(province: str, cities: Dict[str, Dict[str, float]], year: int, api_key: str) -> None:
    """
    处理单个省份的单年数据，使用线程池并行处理
    
    参数:
        province (str): 省份名
        cities (Dict[str, Dict[str, float]]): 城市信息字典
        year (int): 年份
        api_key (str): API密钥
    """
    # 创建断点管理器
    checkpoint_manager = CheckpointManager(DATA_SOURCE)
    
    # 创建线程池
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 提交任务
        future_to_city = {}
        for city, city_info in cities.items():
            # 检查是否已完成
            if checkpoint_manager.is_completed(city, year, province):
                logger.info(f"跳过已处理的城市: {province} - {city} {year}年")
                continue
            
            # 提交任务到线程池
            future = executor.submit(
                process_city_year,
                city, province, city_info, year, api_key, checkpoint_manager
            )
            future_to_city[future] = city
        
        # 处理结果
        for future in concurrent.futures.as_completed(future_to_city):
            city = future_to_city[future]
            try:
                success = future.result()
                if success:
                    logger.info(f"成功处理: {province} - {city} {year}年")
                else:
                    logger.error(f"处理失败: {province} - {city} {year}年")
            except Exception as e:
                logger.error(f"处理时发生异常: {province} - {city} {year}年 - {str(e)}")


def main():
    """
    主函数，协调整个数据收集过程
    """
    # 确保必要的目录存在
    ensure_dirs()
    
    # 加载API密钥
    api_key = load_config()
    if not api_key:
        logger.error("未找到API密钥，程序退出")
        return
    
    # 加载城市列表
    city_data = load_city_list()
    if not city_data:
        logger.error("未找到城市列表，程序退出")
        return
    
    # 处理每个省份和年份
    for province, cities in city_data.items():
        for year in TARGET_YEARS:
            logger.info(f"开始处理: {province} {year}年")
            process_province_year(province, cities, year, api_key)
            logger.info(f"完成处理: {province} {year}年")


if __name__ == "__main__":
    main()