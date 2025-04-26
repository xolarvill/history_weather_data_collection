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
from typing import Dict, List, Tuple, Optional, Any, Set

# 导入断点管理器
from checkpoint_manager import CheckpointManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'openweather.log')),
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
DATA_SOURCE = "openweather"  # 数据源名称

# 确保缓存和存储目录存在
def ensure_dirs():
    """确保必要的目录结构存在"""
    for directory in [CACHE_DIR, STORAGE_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)


def load_config() -> Optional[str]:
    """
    从config.json加载OpenWeather API配置
    
    返回:
        str: API密钥
    """
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            return config['openweather']['apikey']
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


def unix_timestamp(year: int, month: int, day: int) -> int:
    """
    将日期转换为Unix时间戳
    
    参数:
        year (int): 年份
        month (int): 月份
        day (int): 日
        
    返回:
        int: Unix时间戳
    """
    dt = datetime(year, month, day)
    return int(dt.timestamp())


def get_weather_data(city: str, lat: float, lon: float, year: int, api_key: str) -> Optional[Dict]:
    """
    从OpenWeather API获取指定城市和年份的历史天气数据，使用指数退避策略
    
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
    
    # 设置日期范围
    start_date = unix_timestamp(year, 1, 1)  # 1月1日
    end_date = unix_timestamp(year, 12, 31)  # 12月31日
    
    # 构建API请求URL - 使用历史天气数据API
    base_url = "https://history.openweathermap.org/data/2.5/history/city"
    
    # 构建请求参数
    params = {
        'lat': lat,
        'lon': lon,
        'type': 'hour',
        'start': start_date,
        'end': end_date,
        'appid': api_key,
        'units': 'metric'  # 使用公制单位
    }
    
    # 使用指数退避策略进行重试
    for attempt in range(MAX_RETRIES):
        try:
            wait_time = min(BASE_WAIT_TIME * (2 ** attempt) + random.uniform(0, 1), MAX_WAIT_TIME)
            logger.info(f"正在请求OpenWeather API: {city} {year}年 (第{attempt+1}次尝试)...")
            
            response = requests.get(base_url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"成功获取天气数据: {city} {year}年")
                
                # 缓存数据
                save_to_cache(cache_key, data)
                
                return data
            elif response.status_code == 429:
                # 请求过多，API限制
                logger.warning(f"API请求频率限制，等待{wait_time:.2f}秒后重试...")
                time.sleep(wait_time)
            else:
                logger.error(f"获取数据失败: {response.status_code} - {response.text}")
                time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            logger.error(f"请求数据时出错: {str(e)}")
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"处理数据时出错: {str(e)}")
            time.sleep(wait_time)
    
    # 所有尝试都失败
    logger.error(f"所有尝试获取数据均失败: {city} {year}年")
    return None


def process_weather_data(data: Dict) -> Tuple[Optional[float], Optional[float]]:
    """
    处理API返回的天气数据，计算年平均温度和降水量
    
    参数:
        data (Dict): API返回的原始天气数据
        
    返回:
        Tuple[Optional[float], Optional[float]]: (平均温度, 平均降水量)
    """
    if not data:
        logger.error("错误: 传入的数据为空")
        return None, None
    
    if 'list' not in data:
        logger.error(f"错误: 数据中没有'list'字段，返回的数据结构: {list(data.keys())}")
        return None, None
    
    weather_list = data['list']
    if not weather_list or len(weather_list) == 0:
        logger.error("错误: 天气数据列表为空")
        return None, None
    
    # 计算有效天数（包含温度数据的天数）
    valid_temp_days = [day for day in weather_list if 'main' in day and 'temp' in day['main'] and day['main']['temp'] is not None]
    valid_rain_days = [day for day in weather_list if 'rain' in day and '1h' in day['rain'] and day['rain']['1h'] is not None]
    
    if len(valid_temp_days) == 0:
        logger.error("错误: 没有有效的温度数据")
        avg_temp = None
    else:
        # 计算平均温度
        total_temp = sum(day['main']['temp'] for day in valid_temp_days)
        avg_temp = total_temp / len(valid_temp_days)
        logger.info(f"成功计算平均温度，基于{len(valid_temp_days)}/{len(weather_list)}小时的有效数据")
    
    if len(valid_rain_days) == 0:
        logger.info("注意: 没有有效的降水量数据，可能是因为该时期没有降水")
        avg_rain = 0  # 如果没有降水数据，默认为0
    else:
        # 计算平均降水量 (mm/h)
        total_rain = sum(day['rain']['1h'] for day in valid_rain_days)
        avg_rain = total_rain / len(weather_list)  # 除以总小时数，得到平均每小时降水量
        logger.info(f"成功计算平均降水量，基于{len(valid_rain_days)}/{len(weather_list)}小时的有效数据")
    
    return avg_temp, avg_rain


def worker(task_queue: queue.Queue, result_list: List, api_key: str, checkpoint_manager: CheckpointManager, lock: threading.Lock):
    """
    工作线程函数，处理任务队列中的任务
    
    参数:
        task_queue (queue.Queue): 任务队列
        result_list (List): 结果列表
        api_key (str): API密钥
        checkpoint_manager (CheckpointManager): 断点管理器
        lock (threading.Lock): 线程锁
    """
    while not task_queue.empty():
        try:
            # 获取任务
            city, province, lat, lon, year = task_queue.get()
            
            # 检查是否已完成
            if checkpoint_manager.is_completed(city, year, province):
                logger.info(f"跳过已完成的任务: {province} - {city} {year}年")
                task_queue.task_done()
                continue
            
            logger.info(f"处理任务: {province} - {city} {year}年")
            
            # 获取天气数据
            weather_data = get_weather_data(city, lat, lon, year, api_key)
            
            if weather_data:
                # 处理数据
                avg_temp, avg_rain = process_weather_data(weather_data)
                
                if avg_temp is not None:
                    # 添加结果
                    with lock:
                        result_list.append([province, city, year, avg_temp, avg_rain])
                        logger.info(f"{province} - {city} {year}年 数据处理完成: 平均温度 {avg_temp:.2f}°C, 平均降水量 {avg_rain:.2f} mm/h")
                        
                        # 更新完成状态
                        checkpoint_manager.mark_completed(city, year, province)
                else:
                    # 标记失败
                    checkpoint_manager.mark_failed(city, year, "处理数据失败：无有效温度数据", province)
            else:
                # 标记失败
                checkpoint_manager.mark_failed(city, year, "获取数据失败", province)
                logger.error(f"{province} - {city} {year}年 数据获取失败")
        except Exception as e:
            logger.error(f"处理任务时出错: {str(e)}")
            # 如果有城市和年份信息，标记失败
            if 'city' in locals() and 'year' in locals() and 'province' in locals():
                checkpoint_manager.mark_failed(city, year, f"处理异常: {str(e)}", province)
        finally:
            task_queue.task_done()


def save_to_csv(data: List, filename: str):
    """
    将处理后的数据保存到CSV文件
    
    参数:
        data (List): 处理后的数据列表，每个元素为[省份, 城市, 年份, 平均温度, 平均降水量]
        filename (str): 文件名
    """
    filepath = os.path.join(STORAGE_DIR, filename)
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['province', 'city', 'year', 'avg_temperature', 'avg_rainfall'])
        for row in data:
            writer.writerow(row)
    
    logger.info(f"数据已保存到 {filepath}")


def main():
    """
    主函数：获取全国各城市指定年份的天气数据并保存
    """
    # 确保目录存在
    ensure_dirs()
    
    # 获取API密钥并验证
    api_key = load_config()
    if not api_key or len(api_key.strip()) == 0:
        logger.error("错误: API密钥为空，请检查config.json文件")
        return
    
    # 加载城市列表
    city_data = load_city_list()
    if not city_data:
        logger.error("错误: 城市列表为空，请检查city_list.json文件")
        return
    
    # 创建断点管理器
    checkpoint_manager = CheckpointManager(DATA_SOURCE)
    
    # 创建任务队列
    task_queue = queue.Queue()
    
    # 计算总任务数
    total_tasks = 0
    
    # 添加任务
    for province, cities in city_data.items():
        # 跳过香港、澳门、台湾地区
        if province in ["香港特别行政区", "澳门特别行政区", "台湾省"]:
            continue
            
        # 更新省份级别的总任务数
        province_total_tasks = len(cities) * len(TARGET_YEARS)
        checkpoint_manager.update_stats(province_total_tasks, province)
        total_tasks += province_total_tasks
        
        for city, coords in cities.items():
            for year in TARGET_YEARS:
                # 检查是否已完成
                if checkpoint_manager.is_completed(city, year, province):
                    logger.info(f"跳过已完成的任务: {province} - {city} {year}年")
                    continue
                    
                # 添加任务
                task_queue.put((city, province, coords["latitude"], coords["longitude"], year))
    
    # 更新总任务数
    checkpoint_manager.update_stats(total_tasks)
    
    # 如果没有任务，直接返回
    if task_queue.empty():
        logger.info("没有需要处理的任务")
        return
    
    logger.info(f"总任务数: {task_queue.qsize()}")
    logger.info(f"API密钥: {api_key[:4]}{'*' * (len(api_key) - 8)}{api_key[-4:]}")
    logger.info(f"目标年份: {', '.join(map(str, TARGET_YEARS))}")
    
    # 存储处理后的数据
    result_list = []
    lock = threading.Lock()
    
    # 创建并启动工作线程
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for _ in range(MAX_WORKERS):
            executor.submit(worker, task_queue, result_list, api_key, checkpoint_manager, lock)
    
    # 等待所有任务完成
    task_queue.join()
    
    # 获取统计信息
    stats = checkpoint_manager.get_stats()
    
    # 输出统计信息
    logger.info(f"\n===== 数据获取统计 =====")
    logger.info(f"总任务数: {stats['total_tasks']}")
    logger.info(f"成功获取数据的城市-年份对: {stats['completed_tasks']}")
    logger.info(f"失败的城市-年份对: {stats['failed_tasks']}")
    
    # 保存数据到CSV文件
    if result_list:
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"weather_openweather_{timestamp}.csv"
        save_to_csv(result_list, filename)
    else:
        logger.warning("没有数据可保存")


if __name__ == "__main__":
    main()