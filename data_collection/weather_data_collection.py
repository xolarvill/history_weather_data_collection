#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import time
import logging
import threading
import queue
import random
import pickle
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, Optional, Any, Set, Union, Callable
from datetime import datetime
import csv

# 导入各API模块
import visualcrossing
import openweather
import qweather
from checkpoint_manager import CheckpointManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'weather_data_collection.log')),
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
DATA_SOURCE = "combined"  # 数据源名称

# API服务列表
API_SERVICES = ["visualcrossing", "openweather", "qweather"]


class APIRateLimitException(Exception):
    """API请求频率限制异常"""
    pass


class APIRequestException(Exception):
    """API请求异常"""
    pass


def ensure_dirs():
    """
    确保必要的目录结构存在
    """
    for directory in [CACHE_DIR, STORAGE_DIR]:
        if not os.path.exists(directory):
            os.makedirs(directory)


def load_config() -> Dict[str, str]:
    """
    从config.json加载所有API配置
    
    返回:
        Dict[str, str]: API名称到API密钥的映射
    """
    try:
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        api_keys = {}
        if 'visualcrossing' in config and 'apikey' in config['visualcrossing']:
            api_keys['visualcrossing'] = config['visualcrossing']['apikey']
        if 'openweather' in config and 'apikey' in config['openweather']:
            api_keys['openweather'] = config['openweather']['apikey']
        if 'qweather' in config and 'apikey' in config['qweather']:
            api_keys['qweather'] = config['qweather']['apikey']
            
        return api_keys
    except Exception as e:
        logger.error(f"加载配置文件时出错: {str(e)}")
        return {}


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


def get_cache_key(city: str, year: int, api_service: str = None) -> str:
    """
    生成缓存键
    
    参数:
        city (str): 城市名
        year (int): 年份
        api_service (str, optional): API服务名称，如果为None则表示综合数据
        
    返回:
        str: 缓存键
    """
    if api_service:
        return f"{api_service}_{city}_{year}"
    else:
        return f"combined_{city}_{year}"


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
    
    return min(delay + actual_jitter, MAX_WAIT_TIME)


def save_to_csv(data: List[List[Union[str, int, float]]], province: str, year: int) -> str:
    """
    将数据保存到CSV文件
    
    参数:
        data (List[List[Union[str, int, float]]]): 要保存的数据
        province (str): 省份名称
        year (int): 年份
        
    返回:
        str: 保存的文件路径
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{province}_weather_data_{year}_{timestamp}.csv"
    filepath = os.path.join(STORAGE_DIR, filename)
    
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            # 写入表头
            writer.writerow(['城市', '年份', '平均温度', '平均降水量/日照'])
            # 写入数据
            for row in data:
                writer.writerow(row)
        logger.info(f"数据已保存到: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"保存数据到CSV时出错: {str(e)}")
        return ""


def get_weather_data_with_fallback(city: str, province: str, year: int, lat: float, lon: float, 
                                  api_keys: Dict[str, str], api_order: List[str] = None) -> Optional[List[Union[str, int, float]]]:
    """
    使用多个API服务获取天气数据，如果一个API服务失败，则尝试下一个
    
    参数:
        city (str): 城市名称
        province (str): 省份名称
        year (int): 年份
        lat (float): 纬度
        lon (float): 经度
        api_keys (Dict[str, str]): API密钥字典
        api_order (List[str], optional): API服务尝试顺序
        
    返回:
        Optional[List[Union[str, int, float]]]: 处理后的天气数据
    """
    # 检查缓存
    cache_key = get_cache_key(city, year)
    cached_data = load_from_cache(cache_key)
    if cached_data is not None:
        logger.info(f"使用缓存数据: {city} {year}年")
        return cached_data
    
    # 如果没有指定API顺序，使用默认顺序
    if api_order is None:
        api_order = API_SERVICES.copy()
        # 随机打乱顺序，以分散负载
        random.shuffle(api_order)
    
    # 尝试每个API服务
    for api_service in api_order:
        if api_service not in api_keys or not api_keys[api_service]:
            logger.warning(f"跳过 {api_service}，未找到有效的API密钥")
            continue
            
        try:
            logger.info(f"尝试使用 {api_service} 获取 {city} {year}年 的数据")
            
            result = None
            if api_service == "visualcrossing":
                result = visualcrossing.get_city_weather(province, city, year, api_keys[api_service])
            elif api_service == "openweather":
                # 获取OpenWeather数据并处理
                weather_data = openweather.get_weather_data(city, lat, lon, year, api_keys[api_service])
                if weather_data:
                    avg_temp, avg_rain = openweather.process_weather_data(weather_data)
                    if avg_temp is not None:
                        result = [city, year, avg_temp, avg_rain]
            elif api_service == "qweather":
                # 获取和风天气数据并处理
                weather_data = qweather.get_weather_data(city, lat, lon, year, api_keys[api_service])
                if weather_data:
                    avg_temp, avg_precip = qweather.process_weather_data(weather_data)
                    if avg_temp is not None:
                        result = [city, year, avg_temp, avg_precip]
            
            if result:
                logger.info(f"成功使用 {api_service} 获取 {city} {year}年 的数据")
                # 缓存结果
                save_to_cache(cache_key, result)
                return result
            else:
                logger.warning(f"{api_service} 未能获取 {city} {year}年 的有效数据，尝试下一个API服务")
                
        except Exception as e:
            if "API请求频率限制" in str(e) or "rate limit" in str(e).lower():
                logger.warning(f"{api_service} API请求频率限制，尝试下一个API服务: {str(e)}")
            else:
                logger.error(f"使用 {api_service} 获取数据时出错: {str(e)}")
    
    # 所有API服务都失败
    logger.error(f"所有API服务都未能获取 {city} {year}年 的数据")
    return None


def worker(task_queue: queue.Queue, result_list: List, api_keys: Dict[str, str], 
          checkpoint_manager: CheckpointManager, lock: threading.Lock):
    """
    工作线程函数，处理任务队列中的任务
    
    参数:
        task_queue (queue.Queue): 任务队列
        result_list (List): 结果列表
        api_keys (Dict[str, str]): API密钥字典
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
            
            # 获取天气数据，自动尝试不同的API服务
            result = get_weather_data_with_fallback(city, province, year, lat, lon, api_keys)
            
            if result:
                # 添加结果
                with lock:
                    result_list.append(result)
                    logger.info(f"{province} - {city} {year}年 数据处理完成")
                    
                    # 更新完成状态
                    checkpoint_manager.mark_completed(city, year, province)
            else:
                # 标记失败
                checkpoint_manager.mark_failed(city, year, "所有API服务都未能获取数据", province)
                logger.error(f"{province} - {city} {year}年 数据获取失败")
        except Exception as e:
            logger.error(f"处理任务时出错: {str(e)}")
            # 如果有城市和年份信息，标记失败
            if 'city' in locals() and 'year' in locals() and 'province' in locals():
                checkpoint_manager.mark_failed(city, year, f"处理异常: {str(e)}", province)
        finally:
            task_queue.task_done()


def process_province_year(province: str, year: int, api_keys: Dict[str, str], max_workers: int = MAX_WORKERS):
    """
    处理指定省份和年份的天气数据
    
    参数:
        province (str): 省份名称
        year (int): 年份
        api_keys (Dict[str, str]): API密钥字典
        max_workers (int): 最大工作线程数
    """
    logger.info(f"\n===== 开始处理 {province} {year}年 天气数据 =====")
    
    # 加载城市列表
    city_data = load_city_list()
    if province not in city_data:
        logger.error(f"错误: 找不到省份 {province}")
        return
    
    cities = city_data[province]
    if not cities:
        logger.error(f"错误: {province} 没有城市数据")
        return
    
    # 创建断点管理器
    checkpoint_manager = CheckpointManager(DATA_SOURCE)
    
    # 计算总任务数
    total_tasks = len(cities)
    
    # 更新省份级别的总任务数
    checkpoint_manager.update_stats(total_tasks, province)
    
    # 获取已完成和失败的城市列表
    completed_cities = [city for city in cities if checkpoint_manager.is_completed(city, year, province)]
    failed_cities = [city for city in cities if checkpoint_manager.is_failed(city, year, province)]
    
    # 计算待处理城市
    pending_cities = [city for city in cities if city not in completed_cities and city not in failed_cities]
    
    logger.info(f"总城市数: {len(cities)}")
    logger.info(f"已处理城市数: {len(completed_cities)}")
    logger.info(f"失败城市数: {len(failed_cities)}")
    logger.info(f"待处理城市数: {len(pending_cities)}")
    
    # 检查是否所有城市都已处理
    if not pending_cities:
        logger.info(f"{province} {year}年的数据已全部处理完成")
        
        # 如果有已完成的城市，从缓存加载数据并保存
        if completed_cities:
            result_list = []
            for city in completed_cities:
                cache_key = get_cache_key(city, year)
                cached_data = load_from_cache(cache_key)
                if cached_data:
                    result_list.append(cached_data)
            
            if result_list:
                filepath = save_to_csv(result_list, province, year)
                logger.info(f"已完成城市的数据已保存到 {filepath}")
        
        return
    
    # 创建任务队列
    task_queue = queue.Queue()
    
    # 添加任务
    for city, coords in cities.items():
        if city in pending_cities:
            lat = coords.get("latitude")
            lon = coords.get("longitude")
            if lat is not None and lon is not None:
                task_queue.put((city, province, lat, lon, year))
            else:
                logger.warning(f"警告: {province} - {city} 没有经纬度信息，跳过")
                checkpoint_manager.mark_failed(city, year, "缺少经纬度信息", province)
    
    # 存储处理后的数据
    result_list = []
    lock = threading.Lock()
    
    # 创建并启动工作线程
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for _ in range(min(max_workers, task_queue.qsize())):
            executor.submit(worker, task_queue, result_list, api_keys, checkpoint_manager, lock)
    
    # 等待所有任务完成
    task_queue.join()
    
    # 获取统计信息
    stats = checkpoint_manager.get_stats(province)
    
    # 输出统计信息
    logger.info(f"\n===== 数据获取统计 =====")
    logger.info(f"总任务数: {stats['total_tasks']}")
    logger.info(f"成功获取数据的城市-年份对: {stats['completed_tasks']}")
    logger.info(f"失败的城市-年份对: {stats['failed_tasks']}")
    
    # 获取失败的城市列表
    failed_cities = [city for city in cities if checkpoint_manager.is_failed(city, year, province)]
    if failed_cities:
        logger.info("\n获取失败的城市列表:")
        for city in sorted(failed_cities):
            failure_reason = checkpoint_manager.get_failure_reason(city, year, province)
            logger.info(f"- {city}: {failure_reason}")
    
    # 保存数据到CSV文件
    if result_list:
        filepath = save_to_csv(result_list, province, year)
        logger.info(f"数据已保存到 {filepath}")
    else:
        logger.warning("没有新数据可保存")


def collect_all_data(provinces: List[str] = None, years: List[int] = None, max_workers: int = MAX_WORKERS):
    """
    收集所有省份和年份的天气数据
    
    参数:
        provinces (List[str], optional): 要处理的省份列表，如果为None则处理所有省份
        years (List[int], optional): 要处理的年份列表，如果为None则处理所有目标年份
        max_workers (int): 最大工作线程数
    """
    # 确保目录存在
    ensure_dirs()
    
    # 加载API密钥
    api_keys = load_config()
    if not api_keys:
        logger.error("错误: 未找到有效的API密钥，请检查config.json文件")
        return
    
    # 加载城市列表
    city_data = load_city_list()
    if not city_data:
        logger.error("错误: 城市列表为空，请检查city_list.json文件")
        return
    
    # 如果没有指定省份，使用所有省份
    if provinces is None:
        provinces = list(city_data.keys())
        # 排除香港、澳门、台湾地区
        provinces = [p for p in provinces if p not in ["香港特别行政区", "澳门特别行政区", "台湾省"]]
    
    # 如果没有指定年份，使用所有目标年份
    if years is None:
        years = TARGET_YEARS
    
    logger.info(f"开始收集天气数据")
    logger.info(f"处理的省份: {', '.join(provinces)}")
    logger.info(f"处理的年份: {', '.join(map(str, years))}")
    logger.info(f"可用的API服务: {', '.join([api for api in api_keys if api_keys[api]])}")
    
    # 处理每个省份和年份
    for province in provinces:
        for year in years:
            process_province_year(province, year, api_keys, max_workers)
            # 年份之间添加延时，避免API限制
            time.sleep(5)


def main():
    """
    主函数
    """
    # 收集所有数据
    collect_all_data()


if __name__ == "__main__":
    main() # 也可以导出使用