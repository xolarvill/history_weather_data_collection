#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import requests
import csv
import os
import time
import random
import hashlib
import concurrent.futures
import threading
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional, Union, Set
from functools import lru_cache

# 导入断点管理器
from checkpoint_manager import CheckpointManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'visualcrossing.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据源名称
DATA_SOURCE = "visualcrossing"


class APIRateLimitException(Exception):
    """API请求频率限制异常"""
    pass


class APIRequestException(Exception):
    """API请求异常"""
    pass


def load_config() -> Dict[str, Any]:
    """
    加载配置文件，获取API密钥和其他配置信息
    
    返回:
        Dict[str, Any]: 包含API配置信息的字典
    """
    try:
        config_path = Path('config.json')
        if not config_path.exists():
            # 尝试查找上级目录中的配置文件
            config_path = Path('../config.json')
        
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        return config['visualcrossing']
    except Exception as e:
        raise FileNotFoundError(f"加载配置文件失败: {e}")


def load_city_list() -> Dict[str, Any]:
    """
    加载城市列表数据
    
    返回:
        Dict[str, Any]: 包含城市信息的字典
    """
    try:
        city_list_path = Path('city_list.json')
        if not city_list_path.exists():
            # 尝试查找上级目录中的城市列表文件
            city_list_path = Path('../city_list.json')
        
        with open(city_list_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        raise FileNotFoundError(f"加载城市列表失败: {e}")


# 以下函数已被CheckpointManager替代，保留函数签名以兼容旧代码
def load_checkpoint(province: str, year: int) -> Dict[str, Any]:
    """
    加载断点记录，获取已处理的城市信息（已被CheckpointManager替代）
    
    参数:
        province (str): 省份名称
        year (int): 年份
        
    返回:
        Dict[str, Any]: 包含断点信息的字典
    """
    logger.warning("使用了已弃用的load_checkpoint函数，请使用CheckpointManager")
    # 创建一个兼容的返回结构
    return {
        "province": province,
        "year": year,
        "processed_cities": [],
        "failed_cities": [],
        "last_update": datetime.now().isoformat(),
        "is_completed": False
    }


# 以下函数已被CheckpointManager替代，保留函数签名以兼容旧代码
def save_checkpoint(checkpoint_data: Dict[str, Any]) -> None:
    """
    保存断点记录（已被CheckpointManager替代）
    
    参数:
        checkpoint_data (Dict[str, Any]): 断点数据
    """
    logger.warning("使用了已弃用的save_checkpoint函数，请使用CheckpointManager")
    # 此函数不再执行实际操作
    pass


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


def get_cache_key(location: str, year: int) -> str:
    """
    生成缓存键
    
    参数:
        location (str): 位置信息
        year (int): 年份
        
    返回:
        str: 缓存键
    """
    # 使用MD5生成唯一的缓存键
    key_str = f"{location}_{year}"
    return hashlib.md5(key_str.encode()).hexdigest()


def get_cache_path(cache_key: str) -> Path:
    """
    获取缓存文件路径
    
    参数:
        cache_key (str): 缓存键
        
    返回:
        Path: 缓存文件路径
    """
    cache_dir = Path('storage/cache')
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{cache_key}.json"


def save_to_cache(cache_key: str, data: Dict[str, Any]) -> None:
    """
    将数据保存到缓存
    
    参数:
        cache_key (str): 缓存键
        data (Dict[str, Any]): 要缓存的数据
    """
    try:
        cache_path = get_cache_path(cache_key)
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        logger.error(f"保存缓存失败: {str(e)}")


def load_from_cache(cache_key: str) -> Optional[Dict[str, Any]]:
    """
    从缓存加载数据
    
    参数:
        cache_key (str): 缓存键
        
    返回:
        Optional[Dict[str, Any]]: 缓存的数据，如果缓存不存在则返回None
    """
    try:
        cache_path = get_cache_path(cache_key)
        if cache_path.exists():
            with open(cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"读取缓存失败: {str(e)}")
    return None


@lru_cache(maxsize=128)
def get_weather_data(location: str, year: int, api_key: str, max_retries: int = 5, base_delay: float = 1.0) -> Optional[Dict[str, Any]]:
    """
    从VisualCrossing API获取指定位置和年份的天气数据
    
    参数:
        location (str): 位置信息（城市名称或经纬度）
        year (int): 年份
        api_key (str): API密钥
        max_retries (int): 最大重试次数
        base_delay (float): 基础延迟时间（秒）
        
    返回:
        Optional[Dict[str, Any]]: 包含天气数据的字典，获取失败则返回None
        
    异常:
        APIRateLimitException: API请求频率超出限制
        APIRequestException: API请求出错
    """
    # 检查缓存
    cache_key = get_cache_key(location, year)
    cached_data = load_from_cache(cache_key)
    if cached_data:
        logger.info(f"从缓存加载 {location} {year}年 的数据")
        return cached_data
    
    # 构建API请求URL
    base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
    start_date = f"{year}-01-01"
    end_date = f"{year}-12-31"
    
    # 构建请求参数
    params = {
        'unitGroup': 'metric',     # 使用公制单位
        'key': api_key,
        'include': 'days',         # 只包含每日数据
        'elements': 'datetime,temp,tempmax,tempmin,humidity,precip,precipcover,solarradiation,solarenergy,uvindex,sunrise,sunset',
        'contentType': 'json',     # 明确指定返回JSON格式
        'lang': 'zh'               # 使用中文返回可翻译的字段
    }
    
    retries = 0
    while retries < max_retries:
        try:
            # 对位置进行URL编码
            encoded_location = requests.utils.quote(location)
            url = f"{base_url}/{encoded_location}/{start_date}/{end_date}"
            
            # 计算指数退避延迟
            if retries > 0:
                delay = calculate_exponential_backoff(retries - 1, base_delay)
                logger.info(f"尝试请求: {location} (第{retries+1}次尝试，等待{delay:.2f}秒后)")
                time.sleep(delay)
            else:
                logger.info(f"尝试请求: {location} (第{retries+1}次尝试)")
            
            response = requests.get(url, params=params, timeout=30)  # 添加超时设置
            
            if response.status_code == 200:
                logger.info(f"成功获取 {location} 的数据")
                data = response.json()
                # 保存到缓存
                save_to_cache(cache_key, data)
                return data
            elif response.status_code == 429:
                # 请求过多，API限制
                logger.warning(f"API请求频率限制，将使用指数退避策略重试...")
                retries += 1
                # 如果已经到达最大重试次数，抛出异常
                if retries >= max_retries:
                    raise APIRateLimitException(f"API请求频率限制，达到最大重试次数({max_retries})")
            else:
                error_msg = f"获取数据失败: HTTP {response.status_code} - {response.text}"
                logger.error(error_msg)
                retries += 1
        except requests.exceptions.Timeout:
            logger.warning(f"请求超时，将使用指数退避策略重试...")
            retries += 1
        except requests.exceptions.RequestException as e:
            logger.warning(f"请求出错: {str(e)}，将使用指数退避策略重试...")
            retries += 1
        except Exception as e:
            logger.warning(f"处理数据时出错: {str(e)}，将使用指数退避策略重试...")
            retries += 1
    
    # 所有尝试都失败
    raise APIRequestException(f"获取 {location} 的数据失败，已尝试 {max_retries} 次")


def process_weather_data(data: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    处理API返回的天气数据，计算年平均温度和日照数据
    
    参数:
        data (Dict[str, Any]): API返回的原始天气数据
        
    返回:
        Tuple[Optional[float], Optional[float]]: (平均温度, 平均日照能量)
    """
    if not data:
        logger.error("错误: 传入的数据为空")
        return None, None
    
    if 'days' not in data:
        logger.error(f"错误: 数据中没有'days'字段，返回的数据结构: {list(data.keys())}")
        return None, None
    
    days = data['days']
    if not days or len(days) == 0:
        logger.error("错误: 天气数据天数为0")
        return None, None
    
    # 计算有效天数（包含温度数据的天数）
    valid_temp_days = [day for day in days if 'temp' in day and day['temp'] is not None]
    valid_solar_days = [day for day in days if 'solarenergy' in day and day['solarenergy'] is not None]
    
    if len(valid_temp_days) == 0:
        logger.error("错误: 没有有效的温度数据")
        avg_temp = None
    else:
        # 计算平均温度
        total_temp = sum(day['temp'] for day in valid_temp_days)
        avg_temp = total_temp / len(valid_temp_days)
        logger.info(f"成功计算平均温度，基于{len(valid_temp_days)}/{len(days)}天的有效数据")
    
    if len(valid_solar_days) == 0:
        logger.error("错误: 没有有效的日照能量数据")
        avg_solar_energy = None
    else:
        # 计算平均日照能量 (solarenergy，单位: MJ/m²)
        total_solar_energy = sum(day['solarenergy'] for day in valid_solar_days)
        avg_solar_energy = total_solar_energy / len(valid_solar_days)
        logger.info(f"成功计算平均日照能量，基于{len(valid_solar_days)}/{len(days)}天的有效数据")
    
    return avg_temp, avg_solar_energy


def save_to_csv(data: List[List[Union[str, int, float]]], province: str, year: int) -> str:
    """
    将处理后的数据保存为CSV文件
    
    参数:
        data (List[List[Union[str, int, float]]]): 包含城市天气数据的列表
        province (str): 省份名称
        year (int): 年份
        
    返回:
        str: 保存的文件路径
    """
    # 确保storage目录存在
    storage_dir = Path('storage')
    storage_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{province}_weather_data_{year}_{timestamp}.csv"
    filepath = storage_dir / filename
    
    with open(filepath, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # 写入表头
        writer.writerow(['province', 'city', 'year', 'avg_temperature', 'avg_solar_energy'])
        # 写入数据
        for row in data:
            writer.writerow([province] + row)
    
    logger.info(f"数据已保存到 {filepath}")
    return str(filepath)


def get_city_weather(province: str, city: str, year: int, api_key: str) -> Optional[List[Union[str, int, float]]]:
    """
    获取单个城市的天气数据
    
    参数:
        province (str): 省份名称
        city (str): 城市名称
        year (int): 年份
        api_key (str): API密钥
        
    返回:
        Optional[List[Union[str, int, float]]]: [城市名, 年份, 平均温度, 平均日照能量]
    """
    logger.info(f"\n正在获取{province} - {city}的天气数据...")
    
    # 尝试不同的位置格式
    location_formats = [
        # 1. 使用经纬度（如果有）
        None,  # 占位符，将在后面填充
        # 2. 使用英文名称+省份+国家
        f"{city},China",
        # 3. 使用中文名称
        city
    ]
    
    # 获取城市数据
    try:
        city_data = load_city_list()
        if province in city_data["city"] and city in city_data["city"][province]:
            city_info = city_data["city"][province][city]
            if "latitude" in city_info and "longitude" in city_info:
                # 使用经纬度格式: 纬度,经度
                lat = city_info["latitude"]
                lon = city_info["longitude"]
                location_formats[0] = f"{lat},{lon}"
            else:
                logger.warning(f"警告: {province} - {city} 没有经纬度信息")
                location_formats[0] = None
        else:
            logger.warning(f"警告: 在城市列表中找不到 {province} - {city}")
            location_formats[0] = None
    except Exception as e:
        logger.error(f"获取城市信息时出错: {str(e)}")
        location_formats[0] = None
    
    # 过滤掉None值
    location_formats = [loc for loc in location_formats if loc is not None]
    
    # 尝试不同的位置格式获取数据
    weather_data = None
    last_error = None
    
    for location in location_formats:
        try:
            weather_data = get_weather_data(location, year, api_key)
            if weather_data:
                break  # 成功获取数据，跳出循环
        except APIRateLimitException as e:
            logger.warning(f"API请求频率限制: {e}")
            # 这是一个严重错误，需要立即中断并等待
            raise
        except APIRequestException as e:
            last_error = e
            logger.warning(f"尝试位置 '{location}' 失败，尝试下一个位置格式")
        except Exception as e:
            last_error = e
            logger.warning(f"尝试位置 '{location}' 时发生错误: {str(e)}，尝试下一个位置格式")
    
    if not weather_data:
        if last_error:
            logger.error(f"{province} - {city} 数据获取失败: {last_error}")
        else:
            logger.error(f"{province} - {city} 数据获取失败: 所有位置格式均失败")
        return None
    
    # 处理数据
    try:
        avg_temp, avg_solar = process_weather_data(weather_data)
        
        if avg_temp is not None and avg_solar is not None:
            result = [city, year, avg_temp, avg_solar]
            logger.info(f"{province} - {city} 数据处理完成: 平均温度 {avg_temp:.2f}°C, 平均日照能量 {avg_solar:.2f} MJ/m²")
            return result
        else:
            logger.error(f"{province} - {city} 数据处理失败: 无法计算平均值")
            return None
    except Exception as e:
        logger.error(f"{province} - {city} 数据处理时出错: {str(e)}")
        return None


def process_city_batch(province: str, cities: List[str], year: int, api_key: str, 
                     checkpoint_manager: CheckpointManager, lock: threading.Lock) -> List[List[Union[str, int, float]]]:
    """
    处理一批城市的天气数据
    
    参数:
        province (str): 省份名称
        cities (List[str]): 城市列表
        year (int): 年份
        api_key (str): API密钥
        checkpoint_manager (CheckpointManager): 断点管理器
        lock (threading.Lock): 线程锁，用于安全更新共享数据
        
    返回:
        List[List[Union[str, int, float]]]: 处理后的数据列表
    """
    processed_data = []
    
    for city in cities:
        try:
            # 检查是否已完成
            if checkpoint_manager.is_completed(city, year, province):
                logger.info(f"跳过已完成的任务: {province} - {city} {year}年")
                continue
                
            logger.info(f"处理任务: {province} - {city} {year}年")
            result = get_city_weather(province, city, year, api_key)
            
            if result:
                processed_data.append(result)
                # 使用线程锁确保线程安全
                with lock:
                    # 标记为已完成
                    checkpoint_manager.mark_completed(city, year, province)
                logger.info(f"{province} - {city} {year}年 数据处理完成: 平均温度 {result[2]:.2f}°C, 平均日照能量 {result[3]:.2f} MJ/m²")
            else:
                # 使用线程锁确保线程安全
                with lock:
                    # 标记为失败
                    checkpoint_manager.mark_failed(city, year, "获取数据失败", province)
                logger.error(f"{province} - {city} {year}年 数据获取失败")
                
        except APIRateLimitException as e:
            logger.warning(f"API请求频率限制: {e}，暂停处理")
            # 使用线程锁确保线程安全
            with lock:
                # 标记为失败
                checkpoint_manager.mark_failed(city, year, f"API请求频率限制: {e}", province)
            # 等待一段时间后继续下一个城市
            time.sleep(10)
        except Exception as e:
            logger.error(f"处理 {city} 时发生错误: {str(e)}")
            # 使用线程锁确保线程安全
            with lock:
                # 标记为失败
                checkpoint_manager.mark_failed(city, year, f"处理异常: {str(e)}", province)
    
    return processed_data


def get_province_weather(province: str, year: int, api_key: str, max_workers: int = 5, max_api_calls: int = 0) -> None:
    """
    获取指定省份所有城市的天气数据，支持并行处理
    
    参数:
        province (str): 省份名称
        year (int): 年份
        api_key (str): API密钥
        max_workers (int): 最大并行工作线程数
        max_api_calls (int): 单次运行的最大API调用次数，0表示不限制
    """
    logger.info(f"\n===== 开始获取{province} {year}年天气数据 =====")
    
    # 加载城市列表
    try:
        city_data = load_city_list()
        if province not in city_data["city"]:
            logger.error(f"错误: 找不到省份 {province}")
            return
        
        cities = list(city_data["city"][province].keys())
        if not cities:
            logger.error(f"错误: {province} 没有城市数据")
            return
    except Exception as e:
        logger.error(f"加载城市列表失败: {str(e)}")
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
    if not pending_cities and len(completed_cities) + len(failed_cities) >= len(cities):
        logger.info(f"{province} {year}年的数据已全部处理完成")
        return
    
    # 限制API调用次数
    if max_api_calls > 0 and len(pending_cities) > max_api_calls:
        logger.info(f"限制API调用次数为{max_api_calls}，将只处理部分城市")
        pending_cities = pending_cities[:max_api_calls]
    
    # 创建线程锁，用于安全更新共享数据
    lock = threading.Lock()
    
    # 存储处理后的数据
    all_processed_data = []
    
    # 计算每个批次的大小，确保工作均匀分配
    batch_size = max(1, len(pending_cities) // max_workers)
    if batch_size == 0:
        batch_size = 1
    
    # 将城市分成多个批次
    city_batches = [pending_cities[i:i+batch_size] for i in range(0, len(pending_cities), batch_size)]
    
    logger.info(f"将使用{max_workers}个并行工作线程处理{len(city_batches)}个城市批次")
    
    # 使用线程池并行处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有批次的任务
        future_to_batch = {executor.submit(process_city_batch, province, batch, year, api_key, checkpoint_manager, lock): batch 
                          for batch in city_batches}
        
        # 处理完成的任务
        for future in concurrent.futures.as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                batch_data = future.result()
                all_processed_data.extend(batch_data)
                logger.info(f"完成处理城市批次，包含{len(batch)}个城市")
            except Exception as e:
                logger.error(f"处理城市批次时发生错误: {str(e)}")
    
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
    if all_processed_data:
        filepath = save_to_csv(all_processed_data, province, year)
        logger.info(f"数据已保存到 {filepath}")
    else:
        # 尝试从已处理的城市中重新获取数据并保存
        completed_cities = [city for city in cities if checkpoint_manager.is_completed(city, year, province)]
        if completed_cities:
            logger.info("尝试从已处理的城市中重新获取数据并保存...")
            recovery_data = []
            for city in completed_cities:
                try:
                    result = get_city_weather(province, city, year, api_key)
                    if result:
                        recovery_data.append(result)
                except Exception as e:
                    logger.error(f"重新获取 {city} 数据时出错: {str(e)}")
            
            if recovery_data:
                filepath = save_to_csv(recovery_data, province, year)
                logger.info(f"恢复的数据已保存到 {filepath}")
            else:
                logger.warning("没有新数据可保存")
        else:
            logger.warning("没有新数据可保存")


def collect_data_for_years(province: str, years: List[int], api_key: str, max_workers: int = 5, max_api_calls: int = 0) -> None:
    """
    收集指定省份在多个年份的天气数据
    
    参数:
        province (str): 省份名称
        years (List[int]): 年份列表
        api_key (str): API密钥
        max_workers (int): 最大并行工作线程数
        max_api_calls (int): 单次运行的最大API调用次数，0表示不限制
    """
    for year in years:
        print(f"\n======== 开始处理 {province} {year}年数据 ========")
        get_province_weather(province, year, api_key, max_workers, max_api_calls)
        print(f"======== 完成处理 {province} {year}年数据 ========\n")
        # 年份之间添加延时，避免API限制
        time.sleep(5)


def main(provinces=None, years=None, max_workers=5, max_api_calls=20):
    """
    主函数：获取指定省份和年份的天气数据
    
    参数:
        provinces (List[str]): 要处理的省份列表，默认为["浙江省"]
        years (List[int]): 要处理的年份列表，默认为[2010, 2012, 2014, 2016, 2018, 2020, 2022]
        max_workers (int): 最大并行工作线程数，默认为5
        max_api_calls (int): 单次运行的最大API调用次数，默认为20，0表示不限制
    """
    # 确保必要的目录存在
    for directory in [Path('storage'), Path('storage/cache'), Path('storage/checkpoints')]:
        directory.mkdir(parents=True, exist_ok=True)
        
    # 设置默认参数
    if provinces is None:
        provinces = ["浙江省"]
    if years is None:
        years = [2010, 2012, 2014, 2016, 2018, 2020, 2022]
    
    # 获取API密钥并验证
    try:
        config = load_config()
        api_key = config['apikey']
        if not api_key or len(api_key.strip()) == 0:
            logger.error("错误: API密钥为空，请检查config.json文件")
            return
        
        # 获取API限制信息
        if 'max_request_per_day' in config:
            logger.info(f"API每日请求限制: {config['max_request_per_day']}")
            
        # 如果配置中有并行线程数设置，则使用配置值
        if 'max_workers' in config:
            max_workers = config['max_workers']
            logger.info(f"使用配置的并行线程数: {max_workers}")
    except Exception as e:
        logger.error(f"加载配置文件时出错: {str(e)}")
        logger.error("请确保config.json文件存在且格式正确")
        return
    
    # 创建断点管理器
    checkpoint_manager = CheckpointManager(DATA_SOURCE)
    
    # 计算总任务数
    total_tasks = len(provinces) * len(years)
    checkpoint_manager.update_stats(total_tasks)
    
    logger.info(f"正在使用VisualCrossing API获取{years}年份的天气数据...")
    logger.info(f"API密钥: {api_key[:4]}{'*' * (len(api_key) - 8)}{api_key[-4:]}")
    logger.info(f"并行线程数: {max_workers}")
    logger.info(f"单次运行最大API调用次数: {max_api_calls if max_api_calls > 0 else '不限制'}")
    logger.info(f"总任务数: {total_tasks}")
    
    # 处理每个省份的数据
    for province in provinces:
        collect_data_for_years(province, years, api_key, max_workers, max_api_calls)
        
    # 获取最终统计信息
    stats = checkpoint_manager.get_stats()
    
    # 输出统计信息
    logger.info(f"\n===== 全部数据获取统计 =====")
    logger.info(f"总任务数: {stats['total_tasks']}")
    logger.info(f"成功获取数据的城市-年份对: {stats['completed_tasks']}")
    logger.info(f"失败的城市-年份对: {stats['failed_tasks']}")
    
    logger.info("\n所有数据处理完成！")


if __name__ == "__main__":
    import argparse
    
    # 创建命令行参数解析器
    parser = argparse.ArgumentParser(description="获取中国城市历史天气数据")
    parser.add_argument("-p", "--province", nargs="+", default=["浙江省"], help="要处理的省份列表，默认为浙江省")
    parser.add_argument("-y", "--years", nargs="+", type=int, default=[2010, 2012, 2014, 2016, 2018, 2020, 2022], 
                        help="要处理的年份列表，默认为2010, 2012, 2014, 2016, 2018, 2020, 2022")
    parser.add_argument("-w", "--workers", type=int, default=5, help="并行工作线程数，默认为5")
    parser.add_argument("-m", "--max-calls", type=int, default=20, 
                        help="单次运行最大API调用次数，默认为20，设为0表示不限制")
    
    args = parser.parse_args()
    
    # 修改main函数中的默认参数
    provinces = args.province
    years = args.years
    max_workers = args.workers
    max_api_calls = args.max_calls
    
    # 打印参数信息
    print(f"\n===== 参数信息 =====")
    print(f"处理省份: {provinces}")
    print(f"处理年份: {years}")
    print(f"并行线程数: {max_workers}")
    print(f"最大API调用次数: {max_api_calls if max_api_calls > 0 else '不限制'}")
    print(f"=====================\n")
    
    # 调用主函数，传入命令行参数
    main(provinces=provinces, years=years, max_workers=max_workers, max_api_calls=max_api_calls)