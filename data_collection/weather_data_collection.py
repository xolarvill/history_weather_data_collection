
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
from typing import Dict, List, Tuple, Optional, Any, Callable, Union
from datetime import datetime
import csv
from pathlib import Path

# 导入各API模块
from . import visualcrossing
from . import openweather
from . import qweather
from .checkpoint_manager import CheckpointManager

# 配置日志
log_file_path = Path(__file__).parent / 'weather_data_collection.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- 全局配置 ---
# 确保在项目根目录下运行，或相应调整路径
# 使用 Path 对象处理路径，提高跨平台兼容性
PROJECT_ROOT = Path(__file__).parent.parent
CACHE_DIR = PROJECT_ROOT / 'data_collection' / 'cache'
STORAGE_DIR = PROJECT_ROOT / 'storage'
CONFIG_PATH = PROJECT_ROOT / 'config.json'
CITY_LIST_PATH = PROJECT_ROOT / 'city_list.json'

# 目标年份
TARGET_YEARS = [2010, 2012, 2014, 2016, 2018, 2020, 2022]
# 数据源名称，用于断点续传
DATA_SOURCE = "combined"

# --- API 服务调度器 ---
# 将 API 服务名称映射到其处理函数
# 这样做的好处是，当需要添加新的API服务时，只需在此字典中添加一项即可
API_DISPATCHER: Dict[str, Callable] = {
    "visualcrossing": visualcrossing.get_city_weather,
    "openweather": openweather.get_city_weather,
    "qweather": qweather.get_city_weather,
}

# --- 自定义异常 ---
class APIRateLimitException(Exception):
    """API请求频率限制异常"""
    pass

class AllAPIsFailedException(Exception):
    """所有API服务均获取数据失败"""
    pass

# --- 核心功能函数 ---

def ensure_dirs():
    """
    确保必要的目录结构存在
    """
    for directory in [CACHE_DIR, STORAGE_DIR]:
        directory.mkdir(parents=True, exist_ok=True)

def load_config() -> Dict[str, str]:
    """
    从config.json加载所有API配置
    
    返回:
        Dict[str, str]: API名称到API密钥的映射
    """
    try:
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        api_keys = {}
        for service in API_DISPATCHER:
            if service in config and 'apikey' in config[service] and config[service]['apikey']:
                api_keys[service] = config[service]['apikey']
        
        if not api_keys:
            logger.warning("配置文件中未找到任何有效的API密钥。")
        return api_keys
    except FileNotFoundError:
        logger.error(f"配置文件未找到: {CONFIG_PATH}")
        return {}
    except Exception as e:
        logger.error(f"加载配置文件时出错: {e}")
        return {}

def load_city_list() -> Dict[str, Any]:
    """
    从city_list.json加载城市列表
    
    返回:
        Dict: 城市列表字典
    """
    try:
        with open(CITY_LIST_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)['city']
    except FileNotFoundError:
        logger.error(f"城市列表文件未找到: {CITY_LIST_PATH}")
        return {}
    except Exception as e:
        logger.error(f"加载城市列表时出错: {e}")
        return {}

def get_cache_key(city: str, year: int) -> str:
    """
    为综合数据生成缓存键
    """
    return f"combined_{city}_{year}"

def save_to_cache(cache_key: str, data: Any):
    """
    将数据序列化并保存到缓存文件
    """
    try:
        cache_path = CACHE_DIR / f"{cache_key}.pkl"
        with open(cache_path, 'wb') as f:
            pickle.dump(data, f)
        logger.debug(f"数据已缓存: {cache_key}")
    except Exception as e:
        logger.error(f"缓存数据时出错: {e}")

def load_from_cache(cache_key: str) -> Optional[Any]:
    """
    从缓存文件加载并反序列化数据
    """
    try:
        cache_path = CACHE_DIR / f"{cache_key}.pkl"
        if cache_path.exists():
            with open(cache_path, 'rb') as f:
                data = pickle.load(f)
            logger.debug(f"从缓存加载数据: {cache_key}")
            return data
        return None
    except Exception as e:
        logger.error(f"从缓存加载数据时出错: {e}")
        return None

def save_to_csv(data: List[List[Any]], province: str, year: int) -> str:
    """
    将最终数据保存到CSV文件
    """
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"{province}_weather_data_{year}_{timestamp}.csv"
    filepath = STORAGE_DIR / filename
    
    try:
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['province', 'city', 'year', 'avg_temperature', 'avg_solar_energy/precip'])
            for row in data:
                # 确保写入的数据格式为 [province, city, year, ...]
                writer.writerow([province] + row)
        logger.info(f"数据已保存到: {filepath}")
        return str(filepath)
    except Exception as e:
        logger.error(f"保存数据到CSV时出错: {e}")
        return ""

def get_weather_data_with_fallback(
    province: str, city: str, year: int, lat: float, lon: float, 
    api_keys: Dict[str, str], api_order: List[str]
) -> List[Union[str, int, float]]:
    """
    核心调度函数：使用多个API服务获取天气数据，如果一个API服务失败，则自动尝试下一个。
    
    返回:
        List: 处理后的天气数据 [city, year, avg_temp, avg_value]
    
    异常:
        AllAPIsFailedException: 如果所有API都尝试失败
    """
    # 检查缓存
    cache_key = get_cache_key(city, year)
    cached_data = load_from_cache(cache_key)
    if cached_data is not None:
        logger.info(f"使用缓存数据: {province}-{city} {year}年")
        return cached_data
    
    # 尝试每个API服务
    last_exception = None
    for api_service in api_order:
        if api_service not in api_keys:
            logger.debug(f"跳过 {api_service}，未配置API密钥")
            continue
            
        try:
            logger.info(f"尝试使用 {api_service} 获取 {province}-{city} {year}年 的数据")
            api_func = API_DISPATCHER[api_service]
            
            # 根据函数签名传递参数
            if api_service in ["openweather", "qweather"]:
                result = api_func(province=province, city=city, year=year, api_key=api_keys[api_service], lat=lat, lon=lon)
            else:
                result = api_func(province=province, city=city, year=year, api_key=api_keys[api_service])

            if result and isinstance(result, list):
                logger.info(f"成功使用 {api_service} 获取 {province}-{city} {year}年 的数据")
                save_to_cache(cache_key, result)
                return result
            else:
                logger.warning(f"{api_service} 未能获取 {province}-{city} {year}年 的有效数据，尝试下一个API服务")
                
        except Exception as e:
            last_exception = e
            if "rate limit" in str(e).lower() or "429" in str(e):
                logger.warning(f"{api_service} API请求频率限制，尝试下一个API服务: {e}")
            else:
                logger.error(f"使用 {api_service} 获取数据时发生意外错误: {e}", exc_info=True)
    
    # 所有API服务都失败
    error_message = f"所有API服务都未能获取 {province}-{city} {year}年 的数据。"
    if last_exception:
        error_message += f" 最后一次错误: {last_exception}"
    
    raise AllAPIsFailedException(error_message)


def worker(
    task_queue: queue.Queue, 
    results: List, 
    api_keys: Dict[str, str], 
    checkpoint_manager: CheckpointManager, 
    lock: threading.Lock
):
    """
    工作线程函数，从队列中获取任务并处理。
    """
    while not task_queue.empty():
        try:
            province, city, year, lat, lon = task_queue.get_nowait()
        except queue.Empty:
            continue

        thread_name = threading.current_thread().name
        logger.debug(f"线程 {thread_name} 获取任务: {province}-{city} {year}年")

        try:
            # 检查是否已完成
            if checkpoint_manager.is_completed(city, year, province):
                logger.info(f"跳过已完成的任务: {province}-{city} {year}年")
                continue

            # 随机化API尝试顺序以分散负载
            api_order = list(API_DISPATCHER.keys())
            random.shuffle(api_order)

            result = get_weather_data_with_fallback(province, city, year, lat, lon, api_keys, api_order)
            
            with lock:
                results.append(result)
                checkpoint_manager.mark_completed(city, year, province)
            logger.info(f"线程 {thread_name} 成功处理: {province}-{city} {year}年")

        except AllAPIsFailedException as e:
            with lock:
                checkpoint_manager.mark_failed(city, year, str(e), province)
            logger.error(f"线程 {thread_name} 失败: {e}")
        except Exception as e:
            with lock:
                checkpoint_manager.mark_failed(city, year, f"未知异常: {e}", province)
            logger.critical(f"线程 {thread_name} 发生严重错误: {e}", exc_info=True)
        finally:
            task_queue.task_done()


def collect_data_for_province(
    province: str, 
    years: List[int], 
    api_keys: Dict[str, str], 
    city_data: Dict[str, Any],
    max_workers: int
):
    """
    为单个省份收集指定年份的所有城市数据。
    """
    if province not in city_data:
        logger.error(f"在城市列表中未找到省份: {province}")
        return

    cities_in_province = city_data[province]
    
    for year in years:
        logger.info(f"\n===== 开始处理 {province} {year}年 天气数据 =====")
        
        checkpoint_manager = CheckpointManager(DATA_SOURCE)
        
        # 筛选出待处理的城市
        pending_tasks = []
        for city, coords in cities_in_province.items():
            if not checkpoint_manager.is_completed(city, year, province):
                lat, lon = coords.get("latitude"), coords.get("longitude")
                if lat is not None and lon is not None:
                    pending_tasks.append((province, city, year, lat, lon))
                else:
                    logger.warning(f"跳过 {city} 因为缺少经纬度信息。")
                    checkpoint_manager.mark_failed(city, year, "缺少经纬度信息", province)
        
        if not pending_tasks:
            logger.info(f"{province} {year}年 的所有城市数据均已处理。")
            continue

        logger.info(f"总城市数: {len(cities_in_province)}, 待处理任务数: {len(pending_tasks)}")

        task_queue = queue.Queue()
        for task in pending_tasks:
            task_queue.put(task)

        results: List[List[Any]] = []
        lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=f"{province[:2]}-{year}") as executor:
            futures = [
                executor.submit(worker, task_queue, results, api_keys, checkpoint_manager, lock)
                for _ in range(min(max_workers, len(pending_tasks)))
            ]
            for future in futures:
                future.result() # 等待线程完成

        task_queue.join()

        if results:
            save_to_csv(results, province, year)
        else:
            logger.warning(f"{province} {year}年 未获取到任何新数据。")

        # 打印统计信息
        stats = checkpoint_manager.get_stats(province)
        logger.info(f"统计: 总任务 {stats.get('total_tasks', 0)}, "
                    f"已完成 {stats.get('completed_tasks', 0)}, "
                    f"失败 {stats.get('failed_tasks', 0)}")


def collect_all_data(provinces: Optional[List[str]], years: Optional[List[int]], max_workers: int):
    """
    数据收集总入口函数。
    
    参数:
        provinces: 要处理的省份列表，None表示所有省份。
        years: 要处理的年份列表，None表示默认年份。
        max_workers: 并行线程数。
    """
    ensure_dirs()
    api_keys = load_config()
    if not api_keys:
        logger.error("错误: 未找到有效的API密钥，程序退出。")
        return

    city_data = load_city_list()
    if not city_data:
        logger.error("错误: 城市列表为空，程序退出。")
        return

    # 确定处理范围
    provinces_to_process = provinces or [p for p in city_data.keys() if p not in ["香港特别行政区", "澳门特别行政区", "台湾省"]]
    years_to_process = years or TARGET_YEARS

    logger.info("===== 开始数据收集任务 =====")
    logger.info(f"处理省份: {', '.join(provinces_to_process)}")
    logger.info(f"处理年份: {', '.join(map(str, years_to_process))}")
    logger.info(f"可用API: {', '.join(api_keys.keys())}")
    logger.info(f"最大线程数: {max_workers}")

    for province in provinces_to_process:
        collect_data_for_province(province, years_to_process, api_keys, city_data, max_workers)
        logger.info(f"===== 完成省份 {province} 的处理 =====\n")
        # 省份之间可以加入短暂延时
        time.sleep(2)

    logger.info("===== 所有数据收集任务完成 =====")
