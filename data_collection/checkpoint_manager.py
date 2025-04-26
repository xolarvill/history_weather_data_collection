#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import threading
import logging
from typing import Dict, List, Set, Any, Optional, Union, Tuple
from datetime import datetime
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'checkpoint_manager.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class CheckpointManager:
    """
    通用断点管理器，用于管理数据收集过程中的断点记录
    支持按省份、城市、年份和数据源进行记录
    提供线程安全的操作，支持并发环境
    """
    
    def __init__(self, data_source: str, checkpoint_dir: Optional[str] = None):
        """
        初始化断点管理器
        
        参数:
            data_source (str): 数据源名称，如 'openweather', 'visualcrossing' 等
            checkpoint_dir (Optional[str]): 断点文件存储目录，默认为项目根目录下的 storage/checkpoints
        """
        self.data_source = data_source
        
        # 设置断点文件存储目录
        if checkpoint_dir:
            self.checkpoint_dir = Path(checkpoint_dir)
        else:
            # 默认存储在项目根目录下的 storage/checkpoints 目录
            self.checkpoint_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / 'storage' / 'checkpoints'
        
        # 确保目录存在
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        # 线程锁，用于保证并发环境下的线程安全
        self.lock = threading.RLock()
        
        # 缓存已加载的断点数据，避免频繁IO操作
        self._checkpoint_cache = {}
        
        logger.info(f"断点管理器初始化完成，数据源: {data_source}, 断点目录: {self.checkpoint_dir}")
    
    def _get_checkpoint_path(self, province: Optional[str] = None, year: Optional[int] = None) -> Path:
        """
        获取断点文件路径
        
        参数:
            province (Optional[str]): 省份名称，如果为None则返回数据源级别的断点文件
            year (Optional[int]): 年份，如果为None则返回省份级别的断点文件
            
        返回:
            Path: 断点文件路径
        """
        if province is None:
            # 数据源级别的断点文件
            return self.checkpoint_dir / f"{self.data_source}_checkpoint.json"
        elif year is None:
            # 省份级别的断点文件
            return self.checkpoint_dir / f"{self.data_source}_{province}_checkpoint.json"
        else:
            # 省份+年份级别的断点文件
            return self.checkpoint_dir / f"{self.data_source}_{province}_{year}_checkpoint.json"
    
    def _get_cache_key(self, province: Optional[str] = None, year: Optional[int] = None) -> str:
        """
        获取缓存键
        
        参数:
            province (Optional[str]): 省份名称
            year (Optional[int]): 年份
            
        返回:
            str: 缓存键
        """
        if province is None:
            return f"{self.data_source}"
        elif year is None:
            return f"{self.data_source}_{province}"
        else:
            return f"{self.data_source}_{province}_{year}"
    
    def load_checkpoint(self, province: Optional[str] = None, year: Optional[int] = None) -> Dict[str, Any]:
        """
        加载断点数据
        
        参数:
            province (Optional[str]): 省份名称，如果为None则加载数据源级别的断点
            year (Optional[int]): 年份，如果为None则加载省份级别的断点
            
        返回:
            Dict[str, Any]: 断点数据
        """
        cache_key = self._get_cache_key(province, year)
        
        # 使用线程锁确保线程安全
        with self.lock:
            # 检查缓存中是否已有数据
            if cache_key in self._checkpoint_cache:
                logger.debug(f"从缓存加载断点数据: {cache_key}")
                return self._checkpoint_cache[cache_key]
            
            checkpoint_path = self._get_checkpoint_path(province, year)
            
            # 检查断点文件是否存在
            if checkpoint_path.exists():
                try:
                    with open(checkpoint_path, 'r', encoding='utf-8') as f:
                        checkpoint_data = json.load(f)
                        
                    # 转换年份列表为集合，提高查找效率
                    if 'completed' in checkpoint_data:
                        for city, years in checkpoint_data['completed'].items():
                            if isinstance(years, list):
                                checkpoint_data['completed'][city] = set(years)
                    
                    # 缓存加载的数据
                    self._checkpoint_cache[cache_key] = checkpoint_data
                    logger.info(f"成功加载断点数据: {checkpoint_path}")
                    return checkpoint_data
                except Exception as e:
                    logger.error(f"加载断点文件时出错: {str(e)}")
            
            # 如果文件不存在或加载失败，创建新的断点数据
            checkpoint_data = self._create_new_checkpoint(province, year)
            self._checkpoint_cache[cache_key] = checkpoint_data
            return checkpoint_data
    
    def _create_new_checkpoint(self, province: Optional[str] = None, year: Optional[int] = None) -> Dict[str, Any]:
        """
        创建新的断点数据
        
        参数:
            province (Optional[str]): 省份名称
            year (Optional[int]): 年份
            
        返回:
            Dict[str, Any]: 新的断点数据
        """
        checkpoint_data = {
            "data_source": self.data_source,
            "created_at": datetime.now().isoformat(),
            "last_updated": datetime.now().isoformat(),
            "completed": {},
            "failed": {},
            "in_progress": {},
            "stats": {
                "total_tasks": 0,
                "completed_tasks": 0,
                "failed_tasks": 0
            }
        }
        
        if province is not None:
            checkpoint_data["province"] = province
        
        if year is not None:
            checkpoint_data["year"] = year
        
        logger.info(f"创建新的断点数据: {self.data_source} {province or ''} {year or ''}")
        return checkpoint_data
    
    def save_checkpoint(self, checkpoint_data: Dict[str, Any], province: Optional[str] = None, year: Optional[int] = None) -> bool:
        """
        保存断点数据
        
        参数:
            checkpoint_data (Dict[str, Any]): 断点数据
            province (Optional[str]): 省份名称
            year (Optional[int]): 年份
            
        返回:
            bool: 是否保存成功
        """
        # 更新最后修改时间
        checkpoint_data["last_updated"] = datetime.now().isoformat()
        
        # 将集合转换为列表以便JSON序列化
        serializable_data = checkpoint_data.copy()
        if 'completed' in serializable_data:
            for city, years in serializable_data['completed'].items():
                if isinstance(years, set):
                    serializable_data['completed'][city] = list(years)
        
        checkpoint_path = self._get_checkpoint_path(province, year)
        cache_key = self._get_cache_key(province, year)
        
        # 使用线程锁确保线程安全
        with self.lock:
            try:
                with open(checkpoint_path, 'w', encoding='utf-8') as f:
                    json.dump(serializable_data, f, ensure_ascii=False, indent=2)
                
                # 更新缓存
                self._checkpoint_cache[cache_key] = checkpoint_data
                logger.debug(f"断点数据已保存: {checkpoint_path}")
                return True
            except Exception as e:
                logger.error(f"保存断点文件时出错: {str(e)}")
                return False
    
    def mark_completed(self, city: str, year: int, province: Optional[str] = None) -> bool:
        """
        标记城市-年份对为已完成
        
        参数:
            city (str): 城市名称
            year (int): 年份
            province (Optional[str]): 省份名称，如果提供则同时更新省份级别的断点
            
        返回:
            bool: 是否标记成功
        """
        # 使用线程锁确保线程安全
        with self.lock:
            # 更新数据源级别的断点
            source_checkpoint = self.load_checkpoint()
            
            if 'completed' not in source_checkpoint:
                source_checkpoint['completed'] = {}
            
            if city not in source_checkpoint['completed']:
                source_checkpoint['completed'][city] = set()
            
            source_checkpoint['completed'][city].add(year)
            
            # 更新统计信息
            if 'stats' not in source_checkpoint:
                source_checkpoint['stats'] = {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0
                }
            
            source_checkpoint['stats']['completed_tasks'] += 1
            
            # 保存数据源级别的断点
            self.save_checkpoint(source_checkpoint)
            
            # 如果提供了省份，同时更新省份级别的断点
            if province:
                province_checkpoint = self.load_checkpoint(province)
                
                if 'completed' not in province_checkpoint:
                    province_checkpoint['completed'] = {}
                
                if city not in province_checkpoint['completed']:
                    province_checkpoint['completed'][city] = set()
                
                province_checkpoint['completed'][city].add(year)
                
                # 更新统计信息
                if 'stats' not in province_checkpoint:
                    province_checkpoint['stats'] = {
                        "total_tasks": 0,
                        "completed_tasks": 0,
                        "failed_tasks": 0
                    }
                
                province_checkpoint['stats']['completed_tasks'] += 1
                
                # 保存省份级别的断点
                self.save_checkpoint(province_checkpoint, province)
                
                # 如果提供了年份，同时更新省份-年份级别的断点
                year_checkpoint = self.load_checkpoint(province, year)
                
                if 'completed' not in year_checkpoint:
                    year_checkpoint['completed'] = {}
                
                if city not in year_checkpoint['completed']:
                    year_checkpoint['completed'][city] = set()
                
                year_checkpoint['completed'][city].add(year)
                
                # 更新统计信息
                if 'stats' not in year_checkpoint:
                    year_checkpoint['stats'] = {
                        "total_tasks": 0,
                        "completed_tasks": 0,
                        "failed_tasks": 0
                    }
                
                year_checkpoint['stats']['completed_tasks'] += 1
                
                # 保存省份-年份级别的断点
                self.save_checkpoint(year_checkpoint, province, year)
            
            logger.info(f"已标记完成: {city} {year}年 (数据源: {self.data_source}, 省份: {province or 'N/A'})")
            return True
    
    def mark_failed(self, city: str, year: int, reason: str, province: Optional[str] = None) -> bool:
        """
        标记城市-年份对为失败
        
        参数:
            city (str): 城市名称
            year (int): 年份
            reason (str): 失败原因
            province (Optional[str]): 省份名称，如果提供则同时更新省份级别的断点
            
        返回:
            bool: 是否标记成功
        """
        # 使用线程锁确保线程安全
        with self.lock:
            # 更新数据源级别的断点
            source_checkpoint = self.load_checkpoint()
            
            if 'failed' not in source_checkpoint:
                source_checkpoint['failed'] = {}
            
            if city not in source_checkpoint['failed']:
                source_checkpoint['failed'][city] = {}
            
            source_checkpoint['failed'][city][str(year)] = {
                "timestamp": datetime.now().isoformat(),
                "reason": reason
            }
            
            # 更新统计信息
            if 'stats' not in source_checkpoint:
                source_checkpoint['stats'] = {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0
                }
            
            source_checkpoint['stats']['failed_tasks'] += 1
            
            # 保存数据源级别的断点
            self.save_checkpoint(source_checkpoint)
            
            # 如果提供了省份，同时更新省份级别的断点
            if province:
                province_checkpoint = self.load_checkpoint(province)
                
                if 'failed' not in province_checkpoint:
                    province_checkpoint['failed'] = {}
                
                if city not in province_checkpoint['failed']:
                    province_checkpoint['failed'][city] = {}
                
                province_checkpoint['failed'][city][str(year)] = {
                    "timestamp": datetime.now().isoformat(),
                    "reason": reason
                }
                
                # 更新统计信息
                if 'stats' not in province_checkpoint:
                    province_checkpoint['stats'] = {
                        "total_tasks": 0,
                        "completed_tasks": 0,
                        "failed_tasks": 0
                    }
                
                province_checkpoint['stats']['failed_tasks'] += 1
                
                # 保存省份级别的断点
                self.save_checkpoint(province_checkpoint, province)
            
            logger.info(f"已标记失败: {city} {year}年 (数据源: {self.data_source}, 省份: {province or 'N/A'})，原因: {reason}")
            return True
    
    def is_completed(self, city: str, year: int, province: Optional[str] = None) -> bool:
        """
        检查城市-年份对是否已完成
        
        参数:
            city (str): 城市名称
            year (int): 年份
            province (Optional[str]): 省份名称，如果提供则优先检查省份级别的断点
            
        返回:
            bool: 是否已完成
        """
        # 使用线程锁确保线程安全
        with self.lock:
            # 如果提供了省份，优先检查省份级别的断点
            if province:
                province_checkpoint = self.load_checkpoint(province)
                
                if 'completed' in province_checkpoint and city in province_checkpoint['completed']:
                    if isinstance(province_checkpoint['completed'][city], set):
                        return year in province_checkpoint['completed'][city]
                    elif isinstance(province_checkpoint['completed'][city], list):
                        return year in province_checkpoint['completed'][city]
            
            # 检查数据源级别的断点
            source_checkpoint = self.load_checkpoint()
            
            if 'completed' in source_checkpoint and city in source_checkpoint['completed']:
                if isinstance(source_checkpoint['completed'][city], set):
                    return year in source_checkpoint['completed'][city]
                elif isinstance(source_checkpoint['completed'][city], list):
                    return year in source_checkpoint['completed'][city]
            
            return False
    
    def get_completed_tasks(self, province: Optional[str] = None) -> Dict[str, Set[int]]:
        """
        获取已完成的任务列表
        
        参数:
            province (Optional[str]): 省份名称，如果提供则返回该省份的已完成任务
            
        返回:
            Dict[str, Set[int]]: 已完成的城市-年份对，格式为 {城市: {年份1, 年份2, ...}}
        """
        # 使用线程锁确保线程安全
        with self.lock:
            if province:
                checkpoint = self.load_checkpoint(province)
            else:
                checkpoint = self.load_checkpoint()
            
            if 'completed' not in checkpoint:
                return {}
            
            # 确保返回的是集合类型
            result = {}
            for city, years in checkpoint['completed'].items():
                if isinstance(years, list):
                    result[city] = set(years)
                else:
                    result[city] = years
            
            return result
    
    def get_failed_tasks(self, province: Optional[str] = None) -> Dict[str, Dict[str, Dict[str, str]]]:
        """
        获取失败的任务列表
        
        参数:
            province (Optional[str]): 省份名称，如果提供则返回该省份的失败任务
            
        返回:
            Dict[str, Dict[str, Dict[str, str]]]: 失败的城市-年份对及失败原因
        """
        # 使用线程锁确保线程安全
        with self.lock:
            if province:
                checkpoint = self.load_checkpoint(province)
            else:
                checkpoint = self.load_checkpoint()
            
            if 'failed' not in checkpoint:
                return {}
            
            return checkpoint['failed']
    
    def get_stats(self, province: Optional[str] = None, year: Optional[int] = None) -> Dict[str, int]:
        """
        获取统计信息
        
        参数:
            province (Optional[str]): 省份名称
            year (Optional[int]): 年份
            
        返回:
            Dict[str, int]: 统计信息
        """
        # 使用线程锁确保线程安全
        with self.lock:
            checkpoint = self.load_checkpoint(province, year)
            
            if 'stats' not in checkpoint:
                return {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0
                }
            
            return checkpoint['stats']
    
    def update_stats(self, total_tasks: Optional[int] = None, province: Optional[str] = None, year: Optional[int] = None) -> bool:
        """
        更新统计信息
        
        参数:
            total_tasks (Optional[int]): 总任务数
            province (Optional[str]): 省份名称
            year (Optional[int]): 年份
            
        返回:
            bool: 是否更新成功
        """
        # 使用线程锁确保线程安全
        with self.lock:
            checkpoint = self.load_checkpoint(province, year)
            
            if 'stats' not in checkpoint:
                checkpoint['stats'] = {
                    "total_tasks": 0,
                    "completed_tasks": 0,
                    "failed_tasks": 0
                }
            
            if total_tasks is not None:
                checkpoint['stats']['total_tasks'] = total_tasks
            
            # 重新计算已完成和失败的任务数
            completed_count = 0
            if 'completed' in checkpoint:
                for city, years in checkpoint['completed'].items():
                    if isinstance(years, set):
                        completed_count += len(years)
                    elif isinstance(years, list):
                        completed_count += len(years)
            
            failed_count = 0
            if 'failed' in checkpoint:
                for city, years_data in checkpoint['failed'].items():
                    failed_count += len(years_data)
            
            checkpoint['stats']['completed_tasks'] = completed_count
            checkpoint['stats']['failed_tasks'] = failed_count
            
            # 保存更新后的断点
            return self.save_checkpoint(checkpoint, province, year)
    
    def clear_cache(self) -> None:
        """
        清除缓存
        """
        with self.lock:
            self._checkpoint_cache.clear()
            logger.debug("断点缓存已清除")
    
    def merge_checkpoints(self, source_data_source: str) -> bool:
        """
        合并来自其他数据源的断点数据
        
        参数:
            source_data_source (str): 源数据源名称
            
        返回:
            bool: 是否合并成功
        """
        try:
            # 创建临时的断点管理器来加载源数据源的断点
            source_manager = CheckpointManager(source_data_source, str(self.checkpoint_dir))
            source_checkpoint = source_manager.load_checkpoint()
            
            # 加载当前数据源的断点
            target_checkpoint = self.load_checkpoint()
            
            # 合并已完成的任务
            if 'completed' in source_checkpoint:
                if 'completed' not in target_checkpoint:
                    target_checkpoint['completed'] = {}
                
                for city, years in source_checkpoint['completed'].items():
                    if city not in target_checkpoint['completed']:
                        target_checkpoint['completed'][city] = set()
                    
                    if isinstance(years, set):
                        target_checkpoint['completed'][city].update(years)
                    elif isinstance(years, list):
                        target_checkpoint['completed'][city].update(set(years))
            
            # 保存更新后的断点
            self.save_checkpoint(target_checkpoint)
            
            logger.info(f"成功合并来自 {source_data_source} 的断点数据到 {self.data_source}")
            return True
        except Exception as e:
            logger.error(f"合并断点数据时出错: {str(e)}")
            return False


# 示例用法
def example_usage():
    # 创建断点管理器
    checkpoint_manager = CheckpointManager("openweather")
    
    # 标记任务完成
    checkpoint_manager.mark_completed("北京", 2020, "北京市")
    checkpoint_manager.mark_completed("上海", 2020, "上海市")
    
    # 标记任务失败
    checkpoint_manager.mark_failed("广州", 2020, "API请求超时", "广东省")
    
    # 检查任务是否完成
    is_completed = checkpoint_manager.is_completed("北京", 2020)
    print(f"北京2020年的任务是否完成: {is_completed}")
    
    # 获取已完成的任务
    completed_tasks = checkpoint_manager.get_completed_tasks()
    print(f"已完成的任务: {completed_tasks}")
    
    # 获取失败的任务
    failed_tasks = checkpoint_manager.get_failed_tasks()
    print(f"失败的任务: {failed_tasks}")
    
    # 获取统计信息
    stats = checkpoint_manager.get_stats()
    print(f"统计信息: {stats}")


if __name__ == "__main__":
    example_usage()