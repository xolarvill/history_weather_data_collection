#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
断点管理器使用示例

本脚本展示如何在数据收集模块中使用通用断点管理器
"""

import os
import json
import logging
from typing import Dict, List, Set, Any, Optional
from datetime import datetime
from pathlib import Path

# 导入断点管理器
from checkpoint_manager import CheckpointManager

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('checkpoint_example.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


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


def simulate_data_collection(data_source: str, province: str, city: str, year: int) -> bool:
    """
    模拟数据收集过程
    
    参数:
        data_source (str): 数据源名称
        province (str): 省份名称
        city (str): 城市名称
        year (int): 年份
        
    返回:
        bool: 是否成功
    """
    # 这里只是模拟，实际应用中会调用相应的API获取数据
    import random
    success = random.random() > 0.2  # 80%的成功率
    
    if success:
        logger.info(f"成功获取数据: {province} - {city} {year}年 (数据源: {data_source})")
    else:
        logger.error(f"获取数据失败: {province} - {city} {year}年 (数据源: {data_source})")
    
    return success


def main():
    """
    主函数：演示如何使用断点管理器
    """
    # 目标年份
    TARGET_YEARS = [2010, 2012, 2014, 2016, 2018, 2020, 2022]
    
    # 数据源
    DATA_SOURCES = ["openweather", "visualcrossing"]
    
    # 加载城市列表
    city_data = load_city_list()
    if not city_data:
        logger.error("错误: 城市列表为空，请检查city_list.json文件")
        return
    
    # 选择一个省份进行演示
    demo_province = "浙江省"
    if demo_province not in city_data:
        logger.error(f"错误: 城市列表中没有{demo_province}")
        return
    
    # 创建断点管理器
    for data_source in DATA_SOURCES:
        logger.info(f"\n===== 开始处理数据源: {data_source} =====\n")
        checkpoint_manager = CheckpointManager(data_source)
        
        # 更新总任务数
        total_tasks = len(city_data[demo_province]) * len(TARGET_YEARS)
        checkpoint_manager.update_stats(total_tasks, demo_province)
        
        # 处理每个城市和年份
        for city, coords in city_data[demo_province].items():
            for year in TARGET_YEARS:
                # 检查是否已完成
                if checkpoint_manager.is_completed(city, year, demo_province):
                    logger.info(f"跳过已完成的任务: {demo_province} - {city} {year}年 (数据源: {data_source})")
                    continue
                
                # 模拟数据收集
                success = simulate_data_collection(data_source, demo_province, city, year)
                
                # 更新断点
                if success:
                    checkpoint_manager.mark_completed(city, year, demo_province)
                else:
                    checkpoint_manager.mark_failed(city, year, "模拟失败", demo_province)
        
        # 获取统计信息
        stats = checkpoint_manager.get_stats(demo_province)
        logger.info(f"\n===== 数据源 {data_source} 处理统计 =====")
        logger.info(f"总任务数: {stats['total_tasks']}")
        logger.info(f"已完成任务数: {stats['completed_tasks']}")
        logger.info(f"失败任务数: {stats['failed_tasks']}")
        
        # 获取已完成的任务
        completed_tasks = checkpoint_manager.get_completed_tasks(demo_province)
        logger.info(f"已完成的城市数: {len(completed_tasks)}")
        
        # 获取失败的任务
        failed_tasks = checkpoint_manager.get_failed_tasks(demo_province)
        logger.info(f"失败的城市数: {len(failed_tasks)}")
    
    # 演示合并断点数据
    logger.info("\n===== 演示合并断点数据 =====\n")
    openweather_manager = CheckpointManager("openweather")
    visualcrossing_manager = CheckpointManager("visualcrossing")
    
    # 将visualcrossing的断点数据合并到openweather
    if openweather_manager.merge_checkpoints("visualcrossing"):
        logger.info("成功合并断点数据")
        
        # 获取合并后的统计信息
        stats = openweather_manager.get_stats(demo_province)
        logger.info(f"\n===== 合并后的统计 =====")
        logger.info(f"总任务数: {stats['total_tasks']}")
        logger.info(f"已完成任务数: {stats['completed_tasks']}")
        logger.info(f"失败任务数: {stats['failed_tasks']}")
    else:
        logger.error("合并断点数据失败")


if __name__ == "__main__":
    main()