#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import time
from pathlib import Path
from typing import Dict, Any

try:
    from geopy.geocoders import Nominatim
except ImportError:
    print("请先安装geopy库: pip install geopy")
    exit(1)


class APIFailureException(Exception):
    """
    API故障异常，用于标识API调用过程中的严重错误（如超时、连接问题等）
    """
    pass


def load_city_list(file_path: str) -> Dict[str, Any]:
    """
    加载城市列表数据
    
    Args:
        file_path: city_list.json文件路径
        
    Returns:
        包含城市信息的字典
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"加载城市列表失败: {e}")
        return {}


def save_city_list(file_path: str, city_data: Dict[str, Any]) -> None:
    """
    保存更新后的城市列表数据
    
    Args:
        file_path: 保存文件路径
        city_data: 包含城市信息的字典
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(city_data, f, ensure_ascii=False, indent=2)
        print(f"城市列表已保存到 {file_path}")
    except Exception as e:
        print(f"保存城市列表失败: {e}")


def get_lat_lon(city_name: str, province_name: str = "") -> tuple:
    """
    使用geopy获取城市的经纬度
    
    Args:
        city_name: 城市名称
        province_name: 省份名称，用于提高查询精度
        
    Returns:
        (纬度, 经度) 的元组，如果查询失败则返回(None, None)
        如果发生API故障（如超时），则抛出APIFailureException异常
    """
    geolocator = Nominatim(user_agent="weather_data_collector")
    
    # 构建查询字符串，添加中国作为国家以提高精度
    query = f"{city_name}, {province_name}, China"
    if city_name == province_name:
        query = f"{city_name}, China"
    
    try:
        # 添加延时以避免API限制
        time.sleep(1)
        location = geolocator.geocode(query)
        if location:
            return (location.latitude, location.longitude)
        else:
            print(f"无法获取 {query} 的地理位置信息")
            return (None, None)
    except Exception as e:
        error_msg = str(e)
        print(f"获取 {query} 的地理位置信息时出错: {e}")
        
        # 检测API故障（超时、连接问题等）
        if "Max retries exceeded" in error_msg or "Read timed out" in error_msg or "ReadTimeoutError" in error_msg:
            print(f"检测到API故障: {error_msg}")
            raise APIFailureException(f"API故障: {error_msg}")
            
        return (None, None)


def update_city_list_with_coordinates(city_data: Dict[str, Any], max_requests: int = 100) -> Dict[str, Any]:
    """
    更新城市列表，添加经纬度信息
    
    Args:
        city_data: 包含城市信息的字典
        max_requests: 单次运行时的最大请求次数，用于限制API调用次数，默认为100
        
    Returns:
        更新后的城市字典
    """
    updated_data = city_data.copy()
    city_dict = updated_data.get("city", {})
    
    # 获取进度信息
    progress = updated_data.get("progress", {})
    last_province = progress.get("last_province", "")
    last_city = progress.get("last_city", "")
    # 每次运行时请求计数从0开始，确保max_requests是单次运行的限制
    request_count = 0
    
    total_cities = sum(len(cities) for cities in city_dict.values())
    processed = 0
    resume_mode = False
    
    # 检查是否需要恢复之前的进度
    if last_province and last_city:
        print(f"发现上次处理进度，将从 {last_province} 省的 {last_city} 市继续...")
        resume_mode = True
    
    try:
        for province, cities in city_dict.items():
            # 如果是恢复模式且还没到上次处理的省份，则跳过
            if resume_mode and province != last_province:
                processed += len(cities)
                continue
                
            print(f"正在处理 {province} 的城市...")
            
            for city in cities:
                # 如果是恢复模式且在上次处理的省份但还没到上次处理的城市，则跳过
                if resume_mode and province == last_province and city != last_city:
                    processed += 1
                    continue
                    
                # 一旦找到上次处理的城市，关闭恢复模式，开始正常处理
                if resume_mode and province == last_province and city == last_city:
                    resume_mode = False
                
                processed += 1
                print(f"进度: [{processed}/{total_cities}] 正在获取 {province} - {city} 的经纬度...")
                
                # 检查是否已有经纬度数据
                if "latitude" in city_dict[province][city] and "longitude" in city_dict[province][city]:
                    print(f"{province} - {city} 已有经纬度数据，跳过")
                    continue
                
                # 检查API请求次数限制
                if request_count >= max_requests:
                    print(f"已达到最大API请求次数限制({max_requests})，暂停处理")
                    # 保存当前进度
                    updated_data["progress"] = {
                        "last_province": province,
                        "last_city": city,
                        "request_count": request_count
                    }
                    return updated_data
                
                try:
                    # 获取经纬度
                    lat, lon = get_lat_lon(city, province)
                    request_count += 1
                    
                    # 更新进度信息
                    updated_data["progress"] = {
                        "last_province": province,
                        "last_city": city,
                        "request_count": request_count
                    }
                    
                    if lat is not None and lon is not None:
                        city_dict[province][city]["latitude"] = lat
                        city_dict[province][city]["longitude"] = lon
                        print(f"{province} - {city}: 纬度={lat}, 经度={lon}")
                    else:
                        print(f"警告: 无法获取 {province} - {city} 的经纬度")
                        
                except APIFailureException as e:
                    print(f"API故障，中断处理: {e}")
                    # 保存当前进度，记录失败位置
                    updated_data["progress"] = {
                        "last_province": province,
                        "last_city": city,
                        "request_count": request_count,
                        "api_failure": True,
                        "failure_reason": str(e)
                    }
                    print(f"已保存当前进度，下次运行时将从 {province} - {city} 继续")
                    return updated_data
                    
    except Exception as e:
        print(f"处理过程中发生错误: {e}")
        # 保存当前进度
        updated_data["progress"] = {
            "last_province": province,
            "last_city": city,
            "request_count": request_count
        }
        return updated_data
    
    # 处理完成后，清除进度信息
    if "progress" in updated_data:
        del updated_data["progress"]
        print("所有城市处理完成，已清除进度信息")
    
    return updated_data


def check_missing_coordinates(city_data: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    检查城市列表中是否有缺少经纬度信息的城市
    
    Args:
        city_data: 包含城市信息的字典
        
    Returns:
        包含缺少经纬度信息的城市的字典，格式为 {省份: {城市名: 城市信息}}
    """
    missing_cities = {}
    city_dict = city_data.get("city", {})
    
    for province, cities in city_dict.items():
        for city, city_info in cities.items():
            # 检查是否缺少经纬度信息
            if "latitude" not in city_info or "longitude" not in city_info:
                if province not in missing_cities:
                    missing_cities[province] = {}
                missing_cities[province][city] = city_info
    
    return missing_cities


def main():
    """
    主函数，处理城市列表并添加经纬度信息
    """
    # 获取项目根目录
    project_root = Path(__file__).parent.parent
    city_list_path = project_root / "city_list.json"
    
    print(f"正在从 {city_list_path} 加载城市列表...")
    city_data = load_city_list(str(city_list_path))
    if not city_data:
        print("城市列表为空或加载失败，退出程序")
        return
    
    # 设置最大API请求次数，可以通过命令行参数传入
    import argparse
    parser = argparse.ArgumentParser(description="获取城市经纬度信息")
    parser.add_argument("--max-requests", type=int, default=100, help="单次运行时的最大API请求次数，默认为100")
    parser.add_argument("--check-missing", action="store_true", help="检查并处理缺少经纬度信息的城市")
    args = parser.parse_args()
    
    # 检查是否有进度信息，如果有则表示处理未完成
    if "progress" in city_data:
        print(f"发现未完成的处理进度，将从断点继续...")
        print(f"开始获取城市经纬度信息，最大API请求次数限制为 {args.max_requests}...")
        updated_city_data = update_city_list_with_coordinates(city_data, args.max_requests)
    else:
        # 检查是否有缺少经纬度信息的城市
        missing_cities = check_missing_coordinates(city_data)
        if missing_cities:
            missing_count = sum(len(cities) for cities in missing_cities.values())
            print(f"发现 {missing_count} 个城市缺少经纬度信息")
            
            # 如果指定了--check-missing参数或没有进度信息，则处理缺少经纬度信息的城市
            if args.check_missing or "progress" not in city_data:
                print("开始处理缺少经纬度信息的城市...")
                # 创建一个新的数据结构，只包含缺少经纬度信息的城市
                missing_data = {"city": missing_cities}
                if "progress" in city_data:
                    missing_data["progress"] = city_data["progress"]
                
                # 更新缺少经纬度信息的城市
                print(f"开始获取缺失城市经纬度信息，最大API请求次数限制为 {args.max_requests}...")
                updated_missing_data = update_city_list_with_coordinates(missing_data, args.max_requests)
                
                # 将更新后的城市信息合并回原始数据
                for province, cities in updated_missing_data.get("city", {}).items():
                    for city, city_info in cities.items():
                        if "latitude" in city_info and "longitude" in city_info:
                            city_data["city"][province][city] = city_info
                
                # 如果更新过程中有进度信息，则保存到原始数据中
                if "progress" in updated_missing_data:
                    city_data["progress"] = updated_missing_data["progress"]
                elif "progress" in city_data:
                    del city_data["progress"]
                
                updated_city_data = city_data
            else:
                print("使用 --check-missing 参数运行程序以处理这些城市")
                for province, cities in missing_cities.items():
                    print(f"  {province}: {', '.join(cities.keys())}")
                updated_city_data = city_data
        else:
            print("所有城市都已有经纬度信息")
            if "progress" in city_data:
                del city_data["progress"]
                print("清除了旧的进度信息")
            updated_city_data = city_data
    
    # 检查更新后的数据中是否有进度信息
    if "progress" in updated_city_data:
        progress = updated_city_data["progress"]
        print(f"当前进度：省份={progress['last_province']}，城市={progress['last_city']}")
        print(f"本次运行已处理请求数：{progress['request_count']}")
        
        # 检查是否因API故障而中断
        if progress.get("api_failure", False):
            print(f"处理因API故障而中断: {progress.get('failure_reason', '未知原因')}")
            print("API可能存在限流或超时问题，请稍后再试")
        else:
            print(f"已达到API请求限制或发生其他错误")
            
        print("正在保存当前进度...")
    else:
        # 再次检查是否有缺少经纬度信息的城市
        missing_cities = check_missing_coordinates(updated_city_data)
        if missing_cities:
            missing_count = sum(len(cities) for cities in missing_cities.values())
            print(f"警告：仍有 {missing_count} 个城市缺少经纬度信息")
            print("下次运行时使用 --check-missing 参数以处理这些城市")
        else:
            print("所有城市经纬度信息获取完成！")
    
    print("正在保存更新后的城市列表...")
    save_city_list(str(city_list_path), updated_city_data)
    
    if "progress" in updated_city_data:
        print("处理未完成，下次运行时将从断点继续")
    else:
        missing_cities = check_missing_coordinates(updated_city_data)
        if missing_cities:
            print("处理完成，但仍有城市缺少经纬度信息")
            print("下次运行时使用 --check-missing 参数以处理这些城市")
        else:
            print("处理完成！所有城市都已有经纬度信息")




if __name__ == "__main__":
    main()