# 通用断点管理器

## 简介

通用断点管理器（CheckpointManager）是一个用于管理数据收集过程中断点记录的工具，支持按省份、城市、年份和数据源进行记录。它提供线程安全的操作，支持并发环境，并提供断点恢复功能，让不同API可以无缝接替工作。

## 特性

- **多级断点记录**：支持数据源级别、省份级别和年份级别的断点记录
- **线程安全**：使用线程锁确保并发环境下的数据一致性
- **缓存机制**：缓存已加载的断点数据，减少IO操作
- **断点恢复**：支持从上次中断的位置继续执行
- **统计功能**：提供任务完成情况的统计信息
- **断点合并**：支持合并不同数据源的断点数据

## 使用方法

### 初始化断点管理器

```python
from checkpoint_manager import CheckpointManager

# 创建断点管理器，指定数据源名称
checkpoint_manager = CheckpointManager("openweather")

# 可以指定自定义的断点文件存储目录
# checkpoint_manager = CheckpointManager("openweather", "/path/to/checkpoints")
```

### 标记任务完成

```python
# 标记城市-年份对为已完成
checkpoint_manager.mark_completed("北京", 2020, "北京市")

# 如果不需要按省份记录，可以省略省份参数
# checkpoint_manager.mark_completed("北京", 2020)
```

### 标记任务失败

```python
# 标记城市-年份对为失败，并记录失败原因
checkpoint_manager.mark_failed("广州", 2020, "API请求超时", "广东省")
```

### 检查任务是否已完成

```python
# 检查城市-年份对是否已完成
is_completed = checkpoint_manager.is_completed("北京", 2020, "北京市")
if is_completed:
    print("任务已完成，跳过处理")
else:
    print("任务未完成，开始处理")
```

### 获取已完成的任务

```python
# 获取所有已完成的任务
completed_tasks = checkpoint_manager.get_completed_tasks()
print(f"已完成的任务: {completed_tasks}")

# 获取指定省份的已完成任务
province_completed = checkpoint_manager.get_completed_tasks("北京市")
print(f"北京市已完成的任务: {province_completed}")
```

### 获取失败的任务

```python
# 获取所有失败的任务及失败原因
failed_tasks = checkpoint_manager.get_failed_tasks()
print(f"失败的任务: {failed_tasks}")

# 获取指定省份的失败任务
province_failed = checkpoint_manager.get_failed_tasks("广东省")
print(f"广东省失败的任务: {province_failed}")
```

### 获取统计信息

```python
# 获取总体统计信息
stats = checkpoint_manager.get_stats()
print(f"总任务数: {stats['total_tasks']}")
print(f"已完成任务数: {stats['completed_tasks']}")
print(f"失败任务数: {stats['failed_tasks']}")

# 获取指定省份的统计信息
province_stats = checkpoint_manager.get_stats("浙江省")
print(f"浙江省总任务数: {province_stats['total_tasks']}")
```

### 更新统计信息

```python
# 更新总任务数
checkpoint_manager.update_stats(total_tasks=100, province="浙江省")
```

### 合并断点数据

```python
# 将visualcrossing的断点数据合并到openweather
openweather_manager = CheckpointManager("openweather")
openweather_manager.merge_checkpoints("visualcrossing")
```

### 清除缓存

```python
# 清除内存中的断点缓存
checkpoint_manager.clear_cache()
```

## 在数据收集模块中的应用

以下是在数据收集模块中使用断点管理器的典型流程：

```python
# 创建断点管理器
checkpoint_manager = CheckpointManager("openweather")

# 加载城市列表
city_data = load_city_list()

# 更新总任务数
total_tasks = len(city_data) * len(TARGET_YEARS)
checkpoint_manager.update_stats(total_tasks)

# 处理每个城市和年份
for province, cities in city_data.items():
    for city, coords in cities.items():
        for year in TARGET_YEARS:
            # 检查是否已完成
            if checkpoint_manager.is_completed(city, year, province):
                logger.info(f"跳过已完成的任务: {province} - {city} {year}年")
                continue
            
            try:
                # 获取数据
                success = get_weather_data(city, coords["latitude"], coords["longitude"], year, api_key)
                
                if success:
                    # 处理数据
                    process_data(success)
                    
                    # 标记任务完成
                    checkpoint_manager.mark_completed(city, year, province)
                else:
                    # 标记任务失败
                    checkpoint_manager.mark_failed(city, year, "获取数据失败", province)
            except Exception as e:
                # 记录异常
                checkpoint_manager.mark_failed(city, year, str(e), province)
```

## 断点文件结构

断点文件采用JSON格式存储，包含以下主要字段：

- `data_source`: 数据源名称
- `province`: 省份名称（省份级别的断点文件）
- `year`: 年份（年份级别的断点文件）
- `created_at`: 创建时间
- `last_updated`: 最后更新时间
- `completed`: 已完成的任务，格式为 `{城市: [年份1, 年份2, ...]}`
- `failed`: 失败的任务，格式为 `{城市: {年份1: {timestamp: 时间戳, reason: 失败原因}, ...}}`
- `stats`: 统计信息，包含 `total_tasks`, `completed_tasks`, `failed_tasks`

## 注意事项

1. 断点文件默认存储在项目根目录下的 `storage/checkpoints` 目录中
2. 断点管理器会自动创建必要的目录结构
3. 在多线程环境中使用时，断点管理器已内置线程锁，无需额外加锁
4. 合并断点数据时，只会合并已完成的任务，不会合并失败的任务