# weather data collection and aggregation

this is a sub project of the my [master thesis project](github.com/xolarvill/KWL).

## project description

- collect history weather data from api services, store it in a csv file, and aggregate it.
- the data collected is made of observations of one city in hour level (or day level). the aggregation function needs to aggregate the data into year level and province level.
- 不包含香港、澳门、台湾地区

## program structure description

- `config.json` stores api keys
  - 本项目使用 `config.json` 文件存储配置信息。为了保护敏感数据，我们提供了 `config.template.json` 作为模板文件。
  - 使用步骤：复制 `config.template.json` 并重命名为 `config.json`：
```bash
cp config.template.json config.json
```
- `city_list.json` stores provinces and cities of each province
- `./data_collection` contains scripts to get data from api services
  - `lat_lon.py` is to get the latitude and longitude of each city
  - `visualcrossing.py` is to get data from the api of visualcrossing
  - `openweather.py` is to get data from the api of openweather
  - `qweather.py` is to get data from the api of qweather
  - `weather_data_collection.py` is the commander program to use all other api services. the idea is that a single api has limitations, such as request limit in a day. so the program should be able to collect data from all api services in parallel, and if one api service is limited, the program should be able to switch to another api service.
```python
# 在其他脚本中导入并使用特定功能
from data_collection.weather_data_collection import collect_all_data
# 收集特定省份和年份的数据
collect_all_data(provinces=["浙江省", "江苏省"], years=[2020, 2022])
```
- `./storage` is where to store raw data. it should be a csv file of panel data. e.g. for a single observation the csv file could likely be:
```
city = 'hangzhou', year = 2020, aqi = 111, min_temperature_days = 35
```
- `./output` is where to store aggregated data (year level and province level), where datas are panel data stored in a csv file as well.
- `main.py` is the main program to collect data and aggregate data.

## tasks

1. get `city_list.json` completed from the internet.
2. use `lat_lon.py` to get the latitude and longitude of each city in `city_list.json`, based on `geopy`.
3. complete data collection function.
4. the data collection function has to collect data of all cities in `city_list.json` in parallel. the time range should be the whole years of 2010, 2012, 2014, 2016, 2018, 2020, 2022. the data collected should be stored in a csv file. considering the api rate limit, the data collection should be done in parallel (or by some other clever methods if possible). `visualcrossing.py`应该进行以下优化：
   1. 批量请求而非单日请求，大多数天气API支持按时间范围批量获取数据。例如，VisualCrossing API允许一次请求一整年的数据。
   2. 断点续传机制：实现健壮的断点续传机制，记录已完成的请求，在程序中断后能够从上次停止的地方继续
   3. 智能重试机制：针对API限制，实现指数退避（exponential backoff）重试策略
   4. 并行处理与请求限流：使用多线程或异步IO并行处理请求，但同时控制并发数量，避免触发API限制
   5. 数据缓存策略：缓存已获取的数据，避免重复请求
5. similarly, `data_collection/openweather.py` should be completed and optimized as well. it should be like `visualcrossing.py`.
6. `data_collection/qweather.py` should be completed and optimized as well. it should be like `visualcrossing.py`.
7. `data_collection/weather_data_collection.py` should be completed and optimized as well. it should be able to command all other api services to collect data in parallel, and if one api service is limited, the program should be able to switch to another api service.
   1. 直接调用其他api服务
   2. 智能重试机制：针对API限制，如果单个api服务触发限制，切换到另一个api服务
   3. 数据缓存策略：缓存已获取的数据，避免重复请求
8. complete data aggregation scripts.
9. complete `main.py` to collect data and aggregate data.
10. complete `README.md` to describe the project.