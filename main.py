
import argparse
import sys
from pathlib import Path

# 将项目根目录添加到 Python 路径中
# 这使得我们可以使用绝对导入，例如 from data_collection.weather_data_collection import collect_all_data
# 避免了复杂的相对导入问题
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from data_collection.weather_data_collection import collect_all_data

def main():
    """
    项目主入口函数，负责解析命令行参数并启动数据采集流程。
    """
    parser = argparse.ArgumentParser(
        description="从多个API源收集中国城市历史天气数据。",
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument(
        "-p", "--provinces", 
        nargs="+", 
        default=None,
        help="要处理的省份列表。\n"
             "例如: --provinces 浙江省 江苏省\n"
             "如果未提供，则默认处理所有省份。"
    )
    
    parser.add_argument(
        "-y", "--years", 
        nargs="+", 
        type=int, 
        default=None,
        help="要处理的年份列表。\n"
             "例如: --years 2020 2022\n"
             "如果未提供，则默认处理预设的年份范围。"
    )
    
    parser.add_argument(
        "-w", "--workers", 
        type=int, 
        default=5,
        help="并行工作线程数，默认为5。\n"
             "根据您的网络和机器性能调整。"
    )
    
    args = parser.parse_args()
    
    # 打印启动信息
    print("===== 启动天气数据采集程序 =====")
    print(f"处理省份: {'所有省份' if args.provinces is None else ', '.join(args.provinces)}")
    print(f"处理年份: {'默认年份' if args.years is None else ', '.join(map(str, args.years))}")
    print(f"并行线程数: {args.workers}")
    print("===================================")
    
    # 调用核心数据采集函数
    try:
        collect_all_data(
            provinces=args.provinces,
            years=args.years,
            max_workers=args.workers
        )
    except Exception as e:
        print(f"程序执行过程中发生未捕获的异常: {e}")
        sys.exit(1)
    
    print("\n===== 数据采集程序执行完毕 =====")

if __name__ == "__main__":
    main()
