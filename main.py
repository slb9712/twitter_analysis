import os
import sys
import time
import logging
import argparse
import asyncio
from dotenv import load_dotenv
from logging.handlers import TimedRotatingFileHandler

from config.config import MYSQL_CONFIG, MONGO_CONFIG, OPENAI_CONFIG, TASK_CONFIG, DEEPSEEK_CONFIG
from task.scheduler import DataProcessor

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, "analysis_bot.log")

# 设置日志配置（按天分割）
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        TimedRotatingFileHandler(
            filename=log_file,
            when="midnight",
            interval=1,
            backupCount=30,
            encoding="utf-8",
            utc=True
        )
    ]
)
logger = logging.getLogger('main')
sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()


def check_environment():
    required_vars = ['OPENAI_API_KEY', 'TG_BOT_TOKEN']
    missing_vars = [var for var in required_vars if not os.getenv(var)]

    if missing_vars:
        logger.error(f"错误: 未设置以下环境变量: {', '.join(missing_vars)}")
        logger.error("请创建.env文件并添加必要的环境变量")
        return False

    return True


async def main_async(args):
    if not check_environment():
        return
    try:
        processor = DataProcessor(
            mysql_config=MYSQL_CONFIG,
            mongo_config=MONGO_CONFIG,
            openai_config=DEEPSEEK_CONFIG,
            task_config=TASK_CONFIG
        )

        if args.once:
            logger.info("执行单次数据处理...")
            await processor.start_bot_async()  # 先启动 bot（用于发送）
            await processor._process_summary_tweets()
            logger.info("单次数据处理完成")
            await processor.stop()  # 完成后停止
        else:
            logger.info(f"启动定时任务")
            await processor.start()

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")


def main():
    parser = argparse.ArgumentParser(description='Analysis Bot')
    parser.add_argument('--once', action='store_true', help='只运行一次，不启动定时任务')
    args = parser.parse_args()

    asyncio.run(main_async(args))

if __name__ == '__main__':
    main()
