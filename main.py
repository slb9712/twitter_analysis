import os
import sys
import time
import logging
import argparse
import asyncio
from dotenv import load_dotenv
from logging.handlers import TimedRotatingFileHandler

from config.config import MYSQL_CONFIG, MONGO_CONFIG, OPENAI_CONFIG, TASK_CONFIG, DEEPSEEK_CONFIG, LOCAL_MODEL_CONFIG
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
            when="midnight",       # 每天午夜轮换
            interval=1,
            backupCount=30,        # 最多保留 30 个文件
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


def main():
    parser = argparse.ArgumentParser(description='Analysis Bot')
    parser.add_argument('--once', action='store_true', help='只运行一次，不启动定时任务')
    args = parser.parse_args()

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
            async def process_once():
                # processor._cluster_news()
                
                await processor._process_summary_tweets()
                logger.info("单次数据处理完成")

            asyncio.run(process_once())
        else:
            logger.info(f"启动定时任务")
            processor.start()

            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("接收到终止信号，正在停止...")
                processor.stop()
                # logger.info("正在停止TG_bot...")
                # stop_bot()
                # logger.info("TG_bot已停止")

    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")


if __name__ == '__main__':
    main()
