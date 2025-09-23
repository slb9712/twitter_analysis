import json
import time
import logging
import threading
import asyncio
from zoneinfo import ZoneInfo

from bson import ObjectId
import httpx

from prompt import *
from database.db_manager import MySQLManager, MongoDBManager
from model.text_analyzer import TextAnalyzer
from utils.format_msg import replace_newlines_with_space
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, UTC, timedelta

logger = logging.getLogger('scheduler')
logging.getLogger("apscheduler.executors.default").setLevel(logging.ERROR)
logging.getLogger("apscheduler.scheduler").setLevel(logging.ERROR)
logging.getLogger("apscheduler.executors").setLevel(logging.ERROR)


class DataProcessor:
    """数据处理器，负责从数据源获取数据并进行处理"""

    def __init__(self, mysql_config, mongo_config, openai_config, task_config):
        """初始化数据处理器

        Args:
            mysql_config (dict): MySQL配置
            mongo_config (dict): MongoDB配置
            openai_config (dict): OpenAI配置
            task_config (dict): 任务配置
        """
        self.mysql_manager = MySQLManager(mysql_config)
        self.mongo_manager = MongoDBManager(mongo_config)
        self.text_analyzer = TextAnalyzer(openai_config)
        self.task_config = task_config
        self.running = False
        self.thread = None
        self.is_first_run = True
        self.concurrency = 5
        self.limit_count = self.task_config['process_limit_count']
        self.latest_kol_tweets_time = int(datetime.now(ZoneInfo("Asia/Shanghai")).timestamp())
        self.updated_projects_list = set()
        self.inner_group = '-4879675579'
        self.outer_group = '-4892377641'
        self.daily_group = '-4980813719'
        self.daily_hyper_group = '-4942034777'
        self.test_inner_group = '-4878412167'
        self.test_outer_group = '-4878412167'
        # self.qdrant_client = QdrantService()
        # self.embedder = EmbeddingService()

        # 确保必要的表存在
        self._ensure_tables_exist()

        logger.info("数据处理器初始化完成")

    def _ensure_tables_exist(self):
        """确保必要的表存在"""
        create_kol_tweets_table_sql = """
            CREATE TABLE IF NOT EXISTS structured_kol_tweets (
            id INT AUTO_INCREMENT PRIMARY KEY,
            content TEXT COMMENT '推文内容',
            source_id TEXT COMMENT '推文原始id',
            token VARCHAR(255) COMMENT '涉及token',
            project VARCHAR(255) COMMENT '涉及项目',
            tags TEXT COMMENT '项目相关tag',
            created_at INT NOT NULL COMMENT '创建时间'
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """

        create_kol_tweets_summary_sql = """
                    CREATE TABLE IF NOT EXISTS kol_tweets_summary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    events TEXT COMMENT '总结事件',
                    projects TEXT COMMENT '涉及项目',
                    source_ids TEXT COMMENT '数据源',
                    created_at INT NOT NULL COMMENT '创建时间'
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """

        try:
            self.mysql_manager.execute_update(create_kol_tweets_table_sql)
            self.mysql_manager.execute_update(create_kol_tweets_summary_sql)
            logger.info("已确保必要的表存在")
        except Exception as e:
            logger.error(f"创建表失败: {str(e)}")
            raise

    def start(self):
        """启动调度器并注册任务"""
        if self.running:
            logger.warning("数据处理器已经在运行中")
            return

        self.running = True
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.scheduler = AsyncIOScheduler(event_loop=self.loop)

        # self.scheduler.add_job(
        #     self._process_all_sources,
        #     trigger=IntervalTrigger(minutes=self.task_config['interval_minutes']),
        #     next_run_time=datetime.now(),
        #     max_instance=1,
        #     name="定时处理数据库数据"
        # )

        self.scheduler.add_job(
            self._process_kol_tweets,
            trigger=IntervalTrigger(seconds=self.task_config['important_interval_seconds']),
            next_run_time=datetime.now(),
            max_instance=1,
            name="实时处理 KOL 推文"
        )

        self.scheduler.add_job(
            func=self._process_summary_tweets,
            trigger="cron",
            minute=0,
            max_instances=1,
            name="定时处理总结推文"
        )

        # self.scheduler.add_job(
        #     self._process_daily_tasks,
        #     trigger=CronTrigger(hour=0, minute=1),
        #     # next_run_time=datetime.now(),
        #     misfire_grace_time=60,
        #     name="每日定时总结任务"
        # )

        # self.scheduler.add_job(
        #     self._process_send_daily_msg,
        #     trigger=CronTrigger(hour=1, minute=0),
        #     misfire_grace_time=60,
        #     # next_run_time=datetime.now(),
        #     name="每日定时发送日报"
        # )

        # self.scheduler.add_job(
        #     self._clear_updated_set,
        #     trigger=CronTrigger(hour=23, minute=55),
        #     misfire_grace_time=60,
        #     name="每日重置更新项目信息"
        # )


        self.scheduler.start()
        logger.info("调度器已启动")

        try:
            self.loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            logger.info("调度器停止")
        finally:
            self.scheduler.shutdown()

    async def _process_all_sources(self):
        """间隔时间执行的任务"""
        start_time = time.time()
        logger.info("开始定时处理数据源")

        tasks = []

        # for source in self.task_config['mysql_sources']:
        #     tasks.append(self._process_mysql_source(source))
        #
        # for source in self.task_config['mongo_sources']:
        #     tasks.append(self._process_mongo_source(source))

        await asyncio.gather(*tasks)

        if self.is_first_run:
            self.is_first_run = False
            logger.info("首次运行完成，之后将只处理新数据")

        elapsed = time.time() - start_time
        logger.info(f"定时处理完成，耗时 {elapsed:.2f} 秒")

    def stop(self):
        """停止数据处理"""
        if not self.running:
            logger.warning("数据处理器未在运行")
            return

        self.running = False
        if self.thread:
            self.thread.join(timeout=5.0)

        self.mysql_manager.close()
        self.mongo_manager.close()

        logger.info("数据处理器已停止")


    async def _process_kol_tweets(self):

        tweets = self.mysql_manager.get_latest_kol_tweets(self.latest_kol_tweets_time)
        if not tweets:
            return
        logger.info(f"获取 {len(tweets)} 条最新tweets")
        for tweet in tweets:
            tweet_id = tweet["twitter_id"]
            post_time = tweet["tweet_date"]
            content = tweet["text"]
            tweet_user = tweet['twitter_username']

            result = await self.text_analyzer.analyze_text(kol_tweet_template, text=replace_newlines_with_space(content))
            logger.info(result)
            if post_time > self.latest_kol_tweets_time:
                logger.info(f"更新时间 {self.latest_kol_tweets_time}")
                self.latest_kol_tweets_time = int(post_time)
            if not result:
                logger.warning(f"分析文本失败，跳过推文，ID: {tweet_id}")
                continue
            project_data = result.get('project', '')
            token_data = result.get('token', [])

            proj_related_tags = []
            if len(project_data) > 0:
                proj_related_tags = self.mysql_manager.get_projects_tags(project_data, token_data)
            structured_data = {
                'source_id': str(tweet_id),
                'project': json.dumps(project_data),
                'token': json.dumps(token_data),
                'content': content,
                'tags': json.dumps(proj_related_tags)
            }
            logger.info(f"保存处理后的 {tweet_id} 推文到数据库")
            self.mysql_manager.save_processed_kol_tweets(structured_data)

    def _format_tweets(self, tweets):
        lines = []
        for idx, t in enumerate(tweets, start=1):
            text = t.get("text", "").strip()
            if text:
                # 连续空行去掉，替换成单个空格
                clean_text = " ".join(text.split())
                lines.append(f"Tweet {idx}: {clean_text}")
        return "\n\n".join(lines)

    async def _process_summary_tweets(self):
        sh_tz = ZoneInfo("Asia/Shanghai")
        now = datetime.now(sh_tz)
        end = now.replace(minute=0, second=0, microsecond=0)
        end_ts = int(end.timestamp())
        start_ts = int((end - timedelta(hours=1)).timestamp())
        all_tweets = self.mysql_manager.get_target_kol_tweets(start_ts, end_ts)

        formated_tweets = self._format_tweets(all_tweets)
        result = await self.text_analyzer.analyze_text(
            tweet_summary_template,
            all_tweets=formated_tweets
        )

        structured_data = {
            "events": json.dumps(result.get("events", [])),
            "projects": json.dumps(result.get("projects", [])),
            "source_ids": json.dumps([t.get("twitter_id") for t in all_tweets if t.get("twitter_id")])
        }
        logger.info(structured_data)
        self.mysql_manager.save_kol_summary_tweets(structured_data)




