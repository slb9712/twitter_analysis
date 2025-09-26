import json
import logging
import asyncio
from zoneinfo import ZoneInfo


from prompt import *
from database.db_manager import MySQLManager, MongoDBManager
from model.text_analyzer import TextAnalyzer
from tg_bot.bot import send_message, tg_bot
from utils.format_msg import replace_newlines_with_space, format_kol_day_count, format_kol_hour_message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta

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
        self.bot = tg_bot
        self.task_config = task_config
        self.running = False
        self.thread = None
        self.is_first_run = True
        self.concurrency = 5
        self.loop = None
        self.limit_count = self.task_config['process_limit_count']
        self.latest_kol_tweets_time = int(datetime.now(ZoneInfo("Asia/Shanghai")).timestamp())
        self.updated_projects_list = set()
        self.inner_group = '-4879675579'
        self.outer_group = '-4892377641'
        self.daily_group = '-4871521904'#'-4980813719'
        self.daily_hyper_group = '-4942034777'
        self.test_inner_group = '-4834242214'
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

    async def start_bot_async(self):
        """在当前 loop 中启动 bot"""
        await self.bot.start()

    async def start(self):
        """启动调度器并注册任务"""
        if self.running:
            logger.warning("数据处理器已经在运行中")
            return

        self.running = True
        self.loop = asyncio.get_running_loop()
        # asyncio.set_event_loop(self.loop)

        await self.start_bot_async()
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

        self.scheduler.add_job(
            timezone='Asia/Shanghai',
            func=self._send_projects_trends,
            trigger='cron',
            hour=9,
            minute=0,
            max_instances=1,
            name="定时发送项目热度"
        )


        self.scheduler.start()
        logger.info("调度器已启动")

        try:
            await asyncio.Event().wait()
        except (KeyboardInterrupt, SystemExit):
            logger.info("调度器停止")
        finally:
            self.scheduler.shutdown()

    async def stop(self):
        """停止数据处理"""
        await self.bot.stop()
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
            author_uid = t.get("uid", "未知博主")
            if text:
                clean_text = " ".join(text.split())
                lines.append(f"Tweet {idx} (博主: {author_uid}): {clean_text}")
        return "\n\n".join(lines)

    async def _process_summary_tweets(self):
        sh_tz = ZoneInfo("Asia/Shanghai")
        now = datetime.now(sh_tz)
        end = now.replace(minute=0, second=0, microsecond=0)
        end_ts = int(end.timestamp())
        start_ts = int((end - timedelta(hours=1)).timestamp())
        all_tweets = self.mysql_manager.get_target_kol_tweets(start_ts, end_ts)
        if len(all_tweets) == 0:
            logger.warning("No tweets found during the past 1 hour")
        formated_tweets = self._format_tweets(all_tweets)
        logger.info(formated_tweets)
        result = await self.text_analyzer.analyze_text(
            tweet_summary_template,
            all_tweets=formated_tweets
        )
        events = result.get("events", [])
        if len(events) == 0:
            logger.info("近一小时暂无热点事件")
            return
        all_projects = []
        for idx, e in enumerate(events):
            extracted_projects = [item.strip('$') for item in e.get("projects", [])]
            project_token_tags = self.mysql_manager.get_projects_tokens_tags(extracted_projects)
            all_projects.extend(project_token_tags)
            events[idx]['projects'] = project_token_tags
        structured_data = {
            "events": json.dumps(events, ensure_ascii=False),
            "projects": json.dumps(all_projects, ensure_ascii=False),
            "source_ids": json.dumps([t.get("twitter_id") for t in all_tweets if t.get("twitter_id")],
                                     ensure_ascii=False)
        }
        self.mysql_manager.save_kol_summary_tweets(structured_data)
        format_msg = format_kol_hour_message(events)
        success = await send_message(self.daily_group, format_msg)
        if not success:
            logger.error("发送项目热度消息失败")

    async def _once_process_summary_tweets(self):
        sh_tz = ZoneInfo("Asia/Shanghai")
        now = datetime.now(sh_tz)

        yesterday_start = (now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        yesterday_end = now.replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        current_period_end = yesterday_start
        while current_period_end < yesterday_end:
            current_period_start = current_period_end
            current_period_end = current_period_start + timedelta(hours=1)

            start_ts = int(current_period_start.timestamp())
            end_ts = int(current_period_end.timestamp())

            all_tweets = self.mysql_manager.get_target_kol_tweets(start_ts, end_ts)

            formated_tweets = self._format_tweets(all_tweets)
            logger.info(formated_tweets)
            result = await self.text_analyzer.analyze_text(
                tweet_summary_template,
                all_tweets=formated_tweets
            )
            events = result.get("events", [])
            all_projects = []
            for idx, e in enumerate(events):

                extracted_projects = [item.strip('$') for item in e.get("projects", [])]
                project_token_tags = self.mysql_manager.get_projects_tokens_tags(extracted_projects)
                all_projects.extend(project_token_tags)
                events[idx]['projects'] = project_token_tags
            structured_data = {
                "events": json.dumps(events, ensure_ascii=False),
                "projects": json.dumps(all_projects, ensure_ascii=False),
                "source_ids": json.dumps([t.get("twitter_id") for t in all_tweets if t.get("twitter_id")],
                                         ensure_ascii=False)
            }
            logger.info(structured_data)
            self.mysql_manager.save_kol_summary_tweets(structured_data)

    async def _send_projects_trends(self):
        """
        获取昨日所有twitter的tags字段，统计提到的项目排序
        """
        sh_tz = ZoneInfo("Asia/Shanghai")
        now = datetime.now(sh_tz)

        start_ts = int((now - timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).timestamp())
        end_ts = int(now.replace(
            hour=0, minute=0, second=0, microsecond=0
        ).timestamp())
        all_tweets_tags = self.mysql_manager.get_target_structured_tweets(start_ts, end_ts)
        logger.info(all_tweets_tags)
        name_stats = {}
        for item in all_tweets_tags:
            try:
                tags = json.loads(item.get('tags', '[]'))
            except json.JSONDecodeError:
                continue

            for tag_item in tags:
                name = tag_item.get('project_name')
                tag_list = tag_item.get('tags', [])

                if not name:
                    continue

                if name not in name_stats:
                    unique_tags = list(set(tag_list))
                    name_stats[name] = {
                        'tag': unique_tags,
                        'count': 1
                    }
                else:
                    name_stats[name]['count'] += 1
                    merged_tags = list(set(name_stats[name]['tag'] + tag_list))
                    name_stats[name]['tag'] = merged_tags

        result = [
            {
                'name': name,
                'tag': stats['tag'],
                'count': stats['count']
            }
            for name, stats in name_stats.items()
        ]
        logger.info(result)
        result.sort(key=lambda x: (-x['count'], x['name']))

        if len(result) == 0:
            logger.warning("No tweets data")
            return
        format_msg = format_kol_day_count(result)
        success = await send_message(self.daily_group, format_msg)
        if not success:
            logger.error("发送项目热度消息失败")