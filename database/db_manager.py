import datetime
import json
import logging
import time
import pymysql
from database.db_factory import db_factory
from bson.objectid import ObjectId

logger = logging.getLogger('db_manager')


class MySQLManager:
    """MySQL数据库管理类，兼容旧代码，内部使用新的数据源抽象"""

    def __init__(self, config):
        """初始化MySQL连接
        
        Args:
            config (dict): MySQL连接配置
        """
        self.config = config
        self.mysql_source = None
        self.current_database = config.get('database')
        self.connect()

    def connect(self, max_retries=3):
        """创建数据库连接
        
        Args:
            max_retries (int, optional): 最大重试次数
        """
        retries = 0
        last_error = None

        while retries <= max_retries:
            try:
                # 使用数据库工厂获取MySQL数据源
                self.mysql_source = db_factory.get_mysql_source(self.config)
                self.current_database = self.mysql_source.current_database

                logger.info(f"成功连接到MySQL服务器: {self.config['host']}:{self.config['port']}")
                if self.current_database:
                    logger.info(f"当前数据库: {self.current_database}")
                return
            except Exception as e:
                last_error = e
                logger.error(f"MySQL连接失败: {str(e)}")

            retries += 1
            if retries <= max_retries:
                # 指数退避重试
                wait_time = 2 ** retries
                logger.info(f"第 {retries} 次重试连接MySQL，等待 {wait_time} 秒")
                time.sleep(wait_time)

        # 达到最大重试次数，抛出最后一个错误
        if last_error:
            logger.error(f"达到最大重试次数 {max_retries}，MySQL连接失败")
            raise last_error

    def close(self):
        """关闭数据库连接"""
        # 数据库工厂会管理连接的关闭，这里不需要显式关闭
        pass

    def switch_database(self, database_name):
        """切换到指定的数据库
        
        Args:
            database_name (str): 要切换到的数据库名称
            
        Returns:
            bool: 切换是否成功
        """
        result = self.mysql_source.switch_database(database_name)
        if result:
            self.current_database = database_name
        return result

    def executemany(self, query, params_list, max_retries=3):
        """执行多条 SQL 插入/更新操作，支持断线自动重连"""
        retries = 0
        last_error = None

        while retries <= max_retries:
            try:
                if not self.mysql_source.is_alive():
                    logger.warning("MySQL连接不可用，尝试重新连接")
                    self.connect()
                return self.mysql_source.execute_many(query, params_list)


            except pymysql.err.OperationalError as e:
                last_error = e
                error_code = e.args[0] if len(e.args) > 0 else None

                # 错误码 2006 (MySQL server has gone away) / 2013 (Lost connection)
                if error_code in (2006, 2013):
                    logger.warning(f"MySQL连接错误 (错误码: {error_code})，尝试重新连接: {e}")
                    self.connect()
                else:
                    logger.error(f"MySQL执行失败: {e}")
                    raise
            except Exception as e:
                last_error = e
                logger.error(f"MySQL执行失败: {e}")
                raise

            retries += 1
            if retries <= max_retries:
                wait_time = 2 ** retries
                logger.info(f"第 {retries} 次重试 executemany，等待 {wait_time} 秒后再试")
                time.sleep(wait_time)

        # 达到最大重试次数仍失败
        logger.error(f"executemany 执行失败，重试次数达到上限: {last_error}")
        raise last_error

    def execute_query(self, query, params=None, max_retries=3):
        """执行查询并返回结果
        
        Args:
            query (str): SQL查询语句
            params (tuple, optional): 查询参数
            max_retries (int, optional): 最大重试次数
            
        Returns:
            list: 查询结果列表
        """
        retries = 0
        last_error = None

        while retries <= max_retries:
            try:
                # 检查连接是否有效，如果无效则重新连接
                if not self.mysql_source.is_alive():
                    logger.warning("MySQL连接已断开，尝试重新连接")
                    self.connect()

                return self.mysql_source.execute_query(query, params)
            except pymysql.err.OperationalError as e:
                last_error = e
                error_code = e.args[0] if len(e.args) > 0 else None

                # 处理特定的错误码
                if error_code in (2006, 2013):
                    logger.warning(f"MySQL连接错误 (错误码: {error_code})，尝试重新连接: {str(e)}")
                    self.connect()
                else:
                    raise
            except Exception as e:
                # 其他未知错误
                last_error = e
                logger.error(f"查询执行失败: {str(e)}\nSQL: {query}\n参数: {params}")
                raise

            retries += 1
            if retries <= max_retries:
                wait_time = 2 ** retries
                logger.info(f"第 {retries} 次重试，等待 {wait_time} 秒")
                time.sleep(wait_time)

        # 达到最大重试次数，抛出最后一个错误
        if last_error:
            logger.error(f"达到最大重试次数 {max_retries}，查询失败: {str(last_error)}")
            raise last_error

        return []

    def execute_update(self, query, params=None, max_retries=3):
        """执行更新操作
        
        Args:
            query (str): SQL更新语句
            params (tuple, optional): 更新参数
            max_retries (int, optional): 最大重试次数
            
        Returns:
            int: 受影响的行数
        """
        retries = 0
        last_error = None

        while retries <= max_retries:
            try:
                # 检查连接是否有效，如果无效则重新连接
                if not self.mysql_source.is_alive():
                    logger.warning("MySQL连接已断开，尝试重新连接")
                    self.connect()

                return self.mysql_source.execute_update(query, params)
            except pymysql.err.OperationalError as e:
                # 捕获操作错误（如连接断开）
                last_error = e
                error_code = e.args[0] if len(e.args) > 0 else None

                # 处理特定的错误码
                if error_code in (2006, 2013):  # 2006: MySQL server has gone away, 2013: Lost connection
                    logger.warning(f"MySQL连接错误 (错误码: {error_code})，尝试重新连接: {str(e)}")
                    self.connect()
                else:
                    # 其他操作错误直接抛出
                    raise
            except Exception as e:
                # 其他未知错误
                last_error = e
                logger.error(f"更新执行失败: {str(e)}\nSQL: {query}\n参数: {params}")
                raise

            retries += 1
            if retries <= max_retries:
                # 指数退避重试
                wait_time = 2 ** retries
                logger.info(f"第 {retries} 次重试，等待 {wait_time} 秒")
                time.sleep(wait_time)

        # 达到最大重试次数，抛出最后一个错误
        if last_error:
            logger.error(f"达到最大重试次数 {max_retries}，更新失败: {str(last_error)}")
            raise last_error

        return 0

    def get_last_processed_id(self, source_name, source_type='mysql'):
        """获取最后处理的记录ID
        
        Args:
            source_name (str): 数据源名称
            source_type (str, optional): 数据源类型，默认为'mysql'
            
        Returns:
            int/str: 最后处理的ID，如果没有则返回0
        """
        query = """SELECT last_id FROM extracted_record 
                  WHERE source_type = %s AND source_name = %s LIMIT 1"""
        result = self.execute_query(query, (source_type, source_name))
        return result[0]['last_id'] if result else 0

    def update_progress(self, source_name, last_id, source_type='mysql'):
        """更新处理进度
        
        Args:
            source_name (str): 数据源名称
            last_id (int/str): 最后处理的ID
            source_type (str, optional): 数据源类型，默认为'mysql'
        """
        # 检查记录是否存在
        check_query = """SELECT id FROM extracted_record 
                       WHERE source_type = %s AND source_name = %s LIMIT 1"""
        result = self.execute_query(check_query, (source_type, source_name))
        timestamp = int(time.time())
        if result:
            # 更新现有记录
            update_query = """UPDATE extracted_record 
                            SET last_id = %s, updated_at = %s 
                            WHERE source_type = %s AND source_name = %s"""
            self.execute_update(update_query, (last_id, timestamp, source_type, source_name))
        else:
            # 插入新记录
            insert_query = """INSERT INTO extracted_record 
                            (source_type, source_name, last_id, created_at, updated_at) 
                            VALUES (%s, %s, %s, %s, %s)"""
            self.execute_update(insert_query, (source_type, source_name, last_id, timestamp, timestamp))

    def get_latest_record_id(self, table_name, id_column):
        """获取表中最新记录的ID
        
        Args:
            table_name (str): 表名
            id_column (str): ID列名
            
        Returns:
            int: 最新记录的ID，如果没有记录则返回None
        """
        query = f"SELECT MAX({id_column}) as max_id FROM {table_name}"
        result = self.execute_query(query)

        if result and result[0]['max_id'] is not None:
            return result[0]['max_id']
        return None

    def get_new_records(self, table_name, id_column, content_column, last_id, limit=100):
        """获取新记录
        
        Args:
            table_name (str): 表名
            id_column (str): ID列名
            content_column (str): 内容列名
            last_id (int): 上次处理的最后ID
            limit (int, optional): 限制返回的记录数
            
        Returns:
            list: 新记录列表
        """
        query = f"""SELECT * FROM (SELECT * FROM {table_name} 
                  WHERE {id_column} > %s ORDER BY {id_column} DESC LIMIT %s) AS sub ORDER BY {id_column} ASC"""
        return self.execute_query(query, (last_id, limit))

    def save_structured_data(self, data):
        """保存结构化数据
        
        Args:
            data (dict): 结构化数据
            
        Returns:
            int: 新插入记录的ID
        """
        try:
            query = """INSERT INTO structured_msg 
                      (source_type,source_db, source_name, source_id, project, token, content, 
                       attitude, date, token_holder, address, related_projects,original_en, original_zh, actions, news_events, predict_actions, is_cmc, created_at) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                      ON DUPLICATE KEY UPDATE 
                        project=VALUES(project),
                        token=VALUES(token),
                        content=VALUES(content),
                        attitude=VALUES(attitude),
                        date=VALUES(date),
                        token_holder=VALUES(token_holder),
                        address=VALUES(address),
                        related_projects=VALUES(related_projects),
                        original_en=VALUES(original_en),
                        original_zh=VALUES(original_zh),
                        actions=VALUES(actions),
                        news_events=VALUES(news_events),
                        predict_actions=VALUES(predict_actions),
                        is_cmc=VALUES(is_cmc),
                        created_at=VALUES(created_at)"""

            params = (
                data['source_type'],
                data['source_db'],
                data['source_name'],
                data['source_id'],
                data['project'],
                data['token'],
                data['content'],
                data['attitude'],
                data['date'],
                data['token_holder'],
                data['address'],
                data.get('related_projects', ''),
                data.get('original_en', None),
                data.get('original_zh', None),
                data.get('actions', None),
                data.get('news_events', None),
                data.get('predict_actions', None),
                data.get('is_cmc', None),
                int(time.time())
            )
            self.execute_update(query, params)
        except Exception as e:
            logger.info(data)
            logger.error(f"保存至数据库失败: {str(e)}")

        # 获取最后插入的ID
        # last_id_query = "SELECT LAST_INSERT_ID() as last_id"
        # result = self.mysql_source.execute_query(last_id_query)
        # return result[0]['last_id'] if result else None

    def save_project_daily_news(self, data):
        """保存项目每日新闻数据
        
        Args:
            data (list): 包含多个项目新闻数据的列表
        """
        if not data:
            return

        try:
            query = """INSERT INTO daily_project_news 
                      (project_name, title, summary, source, source_counts, actions, title_en, summary_en, created_at) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            params_list = []
            now_ts = int(time.time())

            for item in data:
                if not (item.get('title') or not item.get('summary')):
                    continue
                params = (
                    item['project'],
                    item['title'],
                    item['summary'],
                    item['source'],
                    item['source_counts'],
                    item['actions'],
                    item['title_en'],
                    item['summary_en'],
                    now_ts
                )
                params_list.append(params)

            self.executemany(query, params_list)

        except Exception as e:
            logger.error(f"保存项目每日新闻数据失败: {str(e)}")

    def get_recent_news(self, time_window=7):
        """

        :param time_window: int
        :return:
        """
        try:
            now = datetime.datetime.now(tz=datetime.timezone.utc)
            start_ts = int((now - datetime.timedelta(days=time_window)).replace(
                hour=0, minute=0, second=0, microsecond=0
            ).timestamp())
            query = f"""SELECT source_db, source_type, source_name, source_id, related_projects FROM structured_msg WHERE created_at > {start_ts} AND related_projects != ''"""

            return self.execute_query(query)

        except Exception as e:
            logger.error(e)

    def fetch_recent_news(self, start_ts, end_ts, is_related_project=True):
        try:
            sql = """SELECT id, source_type, source_db, source_name, source_id, related_projects, created_at,
                   original_en, actions, news_events, predict_actions, is_cmc
            FROM structured_msg
            WHERE created_at >= %(start_ts)s AND created_at <= %(end_ts)s
            """

            # 根据 is_related_project 追加条件
            if is_related_project:
                sql += " AND (related_projects IS NOT NULL OR is_cmc IS NOT NULL)"
            else:
                sql += " AND (related_projects = '' OR related_projects IS NULL)"

            news = self.execute_query(sql, {"start_ts": start_ts, "end_ts": end_ts})

            news_list = []
            for item in news:
                key = (item['source_type'], item['source_db'], item['source_name'], str(item['source_id']))
                item_dict = {
                    "id": item['id'],
                    "source_type": item['source_type'],
                    "source_db": item['source_db'],
                    "source_name": item['source_name'],
                    "source_id": str(item['source_id']),
                    "related_projects": item['related_projects'],
                    "created_at": item['created_at'],
                    "original_en": item['original_en'],
                    "actions": item['actions'],
                    "news_events": item['news_events'],
                    "predict_actions": item['predict_actions'],
                    "is_cmc": item['is_cmc']
                }
                news_list.append(item_dict)
            # 5. 按 project 聚合
            if is_related_project:
                project_map = {}
                for item in news_list:
                    try:
                        proj = item.get('related_projects')
                        proj_cmc = item.get('is_cmc')
                        related_projects = json.loads(proj) if proj else []
                        is_cmc_list = json.loads(proj_cmc) if proj_cmc else []
                        if not isinstance(related_projects, list):
                            related_projects = []
                        if not isinstance(is_cmc_list, list):
                            is_cmc_list = []

                        projects = related_projects + is_cmc_list
                    except Exception:
                        projects = []
                    projects = list(set(projects))
                    for project in projects:
                        if project not in project_map:
                            project_map[project] = []
                        project_map[project].append(item)
                sorted_projects = sorted(project_map.items(), key=lambda x: len(x[1]), reverse=True)

                sorted_project_list = [
                    {"project_name": project, "recent_news": news_items}
                    for project, news_items in sorted_projects
                ]
                return sorted_project_list
            else:
                return []
        except Exception as e:
            logger.error(f"获取最近新闻数据失败: {str(e)}")

    def get_hyper_related_projects(self):
        """获取与Hyperliquid相关的项目列表
        
        Returns:
            list: 包含项目名称的列表
        """
        try:
            query = "SELECT project_name, project_id FROM projects WHERE is_hyper=1"
            result = self.execute_query(query)
            return [{"uid": result['project_id'], "project_name": result['project_name']} for result in
                    result] if result else []
        except Exception as e:
            logger.error(f"获取Hyperliquid相关项目失败: {str(e)}")
            return []

    def get_hyper_daily_tweets(self, start_ts, end_ts):
        """获取Hyperliquid相关项目的每日推文
        """
        try:
            query = f"SELECT uid, twitter_username, text, permanent_url, tweet_date FROM hyperliquid_tweets WHERE tweet_date >= '{start_ts}' AND tweet_date <= '{end_ts}'"
            result = self.execute_query(query)
            return result
        except Exception as e:
            logger.error(f"获取HyperLiquid相关项目推文信息失败: {str(e)}")
            return []

    def save_hyper_project_tweets_summary(self, data):
        """保存HyperLiquid项目推文摘要
        Args:
            data (list): 包含多个Hyper项目推文总结的列表
        """
        if not data:
            return

        try:
            query = """INSERT INTO daily_project_tweets 
                      (project_name, uid, summary_en, summary_zh, source_counts, created_at) 
                      VALUES (%s, %s, %s, %s, %s, %s)"""

            params_list = []
            now_ts = int(time.time())

            for item in data:
                if item.get("summary_en") == '[]' or item.get("summary_zh") == '[]' or item.get("summary_zh") == '[[]]':
                    continue
                params = (
                    item['project_name'],
                    item['uid'],
                    item['summary_en'],
                    item['summary_zh'],
                    item['source_counts'],
                    now_ts
                )
                params_list.append(params)

            self.executemany(query, params_list)

        except Exception as e:
            logger.error(f"保存项目每日Hyper项目推文总结失败: {str(e)}")

    def get_hyper_tweets_summary(self, start_ts):
        """获取Hyper项目推文总结"""
        try:
            query = f"SELECT project_name, summary_en, summary_zh, source_counts FROM daily_project_tweets WHERE created_at >= {start_ts}"
            result = self.execute_query(query)
            return [res for res in result if res.get('summary_zh') and res.get("summary_zh") != "[]"]
        except Exception as e:
            logger.error(f"获取HyperLiquid相关项目推文信息失败: {str(e)}")
            return []

    def insert_imp_news(self, data):
        try:
            query = f"INSERT INTO imp_news (content, source_id, trend, token, cex, score, created_at)  VALUES (%s, %s,%s, %s, %s, %s, %s)"
            now_ts = int(time.time())
            insert_data = []
            for item in data:
                params = (
                    item.get('content', ''),
                    item.get('source_id', ""),
                    item.get("trend", ""),
                    item.get('token', ''),
                    item.get('cex'),
                    item.get('score', 1),
                    now_ts
                )
                insert_data.append(params)

            self.executemany(query, insert_data)
        except Exception as e:
            logger.error(f"插入重要新闻到数据库失败：: {str(e)}")

    def get_latest_kol_tweets(self, time):
        """
        获取最新的推文
        :param time:
        :return: tweets
        """
        try:
            query = "SELECT uid, twitter_id, twitter_username, text, permanent_url, tweet_date FROM kol_tweets WHERE tweet_date > %s"
            result = self.execute_query(query, (time,))

            return result
        except Exception as e:
            logger.error(f"获取最新推文失败：: {str(e)}")



    def save_processed_kol_tweets(self, data):
        """
        保存处理后的kol数据到数据库
        :param data:
        :return:
        """
        try:
            query = """INSERT INTO structured_kol_tweets 
                      ( source_id, project, token, content, created_at) 
                      VALUES (%s, %s, %s, %s, %s)
                      ON DUPLICATE KEY UPDATE 
                        source_id = VALUES(source_id),
                        project=VALUES(project),
                        token=VALUES(token),
                        content=VALUES(content),
                        created_at=VALUES(created_at)"""

            params = (
                data['source_id'],
                data['project'],
                data['token'],
                data['content'],
                int(time.time())
            )
            self.execute_update(query, params)
        except Exception as e:
            logger.info(data)
            logger.error(f"保存至数据库失败: {str(e)}")


class MongoDBManager:
    """MongoDB数据库管理类，兼容旧代码，内部使用新的数据源抽象"""

    def __init__(self, config):
        """初始化MongoDB连接
        
        Args:
            config (dict): MongoDB连接配置
        """
        self.config = config
        self.mongo_source = None
        self.connect()

    def connect(self, max_retries=3):
        """创建数据库连接
        
        Args:
            max_retries (int, optional): 最大重试次数
        """
        retries = 0
        last_error = None

        while retries <= max_retries:
            try:
                # 使用数据库工厂获取MongoDB数据源
                self.mongo_source = db_factory.get_mongo_source(self.config)
                logger.info(f"成功连接到MongoDB: {self.config['host']}:{self.config['port']}")
                return
            except Exception as e:
                last_error = e
                logger.error(f"MongoDB连接失败: {str(e)}")

            retries += 1
            if retries <= max_retries:
                # 指数退避重试
                wait_time = 2 ** retries
                logger.info(f"第 {retries} 次重试连接MongoDB，等待 {wait_time} 秒")
                time.sleep(wait_time)

        # 达到最大重试次数，抛出最后一个错误
        if last_error:
            logger.error(f"达到最大重试次数 {max_retries}，MongoDB连接失败")
            raise last_error

    def close(self):
        """关闭数据库连接"""
        # 数据库工厂会管理连接的关闭，这里不需要显式关闭
        pass

    def get_last_processed_id(self, collection_name, mysql_manager, max_retries=3):
        """获取最后处理的记录ID
        
        Args:
            collection_name (str): 集合名称
            mysql_manager (MySQLManager): MySQL管理器实例
            max_retries (int, optional): 最大重试次数
            
        Returns:
            str: 最后处理的ID，如果没有则返回None
        """
        # 直接调用mysql_manager的方法，但传入'mongodb'作为source_type
        query = """SELECT last_id FROM extracted_record 
                  WHERE source_type = 'mongodb' AND source_name = %s LIMIT 1"""
        try:
            result = mysql_manager.execute_query(query, (collection_name,), max_retries)
            return result[0]['last_id'] if result else None
        except Exception as e:
            logger.error(f"获取MongoDB数据源 {collection_name} 最后处理ID失败: {str(e)}")
            # 返回None而不是抛出异常，允许处理继续进行
            return None

    def update_progress(self, collection_name, last_id, mysql_manager, max_retries=3):
        """更新处理进度
        
        Args:
            collection_name (str): 集合名称
            last_id (str): 最后处理的ID
            mysql_manager (MySQLManager): MySQL管理器实例
            max_retries (int, optional): 最大重试次数
        """
        try:
            # 检查记录是否存在
            check_query = """SELECT id FROM extracted_record 
                           WHERE source_type = 'mongodb' AND source_name = %s LIMIT 1"""
            result = mysql_manager.execute_query(check_query, (collection_name,), max_retries)
            timestamp = int(time.time())
            if result:
                # 更新现有记录
                update_query = """UPDATE extracted_record 
                                SET last_id = %s, updated_at = %s 
                                WHERE source_type = 'mongodb' AND source_name = %s"""
                mysql_manager.execute_update(update_query, (str(last_id), timestamp, collection_name), max_retries)
            else:
                # 插入新记录
                insert_query = """INSERT INTO extracted_record 
                                (source_type, source_name, last_id, created_at, updated_at) 
                                VALUES ('mongodb', %s, %s, %s, %s)"""
                mysql_manager.execute_update(insert_query, (collection_name, str(last_id), timestamp, timestamp),
                                             max_retries)

            logger.info(f"MongoDB数据源 {collection_name} 处理进度已更新，最后处理ID: {last_id}")
        except Exception as e:
            logger.error(f"更新MongoDB数据源 {collection_name} 处理进度失败: {str(e)}")
            # 不抛出异常，允许处理继续进行

    def get_latest_record_id(self, collection_name, id_field, database_name=None):
        """获取集合中最新记录的ID
        
        Args:
            collection_name (str): 集合名称
            id_field (str): ID字段名
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            str/ObjectId: 最新记录的ID，如果没有记录则返回None
        """
        # 使用新的数据源API
        sort = [(id_field, -1)]
        results = self.mongo_source.find(collection_name, query=None, projection=None, sort=sort, limit=1,
                                         database_name=database_name)

        if results:
            # 确保返回的ID是字符串格式
            result = results[0]
            return str(result[id_field]) if id_field != '_id' else str(result['_id'])
        return None

    def get_latest_proposal_id(self, collection_name, id_field, database_name=None):
        """获取符合特定条件的最新提案ID
        
        Args:
            collection_name (str): 集合名称
            id_field (str): ID字段名
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            str/ObjectId: 最新记录的ID，如果没有记录则返回None
        """
        current_timestamp = int(time.time())
        # 构建查询条件：end字段小于当前时间戳，scoresState为final，state为closed
        query = {
            'created': {'$lt': current_timestamp},
            # 'scoresState': 'final',
            # 'state': 'closed'
        }

        # 按end字段降序排序并获取第一条记录
        sort = [('created', -1)]
        results = self.mongo_source.find(collection_name, query=None, projection=None, sort=sort, limit=1,
                                         database_name=database_name)

        if results:
            # 确保返回的ID是字符串格式
            result = results[0]
            return str(result[id_field]) if id_field != '_id' else str(result['_id'])
        return None

    def get_database(self, database_name=None):
        """获取指定的数据库实例
        
        Args:
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            pymongo.database.Database: 数据库实例
        """
        return self.mongo_source.get_database(database_name)

    def get_new_records(self, collection_name, id_field, content_field, last_id=None, limit=100, database_name=None):
        """获取新记录
        
        Args:
            collection_name (str): 集合名称
            id_field (str): ID字段名
            content_field (str): 内容字段名
            last_id (str, optional): 上次处理的最后ID
            limit (int, optional): 限制返回的记录数
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            list: 新记录列表
        """
        # 构建查询条件
        query = {}
        if last_id:
            if id_field == '_id':
                try:
                    query[id_field] = {'$gt': ObjectId(last_id)}
                except Exception:
                    query[id_field] = {'$gt': last_id}
            else:
                query[id_field] = {'$gt': last_id}

        sort = [(id_field, -1)]
        projection = {id_field: 1, content_field: 1, 'channelName': 1, 'includeUrl': 1}

        results = self.mongo_source.find(collection_name, query, projection, sort, limit, 0, database_name)

        return list(reversed(results))

    def get_new_proposals(self, collection_name, id_field, last_id=None, limit=100, database_name=None):
        """获取新提案
        
        Args:
            collection_name (str): 集合名称
            id_field (str): ID字段名
            last_id (str, optional): 上次处理的最后ID
            limit (int, optional): 限制返回的记录数
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            list: 新提案列表
        """
        # 构建查询条件
        query = {
            # 'scoresState': 'final',
            # 'state': 'closed'
        }

        current_timestamp = int(time.time())
        # query['created'] = {'$lt': current_timestamp}

        if last_id:

            if id_field == '_id':
                try:
                    query[id_field] = {'$gt': ObjectId(last_id)}
                except Exception:
                    # 如果转换失败，则使用字符串比较
                    query[id_field] = {'$gt': last_id}
            else:
                query[id_field] = {'$gt': last_id}

        sort = [(id_field, 1)]  # 按ID升序排序

        # 查询数据
        return self.mongo_source.find(collection_name, query, None, sort, limit, 0, database_name)

    def get_latest_techflow_data(self, last_id=None):
        """
        获取 news.channel 集合中指定条件的数据

        Args:
            last_id (str|None): 上次的最后一条记录的 _id（字符串形式的 ObjectId）

        Returns:
            list: 满足条件的文档列表
        """
        query = {"channelName": "theblockbeats"}

        if last_id:
            try:
                query["_id"] = {"$gt": ObjectId(last_id)}
            except Exception:
                raise ValueError("last_id 必须是合法的 ObjectId 字符串")

            # 按时间顺序返回
            sort = [("_id", 1)]
            return self.mongo_source.find(
                collection_name="channel",
                query=query,
                sort=sort,
                database_name="news"
            )

        else:
            sort = [("_id", -1)]
            return self.mongo_source.find(
                collection_name="channel",
                query=query,
                sort=sort,
                limit=1,
                database_name="news"
            )

    def get_target_techflow_data(self, start_t: datetime, end_t:datetime):
        """
        获取 news.channel 集合中指定时间范围内的数据

        Args:
            start_t (datetime): 开始时间
            end_t (datetime): 结束时间
        Returns:
            list: 满足条件的文档列表
        """

        query = {
            "channelName": "theblockbeats",
            "createdAt": {"$gte": start_t, "$lte": end_t}
        }
        sort = [("createdAt", -1)]

        return self.mongo_source.find(
            collection_name="channel",
            query=query,
            sort=sort,
            database_name="news"
        )


