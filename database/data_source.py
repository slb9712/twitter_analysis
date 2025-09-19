import logging
import pymysql
import pymongo
from pymysql.cursors import DictCursor
from abc import ABC, abstractmethod

logger = logging.getLogger('data_source')


class DataSource(ABC):
    """数据源抽象基类，定义所有数据源通用的接口"""
    
    def __init__(self, config):
        """初始化数据源
        
        Args:
            config (dict): 数据源连接配置
        """
        self.config = config
        self.is_connected = False
    
    @abstractmethod
    def connect(self):
        """连接到数据源"""
        pass
    
    @abstractmethod
    def close(self):
        """关闭数据源连接"""
        pass
    
    @abstractmethod
    def is_alive(self):
        """检查连接是否有效
        
        Returns:
            bool: 连接是否有效
        """
        pass


class MySQLSource(DataSource):
    """MySQL数据源实现"""
    
    def __init__(self, config):
        """初始化MySQL连接
        
        Args:
            config (dict): MySQL连接配置
        """
        super().__init__(config)
        self.conn = None
        self.current_database = config.get('database')
        self.connect()
    
    def connect(self):
        """创建数据库连接"""
        try:
            connection_params = {
                'host': self.config['host'],
                'port': self.config['port'],
                'user': self.config['user'],
                'password': self.config['password'],
                'charset': self.config['charset'],
                'cursorclass': DictCursor,
                'autocommit': True
            }
            
            # 如果配置中指定了数据库，则连接到该数据库
            if self.config.get('database'):
                connection_params['database'] = self.config['database']
                self.current_database = self.config['database']
            
            self.conn = pymysql.connect(**connection_params)
            # 确保会话级设置（比如隔离级别、autocommit）正确
            self._configure_session()
            self.is_connected = True
            
            logger.info(f"成功连接到MySQL服务器: {self.config['host']}:{self.config['port']}")
            if self.current_database:
                logger.info(f"当前数据库: {self.current_database}")
        except Exception as e:
            logger.error(f"MySQL连接失败: {str(e)}")
            self.is_connected = False
            raise
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            self.is_connected = False
            logger.info("MySQL连接已关闭")
    
    def is_alive(self):
        """检查连接是否有效
        
        Returns:
            bool: 连接是否有效
        """
        if not self.conn:
            return False
        
        try:
            self.conn.ping(reconnect=True)
            # 连接可能在 ping 时自动重连，需重新应用会话设置
            self._configure_session()
            return True
        except Exception:
            return False

    def _configure_session(self):
        """为当前连接应用会话级配置。"""
        if not self.conn:
            return
        try:
            try:
                self.conn.autocommit(True)
            except Exception:
                pass
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            except Exception:
                pass
        except Exception:
            pass
    
    def switch_database(self, database_name):
        """切换到指定的数据库
        
        Args:
            database_name (str): 要切换到的数据库名称
            
        Returns:
            bool: 切换是否成功
        """
        if not self.conn:
            logger.error("数据库连接不存在，无法切换数据库")
            return False
            
        if database_name == self.current_database:
            # 已经是当前数据库，无需切换
            return True
            
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(f"USE {database_name}")
                self.current_database = database_name
                logger.info(f"已切换到数据库: {database_name}")
                return True
        except Exception as e:
            logger.error(f"切换到数据库 {database_name} 失败: {str(e)}")
            return False
    
    def execute_query(self, query, params=None):
        """执行查询并返回结果
        
        Args:
            query (str): SQL查询语句
            params (tuple, optional): 查询参数
            
        Returns:
            list: 查询结果列表
        """
        try:
            # 确保连接可用并应用会话设置
            try:
                self.conn.ping(reconnect=True)
            except Exception as ping_error:
                logger.warning(f"MySQL连接ping失败，尝试重连: {ping_error}")
                self.connect()
            self._configure_session()

            with self.conn.cursor() as cursor:
                cursor.execute(query, params or ())
                return list(cursor.fetchall())
        except Exception as e:
            logger.error(f"查询执行失败: {str(e)}\nSQL: {query}\n参数: {params}")
            # 尝试重新连接
            if not self.is_alive():
                self.connect()
            raise
    def execute_many(self, query, params_list):
        """执行批量插入或更新操作，自动重连支持

        Args:
            query (str): SQL语句
            params_list (list): 多条参数记录列表

        Returns:
            int: 总影响行数
        """
        try:
            try:
                self.conn.ping(reconnect=True)  # 确保连接可用
            except Exception as ping_error:
                logger.warning(f"MySQL连接ping失败，尝试重连: {ping_error}")
                self.connect()
            # 确保会话设置在自动重连后被重新应用
            self._configure_session()

            with self.conn.cursor() as cursor:
                cursor.executemany(query, params_list)
            self.conn.commit()
            return cursor.rowcount

        except Exception as e:
            self.conn.rollback()
            logger.error(f"批量执行失败: {e!r}\nSQL: {query}\n参数数目: {len(params_list)}")
            if not self.is_alive():
                self.connect()
            raise

    
    def execute_update(self, query, params=None):
        """执行更新操作
        
        Args:
            query (str): SQL更新语句
            params (tuple, optional): 更新参数
            
        Returns:
            int: 受影响的行数
        """
        try:
            try:
                self.conn.ping(reconnect=True)
            except:
                self.connect()
            # 自动重连后重新应用会话设置
            self._configure_session()
            with self.conn.cursor() as cursor:

                affected_rows = cursor.execute(query, params or ())
                self.conn.commit()
                return affected_rows
        except Exception as e:
            self.conn.rollback()
            logger.error(f"更新执行失败: {e!r}\nSQL: {query}\n参数: {params}")
            # 尝试重新连接
            if not self.is_alive():
                self.connect()
            raise


class MongoSource(DataSource):
    """MongoDB数据源实现"""
    
    def __init__(self, config):
        """初始化MongoDB连接
        
        Args:
            config (dict): MongoDB连接配置
        """
        super().__init__(config)
        self.client = None
        self.db = None
        self.dbs = {}
        self.connect()
    
    def connect(self):
        """创建数据库连接"""
        try:
            # 构建连接URI
            if self.config.get('username') and self.config.get('password'):
                uri = f"mongodb://{self.config['username']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/?authSource=admin"
            else:
                uri = f"mongodb://{self.config['host']}:{self.config['port']}/"
            self.client = pymongo.MongoClient(uri)
            # 默认数据库
            if self.config.get('database'):
                self.db = self.client[self.config['database']]
                self.dbs[self.config['database']] = self.db
            # 测试连接
            self.client.server_info()
            self.is_connected = True
            logger.info(f"成功连接到MongoDB: {self.config['host']}:{self.config['port']}")
        except Exception as e:
            logger.error(f"MongoDB连接失败: {str(e)}")
            self.is_connected = False
            raise
    
    def close(self):
        """关闭数据库连接"""
        if self.client:
            self.client.close()
            self.is_connected = False
            logger.info("MongoDB连接已关闭")
    
    def is_alive(self):
        """检查连接是否有效
        
        Returns:
            bool: 连接是否有效
        """
        if not self.client:
            return False
        
        try:
            self.client.server_info()
            return True
        except Exception:
            return False
    
    def get_database(self, database_name=None):
        """获取指定的数据库实例
        
        Args:
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            pymongo.database.Database: 数据库实例
        """
        if not database_name:
            return self.db

        if database_name not in self.dbs:
            self.dbs[database_name] = self.client[database_name]

        return self.dbs[database_name]
    
    def get_collection(self, collection_name, database_name=None):
        """获取指定的集合实例
        
        Args:
            collection_name (str): 集合名称
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            pymongo.collection.Collection: 集合实例
        """
        db = self.get_database(database_name)
        return db[collection_name]
    
    def find(self, collection_name, query=None, projection=None, sort=None, limit=0, skip=0, database_name=None):
        """查询集合中的文档
        
        Args:
            collection_name (str): 集合名称
            query (dict, optional): 查询条件
            projection (dict, optional): 投影条件
            sort (list, optional): 排序条件
            limit (int, optional): 限制返回的文档数
            skip (int, optional): 跳过的文档数
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            list: 查询结果列表
        """
        collection = self.get_collection(collection_name, database_name)
        cursor = collection.find(query or {}, projection or {})
        
        if sort:
            cursor = cursor.sort(sort)
        
        if skip:
            cursor = cursor.skip(skip)
        
        if limit:
            cursor = cursor.limit(limit)
        
        return list(cursor)
    
    def find_one(self, collection_name, query=None, projection=None, database_name=None):
        """查询集合中的单个文档
        
        Args:
            collection_name (str): 集合名称
            query (dict, optional): 查询条件
            projection (dict, optional): 投影条件
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            dict: 查询结果
        """
        collection = self.get_collection(collection_name, database_name)
        return collection.find_one(query or {}, projection or {})
    
    def insert_one(self, collection_name, document, database_name=None):
        """插入单个文档
        
        Args:
            collection_name (str): 集合名称
            document (dict): 要插入的文档
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            pymongo.results.InsertOneResult: 插入结果
        """
        collection = self.get_collection(collection_name, database_name)
        return collection.insert_one(document)
    
    def insert_many(self, collection_name, documents, database_name=None):
        """插入多个文档
        
        Args:
            collection_name (str): 集合名称
            documents (list): 要插入的文档列表
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            pymongo.results.InsertManyResult: 插入结果
        """
        collection = self.get_collection(collection_name, database_name)
        return collection.insert_many(documents)
    
    def update_one(self, collection_name, filter, update, upsert=False, database_name=None):
        """更新单个文档
        
        Args:
            collection_name (str): 集合名称
            filter (dict): 过滤条件
            update (dict): 更新操作
            upsert (bool, optional): 如果文档不存在是否插入
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            pymongo.results.UpdateResult: 更新结果
        """
        collection = self.get_collection(collection_name, database_name)
        return collection.update_one(filter, update, upsert=upsert)
    
    def update_many(self, collection_name, filter, update, upsert=False, database_name=None):
        """更新多个文档
        
        Args:
            collection_name (str): 集合名称
            filter (dict): 过滤条件
            update (dict): 更新操作
            upsert (bool, optional): 如果文档不存在是否插入
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            pymongo.results.UpdateResult: 更新结果
        """
        collection = self.get_collection(collection_name, database_name)
        return collection.update_many(filter, update, upsert=upsert)
    
    def delete_one(self, collection_name, filter, database_name=None):
        """删除单个文档
        
        Args:
            collection_name (str): 集合名称
            filter (dict): 过滤条件
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            pymongo.results.DeleteResult: 删除结果
        """
        collection = self.get_collection(collection_name, database_name)
        return collection.delete_one(filter)
    
    def delete_many(self, collection_name, filter, database_name=None):
        """删除多个文档
        
        Args:
            collection_name (str): 集合名称
            filter (dict): 过滤条件
            database_name (str, optional): 数据库名称，如果为None则使用默认数据库
            
        Returns:
            pymongo.results.DeleteResult: 删除结果
        """
        collection = self.get_collection(collection_name, database_name)
        return collection.delete_many(filter)