import logging
from database.data_source import MySQLSource, MongoSource

logger = logging.getLogger('db_factory')

class DatabaseFactory:
    """数据库工厂类，负责管理所有数据源实例"""
    
    def __init__(self):
        """初始化数据库工厂"""
        self.sources = {}
    
    def get_mysql_source(self, config, source_key=None):
        """获取MySQL数据源实例
        
        Args:
            config (dict): MySQL连接配置
            source_key (str, optional): 数据源键名，如果为None则使用host:port:database作为键名
            
        Returns:
            MySQLSource: MySQL数据源实例
        """
        if source_key is None:
            # 使用host:port:database作为键名
            database = config.get('database', '')
            source_key = f"mysql:{config['host']}:{config['port']}:{database}"
        
        # 从池子中找到相应的连接实例返回
        if source_key in self.sources:
            source = self.sources[source_key]
            # 重新连接
            if not source.is_alive():
                logger.info(f"数据源 {source_key} 连接已断开，尝试重新连接")
                source.connect()
            return source
        
        # 如果没有就重新创建一个
        try:
            source = MySQLSource(config)
            self.sources[source_key] = source
            return source
        except Exception as e:
            logger.error(f"创建MySQL数据源 {source_key} 失败: {str(e)}")
            raise
    
    def get_mongo_source(self, config, source_key=None):
        """获取MongoDB数据源实例
        
        Args:
            config (dict): MongoDB连接配置
            source_key (str, optional): 数据源键名，如果为None则使用host:port:database作为键名
            
        Returns:
            MongoSource: MongoDB数据源实例
        """
        if source_key is None:
            # 使用host:port:database作为键名
            database = config.get('database', '')
            source_key = f"mongodb:{config['host']}:{config['port']}:{database}"
        
        # 如果已存在该数据源实例，则直接返回
        if source_key in self.sources:
            source = self.sources[source_key]
            # 检查连接是否有效，如果无效则重新连接
            if not source.is_alive():
                logger.info(f"数据源 {source_key} 连接已断开，尝试重新连接")
                source.connect()
            return source
        
        # 创建新的数据源实例
        try:
            source = MongoSource(config)
            self.sources[source_key] = source
            return source
        except Exception as e:
            logger.error(f"创建MongoDB数据源 {source_key} 失败: {str(e)}")
            raise
    
    def close_all(self):
        """关闭所有数据源连接"""
        for key, source in self.sources.items():
            try:
                source.close()
                logger.info(f"数据源 {key} 已关闭")
            except Exception as e:
                logger.error(f"关闭数据源 {key} 失败: {str(e)}")
        
        # 清空数据源字典
        self.sources.clear()
    
    def close_source(self, source_key):
        """关闭指定的数据源连接
        
        Args:
            source_key (str): 数据源键名
            
        Returns:
            bool: 是否成功关闭
        """
        if source_key in self.sources:
            try:
                self.sources[source_key].close()
                del self.sources[source_key]
                logger.info(f"数据源 {source_key} 已关闭")
                return True
            except Exception as e:
                logger.error(f"关闭数据源 {source_key} 失败: {str(e)}")
                return False
        return False


# 创建全局数据库工厂实例
db_factory = DatabaseFactory()