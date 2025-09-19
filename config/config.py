import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库配置
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'analysis_db'),
    'charset': 'utf8mb4'
}

MONGO_CONFIG = {
    'host': os.getenv('MONGO_HOST'),
    'port': int(os.getenv('MONGO_PORT')),
    'username': os.getenv('MONGO_USER', ''),
    'password': os.getenv('MONGO_PASSWORD', ''),
    'database': os.getenv('MONGO_DATABASE', 'analysis_db')
}

# 任务配置
TASK_CONFIG = {
    # 任务执行间隔（分钟）
    'interval_minutes': int(os.getenv('TASK_INTERVAL_MINUTES', 10)),
    'important_interval_seconds': int(os.getenv('IMPORTANT_INTERVAL_SECONDS', 5)),
    'process_limit_count': int(os.getenv('TASK_PROCESS_LIMIT_COUNT', 2)),
    # MySQL数据源配置
    'mysql_sources': [
        {
            'table': 'kol_tweets',
            'id_column': 'id',
            'content_column': 'text'
        },
        # {
        #     'table': 'crypto_panic',
        #     'id_column': 'id',
        #     'content_column': 'context'
        # }
    ],

    'mongo_sources': [
        # {
        #     'collection': 'channel',
        #     'id_field': '_id',
        #     'content_field': 'content',
        #     'database': 'news'
        # },
        # {
        #     'collection': 'proposal',
        #     'id_field': '_id',
        #     'is_proposal': True,
        #     'database': 'snapshot'
        # }
    ],
    
    # 存储分析结果
    'target_table': 'structured_msg',
    
    # 进度记录表
    'progress_table': 'extracted_record'
}

# OpenAI配置
OPENAI_CONFIG = {
    'api_key': os.getenv('OPENAI_API_KEY'),
    'base_url': os.getenv('OPENAI_BASE_URL'),
    'model': os.getenv('OPENAI_MODEL')
}

# DEEPSEEK
DEEPSEEK_CONFIG = {
    'api_key': os.getenv('DEEPSEEK_API_KEY'),
    'base_url': os.getenv('DEEPSEEK_BASE_URL'),
    'model': os.getenv('DEEPSEEK_MODEL')
}

# LOCAL MODEL
LOCAL_MODEL_CONFIG = {
    'api_key': os.getenv('MODEL_API_KEY'),
    'base_url': os.getenv('MODEL_BASE_URL'),
    'model': os.getenv('MODEL_NAME')
}
