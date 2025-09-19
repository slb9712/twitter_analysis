import asyncio
import sys
import os
import logging
import threading
import time
from typing import List, Set
from telegram import Bot, Update
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes, CommandHandler, CallbackQueryHandler
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.config import MYSQL_CONFIG, MONGO_CONFIG
from database.db_manager import MySQLManager, MongoDBManager
from utils.split_msg import smart_split_html

logger = logging.getLogger('tg_bot')
logger.setLevel(logging.WARNING)

# 加载环境变量
load_dotenv()

class TelegramBot:
    def __init__(self):
        """初始化TG_bot"""
        self.bot_token = os.getenv('TG_BOT_TOKEN')
        if not self.bot_token:
            logger.error("错误: 未设置TG_BOT_TOKEN环境变量")
            raise ValueError("TG_BOT_TOKEN环境变量未设置")
            
        self.bot = Bot(token=self.bot_token)
        self.app = None
        self.group_ids: Set[str] = set()  # 使用集合存储群组ID，避免重复
        self.group_ids.add("-4892377641")
        self.is_running = False
        self.mysql_manager = MySQLManager(MYSQL_CONFIG)
        self.mongo_manager = MongoDBManager(MONGO_CONFIG)
        self.temp_data = None
        logger.info("TG_bot初始化完成，默认群组ID: -4892377641")
    
    async def _handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """处理接收到的消息，提取群组ID"""
        chat_id = str(update.effective_chat.id)
        if update.effective_chat.type in ['group', 'supergroup']:
            if chat_id not in self.group_ids:
                self.group_ids.add(chat_id)
                logger.info(f"新增群组ID: {chat_id}")
            print(chat_id)
            logger.debug(f"收到来自群组 {chat_id} 的消息: {update.message.text if update.message else '非文本消息'}")
    
    async def send_message_to_group(self, chat_id: str, text: str) -> bool:
        """向指定群组发送消息
        
        Args:
            chat_id: 群组ID
            text: 消息内容
            
        Returns:
            bool: 发送是否成功
         """
        if not self.app:
            logger.error("Application 尚未初始化，无法发送消息")
            return False
            
        msgs = smart_split_html(text)
        max_retries = 3
        retry_interval = 1
        
        for msg in msgs:
            for attempt in range(1, max_retries + 1):
                try:
                    await self.app.bot.send_message(chat_id=chat_id, text=msg, parse_mode='HTML')
                    break
                except Exception as e:
                    if "Timed out" in str(e):
                        if attempt < max_retries:
                            logger.warning(f"向群组 {chat_id} 发送消息超时，第 {attempt} 次重试，等待 {retry_interval} 秒")
                            await asyncio.sleep(retry_interval)
                        else:
                            logger.error(f"向群组 {chat_id} 发送消息失败，已重试 {max_retries} 次: {str(e)}")
                            return False
                    else:
                        # 非超时错误，直接返回失败
                        logger.error(f"向群组 {chat_id} 发送消息失败: {str(e)}")
                        return False
        
        return True
    
    async def broadcast_message(self, text: str) -> dict:
        """向所有群组广播消息
        
        Args:
            text: 消息内容
            
        Returns:
            dict: 包含成功和失败的群组ID列表
        """
        results = {
            "success": [],
            "failed": []
        }
        
        group_ids = self.group_ids.copy()
        for chat_id in group_ids:
            success = await self.send_message_to_group(chat_id, text)
            if success:
                results["success"].append(chat_id)
            else:
                # 移除被移出群组，发送失败的群组id
                results["failed"].append(chat_id)
                # self.group_ids.remove(chat_id)
                # logger.info(f"已从群组列表中移除ID: {chat_id}")
        
        return results
    
    def start(self):
        if self.is_running:
            logger.warning("TG_bot已经在运行中")
            return

        loop = asyncio.new_event_loop()
        async def _run():
            self.app = ApplicationBuilder().token(self.bot_token).build()

            # self.app.add_handler(CommandHandler("7DaysNews", self.handle_7daysnews))  # 注册命令处理器
            # self.app.add_handler(CallbackQueryHandler(self.handle_project_detail, pattern=r'^project_'))
            self.app.add_handler(MessageHandler(filters.ALL, self._handle_message))
            await self.app.initialize()
            await self.app.start()
            await self.app.updater.start_polling()
            self.is_running = True
            logger.info("TG_bot已启动，正在监听群组")

            while self.is_running:
                await asyncio.sleep(1)

            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

        def run_bot():
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_run())

        self.bot_thread = threading.Thread(target=run_bot)
        self.bot_thread.daemon = True
        self.bot_thread.start()

    def stop(self):
        if not self.is_running:
            logger.warning("TG_bot未在运行")
            return
        
        self.is_running = False
        logger.info("TG_bot已停止")
    
    def get_group_ids(self) -> List[str]:
        """获取当前所有群组ID
        
        Returns:
            List[str]: 群组ID列表
        """
        return list(self.group_ids)

    def _cluster_news(self):
        import json
        from bson import ObjectId
        start_time = int(time.time())
        news = self.mysql_manager.get_recent_news(time_window=1)
        # 1. 按 source_type/source_db/source_name 分组收集 id
        grouped = {}
        for item in news:
            key = (item['source_type'], item['source_db'], item['source_name'])
            grouped.setdefault(key, []).append(item['source_id'])
        # 2. 批量查原始数据（分批）
        all_originals = {}
        BATCH_SIZE = 50
        for (stype, sdb, sname), ids in grouped.items():
            n = len(ids)
            for i in range(0, n, BATCH_SIZE):
                batch_ids = ids[i:i + BATCH_SIZE]
                if stype == 'mysql':
                    # self.mysql_manager.switch_database(sdb)  # 如有多库需求可打开
                    query = f"SELECT * FROM {sname} WHERE id IN %s"
                    if batch_ids:
                        results = self.mysql_manager.execute_query(query, (tuple(batch_ids),))
                        for row in results:
                            all_originals[(stype, sdb, sname, str(row['id']))] = row
                elif stype == 'mongodb':
                    id_objs = []
                    for _id in batch_ids:
                        try:
                            id_objs.append(ObjectId(_id))
                        except Exception:
                            id_objs.append(_id)
                    if id_objs:
                        results = self.mongo_manager.mongo_source.find(sname, {'_id': {'$in': id_objs}},
                                                                       database_name=sdb)
                        for row in results:
                            all_originals[(stype, sdb, sname, str(row['_id']))] = row

        for item in news:
            key = (item['source_type'], item['source_db'], item['source_name'], str(item['source_id']))
            item['original_news'] = all_originals.get(key)
        # 4. 按 project 聚合
        project_map = {}
        for item in news:
            try:
                projects = json.loads(item.get('related_projects', '[]'))
            except Exception:
                projects = []
            for project in projects:
                if project not in project_map:
                    project_map[project] = []
                project_map[project].append(item)
        # 打印聚合结果
        total = 0
        for project, items in project_map.items():
            items_count = len(items)
            total += items_count
        end_time = int(time.time())
        print(total)
        print(end_time - start_time)
        return project_map

    async def handle_project_detail(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        await query.answer()

        project = query.data.replace("project_", "")
        chat_id = query.message.chat_id

        if not self.temp_data or project not in self.temp_data:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ 未找到项目 {project} 的新闻")
            return

        items = self.temp_data[project]
        messages = []
        for item in items:
            ctx = item.get("original_news", {}).get("context", "") if item.get("original_news", {}).get("context", "") else item.get("original_news", {}).get("content", "")
            if ctx:
                messages.append(f"📝 {ctx}")
            else:
                messages.append("📝 （无内容）")

        # 拼接长消息，分段发送
        all_msg = "\n\n".join(messages)
        for part in smart_split_html(all_msg):
            await context.bot.send_message(chat_id=chat_id, text=part, parse_mode="HTML")

    async def handle_7daysnews(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat_id = update.effective_chat.id
        print('chat_id', chat_id)
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(None, self._cluster_news)
            self.temp_data = result
            sorted_projects = sorted(result.items(), key=lambda x: len(x[1]), reverse=True)
            print(result)
            keyboard = []
            row = []
            columns = 4

            for idx, (project, items) in enumerate(sorted_projects):
                count = len(items)
                row.append(
                    InlineKeyboardButton(
                        text=f"{project} ({count}条)",
                        callback_data=f"project_{project}"
                    )
                )
                # 每满N个按钮就加入一行
                if (idx + 1) % columns == 0:
                    keyboard.append(row)
                    row = []

            # 处理最后剩余不足一行的按钮
            if row:
                keyboard.append(row)
            reply_markup = InlineKeyboardMarkup(keyboard)


        except Exception as e:
            await context.bot.send_message(chat_id=chat_id, text=f"处理新闻时出错: {e}")
            return
        await context.bot.send_message(
            chat_id=chat_id,
            text="\n<b>📊 近1日 PROJECT NEWS 聚合：</b>",
            reply_markup=reply_markup,
            parse_mode='HTML'
        )

# 创建全局单例实例
tg_bot = TelegramBot()



# 对外提供的简便函数
async def send_message(text: str, chat_id: str) -> bool:
    """向指定群组发送消息"""
    return await tg_bot.send_message_to_group(chat_id, text)

async def broadcast_message(text: str) -> dict:
    """向所有群组广播消息"""
    return await tg_bot.broadcast_message(text)

def start_bot():
    tg_bot.start()

def stop_bot():
    tg_bot.stop()

def get_group_ids() -> List[str]:
    return tg_bot.get_group_ids()


# 测试
async def main():
    print('1')
    start_bot()
    # 自己群 -4878412167
    try:
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        print("main loop error:", e)
if __name__ == '__main__':
    asyncio.run(main())
    # start_bot()
    # try:
    #     while True:
    #         asyncio.sleep(1)
    # except KeyboardInterrupt:
    #     print("正在停止TG_bot...")
    #     stop_bot()