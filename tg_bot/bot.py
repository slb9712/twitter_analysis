import asyncio
import sys
import os
import logging
from typing import List, Set
from telegram import Bot, Update
from telegram.error import ChatMigrated
from telegram.ext import ApplicationBuilder, MessageHandler, filters, ContextTypes
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
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
        self.group_ids: Set[str] = set()
        self.group_ids.add("-4892377641")  # 默认群组
        self.is_running = False
        self.loop = None  # 将在 start() 中设置
        self.temp_data = None
        logger.info("TG_bot初始化完成，默认群组ID: -4892377641")

    async def _handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """处理接收到的消息，提取群组ID（可选，如果不需要监听可移除）"""
        chat_id = str(update.effective_chat.id)
        if update.effective_chat.type in ['group', 'supergroup']:
            if chat_id not in self.group_ids:
                self.group_ids.add(chat_id)
                logger.info(f"新增群组ID: {chat_id}")
            print(chat_id)
            logger.debug(f"收到来自群组 {chat_id} 的消息: {update.message.text if update.message else '非文本消息'}")

    async def send_message_to_group(self, chat_id: str, text: str) -> bool:
        """向指定群组发送消息（新增：自动处理迁移）"""
        if not self.app:
            logger.error("Application 尚未初始化，无法发送消息")
            return False

        # 确保 chat_id 是 str
        chat_id = str(chat_id)
        msgs = smart_split_html(text)
        max_retries = 3
        retry_interval = 1

        for msg in msgs:
            current_chat_id = chat_id  # 可变，用于迁移更新
            for attempt in range(1, max_retries + 1):
                try:
                    await self.app.bot.send_message(chat_id=current_chat_id, text=msg, parse_mode='HTML')
                    # 更新 group_ids 如果是新 ID
                    if current_chat_id != chat_id:
                        self.group_ids.add(current_chat_id)
                        logger.info(f"群组迁移更新: {chat_id} -> {current_chat_id}")
                    break
                except ChatMigrated as e:
                    new_chat_id = str(e.new_chat_id)
                    logger.warning(f"群组迁移检测: {current_chat_id} -> {new_chat_id}")
                    current_chat_id = new_chat_id  # 更新为新 ID
                    self.group_ids.add(new_chat_id)  # 永久保存
                    if attempt < max_retries:
                        await asyncio.sleep(retry_interval)
                    else:
                        logger.error(f"迁移后发送仍失败: {str(e)}")
                        return False
                except Exception as e:
                    if "Timed out" in str(e):
                        if attempt < max_retries:
                            logger.warning(
                                f"向群组 {current_chat_id} 发送消息超时，第 {attempt} 次重试，等待 {retry_interval} 秒")
                            await asyncio.sleep(retry_interval)
                        else:
                            logger.error(f"向群组 {current_chat_id} 发送消息失败，已重试 {max_retries} 次: {str(e)}")
                            return False
                    else:
                        logger.error(f"向群组 {current_chat_id} 发送消息失败: {str(e)}")
                        return False

        return True
    async def start(self):  # 改为 async，移除线程，统一在外部 loop 中运行
        """启动 TG Bot（polling + 消息队列处理）"""
        if self.is_running:
            logger.warning("TG_bot已经在运行中")
            return

        self.app = ApplicationBuilder().token(self.bot_token).build()
        self.app.add_handler(MessageHandler(filters.ALL, self._handle_message))  # 可选监听
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling()

        self.loop = asyncio.get_running_loop()  # 保存当前 loop（统一使用）
        self.is_running = True
        logger.info("TG_bot已启动，正在监听群组")

        # 如果不需要监听，可注释掉 polling 行，只保留 app 初始化用于发送

    async def stop(self):
        """停止 TG Bot"""
        if not self.is_running:
            logger.warning("TG_bot未在运行")
            return

        self.is_running = False
        try:
            await self.app.updater.stop()
            await asyncio.sleep(2)
        except Exception as e:
            logger.warning(f"停止 Updater 时出错（正常）：{str(e)}")
        await self.app.stop()
        await self.app.shutdown()
        logger.info("TG_bot已停止")

    def get_group_ids(self) -> List[str]:
        """获取当前所有群组ID"""
        return list(self.group_ids)


# 创建全局单例实例
tg_bot = TelegramBot()


# 全局发送函数（供外部导入使用）
async def send_message(chat_id: str, text: str) -> bool:
    """便捷发送消息函数（使用全局 tg_bot）"""
    return await tg_bot.send_message_to_group(chat_id, text)


def start_bot():  # 保留 sync 版本，但内部调用 async（需在 async 上下文中）
    # 这个函数现在是占位，实际在 DataProcessor 中 await tg_bot.start()
    pass


def stop_bot():
    # 类似，实际 await tg_bot.stop()
    pass