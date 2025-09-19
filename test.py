# # import requests
# # import json
# #
# # # 请求 URL
# # # url = "https://api.aixbt.tech/terminal/projects/66f8bc84481813460f486385/summaries"
# # # url = "https://api.aixbt.tech/terminal/projects/66f4fdc76811ccaef955de38/summaries"
# # url = "https://api.aixbt.tech/terminal/projects/66f4fdc76811ccaef955de3e/summaries"
# # # 发送 GET 请求
# # response = requests.get(url)
# # if response.status_code == 200:
# #     data = response.json()
# #
# #     results = []
# #
# #     # 遍历 dates 字段
# #     dates = data.get("dates", {})
# #     for date_str, content in dates.items():
# #         result = {
# #             # "date": date_str,
# #             "description": content.get("description", ""),
# #             "clustersId": content.get("clusters", None)
# #         }
# #         results.append(result)
# #
# #     # 输出结果或保存到文件
# #     print(json.dumps(results, indent=2, ensure_ascii=False))
# #
# #
# # else:
# #     print(f"Request failed with status code {response.status_code}")
# import datetime
import asyncio
import time
# #
# # a = [1, 2]
# # b = [3, 4]
# # print(a + b)

# # now = datetime.datetime.now(tz=datetime.timezone.utc)
# # start_ts = int((now - datetime.timedelta(days=7)).replace(
# #     hour=0, minute=0, second=0, microsecond=0
# # ).timestamp())
# # print(now, start_ts)


# import sys
# import os
# import re
# import json


# def _extract_json_from_response(text):
#         cleaned = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)

#         json_match = re.search(r"```json\s*(.*?)\s*```", cleaned, re.DOTALL | re.IGNORECASE)
#         if json_match:
#             return json_match.group(1).strip()

#         return cleaned.strip()
        
# a = _extract_json_from_response("""
# <think>
# Okay, let me break this down. The user shared a message in Chinese about 0xSun's positions on ETH and山寨币, which is a vague term but likely means altcoins. The task requires extracting several elements and structuring them into a strict JSON format.

# First, analyzing what's explicitly mentioned: "ETH" clearly meets the token criteria. For projects, "web3-related projects or tokens" - the message doesn't name specific projects but envisions future ones like token merges or upgrades.

# The overall attitude is bullish since 0xSun is "做多ETH" (longing/betting on ETH rising) and "做空一揽子山寨币" (shorting altcoins). The China TechFlowPost newsletter context suggests this is from a crypto expert's perspective.

# For actions, ETH buy/sell are implied in a trading scenario but no actual operations are described. The market analysis actions like "exceeds" (breakouts) should be included under prediction_actions.

# Details needed include:
# - Project list
# - Token list (just ETH here)
# - Attitude: bull
# - Actions: trading terms for ETH
# - News events: Ethereum's listing on the newsletter's site
# - Predictions: market movement projections
# - Original translations maintaining proper names

# The challenge is handling "山寨币" - it won't be translated directly to ensure consistency. All extracted elements must be presented in the exact JSON format requested, with Chinese translation preserving the original terminology following the user query.
# </think>
# {
#   "project": ["Ethereum"],
#   "token": ["ETH", "山寨币"],
#   "attitude": "bull",
#   "actions": [
#     {"token": "ETH", "action": "buy", "nums": ""},
#     {"token": "山寨币", "action": "sell", "nums": ""}
#   ],
#   "news_events": [
#     {"entity": "KOL 0xSun", "action": "release a newsletter"},
#     {"entity": "Ethereum Foundation / Ethereum community", "action": "potential listing of Ethereum on China TechFlowPost"}
#   ],
#   "predict_actions": [
#     {"entity": "KOL 0xSun", "prediction": "the price of Ethereum will rise", "subject": "ETH"},
#     {"entity": "KOL 0xSun", "prediction": "the price of a basket of altcoins will fall", "subject": "山寨币"}
#   ],
#   "original_en": "Crypto KOL 0xSun: Currently long ETH and short a basket of altcoins. https://www.techflowpost.com/newsletter/detail_93680.html",
#   "original_zh": "加密领域意见领袖0xSun：目前做多ETH并做空一揽子山寨币，详情参见 https://www.techflowpost.com/newsletter/detail_93680.html"
# }
# """)


# import datetime
# ts = int(datetime.datetime(2025, 8, 9, 0, 0, 0).timestamp())
# print(ts)  # 输出：1754246400
import uuid
from datetime import datetime



import gspread
from google.oauth2 import service_account


creds = service_account.Credentials.from_service_account_file(
    "service-account-credentials.json",
    scopes=["https://www.googleapis.com/auth/spreadsheets"],
)
client = gspread.authorize(creds)

sheet = client.open_by_key(
    "1DRt7axVIilFytqLCZBOidgprooERXdfNnsbdJOGt8qs"
).get_worksheet_by_id(1272007175)
expected_headers = ["Twitter(url)", "Name"]
data = sheet.get_all_records(
    head=3, expected_headers=expected_headers,
)

filter_data = [d for d in data if d["综合推荐指数"] == "5🌟" or d["综合推荐指数"] == "4.5🌟"]
print(filter_data)

import pymysql

from config.config import MONGO_CONFIG, DEEPSEEK_CONFIG
from database.db_manager import MongoDBManager
from model.text_analyzer import TextAnalyzer

# 连接数据库
conn = pymysql.connect(
    host="192.168.11.51",
    user="root",
    password="p6e2VY0uMWgBe",
    database="chain_project",
    charset='utf8mb4'
)
cursor = conn.cursor()
print("已获取，开始插入数据")
def generate_random_project_id(key_string):
    namespace_url = uuid.uuid4()
    return str(uuid.uuid5(namespace_url, key_string))



try:
    # 开启事务（大部分连接默认自动开启事务模式，设置 autocommit=False 以便显式提交）
    conn.autocommit = False

    for item in filter_data:
        rating = item.get("综合推荐指数", "").strip()
        score = str(item.get("分数", ''))
        name = item.get("Name", "").strip()
        twitter_url= item.get("Twitter(url)", '')
        region = item.get("地区", '')
        type = item.get("Type", '')
        review_url = item.get("📋测评结果 url", '')
        features = item.get("特点", '')
        avatar_url = item.get("头像", '')

        sql_project = """
                INSERT
                    INTO
                    kol_list(rating, score, name, twitter_url, region, type, review_url, features, avatar_url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql_project, (rating, score, name, twitter_url, region, type, review_url, features, avatar_url))
    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Transaction failed and rolled back. Error: {e}")

finally:
    # 关闭连接
    cursor.close()
    conn.close()

# try:
#     # 开启事务（大部分连接默认自动开启事务模式，设置 autocommit=False 以便显式提交）
#     conn.autocommit = False
#
#     for item in data:
#         people_name = item.get("Name", "").strip()
#         url = item.get("Twitter(url)", "").strip()
#         introduction = item.get("特点", "").strip()
#         if not people_name:
#             continue
#         people_id = generate_random_project_id(people_name)
#
#         sql_project = """
#             INSERT INTO people (people_id, url, name, title, introduce_text)
#             VALUES (%s, %s, %s, %s, %s)
#         """
#         cursor.execute(sql_project, (people_id, "", people_name, "Kol", introduction))
#
#         sql_social = """
#             INSERT INTO people_social_links (people_id, text, link)
#             VALUES (%s, %s, %s)
#         """
#         cursor.execute(sql_social, (people_id, "X", url))
#
#     conn.commit()
#
# except Exception as e:
#     conn.rollback()
#     print(f"Transaction failed and rolled back. Error: {e}")
#
# finally:
#     # 关闭连接
#     cursor.close()
#     conn.close()



# from datetime import datetime
# import pytz

# # 设置 UTC 时区
# utc_zone = pytz.utc

# # 获取 8月5日 0点 UTC 时间
# utc_time = datetime(2025, 8, 10, 0, 0, 0, 0, tzinfo=utc_zone)

# # 获取 UTC 时间戳
# timestamp = utc_time.timestamp()

# print("UTC 8月5日 0点的时间戳:", timestamp)


# import yfinance as yf

# def get_change(index_symbol):
#     index = yf.Ticker(index_symbol)
#     data = index.history(period="3d") 
#     last_close = data['Close'][-1]
#     prev_close = data['Close'][-2]
    
#     change_value = last_close - prev_close
#     change_percentage = (change_value / prev_close) * 100
    
#     return round(last_close, 2), round(change_value, 2), round(change_percentage, 2)

# sp_last_close, sp500_change_value, sp500_change_percentage = get_change('^GSPC')
# nasdaq_last_close, nasdaq_change_value, nasdaq_change_percentage = get_change('^IXIC')
# dow_jones_last_close, dow_jones_change_value, dow_jones_change_percentage = get_change('^DJI')

# print(f"S&P 500 收盘价: {sp_last_close}, 变动值: {sp500_change_value}, 变动比例: {sp500_change_percentage}%")
# print(f"纳斯达克 收盘价: {nasdaq_last_close}, 变动值: {nasdaq_change_value}, 变动比例: {nasdaq_change_percentage}%")
# print(f"道琼斯 收盘价: {dow_jones_last_close}, 变动值: {dow_jones_change_value}, 变动比例: {dow_jones_change_percentage}%")





# **目标**：
# - 筛选新闻：筛选出已发生或官方明确将要执行的**确定会立即影响整体加密货币市场资金大量流入/流出，或立即引发某一代币价格剧烈波动，利多(bull)/利空(bear)的重大新闻事件**，忽略预测、分析、假设或条件性新闻消息。 

# **只关注美国中国相关以下新闻方向**：
# - 宏观经济因素：已发布美国中国官方确认的利率变动、通胀数据、宏观市场整体趋势，且会极大影响整体加密货币市场资金大量流入/流出。
# - 国家/监管层面：美国中国或国际监管机构已发布或官方确认的重大政策、决定或禁令（例如，美国SEC批准或否决比特币ETF），会极大影响整体加密货币市场资金大量流入/流出。  
# - 名人/组织：具有全球影响力的政治人物、金融巨头、国际机构的重大官方事件确定采取的行动，并且会极大影响整体加密货币市场资金大量流入/流出。  
# - 战争层面：贸易战/热战/金融战，会极大影响避险性资金流动性。

# **排除以下新闻**：
# - 没有明显利多或者利空因素的新闻。
# - 影响范围有限的小国或非主要经济实体的政策和法规。 
# - 忽略分析师、媒体或机构的预测性内容，含“预计、可能、或许、若…将”等表述的新闻，以及对以往趋势、流入、流出等的总结性新闻。





# prompt = """你的背景是Web3和加密货币市场的专家分析师，要审查提供的实时新闻。
# **目标**：
# - 筛选新闻：筛选出已发生或明确将要执行的**确定会立即引起加密货币价格大幅波动，利多(bull)/利空(bear)的重大新闻事件**，忽略预测、分析、假设或条件性新闻消息。
#
# **监控以下高波动性触发事件**：
# - 美国宏观经济政策：美联储利率决议、通胀数据发布。
# - 战争层面：贸易战、热战、金融战。
# - 美国监管动态：美国养老金等政府基金进入加密货币的法案进展。
# - 安全事件：稳定币脱锚。
# - 名人言论：Trump、Eric Trump关于买入加密货币的发言。
# - 代币供应量变化：代币增发、销毁。
# - 代币锁仓量变化：代币锁仓、解锁。
# - 代币质押量变化：代币质押、退出质押。
#
# **忽略以下新闻**：
# - 不能一句话概括出时间、主体、动作的新闻。
# - 含“预计、可能、或许、若…将、疑似、呼吁、考虑、敦促”等不确定表述的新闻。
# - 对以往趋势总结性新闻。
# - 对ETF流入、流出统计新闻。
#
# 如果有筛选出**目标**新闻，执行以下任务：
# - 提取代币：进一步处理筛选出的新闻，如果新闻中提到具体代币（如 BTC、ETH、SOL、XRP、比特币、以太坊 等），提取该代币符号。如果筛选出的新闻只影响整体加密市场（未涉及具体代币），token 返回 ""。
# - 匹配交易所：进一步处理token, 如果提取 token 不为空，则判断该代币是否能在以下三家中心化交易所（CEX）交易：Binance、OKX、Bybit。如果确定支持，返回一个交易所的名称。如果无法确定支持或没有token，返回 ""。
# - 波动逻辑链：解释事件如何传导至价格的逻辑(logic)。
# - 事件摘要：用一句话概括新闻含时间、主体、动作的核心（event）。
# - 交易信号分数：对筛选出的新闻进一步判断分析，确定推荐买/卖交易信号强度，数值越高，代表趋势越强烈、越紧迫。数值定义如下：
#     1-> 中等确定性，较大概率引发显著波动，新闻有很强市场驱动力，会带来明显的短期价格变化。
#     2-> 高确定性，几乎必然引发大行情，市场极高概率出现夸苏单边波动，适合立即建仓/平仓。
#     3-> 极高确定性，决定性事件，百分之百引发整体/某个代币价格行情剧烈波动，建议马上执行操作。
#
# 全部新闻如下:
# <formatted_news>
#
#
# 输出要求：
# -输出严格使用 JSON 格式：
# {
#   "important_ids": [
#         {
#         "id": 筛选出的重点关注新闻的整数ID,
#         "trend": "bull" 或者 "bear",
#         "score": 买入/卖出信号强度,
#         "token": 该新闻影响的代币token,
#         "cex": token代币相关的交易所,
#         "logic": 事件如何传导至价格的逻辑,
#         "event": 新闻的时间、主体、动作
#     }
#   ]
# }
#
# - 如果没有符合条件的新闻，请返回一个空列表。
# """
#
#
#
# """你的背景是Web3和加密货币市场的专家分析师，要审查提供的实时新闻。
#
# 处理步骤（必须严格按照顺序执行，不能跳过）：
# 1. **新闻筛选**：筛选出已发生或官方明确将要执行的、确定会立即影响全球加密市场价格波动，或立即引发某一代币价格剧烈波动，利多(bull)或利空(bear)的重大新闻事件。
#    - 忽略预测、分析、假设或条件性新闻消息。
#    - 忽略仅为口头表态、没有明确行动或政策落地的新闻（例如“某官员发表积极讲话”）。
#    - 如果新闻不符合条件，直接丢弃，不进入后续步骤。
#
# 2. **代币提取**：对筛选出的新闻，提取具体代币（如 BTC、ETH、SOL、XRP 等）。如果只影响整体加密市场（未涉及具体代币），返回 ""。
#
# 3. **交易所匹配**：如果 token 不为空，则判断该代币是否能在以下三家中心化交易所（CEX）交易：Binance、OKX、Bybit。如果确定支持，返回一个交易所的名称；否则返回 ""。
#
# 4. **交易信号评分**：对筛选出的新闻进行强度打分，分为 1~5 档（比原先更细化）：
#    - 1 -> 新闻会导致市场关注，但对价格影响有限或方向不明，需要进一步观察。
#    - 2 -> 新闻可能引发轻微价格波动，但缺乏明确的单边行情依据。
#    - 3 -> 新闻大概率引发价格波动，但行情方向未必完全确定，操作需谨慎。
#    - 4 -> 新闻大概率引发快速单边波动，适合立即建仓/平仓。
#    - 5 -> 新闻确定性极高，几乎必然引发剧烈行情，必须马上执行操作。
#
# **只关注以下新闻方向**：
# - 宏观经济因素：已发布或官方确认的利率变动、通胀数据、宏观市场整体趋势，且会极大概率直接影响整体加密市场波动。
# - 国家/监管层面：全球主要经济体或国际监管机构已发布或官方确认的重大政策、决定或禁令（例如，美国SEC批准或否决比特币ETF）。
# - 名人/组织：具有全球影响力的政治人物、金融巨头、国际机构的重大官方事件表态或确定采取的行动，并将极大概率直接影响加密市场价格波动。
#
# **排除以下新闻**：
# - 单条新闻中存在多个主题。
# - 没有明显利多或者利空因素的新闻，对以往时间点内容总结性的报道。
# - 含“预计、可能、或许、若…将”等分析师、媒体或机构的预测性内容，对以往趋势、流入、流出等的总结性报道。
# - 仅有表态、没有实质政策或行动落地的新闻。
#
# 输入：
# 全部新闻如下:
# <formatted_news>
#
# 输出要求：
# - 输出严格使用 JSON 格式：
# {
#   "important_ids": [
#         {
#         "id": 筛选出的重点关注新闻的整数ID,
#         "trend": "bull" 或者 "bear",
#         "score": 交易信号强度 (1~5),
#         "token": 该新闻影响的代币token,
#         "cex": token代币相关的交易所
#     }
#   ]
# }
#
# - 如果没有符合条件的新闻，请返回一个空列表。
# """
#
#
#
#
#
# mongo_manager = MongoDBManager(MONGO_CONFIG)
# text_analyzer = TextAnalyzer(DEEPSEEK_CONFIG)
# async def _process_important_news():
#     def format_imp_news(all_news: list[dict]):
#         formatted = ""#"Here is the list of recent news items:\n\n"
#         for idx, item in enumerate(all_news):
#             formatted += f"News ID: {idx}\nContent: {item['content']}\n\n"
#         return formatted.strip()
#
#     start_t = datetime.strptime("2025-08-28 00:00:00", "%Y-%m-%d %H:%M:%S")
#     end_t = datetime.strptime("2025-08-29 00:00:00", "%Y-%m-%d %H:%M:%S")
#
#     news = mongo_manager.get_target_techflow_data(start_t, end_t)
#     # for new in news:
#     #     print(new.get("content", ''))
#     # return
#     # print(news)
#     # return
#     if not news:
#         return
#     result = await text_analyzer.analyze_text(prompt, formatted_news=format_imp_news(news))
#     res = result.get("important_ids", [])
#     if len(res) > 0:
#         data = [
#             {**item, "content": news[item.get("id")].get("content", ""), "source_id": str(news[item.get("id")].get("_id", ""))}
#             for item in res if item.get("id") < len(news)
#         ]
#         print(data)
#
# asyncio.run(_process_important_news())
