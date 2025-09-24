
kol_tweet_template ="""You are given a Web3-related tweet from a KOL:
Tweet: <text>

Task: Extract **all explicitly mentioned Web3 projects and tokens** in the tweet.
- Do NOT infer, guess, or include anything not directly mentioned.
- Only extract mentions of protocols, platforms, projects, companies, or tokens that appear in the text.
- The extracted items and tokens can only be in English numbers.

Return your result in the **strict JSON format** below:

{
  "project": [],
  "token": [],
}

Instructions:
project(type: list[str]): Names of protocols, platforms, companies (e.g., uniswap, coinbase, ethereum).
token(type: list[str]): Ticker symbols or token names (e.g., BTC, ETH, PEPE).

Always include all keys in the JSON result
"""

tweet_summary_template = """
你的任务是分析并总结过去1小时内的推文，内容如下：
<all_tweets>

请按照以下步骤生成结构化、有针对性的总结：

1. **筛选相关推文**
   - 忽略与Web3、加密货币项目、代币或市场活动无关的推文；
   - 重点关注官方公告、生态系统更新及被广泛讨论的事件；
   - 排除日常闲聊、垃圾信息或无关观点类内容。

2. **识别关键事件**
   - 捕捉项目/代币的官方的活动（例如：上线、合作公告、产品发布、治理投票、空头等）；
   - 不属于官方活动，但被多条推文同时提及的事件（存在重复讨论）；
   - 对于内容重复或重叠的事件提及，需合并为一个统一事件（避免重复记录）；
   - 记录每个事件提及的作者。

3. **提取关联项目或代币**
   - 针对每个识别出的事件，提取其中明确提及的项目名称或代币；
   - 对项目/代币名称进行归一化处理（例如：统一“ETH”与“Ethereum”为同一表述，避免同一主体重复出现）。

4. **输出格式**
   event字段需使用中文表述，最终结果以JSON格式返回，结构严格遵循以下示例：
```json
{
    "events":[
      {
        "event": "事件1，详细描述事件内容核心信息，确保信息完整",
        "authors": ["提及该事件的作者/作者1", "提及该事件的作者/作者2],
        "projects": ["事件关联的项目/代币1", "事件关联的项目/代币2"]
      },
    ]
}
"""

