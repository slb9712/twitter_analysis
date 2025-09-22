
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

