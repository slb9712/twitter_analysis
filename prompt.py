
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
Your task is to analyze and summarize tweets from the past one hour, provided in {{all_tweets}}.  

Follow these steps to create a structured and relevant summary:

1. **Filter Relevant Tweets**
   - Ignore tweets unrelated to Web3, crypto projects, tokens, or market activities.
   - Focus on official announcements, ecosystem updates, and widely discussed events.
   - Exclude casual conversations, spam, or irrelevant opinions.

2. **Identify Key Events**
   - Detect official project/token activities (e.g., listings, partnerships, launches, governance votes).
   - Detect other significant events if they are mentioned across multiple tweets.
   - Merge duplicate or overlapping mentions into one unified event.

3. **Extract Associated Projects or Tokens**
   - For each event, identify and extract project names or token tickers mentioned.
   - Ensure names are normalized and avoid duplicates.

4. **Output Format**
   All fields are in English
   Return the result as JSON:
   ```json
   {
     "events": [
       "Event 1 description",
       "Event 2 description",
       ...
     ],
     "projects": [
       "Project/Token 1",
       "Project/Token 2",
       ...
     ]
   }
"""

