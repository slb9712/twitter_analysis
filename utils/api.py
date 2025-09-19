import yfinance as yf
import httpx
import logging
import asyncio

logger = logging.getLogger('api')

async def check_is_cmc(projects: list[str]) -> list:
    """
    检查项目是否来自CMC
    :param projects: 项目列表
    :return: 如果项目来自CMC，返回存在的项目list，否则返回[]
    """
    url = 'http://192.168.11.51:18080/api/v1/crypto-info/pairs/check'
    payload = {"pairs": projects}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            existing_pairs = data.get("data", {}).get("existing_pairs") or []
            names = [item.get("name") for item in existing_pairs if item.get("name")]
            return names
    except Exception as e:
        logger.error(f"检查项目是否来自CMC时出错: {str(e)}")
        return []



def get_us_stocks_change(index_symbol):
    index = yf.Ticker(index_symbol)
    data = index.history(period="3d") 
    last_close = data['Close'][-1]
    prev_close = data['Close'][-2]
    
    change_value = last_close - prev_close
    change_percentage = (change_value / prev_close) * 100
    
    return round(last_close, 2), round(change_value, 2), round(change_percentage, 2)

if __name__ == "__main__":
    async def main():
        a = await check_is_cmc(["BTC", "ETH", "XRP"])
        print(a)
    asyncio.run(main())