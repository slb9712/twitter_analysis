import asyncio
import logging


import json
import os
import re
import time

import httpx
from dotenv import load_dotenv
from openai import AsyncOpenAI

# os.environ["http_proxy"] = "http://192.168.11.51:11434"
# os.environ["https_proxy"] = "http://192.168.11.51:11434"
logger = logging.getLogger('text_analyzer')

load_dotenv()

class TextAnalyzer:

    def __init__(self, config):
        """初始化文本分析器

        Args:
            config (dict): 包含 api_key, base_url, model 的配置
        """
        self.config = config
        self.model = config["model"]
        self.is_ollama = "localhost" in config["base_url"] or "192." in config["base_url"] or "127." in config[
            "base_url"]

        if not self.is_ollama:
            # 仅在非 Ollama 时使用 OpenAI SDK
            self.client = AsyncOpenAI(
                api_key=config["api_key"],
                base_url=config["base_url"]
            )
        logger.info(f"文本分析器初始化完成，使用模型: {self.model}，类型: {'Ollama' if self.is_ollama else 'OpenAI'}")

    async def analyze_text(self, prompt_template, **kwargs):
        """分析文本并提取结构化数据"""
        try:
            prompt = prompt_template
            for key, value in kwargs.items():
                prompt = prompt.replace(f"<{key}>", str(value))

            if self.is_ollama:
                logger.info(prompt)
                return await self._analyze_with_ollama(prompt)
            else:
                logger.info(prompt)
                return await self._analyze_with_openai(prompt)

        except Exception as e:
            logger.error(f"文本解析失败: {str(e)}")
            return {}

    def _extract_json_from_response(self, text):
        cleaned = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        json_match = re.search(r"```json\s*(.*?)\s*```", cleaned, re.DOTALL | re.IGNORECASE)
        json_text = json_match.group(1).strip() if json_match else cleaned.strip()

        json_text = re.sub(r',(\s*[}\]])', r'\1', json_text)

        try:
            return json.loads(json_text)
        except json.JSONDecodeError as e:
            raise

    async def _analyze_with_openai(self, prompt):
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "user", "content": prompt}
            ],
            stream=False,
            response_format={"type": "json_object"}
        )
        res = response.choices[0].message.content
        print(res)
        return json.loads(res) if res else {}

    async def _analyze_with_ollama(self, prompt):
        """使用 Ollama HTTP API 分析文本"""
        url = f"{self.config['base_url'].rstrip('/')}/chat/completions"
        payload = {
            "model": "Qwen3-14B-AWQ",
            # "stream": False,
            # "prompt": prompt
            "messages": [
                {"role": "user", "content": prompt}
            ]
        }
        headers = {"Content-Type": "application/json"}

        # 设置最大重试次数
        max_retries = 3
        for attempt in range(max_retries):
            async with httpx.AsyncClient(timeout=60) as client:
                try:
                    response = await client.post(url, json=payload, headers=headers)
                    response.raise_for_status()
                except httpx.RequestError as e:
                    logger.error(f"请求失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:  # 如果不是最后一次尝试
                        time.sleep(2)  # 等待 2 秒后重试
                        continue
                    else:
                        logger.error("达到最大重试次数，返回空数据")
                        return {}

                except httpx.HTTPStatusError as e:
                    logger.error(f"接口返回错误状态码 (尝试 {attempt + 1}/{max_retries}): {e.response.status_code}, 内容: {e.response.text}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        logger.error("失败达到最大重试次数，返回空数据")
                        return {}

                try:
                    result = response.json()
                except Exception as e:
                    logger.error(f"响应无法解析为 JSON (尝试 {attempt + 1}/{max_retries}): {response.text[:200]}... 错误: {str(e)}")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    else:
                        logger.error("失败达到最大重试次数，返回空数据")
                        return {}

                try:
                    content = result["choices"][0]["message"]["content"]
                    json_text = self._extract_json_from_response(content)
                    logger.info(json_text)
                    return json_text

                except (KeyError, IndexError) as e:
                    logger.error(f"Ollama 返回格式缺失字段 (尝试 {attempt + 1}/{max_retries}): {str(e)}")
                    if attempt < max_retries - 1:
                        continue
                    else:
                        logger.error("失败达到最大重试次数，返回空数据")
                        return {}

                except json.JSONDecodeError as e:
                    logger.error(f"JSON 解析失败 (尝试 {attempt + 1}/{max_retries}): {e.msg} at line {e.lineno} column {e.colno} (char {e.pos})")
                    if attempt < max_retries - 1:
                        continue
                    else:
                        logger.error("失败达到最大重试次数，返回空数据")
                        return {}

        return {}



