"""
core/msg_builder.py
解析消息模板构建具体发送的消息内容
"""

from typing import Optional

from utils.config import get_config
from utils.hitokoto import request_hitokoto


def build_message(config: Optional[dict] = None) -> str:
    active_config = config or get_config()
    message = active_config.get("messageTemplate", "续火花")
    if "[API]" in message:
        api_content = request_hitokoto()
        message = message.replace("[API]", api_content)
    return message.strip()
