import re

MAX_LENGTH = 3700
CLOSE_TAG_RE = re.compile(r'</[^>]+>')


def smart_split_html(text: str, max_len: int = MAX_LENGTH):
    chunks = []
    start = 0
    while start < len(text):
        end = min(start + max_len, len(text))
        sub = text[start:end]

        # 如果已经是最后一段，直接加
        if end == len(text):
            chunks.append(sub)
            break

        # 查找所有闭合标签
        close_tags = list(CLOSE_TAG_RE.finditer(sub))
        if close_tags:
            # 找到最后一个闭合标签的位置分割
            split_pos = close_tags[-1].end()
        else:
            # 如果没有闭合标签，退回一点点分（比如最后一个空格）
            split_pos = sub.rfind(' ')
            if split_pos == -1:
                split_pos = end  # 没有就强切

        chunk = sub[:split_pos]
        chunks.append(chunk)
        start += split_pos

    for i in range(1, len(chunks)):
        chunks[i] = f'⬆️ <i>Continued...</i>\n{chunks[i]}'

    return chunks

