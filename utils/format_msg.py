import html
import logging
import re
import json
from datetime import datetime

logger = logging.getLogger('scheduler')

def replace_newlines_with_space(text: str) -> str:
    return text.replace('\n', ' ').replace('\r', ' ')


def escape_md(text: str) -> str:
    escape_chars = r"_*[]()~`>#+-=|{}.!\\"
    return re.sub(f"([{re.escape(escape_chars)}])", r"\\\1", text)


def escape_html(text: str) -> str:
    return html.escape(text).replace("'", "&#39;")


def center_line(text: str, total_width: int = 50) -> str:
    line_char = '─'
    text_len = len(text)
    line_len = total_width - text_len - 2  # 两边空格
    left = line_char * (line_len // 2)
    right = line_char * (line_len - len(left))
    return f"<code>{left} <b>{text}</b> {right}</code>"

def format_change(value, is_percentage=False):
    if value >= 0:
        formatted_value = f"+{round(value, 2)}" if not is_percentage else f"+{round(value, 2)}%"
        arrow = "↑" if is_percentage and value > 0 else "↓" if is_percentage and value < 0 else ""
    else:
        formatted_value = f"{round(value, 2)}" if not is_percentage else f"{round(value, 2)}%"
        arrow = "↓" if is_percentage and value < 0 else "↑" if is_percentage and value > 0 else ""

    return f"{formatted_value} {arrow}".strip()

def format_stocks_msg(sp_last_close, sp500_change_value, sp500_change_percentage,
                      nasdaq_last_close, nasdaq_change_value, nasdaq_change_percentage,
                      dow_jones_last_close, dow_jones_change_value, dow_jones_change_percentage) -> str:
    """格式化美国股市指数的消息"""
    message_parts = []
    message_parts.append("★每日新闻★")
    message_parts.append(f"S&P 500 收盘: {sp_last_close}, 相较前日: {format_change(sp500_change_value)}, ({format_change(sp500_change_percentage, True)})")
    message_parts.append(f"纳斯达克 收盘: {nasdaq_last_close}, 相较前日: {format_change(nasdaq_change_value)}, ({format_change(nasdaq_change_percentage, True)})")
    message_parts.append(f"道琼斯 收盘: {dow_jones_last_close}, 相较前日: {format_change(dow_jones_change_value)}, ({format_change(dow_jones_change_percentage, True)})")
    return "\n".join(message_parts).strip() + '\n\n'

def format_hyper_news_message(data: list, nums: int) -> str:
    """单独格式化Hyper的news和tweets"""
    message_parts = []
    for project in data:
        pname = escape_html(project.get("project_name", "未知项目"))
        filtered_news = [n for n in project.get("news", []) if n.get("title") and n.get("summary")]

        tweets_data = project.get("tweet_summary", [])
        # 然后从筛选后的新闻列表中取前 nums 条数据
        news_list = filtered_news[:nums]

        if not news_list and not tweets_data:
            continue

        message_parts.append(f"<b>📌 {pname}</b>")
        for n in news_list:
            title = escape_html(n.get("title", ""))
            summary = escape_html(n.get("summary", ""))
            message_parts.append(f"• <b>{title}</b>\n{summary}\n")
        if len(tweets_data) > 0:
            message_parts.append(f"<b>项目 X 消息</b>")
            for t in tweets_data:
                message_parts.append(f"  ↳{t}")
        message_parts.append("") 

    return "\n".join(message_parts).strip()

def format_top_projects_news_message(top_projects: list, nums: int) -> str:
    """
    格式化项目新闻为 Telegram 支持的 HTML 消息字符串。
    """
    message_parts = []
    for project in top_projects:
        pname = escape_html(project.get("project_name", "未知项目"))
        filtered_news = [n for n in project.get("news", []) if n.get("title") and n.get("summary")]

        # 然后从筛选后的新闻列表中取前 nums 条数据
        news_list = filtered_news[:nums]

        if not news_list:
            continue

        message_parts.append(f"<b>📌 {pname}</b>")
        for n in news_list:
            title = escape_html(n.get("title", ""))
            summary = escape_html(n.get("summary", ""))
            message_parts.append(f"• <b>{title}</b>\n{summary}")

        message_parts.append("")  # 空行分隔项目

    return "\n".join(message_parts).strip()


def format_project_info(project):
    lines = []
    if 'basic_info' in project:
        info = project.get("basic_info", {})
        try:
            if info.get("project_name"):
                lines.append(f"📌 <b>Project:</b> {escape_html(info['project_name'])}")
            if info.get("token_name"):
                lines.append(f"💰 <b>Token:</b> {escape_html(info['token_name'])}")
            if info.get("introduction"):
                lines.append(f"📝 <b>Introduction:</b> {escape_html(info['introduction'])}")
            if info.get("founded_date"):
                lines.append(f"📅 <b>Founded:</b> {escape_html(info['founded_date'])}")
        except Exception as e:
            logger.error(f"[basic_info] Error: {e}")
        socials = project.get("social_links", [])
        try:

            if socials:
                social_parts = []
                for s in socials:
                    if s.get("text") and s.get("link"):
                        platform = escape_html(s["text"])
                        url = s["link"]
                        social_parts.append(f'<a href="{url}">{platform}</a>')
                if social_parts:
                    lines.append(f"🌐 <b>Socials:</b> {' | '.join(social_parts)}")
        except Exception as e:
            logger.error(f"[social_links] Error: {e}")
        ecosystems = project.get("ecosystems", [])
        try:

            if ecosystems:
                eco_names = [escape_html(e["text"]) for e in ecosystems if e.get("text")]
                if eco_names:
                    lines.append(f"🔗 <b>Ecosystem:</b> {', '.join(eco_names)}")
        except Exception as e:
            logger.error(f"[ecosystems] Error: {e}")
        tags = project.get("tags", [])
        try:

            if tags:
                tag_names = [escape_html(t["text"]) for t in tags if t.get("text")]
                if tag_names:
                    lines.append(f"🏷️ <b>Tags:</b> {', '.join(tag_names)}")
        except Exception as e:
            logger.error(f"[tags] Error: {e}")
        fundraisings = project.get("fundraising", [])
        try:

            if fundraisings:
                funding_names = [escape_html(f['name']) for f in fundraisings if f.get("name")]
                if funding_names:
                    lines.append(f"<b>💸 Fundraising:</b> {' | '.join(funding_names)}")
        except Exception as e:
            logger.error(f"[fundraising] Error: {e}")

        fundraising_rounds = project.get("fundraising_rounds", [])
        try:

            valid_rounds = [
                r for r in fundraising_rounds
                if r.get("round") or r.get("amount") or r.get("date")
            ]
            if valid_rounds:
                lines.append("<b>📊 Fundraising Rounds:</b>")
                total_rounds = len(valid_rounds)
                for i, round_data in enumerate(valid_rounds):
                    round_index = total_rounds - i
                    round_name = round_data.get("round", "").strip() or "-"
                    amount = round_data.get("amount", "").strip() or "-"
                    date_ts = round_data.get("date", "")
                    try:
                        date = datetime.fromtimestamp(int(date_ts)).strftime("%Y-%m-%d")
                    except Exception:
                        date = "-"
                    lines.append(f"Round-{round_index} ({round_name}) | 💰 {amount} | 📅 {date}")
        except Exception as e:
            logger.error(f"[fundraising_rounds] Error: {e}")

        unlocks = project.get("token_unlock_events", [])
        try:
            if unlocks:
                unlocks.sort(key=lambda e: int(e.get("unlock_date", 0)), reverse=True)
                lines.append("<b>🗓️Token Unlock Events:</b> ")
                for event in unlocks:
                    try:
                        unlock_ts = int(event.get("unlock_date", ""))
                        unlock_date = datetime.fromtimestamp(unlock_ts).strftime("%Y-%m-%d")
                    except Exception:
                        unlock_date = "-"
                    try:
                        amount = float(event.get("token_amount", "0"))
                        amount_formatted = f"{int(amount):,}"
                    except Exception:
                        amount_formatted = "-"
                    ratio = event.get("m_cap", "-")
                    lines.append(f"     ↳ {unlock_date} — ${amount_formatted} | {ratio}%")
        except Exception as e:
            logger.error(f"[token_unlock_events] Error: {e}")
        investments = project.get("investments", [])
        try:
            if investments:
                lines.append("<b>💰 Investments:</b>")
                for invest in investments:
                    name = invest.get("name", "-")
                    description = invest.get("description", "-")
                    lines.append(f"     ↳ {name} — {description}")
        except Exception as e:
            logger.error(f"[investments] Error: {e}")

        commits = project.get("github_commit_msg", [])
        try:
            if commits:
                commits = sorted(commits, key=lambda x: x["commit_date"], reverse=True)[:5]
                lines.append("<b>🧾 GitHub Repo Commits:</b>")
                for c in commits:
                    msg = escape_html(c["commit_message"])
                    author = escape_html(c["author"])
                    date_str = c["commit_date"].strftime("%Y-%m-%d %H:%M")
                    lines.append(f"        ↳ <b>{author}</b>: {msg} <i>({date_str})</i>")
        except Exception as e:
            logger.error(f"[github_commit_msg] Error: {e}")

        proposals = project.get("snapshots", [])
        try:
            if proposals:
                lines.append("<b>📋 Recent Snapshots:</b>")
                for item in proposals:
                    title = item.get("title", "").strip()
                    if title:
                        lines.append(f"- {title}")
        except Exception as e:
            logger.error(f"[snapshots] Error: {e}")

        members = project.get("team_members", [])
        try:
            people_details = project.get("people_details", {})
            if members:
                lines.append("<b>👥 Team Members:</b>")
                for m in members:
                    try:
                        name = escape_html(m.get("name", ""))
                        title = escape_html(m.get("title", ""))
                        former = " <i>(Former)</i>" if m.get("is_former") else ""
                        detail = people_details.get(name)
                        if detail:
                            lines_per_member = [f"    - {name}, {title}{former}"]
                            intro = escape_html(detail.get("basic_info", {}).get("introduce_text", ""))
                            if intro:
                                lines_per_member.append(f"        ↳ {intro}")
                            schools = [
                                escape_html(edu["school_name"])
                                for edu in detail.get("education", [])
                                if edu.get("school_name")
                            ]
                            if schools:
                                lines_per_member.append(f"        ↳ 🎓 {' | '.join(schools)}")
                            works = []
                            for w in detail.get("work_experience", [])[:2]:
                                company = escape_html(w.get("company_name", ""))
                                title_ = escape_html(w.get("title", ""))
                                if company:
                                    works.append(f"{title_} at {company}" if title_ else company)
                            if works:
                                lines_per_member.append(f"        ↳ 💼 {' | '.join(works)}")
                            links = [
                                f'<a href="{s["link"]}">{escape_html(s["text"])}</a>'
                                for s in detail.get("social_links", [])
                                if s.get("link")
                            ]
                            if links:
                                lines_per_member.append(f"        ↳ 🔗 {' | '.join(links)}")
                            lines.append("\n".join(lines_per_member))
                        else:
                            lines.append(f"    - {name}, {title}{former}")
                    except Exception as e:
                        logger.error(f"[team_member:{m.get('name')}] Error: {e}")
        except Exception as e:
            logger.error(f"[team_members] Error: {e}")

        recent_tweets = project.get("recent_activity", [])
        try:
            if recent_tweets:
                funding_names = [f['name'] for f in fundraisings if f.get("name")] if fundraisings else []
                member_names = [f['name'] for f in members if f.get("name")] if members else []
                fundraising_html = ["<b>🐦 Fundraising Related Tweets</b>"]
                members_html = ["<b>🐦 Members Related Tweets</b>"]

                for name, tweets in recent_tweets.items():
                    try:
                        group_html = None
                        if name in funding_names:
                            group_html = fundraising_html
                        elif name in member_names:
                            group_html = members_html
                        else:
                            continue
                        group_html.append(f"<b>- {name}</b>")
                        for tweet in tweets:
                            username = tweet["twitter_username"]
                            text = tweet["text"].replace("\n", " ").strip()
                            url = tweet["permanent_url"]
                            date_str = tweet["tweet_date"].strftime("%Y-%m-%d %H:%M")
                            group_html.append(f'• <a href="{url}">@{username}</a>: {text} <i>({date_str})</i>')
                    except Exception as e:
                        logger.error(f"[recent_tweets:{name}] Error: {e}")

                if len(fundraising_html) > 1:
                    lines.extend(fundraising_html)
                if len(members_html) > 1:
                    lines.extend(members_html)
        except Exception as e:
            logger.error(f"[recent_activity] Error: {e}")
    elif 'vc_info' in project:
        info = project.get('vc_info', {})
        if info.get("name"):
            lines.append(f"📌 <b>VC:</b> {escape_html(info.get('name', 'Unknown'))}")

        founded = info.get("founded_date") or info.get("founded") or "N/A"
        lines.append(f"📅 <b>Founded:</b> {escape_html(founded)}")

        location = info.get("location", "N/A")
        lines.append(f"📍 <b>Location:</b> {escape_html(location)}")

        category = info.get("category", "N/A")
        lines.append(f"🏷️ <b>Category:</b> {escape_html(category)}")

        intro = info.get("introduce_text", "")
        if intro:
            # 限制介绍文本长度
            intro_trimmed = (intro[:200] + "...") if len(intro) > 200 else intro
            lines.append(f"📝 <b>Intro:</b> {escape_html(intro_trimmed)}")

        portfolio = info.get("portfolio")
        if portfolio is not None:
            lines.append(f"📊 <b>Portfolio Size:</b> {escape_html(str(portfolio))}")


        investments = project.get("investor_investments", [])
        if investments:
            lines.append("📈 <b>Recent Investments:</b>")
            for inv in investments[:5]:
                name = escape_html(inv.get("name", "Unknown"))
                link = inv.get("link", "")
                if link:
                    lines.append(f"  • <a href=\"{escape_html(link)}\">{name}</a>")
                else:
                    lines.append(f"  • {name}")

    return "\n".join(lines)


def format_all_projects(all_info):
    all_texts = []
    for idx, project in enumerate(all_info):
        project_text = format_project_info(project)
        all_texts.append(project_text)
    return "\n" + ("\n\n" + "-" * 24 + "\n\n").join(all_texts)


def format_for_telegram(data_list):
    """
    将统计数据格式化为Telegram支持的HTML消息格式
    仅使用<b>标签和unicode符号，保持美观易读

    :param data_list: 包含name、tag、count的字典列表
    :return: 格式化后的HTML字符串
    """
    if not data_list:
        return "📊 暂无数据"

    # 消息标题
    message_parts = ["📋 <b>项目统计结果</b>\n\n"]

    # 按count降序排序（确保顺序正确）
    sorted_data = sorted(data_list, key=lambda x: (-x['count'], x['name']))

    for idx, item in enumerate(sorted_data, 1):
        # 转义特殊字符，避免HTML解析错误
        name = html.escape(item['name'])
        count = item['count']
        tags = [html.escape(tag) for tag in item['tag']]

        # 构建行内容：序号 + 名称 + 计数 + 标签
        # 使用不同unicode符号区分不同计数级别
        if count >= 5:
            count_symbol = "🔥"  # 高频率
        elif count >= 3:
            count_symbol = "✨"  # 中频率
        else:
            count_symbol = "🔹"  # 低频率

        # 标签部分用逗号分隔，前后加括号
        tags_str = "(" + ", ".join(tags) + ")"

        # 拼接行（加粗名称，计数带符号）
        line = f"{idx}. <b>{name}</b> {count_symbol} {count} {tags_str}"
        message_parts.append(line)

    # 结尾添加分隔线和说明
    message_parts.append("\n" + "=" * 20)
    message_parts.append("\n💡 统计结果按出现次数排序")

    # 合并所有部分
    return "\n".join(message_parts)
