import logging
import re

from database.chain_project_manager import ChainProjectManager

logger = logging.getLogger('chain_project_manager')


def get_twitter_project_info(values, mysql_config, existing_manager=None):
    if existing_manager and isinstance(existing_manager, ChainProjectManager):
        manager = existing_manager
    else:
        manager = ChainProjectManager(mysql_config)

    try:
        project_info = manager.get_project_info_by_twitter_name(values)
        return project_info
    except Exception as e:
        logger.error(f"Error occurred in get twitter project info: {e}")
        return {}


def get_twitter_people_info(column_name, values, mysql_config, existing_manager=None):
    """根据人名或者twitter用户名获取人物详情

    Args:
        column_name (str): 字段类型 人名 或者 twitter用户名
        values(list): 查询的值
        mysql_config (dict): MySQL配置
        existing_manager (ChainProjectManager, optional): 现有的ChainProjectManager实例，如果提供则复用该实例

    Returns:
        dict: 所有相关信息
    """
    # 检查是否提供了现有的ChainProjectManager实例
    if existing_manager and isinstance(existing_manager, ChainProjectManager):
        manager = existing_manager
    else:
        manager = ChainProjectManager(mysql_config)

    try:
        people_info = []
        if column_name == 'name':
            # 根据人名获取人员的基本信息
            people_info = manager.get_people_info_by_names(values)


        elif column_name == 'twitter_name':
            # 从people_social_links表中查到people_id，然后再查询所有信息
            people_info = manager.get_people_info_by_twitter_name(values)

        if people_info:
            all_people_ids = []
            for name, info in people_info.items():
                if 'people_id' in info:
                    all_people_ids.append(info['people_id'])

                # 获取人员相关的详细信息
            people_education = manager.get_people_education_experience(all_people_ids)
            people_social = manager.get_people_social_link(all_people_ids)
            people_work = manager.get_people_work_experience(all_people_ids)
            people_investments = manager.get_people_investments_info(all_people_ids)

            detailed_people_info = {}
            for name, info in people_info.items():
                people_id = info.get('people_id')
                if people_id:
                    detailed_people_info[name] = {
                        'basic_info': info,
                        'education': people_education.get(people_id, []),
                        'social_links': people_social.get(people_id, []),
                        'work_experience': people_work.get(people_id, []),
                        'investments': people_investments.get(people_id, [])
                    }
                else:
                    detailed_people_info[name] = {
                        'basic_info': info,
                        'education': [],
                        'social_links': [],
                        'work_experience': [],
                        'investments': []
                    }
            return detailed_people_info
        return {}
    except Exception as e:
        logger.error(f"Error occurred in get user info{e}")
        return {}


