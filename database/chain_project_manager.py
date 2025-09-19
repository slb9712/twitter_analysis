import logging
import time
from database.db_factory import db_factory



logger = logging.getLogger('chain_project_manager')


class ChainProjectManager:
    """链上项目数据管理器，负责从chain_project数据库获取项目相关信息"""

    def __init__(self, config, mongo_config=None):
        """初始化数据库连接
        
        Args:
            config (dict): MySQL连接配置
            mongo_config (dict, optional): MongoDB连接配置，如果为None，则尝试从config中获取
        """
        self.config = config
        self.mongo_config = mongo_config
        self.mysql_source = None
        self.connect()

    def connect(self):
        """创建数据库连接，使用chain_project数据库"""
        try:
            # 使用数据库工厂获取MySQL数据源
            self.mysql_source = db_factory.get_mysql_source(self.config)

            # 切换到chain_project数据库
            self.mysql_source.switch_database('chain_project')
        except Exception as e:
            logger.error(f"chain_project数据库连接失败: {str(e)}")
            raise

    def close(self):
        """关闭数据库连接"""
        # 数据库工厂会管理连接的关闭，这里不需要显式关闭
        pass

    def get_projects_by_name(self, project_names):
        """根据项目/Token名称获取项目信息
        
        Args:
            project_names (list): 项目名称列表
            
        Returns:
            list: 项目信息列表
        """
        if not project_names:
            return []

        conditions = []
        params = []

        for name in project_names:
            conditions.append("project_name = %s")
            conditions.append("token_name = %s")

            params.append(name)
            params.append(name)

        query = f"SELECT * FROM projects WHERE {' OR '.join(conditions)}"

        results = self.mysql_source.execute_query(query, params)

        seen = set()
        filter_projects = []
        for row in results:
            pid = row.get("project_id")
            if pid not in seen:
                seen.add(pid)
                filter_projects.append(row)

        return filter_projects

    def get_projects_by_token(self, token_names):
        """根据代币名称获取项目信息
        
        Args:
            token_names (list): 代币名称列表
            
        Returns:
            list: 项目信息列表
        """
        if not token_names:
            return []

        placeholders = ", ".join(["%s"] * len(token_names))
        query = f"SELECT * FROM projects WHERE token_name IN ({placeholders})"

        return self.mysql_source.execute_query(query, token_names)

    def get_project_ecosystems(self, project_ids):
        """获取项目生态系统信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的生态系统信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_ecosystems WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_fundraising(self, project_ids):
        """获取项目融资信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的融资信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_fundraising WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_fundraising_rounds(self, project_ids):
        """获取项目融资轮次信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的融资轮次信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_fundraising_rounds WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_investments(self, project_ids):
        """获取项目投资信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的投资信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_investments WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_social_links(self, project_ids):
        """获取项目社交链接信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的社交链接信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_social_links WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_subsidiary_orgs(self, project_ids):
        """获取项目子组织信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的子组织信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_subsidiary_orgs WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_tags(self, project_ids):
        """获取项目标签信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的标签信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_tags WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_team_members(self, project_ids):
        """获取项目团队成员信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的团队成员信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_team_members WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_token_contracts(self, project_ids):
        """获取项目代币合约信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的代币合约信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_token_contracts WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_token_unlock_events(self, project_ids):
        """获取项目代币解锁事件信息
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的代币解锁事件信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_token_unlock_events WHERE project_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_github_commits(self, project_ids):
        """
        获取project项目的github仓库的提交记录
        :param project_ids:
        :return: dict: 以项目id为键的github提交记录信息
        """
        if not project_ids:
            return {}
        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM github_commits WHERE project_id IN ({placeholders}) ORDER BY commit_date DESC LIMIT 5"
        results = self.mysql_source.execute_query(query, project_ids)
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped

    def get_project_snapshots(self, project_ids):
        """获取项目快照信息
        
        Args:
            project_ids (list): 项目ID列表 (假设这些ID对应MongoDB中的space.name)
            
        Returns:
            dict: 以项目ID为键的快照信息字典
        """
        if not project_ids:
            return {}

        try:
            # 优先使用self.mongo_config，如果没有则从self.config中获取
            mongo_config = self.mongo_config
            if not mongo_config:
                # 尝试从config中获取mongodb配置
                mongo_config = self.config.get('mongodb')

            if not mongo_config:
                logger.error("MongoDB配置未找到")
                return {}
            
            # 创建一个新的配置，基于原始配置但强制使用snapshot数据库
            snapshot_mongo_config = mongo_config.copy()
            snapshot_mongo_config['database'] = 'snapshot'

            mongo_source = db_factory.get_mongo_source(snapshot_mongo_config,)
            
            # 获取proposal集合
            collection = mongo_source.db['proposal']

            current_timestamp = int(time.time())

            query = {
                'space.name': {'$in': project_ids},
                'end': {'$lt': current_timestamp},
                'scoresState': 'final',
                'state': 'closed'
            }

            result_cursor = collection.find(query).sort([('end', -1)]).limit(5)
            results = list(result_cursor)

            grouped = {}
            for item in results:
                space_name = item.get('space', {}).get('name')
                if space_name:
                    if space_name not in grouped:
                        grouped[space_name] = []
                    grouped[space_name].append(item)
            return grouped

        except Exception as e:
            logger.error(f"获取项目快照信息失败: {str(e)}")
            return {}

    def get_active_team_members(self, project_ids):
        """获取项目活跃团队成员信息（过滤掉is_former为1的成员）
        
        Args:
            project_ids (list): 项目ID列表
            
        Returns:
            dict: 以项目ID为键的活跃团队成员信息字典
        """
        if not project_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(project_ids))
        query = f"SELECT * FROM projects_team_members WHERE project_id IN ({placeholders}) AND (is_former = 0 OR is_former IS NULL)"

        results = self.mysql_source.execute_query(query, project_ids)

        # 按项目ID分组
        grouped = {}
        for item in results:
            project_id = item['project_id']
            if project_id not in grouped:
                grouped[project_id] = []
            grouped[project_id].append(item)

        return grouped
        
    def get_recent_tweets(self, twitter_usernames, limit=5):
        """获取Twitter用户最近的推文
        
        Args:
            twitter_usernames (list): Twitter用户名列表
            limit (int, optional): 每个用户返回的推文数量限制
            
        Returns:
            dict: 以Twitter用户名为键的推文列表字典
        """
        if not twitter_usernames:
            return {}
        
        # 批量查询所有用户的推文，只获取最近5天内的推文
        placeholders = ", ".join(["%s"] * len(twitter_usernames))
        query = f"SELECT * FROM tweets WHERE twitter_username IN ({placeholders}) AND tweet_date > FROM_UNIXTIME(UNIX_TIMESTAMP() - (5*24*60*60)) ORDER BY twitter_username, tweet_date DESC"
        
        results = self.mysql_source.execute_query(query, twitter_usernames)
        
        # 在应用层面按用户名分组并限制每个用户的推文数量
        grouped = {}
        for item in results:
            username = item['twitter_username']
            if username not in grouped:
                grouped[username] = []
            
            # 只添加不超过limit数量的推文
            if len(grouped[username]) < limit:
                grouped[username].append(item)
                
        return grouped

    def get_project_info_by_twitter_name(self, twitter_names):
        """根据twitter 用户名获取projects_social_links表中的信息

        :param twitter_names: list:  twitter用户名列表
        :return: dict: 以twitter用户名为键的基本信息字典
        """

        if not twitter_names:
            return {}

        links = ["https://x.com/" + name.strip() for name in twitter_names]
        placeholders = ','.join(["%s"] * len(links))
        query = f"SELECT p.*, ps.* FROM projects_social_links ps JOIN projects p ON ps.project_id = p.project_id WHERE ps.link IN ({placeholders})"

        results = self.mysql_source.execute_query(query, links)
        grouped = {}
        for item in results:
            name = item['link'].split('x.com/')[-1]
            grouped[name] = item
        return grouped

    def get_people_info_by_twitter_name(self, twitter_names):
        """根据twitter 用户名获取people_social_links表中的信息

        :param twitter_names: list:  twitter用户名列表
        :return: dict: 以twitter用户名为键的基本信息字典
        """

        if not twitter_names:
            return {}

        links = ["https://x.com/" + name.strip() for name in twitter_names]
        placeholders = ','.join(["%s"] * len(links))
        query = f"SELECT ppl.*, psl.* FROM people_social_links psl JOIN people ppl ON psl.people_id = ppl.people_id WHERE psl.link IN ({placeholders})"
        results = self.mysql_source.execute_query(query, links)

        grouped = {}
        for item in results:
            name = item['link'].split('x.com/')[-1]
            grouped[name] = item

        return grouped
        
    def get_investor_by_url(self, url):
        """根据URL获取投资者信息
        
        Args:
            url (str): 投资者URL
            
        Returns:
            dict: 投资者信息
        """
        if not url:
            return None
            
        query = "SELECT * FROM investors WHERE url = %s LIMIT 1"
        results = self.mysql_source.execute_query(query, [url])
        
        return results[0] if results else None
        
    def get_people_by_url(self, url):
        """根据URL获取人员信息
        
        Args:
            url (str): 人员URL
            
        Returns:
            dict: 人员信息
        """
        if not url:
            return None
            
        query = "SELECT * FROM people WHERE url = %s LIMIT 1"
        results = self.mysql_source.execute_query(query, [url])
        
        return results[0] if results else None
        
    def get_project_by_url(self, url):
        """根据URL获取项目信息
        
        Args:
            url (str): 项目URL
            
        Returns:
            dict: 项目信息
        """
        if not url:
            return None
            
        query = "SELECT * FROM projects WHERE url = %s LIMIT 1"
        results = self.mysql_source.execute_query(query, [url])
        
        return results[0] if results else None
        
    def get_investor_social_links(self, investor_ids):
        """获取投资者社交链接信息
        
        Args:
            investor_ids (list): 投资者ID列表
            
        Returns:
            dict: 以投资者ID为键的社交链接信息字典
        """
        if not investor_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(investor_ids))
        query = f"SELECT * FROM investors_social_links WHERE investor_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, investor_ids)

        # 按投资者ID分组
        grouped = {}
        for item in results:
            investor_id = item['investor_id']
            if investor_id not in grouped:
                grouped[investor_id] = []
            grouped[investor_id].append(item)

        return grouped

    def get_people_info_by_names(self, names):
        """根据人员名称获取people表中的基本信息
        
        Args:
            names (list): 人员名称列表
            
        Returns:
            dict: 以人员名称为键的基本信息字典
        """
        if not names:
            return {}

        placeholders = ", ".join(["%s"] * len(names))
        query = f"SELECT * FROM people WHERE name IN ({placeholders})"

        results = self.mysql_source.execute_query(query, names)

        # 按名称分组
        grouped = {}
        for item in results:
            name = item['name']
            grouped[name] = item

        return grouped

    def get_people_education_experience(self, people_ids):
        """获取人员教育经历信息
        
        Args:
            people_ids (list): 人员ID列表
            
        Returns:
            dict: 以人员ID为键的教育经历信息字典
        """
        if not people_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(people_ids))
        query = f"SELECT * FROM people_education_experience WHERE people_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, people_ids)

        # 按人员ID分组
        grouped = {}
        for item in results:
            people_id = item['people_id']
            if people_id not in grouped:
                grouped[people_id] = []
            grouped[people_id].append(item)

        return grouped

    def get_people_social_link(self, people_ids):
        """获取人员社交链接信息
        
        Args:
            people_ids (list): 人员ID列表
            
        Returns:
            dict: 以人员ID为键的社交链接信息字典
        """
        if not people_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(people_ids))
        query = f"SELECT * FROM people_social_links WHERE people_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, people_ids)

        # 按人员ID分组
        grouped = {}
        for item in results:
            people_id = item['people_id']
            if people_id not in grouped:
                grouped[people_id] = []
            grouped[people_id].append(item)

        return grouped

    def get_people_work_experience(self, people_ids):
        """获取人员工作经历信息
        
        Args:
            people_ids (list): 人员ID列表
            
        Returns:
            dict: 以人员ID为键的工作经历信息字典
        """
        if not people_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(people_ids))
        query = f"SELECT * FROM people_work_experience WHERE people_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, people_ids)

        # 按人员ID分组
        grouped = {}
        for item in results:
            people_id = item['people_id']
            if people_id not in grouped:
                grouped[people_id] = []
            grouped[people_id].append(item)

        return grouped

    def get_people_investments_info(self, people_ids):
        """获取人员投资信息
        
        Args:
            people_ids (list): 人员ID列表
            
        Returns:
            dict: 以人员ID为键的投资信息字典
        """
        if not people_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(people_ids))
        query = f"SELECT * FROM people_investments_info WHERE people_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, people_ids)

        # 按人员ID分组
        grouped = {}
        for item in results:
            people_id = item['people_id']
            if people_id not in grouped:
                grouped[people_id] = []
            grouped[people_id].append(item)

        return grouped

    def get_vc_by_name(self, project_names):
        """根据项目名称获取投资者相关信息，包括基本信息、投资和融资数据
        
        从investors_list表获取与项目名称匹配的数据，
        根据数据的name字段查询investors表，
        最后investor_id查询investors_fundraising和investors_investments表
        
        Args:
            project_names (list): 项目名称列表
            
        Returns:
            list: 投资者信息列表，每个元素包含basic_vc_info、investors_investments和investors_fundraising字段
        """
        if not project_names:
            return []

        investors_list = self.get_investors_list(project_names)
        if not investors_list:
            return []

        investor_names = []
        for investor in investors_list:
            if 'name' in investor and investor['name']:
                investor_names.append(investor['name'])

        # 根据name字段查询investors表
        placeholders = ", ".join(["%s"] * len(investor_names))
        query = f"SELECT * FROM investors WHERE name IN ({placeholders})"
        investors = self.mysql_source.execute_query(query, investor_names)

        investors_map = {inv['name']: inv for inv in investors}
        # 合并两个数据源
        vc_list_detail = []
        for inv in investors_list:
            name = inv.get('name')
            if not name:
                continue

            merged = {
                **inv,
                **investors_map.get(name, {})
            }
            vc_list_detail.append(merged)

        return vc_list_detail

    def get_investors_investments(self, investor_ids):
        """获取投资者的投资信息
        
        Args:
            investor_ids (list): 投资者ID列表
            
        Returns:
            dict: 以投资者ID为键的投资信息字典
        """
        if not investor_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(investor_ids))
        query = f"SELECT * FROM investors_investments WHERE investor_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, investor_ids)

        grouped = {}
        for item in results:
            investor_id = item['investor_id']
            if investor_id not in grouped:
                grouped[investor_id] = []
            grouped[investor_id].append(item)

        return grouped

    def get_investors_fundraising(self, investor_ids):
        """获取投资者的融资信息
        
        Args:
            investor_ids (list): 投资者ID列表
            
        Returns:
            dict: 以投资者ID为键的融资信息字典
        """
        if not investor_ids:
            return {}

        placeholders = ", ".join(["%s"] * len(investor_ids))
        query = f"SELECT * FROM investors_fundraising WHERE investor_id IN ({placeholders})"

        results = self.mysql_source.execute_query(query, investor_ids)

        # 按投资者ID分组
        grouped = {}
        for item in results:
            investor_id = item['investor_id']
            if investor_id not in grouped:
                grouped[investor_id] = []
            grouped[investor_id].append(item)

        return grouped

    def get_investors_list(self, project_names):
        """根据项目名称获取investors_list表中的数据
        
        Args:
            project_names (list): 项目名称列表
            
        Returns:
            list: 投资者列表信息
        """
        if not project_names:
            return []

        conditions = []
        params = []

        for name in project_names:
            conditions.append("LOWER(name) = LOWER(%s)")
            params.append(f"%{name}%")

        query = f"SELECT * FROM investors_list WHERE {' OR '.join(conditions)}"

        return self.mysql_source.execute_query(query, params)


def extract_twitter_username(link):
    """从Twitter链接中提取用户名
    
    Args:
        link (str): Twitter链接
        
    Returns:
        str: Twitter用户名
    """
    if not link or 'x.com/' not in link:
        return None
    
    # 提取用户名部分
    username = link.split('x.com/')[-1].strip()
    # 移除可能的查询参数
    username = username.split('?')[0].strip()
    return username


def process_fundraising_links(fundraising_data, manager):
    """处理融资数据中的链接，获取对应的Twitter链接
    
    Args:
        fundraising_data (dict): 项目ID为键的融资信息字典
        manager (ChainProjectManager): 数据库管理器实例
        
    Returns:
        dict: 名称为键，Twitter链接为值的字典
    """
    if not fundraising_data:
        return {}
    
    # 收集所有链接
    investor_links = []
    people_links = []
    project_links = []
    name_link_map = {}
    
    for project_id, items in fundraising_data.items():
        for item in items:
            if 'link' not in item or not item['link']:
                continue
                
            link = item['link']
            name = item.get('name', '')
            
            # 根据链接类型分类
            if '/Investors/detail/' in link:
                investor_links.append(link)
                name_link_map[link] = name
            elif '/member/' in link:
                people_links.append(link)
                name_link_map[link] = name
            elif '/Projects/detail/' in link:
                project_links.append(link)
                name_link_map[link] = name
    # 获取投资者Twitter链接
    investor_twitter_links = {}
    if investor_links:
        investor_ids = []
        investor_url_map = {}
        
        # 从investors表获取investor_id
        for link in investor_links:
            result = manager.get_investor_by_url(link)
            if result and 'investor_id' in result:
                investor_id = result['investor_id']
                investor_ids.append(investor_id)
                investor_url_map[investor_id] = link

        # 获取Twitter链接
        if investor_ids:
            investor_social_links = manager.get_investor_social_links(investor_ids)
            for investor_id, links in investor_social_links.items():
                for link_info in links:
                    if link_info.get('text') == 'X' and link_info.get('link'):
                        original_link = investor_url_map.get(investor_id)
                        if original_link:
                            name = name_link_map.get(original_link, '')
                            investor_twitter_links[name] = link_info['link']
    
    # 获取人员Twitter链接
    people_twitter_links = {}
    if people_links:
        people_ids = []
        people_url_map = {}
        
        # 从people表获取people_id
        for link in people_links:
            result = manager.get_people_by_url(link)
            if result and 'people_id' in result:
                people_id = result['people_id']
                people_ids.append(people_id)
                people_url_map[people_id] = link
        
        # 获取Twitter链接
        if people_ids:
            people_social_links = manager.get_people_social_link(people_ids)
            for people_id, links in people_social_links.items():
                for link_info in links:
                    if link_info.get('text') == 'X' and link_info.get('link'):
                        original_link = people_url_map.get(people_id)
                        if original_link:
                            name = name_link_map.get(original_link, '')
                            people_twitter_links[name] = link_info['link']
    
    # 获取项目Twitter链接
    project_twitter_links = {}
    if project_links:
        project_ids = []
        project_url_map = {}
        
        # 从projects表获取project_id
        for link in project_links:
            result = manager.get_project_by_url(link)
            if result and 'project_id' in result:
                project_id = result['project_id']
                project_ids.append(project_id)
                project_url_map[project_id] = link
        
        # 获取Twitter链接
        if project_ids:
            project_social_links = manager.get_project_social_links(project_ids)
            for project_id, links in project_social_links.items():
                for link_info in links:
                    if link_info.get('text') == 'X' and link_info.get('link'):
                        original_link = project_url_map.get(project_id)
                        if original_link:
                            name = name_link_map.get(original_link, '')
                            project_twitter_links[name] = link_info['link']
    
    # 合并所有Twitter链接
    twitter_links = {}
    twitter_links.update(investor_twitter_links)
    twitter_links.update(people_twitter_links)
    twitter_links.update(project_twitter_links)
    return twitter_links


def process_team_members(team_members, manager):
    """处理团队成员信息，获取对应的Twitter链接
    
    Args:
        team_members (dict): 项目ID为键的团队成员信息字典
        manager (ChainProjectManager): 数据库管理器实例
        
    Returns:
        dict: 名称为键，Twitter链接为值的字典
    """
    if not team_members:
        return {}
    
    # 收集所有团队成员名称
    member_names = []
    for project_id, members in team_members.items():
        for member in members:
            if 'name' in member and member['name']:
                member_names.append(member['name'])
    people_info = manager.get_people_info_by_names(member_names)
    people_ids = []
    for name, info in people_info.items():
        if 'people_id' in info:
            people_ids.append(info['people_id'])
    # 获取社交链接信息
    twitter_links = {}
    if people_ids:
        people_social_links = manager.get_people_social_link(people_ids)
        # 将people_id映射回名称
        people_id_name_map = {}
        for name, info in people_info.items():
            if 'people_id' in info:
                people_id_name_map[info['people_id']] = name
        
        # 提取Twitter链接
        for people_id, links in people_social_links.items():
            for link_info in links:
                if link_info.get('text') == 'X' and link_info.get('link'):
                    name = people_id_name_map.get(people_id, '')
                    if name:
                        twitter_links[name] = link_info['link']

    return twitter_links


def get_recent_tweets(twitter_usernames, manager, limit=5):
    """获取最近的推文
    
    Args:
        twitter_usernames (dict): 名称为键，Twitter链接为值的字典
        manager (ChainProjectManager): 数据库管理器实例
        limit (int, optional): 每个用户返回的推文数量限制
        
    Returns:
        dict: 名称为键，推文列表为值的字典
    """
    if not twitter_usernames:
        return {}
    
    # 整合所有Twitter用户名
    usernames = []
    username_name_map = {}
    
    for name, link in twitter_usernames.items():
        username = extract_twitter_username(link)
        if username:
            usernames.append(username)
            username_name_map[username] = name

    tweets_data = manager.get_recent_tweets(usernames, limit)

    result = {}
    for username, tweets in tweets_data.items():
        name = username_name_map.get(username, username)
        result[name] = tweets
    
    return result


def get_all_info(extracted_record, mysql_config, existing_manager=None, mongo_config=None):
    """获取所有相关信息
    
    Args:
        extracted_record (dict): 提取的记录信息
        mysql_config (dict): MySQL配置
        existing_manager (ChainProjectManager, optional): 现有的ChainProjectManager实例，如果提供则复用该实例
        mongo_config (dict, optional): MongoDB配置，用于获取项目快照信息
        
    Returns:
        dict: 所有相关信息
    """
    # 检查是否提供了现有的ChainProjectManager实例
    if existing_manager and isinstance(existing_manager, ChainProjectManager):
        manager = existing_manager
    else:
        manager = ChainProjectManager(mysql_config, mongo_config)

    try:
        project_names = extracted_record.get('project', [])
        token_names = extracted_record.get('token', [])

        projects_by_name = manager.get_projects_by_name(project_names + token_names)
        if not isinstance(projects_by_name, list):
            logger.error(f"get_projects_by_name returned non-list type: {type(projects_by_name)}")
            projects_by_name = []

        # projects_by_token = manager.get_projects_by_token(token_names)
        # if not isinstance(projects_by_token, list):
        #     logger.error(f"get_projects_by_token returned non-list type: {type(projects_by_token)}")
        #     projects_by_token = []

        # 获取VC相关信息（包含投资者基本信息、投资和融资数据）
        vc_by_name = manager.get_vc_by_name(project_names)
        if not isinstance(vc_by_name, list):
            logger.error(f"get_vc_by_name returned non-list type: {type(vc_by_name)}")
            vc_by_name = []

        all_vc = {}
        for vc in vc_by_name:
            investor_id = vc.get('investor_id')
            if investor_id:
                all_vc[investor_id] = vc

        all_projects = {}
        for project in projects_by_name:
            project_id = project['project_id']
            all_projects[project_id] = project

        if not all_projects and not all_vc:
            logger.warning(f"未找到与项目名称 {project_names} 或代币名称 {token_names} 相关的项目或VC")
            return {}

        # 获取所有项目ID
        project_ids = list(all_projects.keys())

        # 获取各个关联表的数据
        project_ecosystems = manager.get_project_ecosystems(project_ids)
        project_fundraising = manager.get_project_fundraising(project_ids)
        project_fundraising_rounds = manager.get_project_fundraising_rounds(project_ids)
        project_investments = manager.get_project_investments(project_ids)
        project_social_links = manager.get_project_social_links(project_ids)
        project_subsidiary_orgs = manager.get_project_subsidiary_orgs(project_ids)
        project_tags = manager.get_project_tags(project_ids)
        project_team_members = manager.get_project_team_members(project_ids)
        project_token_contracts = manager.get_project_token_contracts(project_ids)
        project_token_unlock_events = manager.get_project_token_unlock_events(project_ids)
        project_snapshots = manager.get_project_snapshots(project_names)

        investor_ids = list(all_vc.keys())
        investors_investments = manager.get_investors_investments(investor_ids)
        investors_fundraising = manager.get_investors_fundraising(investor_ids)

        # 根据project_id字段获取github提交记录
        project_github_commits = manager.get_project_github_commits(project_ids)

        # 获取活跃团队成员信息（过滤掉is_former为1的成员）
        active_team_members = manager.get_active_team_members(project_ids)

        # 处理团队成员详细信息
        all_people_names = []
        project_people_map = {}

        # 收集所有活跃团队成员的名称
        for project_id, members in active_team_members.items():
            project_people_map[project_id] = []
            for member in members:
                if 'name' in member and member['name']:
                    all_people_names.append(member['name'])
                    project_people_map[project_id].append(member['name'])
                    
       
        all_people_names = list(set(all_people_names))

        # 获取所有人员的基本信息
        people_info = manager.get_people_info_by_names(all_people_names)

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

        result = []
        for project_id, project in all_projects.items():
            # 获取该项目的所有活跃团队成员的详细信息
            project_people_details = {}
            for name in project_people_map.get(project_id, []):
                if name in detailed_people_info:
                    project_people_details[name] = detailed_people_info[name]
            project_name = project.get('project_name', '')
            # 处理团队成员的Twitter链接
            team_twitter_links = process_team_members({
                project_id: active_team_members.get(project_id, [])
            }, manager)
            
            # 处理融资数据中的Twitter链接
            fundraising_twitter_links = process_fundraising_links({
                project_id: project_fundraising.get(project_id, [])
            }, manager)
            
            # 合并所有Twitter链接再获取推文
            all_twitter_links = {}
            all_twitter_links.update(team_twitter_links)
            all_twitter_links.update(fundraising_twitter_links)

            recent_tweets = {}
            if all_twitter_links:
                recent_tweets = get_recent_tweets(all_twitter_links, manager)
            
            project_info = {
                'basic_info': project,
                'ecosystems': project_ecosystems.get(project_id, []),
                'fundraising': project_fundraising.get(project_id, []),
                'fundraising_rounds': project_fundraising_rounds.get(project_id, []),
                'investments': project_investments.get(project_id, []),
                'social_links': project_social_links.get(project_id, []),
                'subsidiary_orgs': project_subsidiary_orgs.get(project_id, []),
                'tags': project_tags.get(project_id, []),
                'team_members': project_team_members.get(project_id, []),
                'active_team_members': active_team_members.get(project_id, []),
                'github_commit_msg': project_github_commits.get(project_id, []),
                'team_members_details': project_people_details,
                'token_contracts': project_token_contracts.get(project_id, []),
                'token_unlock_events': project_token_unlock_events.get(project_id, []),
                'snapshots': project_snapshots.get(project_name, []),  # 快照信息
                'recent_activity': recent_tweets
            }
            result.append(project_info)

        # for vc_id, vc in all_vc.items():
        #     result.append({
        #         'vc_info': vc,
        #         'investor_investments': investors_investments.get(vc_id, []),
        #         'investor_fundraising': investors_fundraising.get(vc_id, [])
        #     })
        return result

    finally:
        #  不用关闭数据库，工厂统一管理
        pass
