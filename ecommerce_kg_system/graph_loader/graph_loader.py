#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
图数据库加载工具，用于将处理后的数据加载到Neo4j
"""

import os
import sys
import logging
import time
from typing import Dict, List, Any, Optional

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

try:
    from neo4j import GraphDatabase, Session, Transaction
    from neo4j.exceptions import ServiceUnavailable, AuthError
    
    from ecommerce_kg_system.config.config import NEO4J_CONFIG, GRAPH_CONFIG
except ImportError:
    # 如果无法导入，提供简单的模拟实现
    class GraphDatabase:
        @staticmethod
        def driver(*args, **kwargs):
            return MockDriver()
    
    class MockDriver:
        def __init__(self):
            self.is_closed = False
            
        def session(self, *args, **kwargs):
            return MockSession()
            
        def close(self):
            self.is_closed = True
    
    class MockSession:
        def __init__(self):
            pass
            
        def run(self, *args, **kwargs):
            return []
            
        def close(self):
            pass
            
        def write_transaction(self, func, *args, **kwargs):
            return func(MockTransaction(), *args, **kwargs)
    
    class MockTransaction:
        def run(self, *args, **kwargs):
            return MockResult()
    
    class MockResult:
        def single(self):
            return {"count": 0}
    
    # 默认配置
    NEO4J_CONFIG = {
        'uri': 'bolt://localhost:7687',
        'user': 'neo4j',
        'password': 'password',
        'database': 'neo4j',
    }
    
    GRAPH_CONFIG = {
        'batch_size': 5000,
        'timeout': 120,
    }
    
    # 模拟异常
    class ServiceUnavailable(Exception):
        pass
        
    class AuthError(Exception):
        pass

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GraphLoader:
    """Neo4j图数据库加载工具"""
    
    def __init__(self, config: Dict = None):
        """
        初始化图数据加载器
        
        Args:
            config: Neo4j连接配置，如果为None则使用默认配置
        """
        self.config = config or NEO4J_CONFIG
        self.batch_size = GRAPH_CONFIG.get('batch_size', 5000)
        self.driver = None
        
        # 连接到Neo4j
        self._connect()
        
    def _connect(self):
        """连接到Neo4j数据库"""
        try:
            self.driver = GraphDatabase.driver(
                self.config['uri'],
                auth=(self.config['user'], self.config['password'])
            )
            # 测试连接
            with self.driver.session(database=self.config.get('database', 'neo4j')) as session:
                result = session.run("RETURN 1")
                result.single()
            logger.info(f"成功连接到Neo4j数据库: {self.config['uri']}")
        except (ServiceUnavailable, AuthError) as e:
            logger.error(f"连接Neo4j数据库失败: {str(e)}")
            raise
            
    def close(self):
        """关闭Neo4j连接"""
        if self.driver:
            self.driver.close()
            logger.info("Neo4j连接已关闭")
            
    def clear_database(self):
        """清空Neo4j数据库中的所有数据"""
        try:
            with self.driver.session(database=self.config.get('database', 'neo4j')) as session:
                result = session.run("MATCH (n) DETACH DELETE n")
                logger.info("数据库已清空")
        except Exception as e:
            logger.error(f"清空数据库失败: {str(e)}")
            raise
    
    def verify_data_loading(self) -> Dict[str, int]:
        """
        验证数据加载结果
        
        Returns:
            各类型节点和关系的数量统计
        """
        stats = {}
        
        try:
            with self.driver.session(database=self.config.get('database', 'neo4j')) as session:
                # 统计节点数量
                result = session.run("MATCH (n) RETURN labels(n) AS label, count(n) AS count")
                for record in result:
                    label = record["label"][0] if record["label"] else "Unknown"
                    stats[label] = record["count"]
                    
                # 统计关系数量
                result = session.run("MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count")
                for record in result:
                    rel_type = record["type"]
                    stats[f"REL_{rel_type}"] = record["count"]
        except Exception as e:
            logger.error(f"验证数据加载失败: {str(e)}")
            
        return stats
    
    def load_graph_data(self, data_files: Dict[str, str]) -> Dict[str, int]:
        """
        加载图数据到Neo4j
        
        Args:
            data_files: 数据文件路径字典，键为数据类型，值为文件路径
            
        Returns:
            加载数据统计
        """
        stats = {}
        
        # 加载节点
        for entity_type, file_path in data_files.items():
            if entity_type == 'relationships':
                continue
                
            if not os.path.exists(file_path):
                logger.warning(f"文件不存在: {file_path}")
                continue
                
            try:
                count = self._load_nodes(entity_type, file_path)
                stats[entity_type] = count
                logger.info(f"已加载 {count} 个 {entity_type} 节点")
            except Exception as e:
                logger.error(f"加载 {entity_type} 节点失败: {str(e)}")
                stats[entity_type] = 0
        
        # 加载关系
        if 'relationships' in data_files and os.path.exists(data_files['relationships']):
            try:
                count = self._load_relationships(data_files['relationships'])
                stats['relationships'] = count
                logger.info(f"已加载 {count} 个关系")
            except Exception as e:
                logger.error(f"加载关系失败: {str(e)}")
                stats['relationships'] = 0
                
        return stats
    
    def _load_nodes(self, entity_type: str, file_path: str) -> int:
        """
        加载节点数据
        
        Args:
            entity_type: 实体类型
            file_path: CSV文件路径
            
        Returns:
            加载的节点数量
        """
        count = 0
        
        # 根据实体类型确定标签
        label = entity_type[:-1].capitalize()  # 例如，'users' -> 'User'
        if label.endswith('ie'):
            label = label[:-2] + 'y'  # 处理复数形式，例如 'categories' -> 'Category'
        
        # 从CSV文件加载数据
        try:
            # 使用LOAD CSV命令加载数据
            with self.driver.session(database=self.config.get('database', 'neo4j')) as session:
                # 创建索引（如果不存在）
                session.run(f"CREATE INDEX IF NOT EXISTS FOR (n:{label}) ON (n.id)")
                
                # 加载数据
                cypher = f"""
                LOAD CSV WITH HEADERS FROM 'file:///{file_path.replace(os.sep, "/")}' AS row
                CREATE (n:{label} {{id: row.id}})
                SET n += row
                RETURN count(n) AS count
                """
                result = session.run(cypher)
                count = result.single()["count"]
                
        except Exception as e:
            logger.error(f"加载节点数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
            
        return count
    
    def _load_relationships(self, file_path: str) -> int:
        """
        加载关系数据
        
        Args:
            file_path: CSV文件路径
            
        Returns:
            加载的关系数量
        """
        count = 0
        
        # 从CSV文件加载数据
        try:
            # 使用LOAD CSV命令加载数据
            with self.driver.session(database=self.config.get('database', 'neo4j')) as session:
                # 加载数据
                cypher = f"""
                LOAD CSV WITH HEADERS FROM 'file:///{file_path.replace(os.sep, "/")}' AS row
                MATCH (from {{id: row.`:START_ID`}})
                MATCH (to {{id: row.`:END_ID`}})
                CREATE (from)-[r:{row.`:TYPE`}]->(to)
                RETURN count(r) AS count
                """
                result = session.run(cypher)
                count = result.single()["count"]
                
        except Exception as e:
            logger.error(f"加载关系数据失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            raise
            
        return count