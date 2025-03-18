#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
电商知识图谱系统配置文件
"""

import os
from pathlib import Path
import dotenv

# 尝试加载.env文件中的环境变量
dotenv.load_dotenv()

# 项目根目录
ROOT_DIR = Path(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# 数据目录路径
DATA_PATHS = {
    'tbox_dir': os.environ.get('TBOX_DIR', str(ROOT_DIR / 'AliOpenKG_TBox')),
    'abox_dir': os.environ.get('ABOX_DIR', str(ROOT_DIR / 'AliOpenKG_ABox_Part1')),
    'processed_dir': os.environ.get('PROCESSED_DIR', str(ROOT_DIR / 'processed')),
    'sample_dir': os.environ.get('SAMPLE_DIR', str(ROOT_DIR / 'samples')),
}

# 图数据库配置
NEO4J_CONFIG = {
    'uri': os.environ.get('NEO4J_URI', 'bolt://localhost:7687'),
    'user': os.environ.get('NEO4J_USER', 'neo4j'),
    'password': os.environ.get('NEO4J_PASSWORD', 'password'),
    'database': os.environ.get('NEO4J_DATABASE', 'neo4j'),
}

# 图处理配置
GRAPH_CONFIG = {
    'batch_size': int(os.environ.get('BATCH_SIZE', '5000')),
    'timeout': int(os.environ.get('TIMEOUT', '120')),
}

# 实体类型映射
ENTITY_TYPE_MAPPING = {
    'Product': '商品',
    'Category': '类目',
    'Brand': '品牌',
    'User': '用户',
    'Scene': '场景',
    'Crowd': '人群',
    'Time': '时间',
    'Theme': '主题',
    'Market': '细分市场',
    'PlaceOfOrigin': '产地',
}

# 关系类型映射
RELATION_TYPE_MAPPING = {
    'HAS_CATEGORY': '属于类目',
    'HAS_BRAND': '属于品牌',
    'SUITABLE_FOR_SCENE': '适用场景',
    'SUITABLE_FOR_CROWD': '适用人群',
    'SUITABLE_FOR_TIME': '适用时间',
    'HAS_THEME': '主题',
    'IN_MARKET': '细分市场',
    'FROM': '产地',
}

# 创建必要的目录
for path in DATA_PATHS.values():
    os.makedirs(path, exist_ok=True)