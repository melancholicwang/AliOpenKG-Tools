#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
处理大型RAR文件和RDF数据文件的命令行工具
"""

import os
import sys
import logging
import argparse
from typing import List, Optional, Dict, Tuple, Any
import pandas as pd
import random
import json
import re
import time

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from ecommerce_kg_system.utils.rar_processor import RarProcessor
    from ecommerce_kg_system.data_processing.data_processor import DataProcessor
    from ecommerce_kg_system.config.config import DATA_PATHS
except ImportError:
    # 如果无法导入，设置默认配置
    class RarProcessor:
        """RAR文件处理类的简单实现"""
        @staticmethod
        def extract_rar(rar_path, output_dir=None, specific_files=None):
            """提取RAR文件"""
            import rarfile
            
            if output_dir is None:
                output_dir = os.path.splitext(rar_path)[0]
                
            os.makedirs(output_dir, exist_ok=True)
            
            with rarfile.RarFile(rar_path) as rf:
                if specific_files:
                    for file in specific_files:
                        if file in rf.namelist():
                            rf.extract(file, output_dir)
                else:
                    rf.extractall(output_dir)
                    
            return output_dir
    
    # 默认数据路径配置
    DATA_PATHS = {
        'tbox_dir': './AliOpenKG_TBox',
        'abox_dir': './AliOpenKG_ABox_Part1',
        'processed_dir': './processed',
        'sample_dir': './samples'
    }

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_large_nt_file(file_path, output_file=None, chunk_size=1000000, sample_size=10000, random_sample=False):
    """
    处理大型NT文件，使用自定义解析器
    
    Args:
        file_path: NT文件路径
        output_file: 输出文件路径
        chunk_size: 每块的行数，用于分块处理大文件
        sample_size: 采样数量，达到这个数量后停止处理
        random_sample: 是否进行随机采样，默认为False
        
    Returns:
        处理后的文件路径和Neo4j文件路径字典
    """
    if output_file is None:
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(DATA_PATHS.get('processed_dir', './processed'), f"processed_{timestamp}.nt")
        
    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    logger.info(f"处理大型NT文件: {file_path}")
    logger.info(f"块大小: {chunk_size} 行, 采样数量: {sample_size}, 随机采样: {random_sample}")
    
    # 统计实体类型
    entity_counts = {
        'user': 0,
        'product': 0,
        'category': 0,
        'brand': 0,
        'scene': 0,      # 场景
        'crowd': 0,      # 人群
        'time': 0,       # 适用时间
        'theme': 0,      # 主题
        'market': 0,      # 细分市场
        'placeOfOrigin': 0      # 产地（新增）
    }
    
    # 采样数据
    sampled_triples = []
    
    # 实体映射
    entity_map = {
        'users': {},
        'products': {},
        'categories': {},
        'brands': {},
        'scenes': {},    # 场景
        'crowds': {},    # 人群
        'times': {},     # 适用时间
        'themes': {},    # 主题
        'markets': {},    # 细分市场
        'placeOfOrigins': {}    # 产地（新增）
    }
    
    # 关系数据
    relationships = []
    
    # 计数器
    line_count = 0
    valid_line_count = 0
    chunk_count = 0
    chunk_line_count = 0
    
    # 正则表达式编译一次，提高性能
    triple_pattern = re.compile(r'<([^>]+)>\s+<([^>]+)>\s+(.+)\s+\.')
    
    try:
        # 记录开始时间
        start_time = time.time()
        
        # 使用自定义解析器处理文件
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f, open(output_file, 'w', encoding='utf-8') as out_f:
            # 分块读取文件
            current_chunk = []
            
            for line in f:
                line_count += 1
                
                # 跳过空行和注释
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                valid_line_count += 1
                
                # 采样逻辑 - 优化版本
                if random_sample:
                    # 使用蓄水池采样算法
                    if len(sampled_triples) < sample_size:
                        sampled_triples.append(line)
                        # 只有当行被采样时才添加到当前块进行处理
                        current_chunk.append(line)
                        chunk_line_count += 1
                    else:
                        # 随机替换
                        j = random.randint(0, valid_line_count - 1)
                        if j < sample_size:
                            sampled_triples[j] = line
                            # 只有当行被采样时才添加到当前块进行处理
                            current_chunk.append(line)
                            chunk_line_count += 1
                        
                        # 如果已经处理了足够多的行，可以提前停止
                        if valid_line_count >= sample_size * 10:
                            logger.info(f"已处理 {valid_line_count} 行，采样数量已达到 {sample_size}，且处理了足够多的行，停止处理")
                            break
                else:
                    # 简单采样（取前N个）
                    if len(sampled_triples) < sample_size:
                        sampled_triples.append(line)
                        # 只有当行被采样时才添加到当前块进行处理
                        current_chunk.append(line)
                        chunk_line_count += 1
                    else:
                        # 如果已经采样足够的数据，停止处理
                        logger.info(f"已达到采样数量 {sample_size}，停止处理")
                        break
                
                # 如果当前块达到了块大小，处理当前块
                if chunk_line_count >= chunk_size:
                    chunk_count += 1
                    logger.info(f"处理第 {chunk_count} 块，包含 {len(current_chunk)} 行")
                    
                    # 处理当前块中的每一行
                    process_chunk(current_chunk, triple_pattern, entity_map, entity_counts, relationships)
                    
                    # 清空当前块，准备处理下一块
                    current_chunk = []
                    chunk_line_count = 0
                    
                    # 输出当前采样状态
                    logger.info(f"已处理 {line_count} 行，当前采样大小: {len(sampled_triples)}")
                
                # 每处理100000行输出一次日志
                if line_count % 100000 == 0:
                    elapsed_time = time.time() - start_time
                    logger.info(f"已处理 {line_count} 行，当前采样大小: {len(sampled_triples)}, 耗时: {elapsed_time:.2f}秒")
            
            # 处理最后一个不完整的块
            if current_chunk:
                chunk_count += 1
                logger.info(f"处理第 {chunk_count} 块（最后一块），包含 {len(current_chunk)} 行")
                
                # 处理当前块中的每一行
                process_chunk(current_chunk, triple_pattern, entity_map, entity_counts, relationships)
            
            # 保存采样数据
            for triple in sampled_triples:
                out_f.write(triple + '\n')
        
        # 记录总耗时
        total_time = time.time() - start_time
        
        logger.info(f"文件处理完成: {output_file}")
        logger.info(f"实体统计: 用户={entity_counts['user']}, 商品={entity_counts['product']}, 类别={entity_counts['category']}, 品牌={entity_counts['brand']}, 场景={entity_counts['scene']}, 人群={entity_counts['crowd']}, 适用时间={entity_counts['time']}, 主题={entity_counts['theme']}, 细分市场={entity_counts['market']}, 产地={entity_counts['placeOfOrigin']}")
        logger.info(f"总行数: {line_count}, 有效行数: {valid_line_count}, 采样行数: {len(sampled_triples)}")
        logger.info(f"总耗时: {total_time:.2f}秒")
        
        # 保存Neo4j格式文件
        neo4j_files = save_to_neo4j_format(entity_map, relationships)
        
        return output_file, neo4j_files
        
    except Exception as e:
        logger.error(f"处理文件失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return None, {}

def process_chunk(chunk: List[str], pattern: re.Pattern, entity_map: Dict, entity_counts: Dict, relationships: List):
    """
    处理文件的一个数据块
    
    Args:
        chunk: 要处理的行列表
        pattern: 用于解析三元组的正则表达式
        entity_map: 实体映射字典
        entity_counts: 实体计数字典
        relationships: 关系列表
    """
    for line in chunk:
        # 解析三元组
        match = pattern.match(line)
        if not match:
            continue
            
        subject, predicate, obj = match.groups()
        
        # 处理对象，可能是URI或字面量
        if obj.startswith('<') and obj.endswith('>'):
            # URI
            obj = obj[1:-1]
            is_uri = True
        else:
            # 字面量，可能带有数据类型或语言标签
            is_uri = False
            # 解析常见字面量格式，如 "value"^^xsd:string 或 "value"@en
            literal_match = re.match(r'"([^"]*)"(?:\^\^<([^>]+)>|@([a-zA-Z-]+))?', obj)
            if literal_match:
                value, datatype, lang = literal_match.groups()
                obj = value  # 简化处理，仅使用值部分
        
        # 获取主语和谓语URI
        subject = subject.strip('<>')
        predicate = predicate.strip('<>')
        
        # 根据谓语来确定处理方式
        process_triple(subject, predicate, obj, is_uri, entity_map, entity_counts, relationships)

def process_triple(subject: str, predicate: str, obj: Any, is_uri: bool, entity_map: Dict, entity_counts: Dict, relationships: List):
    """
    处理RDF三元组
    
    Args:
        subject: 主语URI
        predicate: 谓语URI
        obj: 对象值
        is_uri: 对象是否为URI
        entity_map: 实体映射字典
        entity_counts: 实体计数字典
        relationships: 关系列表
    """
    # 实体类型识别
    if predicate.endswith('type'):
        # 根据对象识别实体类型
        if 'User' in obj:
            process_user_entity(subject, entity_map, entity_counts)
        elif 'Product' in obj:
            process_product_entity(subject, entity_map, entity_counts)
        elif 'Category' in obj:
            process_category_entity(subject, entity_map, entity_counts)
        elif 'Brand' in obj:
            process_brand_entity(subject, entity_map, entity_counts)
        elif 'Scene' in obj:
            process_scene_entity(subject, entity_map, entity_counts)
        elif 'Crowd' in obj:
            process_crowd_entity(subject, entity_map, entity_counts)
        elif 'Time' in obj:
            process_time_entity(subject, entity_map, entity_counts)
        elif 'Theme' in obj:
            process_theme_entity(subject, entity_map, entity_counts)
        elif 'Market' in obj:
            process_market_entity(subject, entity_map, entity_counts)
        elif 'PlaceOfOrigin' in obj:
            process_placeoforigin_entity(subject, entity_map, entity_counts)
    # 特定关系处理
    elif is_uri and any(rel in predicate for rel in ['hasCategory', 'hasBrand', 'belongsTo', 'appliesTo']):
        # 处理产品和类别/品牌/场景等之间的关系
        add_relationship(subject, obj, predicate, entity_map, relationships)
    # 属性处理
    elif not is_uri:
        # 处理实体属性
        add_property(subject, predicate, obj, entity_map)

# 以下为简化实现，实际应用中可能需要更复杂的处理逻辑
def process_user_entity(uri: str, entity_map: Dict, entity_counts: Dict):
    """处理用户实体"""
    if uri not in entity_map['users']:
        user_id = f"user_{entity_counts['user']}"
        entity_counts['user'] += 1
        
        # 从URI中提取用户名或ID
        label = uri.split('/')[-1]
        if '#' in label:
            label = label.split('#')[-1]
            
        entity_map['users'][uri] = {
            'id': user_id,
            'label': label
        }

def process_product_entity(uri: str, entity_map: Dict, entity_counts: Dict):
    """处理产品实体"""
    if uri not in entity_map['products']:
        product_id = f"product_{entity_counts['product']}"
        entity_counts['product'] += 1
        
        # 从URI中提取产品名或ID
        label = uri.split('/')[-1]
        if '#' in label:
            label = label.split('#')[-1]
            
        entity_map['products'][uri] = {
            'id': product_id,
            'label': label
        }

def save_to_neo4j_format(entity_map: Dict, relationships: List) -> Dict[str, str]:
    """
    将处理后的实体和关系保存为Neo4j导入格式
    
    Args:
        entity_map: 实体映射字典
        relationships: 关系列表
        
    Returns:
        文件路径字典
    """
    # 确保样本目录存在
    sample_dir = DATA_PATHS.get('sample_dir', './samples')
    os.makedirs(sample_dir, exist_ok=True)
    
    neo4j_files = {}
    
    # 保存用户数据
    if entity_map['users']:
        users_file = os.path.join(sample_dir, 'users.csv')
        with open(users_file, 'w', encoding='utf-8') as f:
            f.write('id:ID,label,views,purchases,follows,:LABEL\n')
            for uri, user in entity_map['users'].items():
                f.write(f"{user['id']},{user['label']},,,,User\n")
        neo4j_files['users'] = users_file
        logger.info(f"用户数据已保存: {users_file}")
    
    # 保存商品数据
    if entity_map['products']:
        products_file = os.path.join(sample_dir, 'products.csv')
        with open(products_file, 'w', encoding='utf-8') as f:
            f.write('id:ID,label,:LABEL\n')
            for uri, product in entity_map['products'].items():
                f.write(f"{product['id']},{product['label']},Product\n")
        neo4j_files['products'] = products_file
        logger.info(f"商品数据已保存: {products_file}")
        
    # 保存类别数据
    if entity_map['categories']:
        categories_file = os.path.join(sample_dir, 'categories.csv')
        with open(categories_file, 'w', encoding='utf-8') as f:
            f.write('id:ID,label,:LABEL\n')
            for uri, category in entity_map['categories'].items():
                f.write(f"{category['id']},{category['label']},Category\n")
        neo4j_files['categories'] = categories_file
        logger.info(f"类别数据已保存: {categories_file}")
        
    # 保存品牌数据
    if entity_map['brands']:
        brands_file = os.path.join(sample_dir, 'brands.csv')
        with open(brands_file, 'w', encoding='utf-8') as f:
            f.write('id:ID,label,:LABEL\n')
            for uri, brand in entity_map['brands'].items():
                f.write(f"{brand['id']},{brand['label']},Brand\n")
        neo4j_files['brands'] = brands_file
        logger.info(f"品牌数据已保存: {brands_file}")
        
    # 保存场景数据
    if entity_map['scenes']:
        scenes_file = os.path.join(sample_dir, 'scenes.csv')
        with open(scenes_file, 'w', encoding='utf-8') as f:
            f.write('id:ID,label,:LABEL\n')
            for uri, scene in entity_map['scenes'].items():
                f.write(f"{scene['id']},{scene['label']},Scene\n")
        neo4j_files['scenes'] = scenes_file
        logger.info(f"场景数据已保存: {scenes_file}")
        
    # 保存关系数据
    if relationships:
        relationships_file = os.path.join(sample_dir, 'relationships.csv')
        with open(relationships_file, 'w', encoding='utf-8') as f:
            f.write(':START_ID,:END_ID,:TYPE\n')
            for rel in relationships:
                f.write(f"{rel['from_id']},{rel['to_id']},{rel['type']}\n")
        neo4j_files['relationships'] = relationships_file
        logger.info(f"关系数据已保存: {relationships_file}")
    
    return neo4j_files

def add_relationship(subject_uri: str, object_uri: str, predicate: str, entity_map: Dict, relationships: List):
    """
    添加实体之间的关系
    
    Args:
        subject_uri: 主语URI
        object_uri: 宾语URI
        predicate: 谓语(关系类型)
        entity_map: 实体映射
        relationships: 关系列表
    """
    # 获取关系类型
    rel_type = predicate.split('/')[-1]
    if '#' in rel_type:
        rel_type = rel_type.split('#')[-1]
    
    # 映射常见关系名称
    rel_mapping = {
        'hasCategory': 'BELONGS_TO_CATEGORY',
        'hasBrand': 'HAS_BRAND',
        'belongsTo': 'BELONGS_TO',
        'appliesTo': 'APPLIES_TO',
        'suitable_for_crowd': 'SUITABLE_FOR_CROWD',
        'suitable_for_scene': 'SUITABLE_FOR_SCENE',
        'suitable_for_time': 'SUITABLE_FOR_TIME',
        'hasTheme': 'HAS_THEME',
        'inMarket': 'IN_MARKET'
    }
    
    rel_type = rel_mapping.get(rel_type, rel_type.upper())
    
    # 找到主语和宾语的ID
    from_id = None
    to_id = None
    
    # 查找主语ID
    for entity_type, entities in entity_map.items():
        if subject_uri in entities:
            from_id = entities[subject_uri]['id']
            break
    
    # 查找宾语ID
    for entity_type, entities in entity_map.items():
        if object_uri in entities:
            to_id = entities[object_uri]['id']
            break
    
    # 如果找到了ID，添加关系
    if from_id and to_id:
        relationships.append({
            'from_id': from_id,
            'to_id': to_id,
            'type': rel_type
        })

def main():
    """命令行入口函数"""
    parser = argparse.ArgumentParser(description="处理大型RAR文件和RDF数据文件")
    parser.add_argument('file_path', help="要处理的文件路径，可以是RAR文件或NT/TTL文件")
    parser.add_argument('--extract', action='store_true', help="是否提取RAR文件")
    parser.add_argument('--output', help="输出文件路径")
    parser.add_argument('--sample', type=int, default=10000, help="采样数量")
    parser.add_argument('--chunk-size', type=int, default=100000, help="分块处理大小")
    parser.add_argument('--random-sample', action='store_true', help="是否随机采样")
    parser.add_argument('--convert', action='store_true', help="是否转换为Neo4j格式")
    parser.add_argument('--load', action='store_true', help="是否加载到Neo4j")
    args = parser.parse_args()
    
    # 确保处理目录存在
    os.makedirs(DATA_PATHS.get('processed_dir', './processed'), exist_ok=True)
    
    # 确保样本目录存在
    os.makedirs(DATA_PATHS.get('sample_dir', './samples'), exist_ok=True)
    
    try:
        # 处理文件
        if args.file_path.endswith('.nt'):
            processed_file, neo4j_files = process_large_nt_file(
                args.file_path, 
                output_file=args.output,
                chunk_size=args.chunk_size,
                sample_size=args.sample,
                random_sample=args.random_sample
            )
            
            # 转换为Neo4j格式
            if args.convert:
                logger.info("转换为Neo4j格式")
                logger.info(f"Neo4j文件: {neo4j_files}")
                
                # 加载到Neo4j
                if args.load:
                    logger.info("加载到Neo4j")
                    try:
                        from ecommerce_kg_system.graph_loader.graph_loader import GraphLoader
                        
                        loader = GraphLoader()
                        try:
                            stats = loader.load_graph_data(neo4j_files)
                            logger.info(f"数据加载完成: {stats}")
                            
                            verify_stats = loader.verify_data_loading()
                            logger.info(f"数据验证结果: {verify_stats}")
                        finally:
                            loader.close()
                    except ImportError:
                        logger.warning("无法导入GraphLoader，跳过加载到Neo4j")
        
        return 0
    except Exception as e:
        logger.error(f"处理文件失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())