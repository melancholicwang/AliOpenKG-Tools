#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
处理本体层面(TBox)的JSONLD文件，提取本体结构信息并转换为Neo4j可导入的格式
支持分块处理和采样，并保持树状的层级关系结构
"""

import os
import sys
import json
import logging
import argparse
import pandas as pd
from typing import Dict, List, Set, Tuple, Any, Optional
from pathlib import Path
import time
import random
import csv

# 添加项目根目录到路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    from ecommerce_kg_system.config.config import DATA_PATHS, GRAPH_CONFIG
    from ecommerce_kg_system.graph_loader.graph_loader import GraphLoader
except ImportError:
    # 如果无法导入，设置默认配置
    DATA_PATHS = {
        'tbox_dir': './AliOpenKG_TBox',
        'processed_dir': './processed',
        'sample_dir': './samples'
    }
    GRAPH_CONFIG = {
        'batch_size': 5000,
    }

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TBoxProcessor:
    """本体层面(TBox)的JSONLD文件处理类"""
    
    def __init__(self, input_file: str, output_dir: str = None):
        """
        初始化TBox处理器
        
        Args:
            input_file: JSONLD文件路径
            output_dir: 输出目录
        """
        self.input_file = input_file
        
        if output_dir is None:
            output_dir = DATA_PATHS.get('sample_dir', './samples')
        
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
        
        # 实体计数
        self.entity_counts = {
            'class': 0,
            'concept': 0,
            'property': 0,
            'subclass_relation': 0,
            'broader_relation': 0,
            'subproperty_relation': 0,
            'other_relation': 0
        }
        
        # 实体映射：URI -> {id, label, ...}
        self.entity_mapping = {
            'classes': {},    # 存储owl:Class，如Brand、Category等
            'concepts': {},   # 存储skos:Concept，如Crowd、Market_segment等
            'properties': {}  # 存储owl:Property，如配件、配件名称等
        }
        
        # 关系列表：[{from_id, to_id, type, props}, ...]
        self.relationships = []
        
        # 已处理的URI集合
        self.processed_uris = set()
        
    def process_jsonld_file(self, chunk_size: int = 1000, sample_size: int = 10000, 
                           random_sample: bool = False) -> Tuple[str, Dict[str, str]]:
        """
        处理JSONLD文件，提取本体结构
        
        Args:
            chunk_size: 每次读取的数据块大小
            sample_size: 采样数量
            random_sample: 是否随机采样
            
        Returns:
            处理后文件路径和Neo4j文件路径字典
        """
        logger.info(f"处理JSONLD文件: {self.input_file}")
        logger.info(f"块大小: {chunk_size}条记录, 采样数量: {sample_size}, 随机采样: {random_sample}")
        
        # 输出文件路径
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.output_dir, f"processed_tbox_{timestamp}.jsonld")
        
        # 读取大型JSONLD文件
        try:
            logger.info("读取JSONLD文件...")
            start_time = time.time()
            
            # 如果文件较小，直接一次性读取
            if os.path.getsize(self.input_file) < 500 * 1024 * 1024:  # < 500MB
                with open(self.input_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if '@graph' in data:
                    graph_data = data['@graph']
                    self._process_jsonld_graph(graph_data, sample_size, random_sample)
                else:
                    logger.warning("JSONLD文件未包含@graph字段")
            else:
                # 分块处理大文件
                logger.info("文件较大，采用分块处理...")
                # 使用简单的行扫描方式处理大型JSON文件
                self._process_large_jsonld_file(chunk_size, sample_size, random_sample)
            
            processing_time = time.time() - start_time
            logger.info(f"处理完成，耗时: {processing_time:.2f}秒")
            
            # 保存提取的图数据
            neo4j_files = self._save_to_csv()
            
            logger.info(f"实体统计: {self.entity_counts}")
            return output_file, neo4j_files
            
        except Exception as e:
            logger.error(f"处理JSONLD文件时出错: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None, {}
    
    def _process_large_jsonld_file(self, chunk_size: int, sample_size: int, random_sample: bool):
        """
        分块处理大型JSONLD文件
        
        Args:
            chunk_size: 分块大小
            sample_size: 采样数量
            random_sample: 是否随机采样
        """
        # 使用流式解析处理大型JSON文件
        # 在这里我们简化处理，采用按行读取的方式
        # 高级做法是使用ijson等流式解析库
        
        # 针对@graph数组内的项目一个一个处理
        in_graph = False
        current_item = ""
        item_count = 0
        sampled_count = 0
        
        with open(self.input_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                
                # 识别@graph数组的开始
                if '"@graph" : [' in line:
                    in_graph = True
                    continue
                
                # 识别@graph数组的结束
                if in_graph and line == ']':
                    in_graph = False
                    # 处理最后一个项目
                    if current_item:
                        try:
                            current_item = current_item.rstrip(',')
                            item_json = json.loads('{' + current_item + '}')
                            item_count += 1
                            
                            # 采样处理
                            if random_sample:
                                # 随机采样
                                if random.random() < sample_size / (item_count + sample_size):
                                    self._process_jsonld_item(item_json)
                                    sampled_count += 1
                            else:
                                # 顺序采样
                                if sampled_count < sample_size:
                                    self._process_jsonld_item(item_json)
                                    sampled_count += 1
                        except Exception as e:
                            logger.warning(f"解析JSON项目失败: {str(e)}")
                        current_item = ""
                    continue
                
                # 在@graph数组内处理每个项目
                if in_graph:
                    if line.startswith('  {'):
                        # 新项目开始
                        if current_item:
                            try:
                                current_item = current_item.rstrip(',')
                                item_json = json.loads('{' + current_item + '}')
                                item_count += 1
                                
                                # 采样处理
                                if random_sample:
                                    # 随机采样
                                    if random.random() < sample_size / (item_count + sample_size):
                                        self._process_jsonld_item(item_json)
                                        sampled_count += 1
                                else:
                                    # 顺序采样
                                    if sampled_count < sample_size:
                                        self._process_jsonld_item(item_json)
                                        sampled_count += 1
                                        
                                # 每处理chunk_size个项目输出一次进度
                                if item_count % chunk_size == 0:
                                    logger.info(f"已处理 {item_count} 个项目，采样 {sampled_count} 个")
                            except Exception as e:
                                logger.warning(f"解析JSON项目失败: {str(e)}")
                            current_item = ""
                        current_item = line[2:]  # 去掉前面的两个空格
                    else:
                        current_item += line
        
        logger.info(f"共处理 {item_count} 个项目，采样 {sampled_count} 个")
    
    def _process_jsonld_graph(self, graph_data: List[Dict], sample_size: int, random_sample: bool):
        """
        处理JSONLD中的graph数据
        
        Args:
            graph_data: JSONLD中的@graph数组数据
            sample_size: 采样数量
            random_sample: 是否随机采样
        """
        if not graph_data:
            logger.warning("graph_data为空")
            return
        
        logger.info(f"处理JSONLD图数据，包含 {len(graph_data)} 个实体")
        
        # 如果需要采样，对数据进行采样处理
        if random_sample:
            if len(graph_data) > sample_size:
                graph_data = random.sample(graph_data, sample_size)
                logger.info(f"随机采样 {sample_size} 个实体")
        elif sample_size > 0 and len(graph_data) > sample_size:
            graph_data = graph_data[:sample_size]
            logger.info(f"顺序采样 {sample_size} 个实体")
            
        # 处理每个实体
        for item in graph_data:
            self._process_jsonld_item(item)
    
    def _process_jsonld_item(self, item: Dict):
        """
        处理JSONLD中的单个项目
        
        Args:
            item: 单个JSONLD项目
        """
        # 检查是否是有效项目
        if not isinstance(item, dict):
            return
            
        # 提取URI
        uri = item.get('@id')
        if not uri:
            return
            
        # 如果已处理过该URI，跳过
        if uri in self.processed_uris:
            return
        self.processed_uris.add(uri)
        
        # 处理类型
        types = item.get('@type', [])
        if isinstance(types, str):
            types = [types]
            
        # 提取标签
        label = None
        for label_key in ['rdfs:label', 'http://www.w3.org/2000/01/rdf-schema#label', 'skos:prefLabel']:
            if label_key in item:
                label_value = item[label_key]
                if isinstance(label_value, dict) and '@value' in label_value:
                    label = label_value['@value']
                elif isinstance(label_value, str):
                    label = label_value
                break
                
        # 处理不同类型的实体
        if any(t in ['owl:Class', 'http://www.w3.org/2002/07/owl#Class'] for t in types):
            self._process_class(uri, item, label)
        elif any(t in ['skos:Concept', 'http://www.w3.org/2004/02/skos/core#Concept'] for t in types):
            self._process_concept(uri, item, label)
        elif any(t in ['rdf:Property', 'owl:ObjectProperty', 'owl:DatatypeProperty'] for t in types):
            self._process_property(uri, item, label)
        
        # 处理关系
        self._process_relationships(uri, item)
    
    def _process_class(self, uri: str, item: Dict, label: Optional[str] = None):
        """
        处理owl:Class类型实体
        
        Args:
            uri: 实体URI
            item: JSONLD项目
            label: 实体标签
        """
        # 如果已有该类，返回ID
        if uri in self.entity_mapping['classes']:
            return self.entity_mapping['classes'][uri]['id']
            
        # 生成唯一ID
        class_id = f"class_{self.entity_counts['class']}"
        self.entity_counts['class'] += 1
        
        # 提取描述信息
        description = None
        for desc_key in ['rdfs:comment', 'http://www.w3.org/2000/01/rdf-schema#comment']:
            if desc_key in item:
                desc_value = item[desc_key]
                if isinstance(desc_value, dict) and '@value' in desc_value:
                    description = desc_value['@value']
                elif isinstance(desc_value, str):
                    description = desc_value
                break
                
        # 基于URI提取类名
        class_name = label
        if not class_name:
            class_name = uri.split('/')[-1]
            if '#' in class_name:
                class_name = class_name.split('#')[-1]
                
        # 存储类信息
        self.entity_mapping['classes'][uri] = {
            'id': class_id,
            'uri': uri,
            'name': class_name,
            'label': label,
            'description': description
        }
        
        return class_id

    def _save_to_csv(self) -> Dict[str, str]:
        """
        将提取的图数据保存为CSV文件，用于Neo4j导入
        
        Returns:
            CSV文件路径字典
        """
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        neo4j_files = {}
        
        # 保存类
        if self.entity_mapping['classes']:
            classes_file = os.path.join(self.output_dir, f"tbox_classes.csv")
            with open(classes_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['id:ID', 'uri', 'name', 'label', 'description', ':LABEL'])
                for class_info in self.entity_mapping['classes'].values():
                    writer.writerow([
                        class_info['id'],
                        class_info['uri'],
                        class_info['name'],
                        class_info['label'] or '',
                        class_info['description'] or '',
                        'Class'
                    ])
            neo4j_files['classes'] = classes_file
            logger.info(f"已保存 {len(self.entity_mapping['classes'])} 个类到 {classes_file}")
            
        # 保存概念
        if self.entity_mapping['concepts']:
            concepts_file = os.path.join(self.output_dir, f"tbox_brands.csv")
            with open(concepts_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['id:ID', 'uri', 'name', 'label', 'description', ':LABEL'])
                for concept_info in self.entity_mapping['concepts'].values():
                    writer.writerow([
                        concept_info['id'],
                        concept_info['uri'],
                        concept_info['name'],
                        concept_info['label'] or '',
                        concept_info['description'] or '',
                        'Concept'
                    ])
            neo4j_files['concepts'] = concepts_file
            logger.info(f"已保存 {len(self.entity_mapping['concepts'])} 个概念到 {concepts_file}")
            
        # 保存属性
        if self.entity_mapping['properties']:
            properties_file = os.path.join(self.output_dir, f"tbox_properties.csv")
            with open(properties_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['id:ID', 'uri', 'name', 'label', 'description', 'property_type', ':LABEL'])
                for prop_info in self.entity_mapping['properties'].values():
                    writer.writerow([
                        prop_info['id'],
                        prop_info['uri'],
                        prop_info['name'],
                        prop_info['label'] or '',
                        prop_info['description'] or '',
                        prop_info.get('property_type', ''),
                        'Property'
                    ])
            neo4j_files['properties'] = properties_file
            logger.info(f"已保存 {len(self.entity_mapping['properties'])} 个属性到 {properties_file}")
            
        # 保存关系
        if self.relationships:
            relationships_file = os.path.join(self.output_dir, f"tbox_relationships.csv")
            with open(relationships_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([':START_ID', ':END_ID', ':TYPE', 'uri'])
                for rel in self.relationships:
                    writer.writerow([
                        rel['from_id'],
                        rel['to_id'],
                        rel['type'],
                        rel.get('uri', '')
                    ])
            neo4j_files['relationships'] = relationships_file
            logger.info(f"已保存 {len(self.relationships)} 个关系到 {relationships_file}")
            
        return neo4j_files
        
def main():
    # 参数解析
    parser = argparse.ArgumentParser(description="处理本体层面(TBox)的JSONLD文件，提取本体结构信息")
    parser.add_argument('file_path', help="JSONLD文件路径")
    parser.add_argument('--output-dir', help="输出目录")
    parser.add_argument('--sample', type=int, default=10000, help="采样数量，默认10000")
    parser.add_argument('--chunk-size', type=int, default=1000, help="分块大小，默认1000")
    parser.add_argument('--random-sample', action='store_true', help="是否随机采样")
    parser.add_argument('--load', action='store_true', help="处理后加载到Neo4j")
    args = parser.parse_args()
    
    # 初始化处理器
    processor = TBoxProcessor(args.file_path, args.output_dir)
    
    # 处理文件
    output_file, neo4j_files = processor.process_jsonld_file(
        chunk_size=args.chunk_size,
        sample_size=args.sample,
        random_sample=args.random_sample
    )
    
    # 加载到Neo4j
    if args.load and neo4j_files:
        try:
            from ecommerce_kg_system.graph_loader.graph_loader import GraphLoader
            
            logger.info("加载数据到Neo4j...")
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
    
if __name__ == "__main__":
    main()