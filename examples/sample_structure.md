# 阿里开放知识图谱数据结构说明

本文档描述了阿里开放知识图谱（AliOpenKG）数据集的结构，以帮助研究人员理解和使用本仓库中的处理工具。

## 数据集概述

AliOpenKG是由阿里巴巴开放的大型电商领域知识图谱，包含商品、用户、品牌、类目等多种实体类型及其关系。数据集分为两部分：

1. **本体层(TBox)**: 描述概念类型及其关系的结构化信息
2. **实例层(ABox)**: 包含具体实体实例的数据，分为8个部分

## 文件结构

### 本体层(TBox)

TBox数据以JSONLD格式存储，包含以下主要实体类型：

- `owl:Class`: 类，如Brand、Category等
- `skos:Concept`: 概念，如Crowd、Market_segment等
- `owl:Property`: 属性，如配件、配件名称等

### 实例层(ABox)

ABox数据以NT格式存储，每个文件包含三元组(subject, predicate, object)。主要实体类型包括：

- 用户(User)
- 商品(Product)
- 类目(Category)
- 品牌(Brand)
- 场景(Scene)
- 人群(Crowd)
- 时间(Time)
- 主题(Theme)
- 细分市场(Market)
- 产地(PlaceOfOrigin)

## 样本数据结构

以下是一些示例数据结构（不包含实际数据，仅展示格式）：

### 类目层级结构示例

```
<http://www.alibabaontology.com/ontology/class/类目ID1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#Class> .
<http://www.alibabaontology.com/ontology/class/类目ID2> <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://www.alibabaontology.com/ontology/class/类目ID1> .
<http://www.alibabaontology.com/ontology/class/类目ID1> <http://www.w3.org/2000/01/rdf-schema#label> "家电" .
```

### 商品与属性关系示例

```
<http://www.alibabaontology.com/ontology/instance/商品ID> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.alibabaontology.com/ontology/class/Product> .
<http://www.alibabaontology.com/ontology/instance/商品ID> <http://www.alibabaontology.com/ontology/property/hasCategory> <http://www.alibabaontology.com/ontology/instance/类目ID> .
<http://www.alibabaontology.com/ontology/instance/商品ID> <http://www.alibabaontology.com/ontology/property/hasBrand> <http://www.alibabaontology.com/ontology/instance/品牌ID> .
<http://www.alibabaontology.com/ontology/instance/商品ID> <http://www.alibabaontology.com/ontology/property/hasScene> <http://www.alibabaontology.com/ontology/instance/场景ID> .
```

## 数据处理流程

工作流程通常涉及：
1. 提取压缩文件
2. 解析RDF数据
3. 过滤和采样
4. 转换为Neo4j格式
5. 加载到图数据库