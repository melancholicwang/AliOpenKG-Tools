# 阿里开放知识图谱数据结构说明

本文档描述了阿里开放知识图谱（AliOpenKG）数据集的结构，以帮助研究人员理解和使用本仓库中的处理工具。

## 数据集概述

AliOpenKG是由阿里巴巴开放的大型电商领域知识图谱，包含商品、用户、品牌、类目等多种实体类型及其关系。数据集分为两部分：

1. **本体层(TBox)**: 描述概念类型及其关系的结构化信息
2. **实例层(ABox)**: 包含具体实体实例的数据，分为8个部分（Part1-Part8）

## 目录结构

```
AliOpenKG/
├── AliOpenKG_TBox/
│   └── AliOpenKG_TBox_All_OriginStr.jsonld  # 本体层数据，JSONLD格式
├── AliOpenKG_ABox_Part1/                    # 实例数据第1部分
│   ├── AliOpenKG_ABox_Product_OriginStr_Attributes.nt
│   └── AliOpenKG_ABox_Product_OriginStr_wClass.nt
├── AliOpenKG_ABox_Part2/                    # 实例数据第2部分
├── ...
└── AliOpenKG_ABox_Part8/                    # 实例数据第8部分
```

## 文件结构

### 本体层(TBox)

TBox数据以JSONLD格式存储，包含以下主要实体类型：

- `owl:Class`: 类，如Brand、Category等
- `skos:Concept`: 概念，如Crowd、Market_segment等
- `owl:Property`: 属性，如配件、配件名称等

### 实例层(ABox)

ABox数据以NT/TTL格式存储，每个文件包含三元组(subject, predicate, object)。主要实体类型包括：

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

## 数据示例

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

### 实体类型和关系概览

**主要实体类型**:
- `Product`: 商品实体
- `Category`: 类目实体
- `Brand`: 品牌实体
- `Scene`: 场景实体
- `Crowd`: 人群实体
- `Time`: 时间实体（适用时间）
- `Theme`: 主题实体
- `Market_segment`: 细分市场实体
- `PlaceOfOrigin`: 产地实体

**主要关系类型**:
- `hasCategory`: 商品属于某类目
- `hasBrand`: 商品属于某品牌
- `suitable_for_scene`: 商品适用于某场景
- `suitable_for_crowd`: 商品适用于某人群
- `suitable_for_time`: 商品适用于某时间
- `hasTheme`: 商品关联某主题
- `inMarket`: 商品适用于某细分市场
- `from`: 商品来自某产地

## 数据处理流程

使用本仓库中的工具处理AliOpenKG数据的典型工作流程：

1. **数据准备**:
   - 下载AliOpenKG数据集（TBox和ABox部分）
   - 解压缩RAR文件到适当目录

2. **本体处理**:
   - 使用`process_tbox_jsonld.py`处理TBox数据
   - 提取类、概念和属性定义

3. **实例处理**:
   - 使用`process_large_rar.py`处理ABox数据
   - 对大型文件进行分块和采样处理

4. **数据转换**:
   - 将RDF三元组转换为CSV格式，便于导入图数据库
   - 分别保存节点和关系数据

5. **图数据库导入**:
   - 将处理后的CSV文件导入Neo4j等图数据库
   - 执行查询和分析
   
## 典型使用场景

1. **电商商品推荐**:
   - 基于产品间的类目、品牌和场景关系进行相似性推荐
   - 利用用户-商品交互建立个性化推荐

2. **知识发现**:
   - 分析商品与各概念（场景、人群、时间）之间的关系
   - 挖掘商品属性与市场细分的关联

3. **可视化与分析**:
   - 构建商品类目体系的层次结构可视化
   - 展示不同品牌、场景之间的关联关系