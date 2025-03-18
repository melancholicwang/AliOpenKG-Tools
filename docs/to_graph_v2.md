# Neo4j图谱处理案例文档

本文档记录了使用Python脚本处理大型JSON-LD文件并将数据导入到Neo4j图数据库的完整流程。以下步骤已经过测试并成功运行。

## 数据准备与处理流程

### 1. 清理和准备Neo4j数据库

首先清空Neo4j数据库，确保没有历史数据残留：

```bash
python check_and_clear_neo4j.py --clear --truncate
```

### 2. 本体层(TBox)数据处理

处理本体层(TBox)的JSON-LD文件，提取本体结构并导入Neo4j：

```bash
python process_tbox_jsonld.py AliOpenKG_TBox/AliOpenKG_TBox_All_OriginStr.jsonld --sample 5000 --output-dir ./samples --load
```

### 3. 处理大型RAR归档文件 - 产品数据

从大型RAR归档文件中提取产品数据，并进行分块处理：

```bash
# 产品原始属性数据处理
python process_large_rar.py AliOpenKG_ABox_Part1/AliOpenKG_ABox_Product_OriginStr_Attributes.nt --output wConcept_part1_output.nt --sample 1000 --chunk-size 500 --convert --load

# 产品分类数据处理
python process_large_rar.py AliOpenKG_ABox_Part1/AliOpenKG_ABox_Product_OriginStr_wClass.nt --output output.nt --sample 10000 --chunk-size 500 --convert --load
```

### 4. 处理市场细分数据

从样本提取中处理市场细分相关数据：

```bash
# 市场细分数据处理 - 第1部分
python process_large_rar.py sample_extract_1/AliOpenKG_ABox_Product_OriginStr_wConcept_marketOnly_part1.ttl --output marketOnly_part1_output.nt --sample 3000 --chunk-size 500 --convert --load

# 市场细分数据处理 - 第2部分
python process_large_rar.py sample_extract_1/AliOpenKG_ABox_Product_OriginStr_wConcept_marketOnly_part2.ttl --output marketOnly_part1_output.nt --sample 3000 --chunk-size 500 --convert --load

# 市场细分数据处理 - 第5部分
python process_large_rar.py sample_extract_1/AliOpenKG_ABox_Product_OriginStr_wConcept_marketOnly_part5.ttl --output marketOnly_part1_output.nt --sample 3000 --chunk-size 500 --convert --load

# 市场细分数据处理 - 第20部分
python process_large_rar.py sample_extract_1/AliOpenKG_ABox_Product_OriginStr_wConcept_marketOnly_part20.ttl --output marketOnly_part1_output.nt --sample 3000 --chunk-size 500 --convert --load
```

### 5. 处理概念数据

处理与概念(wConcept)相关的数据文件：

```bash
# 概念数据处理 - 第1部分
python process_large_rar.py sample_extract_1/AliOpenKG_ABox_Product_OriginStr_wConcept_part1.nt --output wConcept_part1_output.nt --sample 1000 --chunk-size 500 --convert --load

# 概念数据处理 - 第9部分
python process_large_rar.py sample_extract_1/AliOpenKG_ABox_Product_OriginStr_wConcept_part9.nt --output wConcept_part1_output.nt --sample 1000 --chunk-size 500 --convert --load
```

## 数据处理说明

1. **抽样处理**：所有命令都使用了`--sample`参数，表示只处理指定数量的数据记录。这适用于测试和开发环境。

2. **分块处理**：通过`--chunk-size`参数，数据被分成较小的块进行处理，减轻内存压力。

3. **格式转换**：`--convert`参数指示脚本将输入数据转换为Neo4j可导入的格式。

4. **直接加载**：`--load`参数使脚本在处理完数据后直接将其加载到Neo4j数据库中。

## 注意事项

- 这些命令仅处理了原始数据的一个样本子集
- 在处理大型数据集时，可能需要调整`--sample`和`--chunk-size`参数
- 确保Neo4j数据库已启动并配置正确的连接参数
- 对于生产环境，建议先处理数据生成CSV文件，然后分批导入Neo4j

## 总结

上述命令展示了一个从多个来源提取、转换和加载(ETL)数据到Neo4j图数据库的完整工作流。通过分块和抽样处理，即使是大型数据集也能高效处理。