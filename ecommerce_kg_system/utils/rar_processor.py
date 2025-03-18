#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
RAR文件处理工具，用于提取和处理大型RAR归档文件
"""

import os
import logging
import shutil
from typing import List, Optional, Tuple
import time

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RarProcessor:
    """RAR文件处理类"""
    
    @staticmethod
    def extract_rar(rar_path: str, output_dir: Optional[str] = None, specific_files: Optional[List[str]] = None) -> str:
        """
        提取RAR文件
        
        Args:
            rar_path: RAR文件路径
            output_dir: 输出目录，默认为RAR文件同名目录
            specific_files: 要提取的特定文件列表，如果为None则提取所有文件
            
        Returns:
            输出目录路径
        """
        if not os.path.exists(rar_path):
            raise FileNotFoundError(f"RAR文件不存在: {rar_path}")
            
        # 设置默认输出目录
        if output_dir is None:
            output_dir = os.path.splitext(rar_path)[0]
            
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            # 尝试导入rarfile模块
            import rarfile
            
            logger.info(f"开始提取RAR文件: {rar_path} 到 {output_dir}")
            start_time = time.time()
            
            with rarfile.RarFile(rar_path) as rf:
                # 获取文件列表
                all_files = rf.namelist()
                
                # 确定要提取的文件
                files_to_extract = specific_files if specific_files else all_files
                
                # 过滤出存在的文件
                files_to_extract = [f for f in files_to_extract if f in all_files]
                
                if not files_to_extract:
                    logger.warning(f"未找到要提取的文件")
                    return output_dir
                    
                logger.info(f"将提取 {len(files_to_extract)} 个文件")
                
                # 提取文件
                for i, file in enumerate(files_to_extract):
                    try:
                        rf.extract(file, output_dir)
                        if (i + 1) % 100 == 0 or i == len(files_to_extract) - 1:
                            elapsed = time.time() - start_time
                            logger.info(f"已提取 {i+1}/{len(files_to_extract)} 个文件，耗时: {elapsed:.2f}秒")
                    except Exception as e:
                        logger.error(f"提取文件失败: {file}, 错误: {str(e)}")
                        
            total_time = time.time() - start_time
            logger.info(f"RAR提取完成，总耗时: {total_time:.2f}秒")
            
            return output_dir
            
        except ImportError:
            logger.error("缺少rarfile模块，请使用 pip install rarfile 安装")
            raise
        except Exception as e:
            logger.error(f"提取RAR文件失败: {str(e)}")
            raise
    
    @staticmethod
    def find_files_in_rar(rar_path: str, pattern: Optional[str] = None) -> List[str]:
        """
        在RAR文件中查找符合模式的文件
        
        Args:
            rar_path: RAR文件路径
            pattern: 文件名模式，支持通配符，如果为None则返回所有文件
            
        Returns:
            文件路径列表
        """
        if not os.path.exists(rar_path):
            raise FileNotFoundError(f"RAR文件不存在: {rar_path}")
            
        try:
            # 尝试导入rarfile和fnmatch模块
            import rarfile
            import fnmatch
            
            logger.info(f"扫描RAR文件: {rar_path}")
            
            with rarfile.RarFile(rar_path) as rf:
                # 获取文件列表
                all_files = rf.namelist()
                
                # 如果提供了模式，过滤文件
                if pattern:
                    matching_files = [f for f in all_files if fnmatch.fnmatch(f, pattern)]
                    logger.info(f"找到 {len(matching_files)}/{len(all_files)} 个匹配 '{pattern}' 的文件")
                    return matching_files
                else:
                    logger.info(f"找到 {len(all_files)} 个文件")
                    return all_files
                    
        except ImportError:
            logger.error("缺少rarfile模块，请使用 pip install rarfile 安装")
            raise
        except Exception as e:
            logger.error(f"扫描RAR文件失败: {str(e)}")
            raise
    
    @staticmethod
    def extract_large_files(rar_path: str, output_dir: Optional[str] = None, min_size_mb: float = 100.0) -> List[str]:
        """
        提取RAR中的大文件
        
        Args:
            rar_path: RAR文件路径
            output_dir: 输出目录，默认为RAR文件同名目录
            min_size_mb: 最小文件大小，单位为MB
            
        Returns:
            提取的文件路径列表
        """
        if not os.path.exists(rar_path):
            raise FileNotFoundError(f"RAR文件不存在: {rar_path}")
            
        # 设置默认输出目录
        if output_dir is None:
            output_dir = os.path.splitext(rar_path)[0]
            
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        try:
            # 尝试导入rarfile模块
            import rarfile
            
            logger.info(f"扫描大文件 (>= {min_size_mb}MB): {rar_path}")
            
            extracted_files = []
            min_size_bytes = min_size_mb * 1024 * 1024
            
            with rarfile.RarFile(rar_path) as rf:
                # 过滤大文件
                large_files = [f for f in rf.infolist() if f.file_size >= min_size_bytes]
                
                if not large_files:
                    logger.info(f"未找到大于 {min_size_mb}MB 的文件")
                    return []
                    
                logger.info(f"找到 {len(large_files)} 个大文件")
                
                # 提取大文件
                for i, file_info in enumerate(large_files):
                    try:
                        file_path = file_info.filename
                        output_path = os.path.join(output_dir, os.path.basename(file_path))
                        
                        # 提取文件
                        rf.extract(file_path, output_dir)
                        extracted_files.append(output_path)
                        
                        size_mb = file_info.file_size / (1024 * 1024)
                        logger.info(f"已提取: {file_path} ({size_mb:.2f}MB)")
                    except Exception as e:
                        logger.error(f"提取文件失败: {file_info.filename}, 错误: {str(e)}")
                        
            return extracted_files
            
        except ImportError:
            logger.error("缺少rarfile模块，请使用 pip install rarfile 安装")
            raise
        except Exception as e:
            logger.error(f"提取大文件失败: {str(e)}")
            raise
    
    @staticmethod
    def create_rar(input_files: List[str], rar_path: str, compress_level: int = 3) -> str:
        """
        创建RAR文件（仅在安装了UnRAR工具的情况下支持）
        
        Args:
            input_files: 要添加到RAR的文件或目录列表
            rar_path: 输出RAR文件路径
            compress_level: 压缩级别，0-5，其中0表示存储，5表示最大压缩
            
        Returns:
            创建的RAR文件路径
        """
        # 检查输入文件是否存在
        for file_path in input_files:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"文件不存在: {file_path}")
                
        # 确保输出目录存在
        output_dir = os.path.dirname(rar_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            
        try:
            # 尝试导入rarfile模块
            import rarfile
            
            logger.info(f"创建RAR文件: {rar_path}")
            
            # 检查是否支持创建RAR文件
            if not rarfile.UNRAR_TOOL or not shutil.which(rarfile.UNRAR_TOOL):
                raise RuntimeError(f"未找到UnRAR工具，无法创建RAR文件。请安装UnRAR工具并设置rarfile.UNRAR_TOOL")
                
            # 创建RAR文件
            with rarfile.RarFile(rar_path, mode='w', compression=compress_level) as rf:
                for file_path in input_files:
                    if os.path.isdir(file_path):
                        for root, dirs, files in os.walk(file_path):
                            for file in files:
                                full_path = os.path.join(root, file)
                                arc_name = os.path.relpath(full_path, os.path.dirname(file_path))
                                rf.write(full_path, arcname=arc_name)
                    else:
                        rf.write(file_path, arcname=os.path.basename(file_path))
                        
            logger.info(f"RAR文件创建完成: {rar_path}")
            return rar_path
            
        except ImportError:
            logger.error("缺少rarfile模块，请使用 pip install rarfile 安装")
            raise
        except Exception as e:
            logger.error(f"创建RAR文件失败: {str(e)}")
            raise