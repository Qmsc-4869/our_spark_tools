import pyspark.sql
import json
import os
import zipfile
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType
from minio import Minio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from py4j.protocol import Py4JJavaError
from pyspark.sql import Window

def add_id(df, start_id):
    df = df.withColumn("_partition", F.lit(1))
    window_spec = Window.partitionBy("_partition").orderBy(F.lit(1))
    df_with_id = df.withColumn("asset_id", F.row_number().over(window_spec) + start_id) \
                   .drop("_partition")
    return df_with_id

def distinct_remain(spark: SparkSession, 
                    col_name, tabel_name) -> pyspark.sql.dataframe.DataFrame:
    df = spark.sql(f"""
        SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY {col_name} ORDER BY (SELECT NULL)) AS rn
                FROM {tabel_name}
            ) t
        WHERE rn = 1
        """) 
    return df.drop('rn') 

class TreeNode:
        def __init__(self, val, name, col, tmp_view_name=None, data_schema=None, left=None, right=None):
            self.val = val
            self.name = name
            self.col = col
            self.tmp_view_name = tmp_view_name
            self.data_schema = data_schema
            self.left = left
            self.right = right
        def __repr__(self):
            return f"TreeNode(val={self.val}, name={self.name}, col={self.col}, tmp_view_name={self.tmp_view_name}, data_schema={self.data_schema})"

class save_tree:
    def __init__(self, 
                 nodes_info= [
                     {"val": 1, "name": "annotations", "col": "annotation_id", "tmp_view_name": None, "data_schema": None},
                     {"val": 2, "name": "categories", "col": "category_id", "tmp_view_name": None, "data_schema": None},
                     {"val": 3, "name": "assets", "col": "asset_id", "tmp_view_name": None, "data_schema": None},
                     None, None,
                     {"val": 4, "name": "sensors", "col": "sensor_id", "tmp_view_name": None, "data_schema": None},
                     {"val": 5, "name": "dataset_info", "col": "data_name", "tmp_view_name": None, "data_schema": None},
                     None, None,
                     {"val": 6, "name": "licenses", "col": "license_id", "tmp_view_name": None, "data_schema": None}]):
        self.root = self._build_tree_from_info(nodes_info)
    
    def create_node(self, node_data: dict):
        if node_data is None:
            return None
        return TreeNode(
             val = node_data.get("val"),
             name = node_data.get('name'), 
             col = node_data.get('col'),
             tmp_view_name = node_data.get('tmp_view_name'),
             data_schema = node_data.get('data_schema')
        )
    
    def _build_tree_from_info(self, info_list):
        root = self.create_node(info_list[0])
        queue = [root]
        index = 1

        while queue and index < len(info_list):
            node = queue.pop(0)
            if node:
                # 左子节点
                left_node = self.create_node(info_list[index]) if index < len(info_list) else None
                node.left = left_node
                if left_node:
                    queue.append(left_node)
                index += 1

                # 右子节点
                right_node = self.create_node(info_list[index]) if index < len(info_list) else None
                node.right = right_node
                if right_node:
                    queue.append(right_node)
                index += 1
        return root
    
    def pretty_print(self):
        """以树状结构打印"""
        self._print_tree(self.root, "", False)

    def _print_tree(self, node, prefix="", is_left=False):
        if node is not None:
            print(prefix + ("├── " if is_left else "└── ") + f"{node.val} ({node.name}, {node.col})")
            children = [child for child in (node.left, node.right) if child]
            for i, child in enumerate(children):
                is_last = (i == len(children) - 1)
                self._print_tree(child, prefix + ("│   " if is_left else "    "), not is_last)

# 输入df结构获得所有字段的路径（包括结构体）
def get_all_column_paths(schema: StructType, prefix=""):
    paths = []
    for field in schema.fields:
        field_name = f"{prefix}{field.name}" if prefix == "" else f"{prefix}.{field.name}"
        if isinstance(field.dataType, StructType):
            paths.extend(get_all_column_paths(field.dataType, field_name))
        else:
            paths.append(field_name)
    return paths

# 去除df中所有空字段（包括结构体）
def drop_all_null_columns(df: DataFrame) -> DataFrame:
    """
    删除 DataFrame 中所有列（包括结构体内部字段）全为 NULL 的列。
    """
    # 获取所有列路径
    all_paths = get_all_column_paths(df.schema)

    # 使用一次 agg 统计所有列的非空数量
    agg_exprs = [
        F.sum(F.col(path).isNotNull().cast("int")).alias(path.replace(".", "__"))
        for path in all_paths
    ]
    counts_row = df.agg(*agg_exprs).collect()[0].asDict()

    # 将统计结果映射回列路径
    non_null_paths = [p for p in all_paths if counts_row[p.replace(".", "__")] > 0]

    # 递归重建 DataFrame 列
    def build_struct(schema: StructType, prefix=""):
        new_fields = []
        for field in schema.fields:
            field_name = f"{prefix}{field.name}" if prefix == "" else f"{prefix}.{field.name}"
            if isinstance(field.dataType, StructType):
                # 递归保留 struct 内非空的子字段
                sub_struct = build_struct(field.dataType, field_name)
                if sub_struct:
                    new_fields.append(F.struct(*sub_struct).alias(field.name))
            else:
                if field_name in non_null_paths:
                    new_fields.append(F.col(field_name).alias(field.name))
        return new_fields

    new_cols = []
    for field in df.schema.fields:
        if isinstance(field.dataType, StructType):
            struct_cols = build_struct(field.dataType, field.name)
            if struct_cols:
                new_cols.append(F.struct(*struct_cols).alias(field.name))
        else:
            if field.name in non_null_paths:
                new_cols.append(F.col(field.name))
    
    return df.select(*new_cols)

# 保存数据
def save_data(spark, df, save_path, download_data=True, zip_path=None):

    bt = save_tree()

    def extract_table(df, col_name, table_name):
        keys = df.select(F.col(col_name)).distinct()
        if isinstance(keys.schema[keys.columns[0]].dataType, ArrayType):
            keys = keys.select(F.explode(col_name).alias(col_name)).distinct()
        df1 = spark.table(f"{db_name}.{table_name}")
        df1 = df1.join(keys, col_name, "left_semi")
        return drop_all_null_columns(df1)

    def _generate_data(node: TreeNode, parent_df):
        df = extract_table(parent_df, node.col, node.name)
        node.data_schema = df.schema
        tmp_view_name = node.name
        df.createOrReplaceTempView(tmp_view_name)
        node.tmp_view_name = tmp_view_name
        return df 
        
    def check_into_node(node: TreeNode, branch: str):
        if getattr(node, branch) is None:
            return False
        data_schema = node.data_schema
        col_name = getattr(node, branch).col
        return col_name in [df.name for df in data_schema.fields]

    def save_to_json(node=bt.root, parent_df=df):
        df = _generate_data(node, parent_df)
        final_json[node.name] = [row.asDict(recursive=True) for row in df.collect()]
        print(f"已收集 {node.name}")

        if check_into_node(node, 'left'):
            save_to_json(node.left, df)
        if check_into_node(node, 'right'):
            save_to_json(node.right, df)

    db_name = "nessie.db_lowaltitude_vision"
    final_json = {}

    save_to_json() 
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(final_json, f, ensure_ascii=False, indent=2, default=str)
        
    print(f"全部完成，写入 {save_path}，总计 {len(final_json)} 张表")

    if download_data:
        print('正在下载并压缩数据')
        if zip_path is None:
            zip_path = os.path.join(os.path.dirname(save_path), '.'.join([os.path.basename(save_path).split('.')[0], 'zip']))
        df = spark.table("assets")
        paths = [row['uri'] for row in df.select('uri').collect()]
        file_name = ['.'.join(row['filename'].split('.')[:-1]) for row in df.select('filename').collect()]
        download_and_zip(paths, zip_path, file_neme=file_name)

# 初始化spark会话
def get_spark(link_name='spark') -> SparkSession:
    spark =  (
        SparkSession.builder
        .remote("sc://127.0.0.1:15002")
        .appName(link_name)
        .getOrCreate())
    print('启动一个带 Iceberg + Nessie 扩展的 Spark 会话: 成功')
    return spark

# 输入使用的数据库名字
def get_db(db_name: str = "nessie.db_lowaltitude_vision"):
    print(f'use db: {db_name}')
    return db_name

# 使用branch分支             
def get_branch(spark: SparkSession, branch: str):
    spark.sql(f"USE REFERENCE {branch} IN nessie")
    print("当前分支:", spark.sql("SHOW REFERENCE IN nessie").collect()[0][1])
    return branch
    
# MinIO下载文件断点续传
def resilient_download(client, bucket, object_name, local_file, retry=10, chunk_size=1024*1024):
    os.makedirs(os.path.dirname(local_file), exist_ok=True)
    offset = os.path.getsize(local_file) if os.path.exists(local_file) else 0

    for attempt in range(1, retry + 1):
        try:
            response = client.get_object(bucket, object_name, offset=offset)
            with open(local_file, "ab") as f:
                for chunk in response.stream(chunk_size):
                    f.write(chunk)
            response.close()
            response.release_conn()
            print(f"下载完成: {local_file}")
            return
        except Exception as e:
            print(f"下载失败 (第 {attempt} 次): {e}")
            if attempt == retry:
                raise
            else:
                offset = os.path.getsize(local_file) if os.path.exists(local_file) else 0

def download_file(client, bucket, object_name, local_file):
    resilient_download(client, bucket, object_name, local_file)
    return local_file

# 数据下载及压缩(并行下载)
def download_and_zip(paths, zip_path, file_neme, 
                     minio_host="172.16.49.9:9000",
                     access_key="admin",
                     secret_key="Transformer666.",
                     secure=False,
                     max_workers=5):
    
    if not paths:
        print("没有找到文件路径！")
        return
    
    client = Minio(minio_host, access_key=access_key, secret_key=secret_key, secure=secure)

    download_dir = os.path.join(os.path.dirname(zip_path), 'temp_files')
    if os.path.exists(download_dir):
        shutil.rmtree(download_dir)
    os.makedirs(download_dir)

    # 解析下载任务
    tasks = []
    local_files = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for path in paths:
            parts = path.replace("s3a://", "").split("/", 1)
            bucket = parts[0]
            object_name = parts[1]
            local_file = os.path.join(download_dir, os.path.basename(object_name))
            local_files.append(local_file)
            tasks.append(executor.submit(download_file, client, bucket, object_name, local_file))
        
        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                print(f"下载失败：{e}")

    # 统一打包
    print('开始压缩')
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for local_file, name in zip(local_files, file_neme):
            name = '.'.join([name, os.path.basename(local_file).split('.')[-1]])
            zf.write(local_file, arcname=name)
            print(f"已添加到 ZIP: {name}")

    print(f"压缩完成：{os.path.abspath(zip_path)}")
    shutil.rmtree(download_dir, ignore_errors=True)

# 查看文件大小（MB）
def get_file_size_in_mb(file_path):
    size_in_bytes = os.path.getsize(file_path)
    size_in_mb = size_in_bytes / (1024 ** 2)  # 转换为 MB
    print(f"{size_in_mb:.3f}MB")

# 获取 Nessie 分支的所有表(返回列表)
def list_tables_in_branch(spark: SparkSession, branch: str, catalog='nessie'):
    get_branch(spark, branch)
    namespaces_df = spark.sql(f"SHOW NAMESPACES IN {catalog}")
    namespaces_list = [row.namespace for row in namespaces_df.collect()]
    tables = []
    for db in namespaces_list:
        tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{db}")
        tables_list = [row.tableName for row in tables_df.collect()]
        for tb in tables_list:
            tables.append(f'{catalog}.{db}.{tb}')
    return tables

# 清除分支中的快照信息
def clear_snapshots(spark, tables, retain_day=7, retain_last=5, catalog='nessie'):
    time = datetime.now() - timedelta(days=retain_day)
    time = time.replace(hour=4, minute=0, second=0, microsecond=0)
    time = time.strftime(r"%Y-%m-%d %H:%M:%S")
    print(f'清理{time}之前的快照, 最少保留{retain_last}条快照信息')

    for table in tables:
        print(f"清理表: {table}")
        spark.sql(f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{time}',
            retain_last => {retain_last}
            )""").show(truncate=False)

    print('全部清理完成')

# 列出文件夹内的所有文件（递归）
def get_MinIOfile_path(spark, client, bucket, prefix) -> DataFrame:
    objects = client.list_objects(bucket, prefix=prefix, recursive=True)
    files = [obj.object_name for obj in objects]
    df = spark.createDataFrame([(f's3a://{bucket}/{f}', os.path.basename(f)) for f in files], ["file_path", "file_name"])
    return df

# tb1, tb2两张表（sqlTABLE），根据tb2的col2匹配tb1的col1，把tb1中的src_col替换为tb2中的tgt_col
def replace_col(spark, tb1, tb2, col1, col2, src_col, tgt_col):
    spark.sql(f"""
        MERGE INTO {tb1} tb1
        USING {tb2} tb2
        ON tb1.{col1} = tb2.{col2}
        WHEN MATCHED THEN UPDATE SET tb1.{src_col} = tb2.{tgt_col};
    """)

# 获取nessie所有分支并返回一个列表
def get_all_branch(spark, catalog='nessie'):
    branches = spark.sql(f"LIST REFERENCES IN {catalog}")
    name_list = [row.name for row in branches.select("name").collect()]
    return name_list

# 合并当前分支到其他所有分支
def merge_current_to_all(spark, current_branch, catalog='nessie'): 
    all_branch = get_all_branch(spark)
    other_branch_list = [branch for branch in all_branch if branch != current_branch]

    for br in other_branch_list:
        try:
            spark.sql(f"MERGE DRY BRANCH {current_branch} INTO {br} IN {catalog}")
            spark.sql(f"MERGE BRANCH {current_branch} INTO {br} IN {catalog}")
            print(f"{current_branch} → {br} 合并成功")

        except Py4JJavaError as e:
            if "ReferenceConflictException" in str(e.java_exception) \
                or "NessieConflictException"   in str(e.java_exception):
                print(f"{current_branch} → {br} 存在冲突，已跳过")
            else:
                raise

# 分页查看数据
def read_table_page(spark, table_name: str, page: int, page_size: int, order_col: str):
    offset = (page - 1) * page_size
    df = spark.read.table(table_name)
    window_spec = Window.orderBy(order_col)
    df_with_index = df.withColumn("row_num", F.row_number().over(window_spec))
    paged_df = df_with_index.filter((F.col("row_num") > offset) & (F.col("row_num") <= offset + page_size))
    paged_df.drop("row_num").show(truncate=False)

# 检测sql表某个字段是否有重复的值
def check_duplication(spark, tb_name, col_name):
    has_duplicates = spark.sql(f"""
        SELECT 1
        FROM {tb_name}
        GROUP BY {col_name}
        HAVING COUNT(*) > 1
        LIMIT 1
    """).count() > 0
    print(f"是否存在_重复值: {has_duplicates}")

# 获取表tb1, 中col1(int)字段的最大值
def get_colmax(spark, tb_name, col_name):
    max_value = spark.sql(f"""
        SELECT {col_name}
        FROM {tb_name}
        ORDER BY {col_name} DESC
        LIMIT 1
    """).collect()[0][0]
    return int(max_value)

