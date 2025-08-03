# 数据集整体描述表
spark.sql("""CREATE TABLE if not exists nessie.db_lowaltitude_vision.dataset_info (
    dataset_id INT,       #  数据集id
    data_name STRING,     #  数据集名字
    task string,          #  数据集干是任务是什么(目标检测，跟踪等等)
    modality string,      #  数据集数据类型（RGB / IR / Depth / 多模态等）
    country string,       #  数据集数据采集国家（如果可以精确到区域）
    source_type string,   #  数据来源方法（原始采集 / 模拟合成 / 合成增强等）
    status string,        #  数据集状态（草稿 / 标注中 / 已 审核 / 已训练使用）
    is_public BOOLEAN,    #  数据集是否公开
    version STRING,       #  数据集版本
    contributor STRING,   #  数据集贡献者
    date_created TIMESTAMP,  #  数据集发布时间
    license_id INT,       #  数据集许可证
    publish_uri STRING,   #  数据集发布链接
    description STRING,   #  数据集简要描述
    
)
USING iceberg
PARTITIONED BY (data_name);""")


# 许可证描述表
spark.sql("""CREATE TABLE if not exists nessie.db_lowaltitude_vision.licenses (
    license_id INT,        #  许可证id 
    name STRING,   #  许可证名字
    url STRING,    #  许可证网站
    notes STRING   #  备注
)
USING iceberg
PARTITIONED BY (id);""")



# 传感器数据表
spark.sql("""CREATE TABLE nessie.db_lowaltitude_vision.sensors (
    sensor_id INT,               #  传感器id
    type STRING,                 #  传感器类型（camera"（摄像头）、"lidar"（激光雷达）、"radar"、"imu"、"other"）
    name STRING,                 #  传感器名字
    intrinsics ARRAY<DOUBLE>,    #  内参
    extrinsics ARRAY<DOUBLE>,    #  外参
)
USING iceberg
PARTITIONED BY (id);""")


#  数据据资产表
spark.sql("""CREATE TABLE if not exists nessie.db_lowaltitude_vision.assets (
    asset_id BIGINT,                 #  资产id（从0开始依次递增1）添加新的内容时请查询表中最后一条数据的id
    data_name STRING,                #  所属数据集名字
    filename STRING,                 #  资产名字
    uri STRING,                      #  资产所存储地址（存储在MinIO内）
    source_type STRING,              #  资产类型
    width INT,                       #  长（如果是图片/视频）
    height INT,                      #  宽（如果是图片/视频）
    depth INT,                       #  通道数（如果是图片/视频）
    duration DOUBLE,                 #  时长（如果是视频）
    point_count INT,                 #  点数量（如果是点云）
    sensor_id ARRAY<int>,            #  传感器（列表）
    frame_rate DOUBLE,               #  帧率（如果是视频）
    timestamp TIMESTAMP,             #  数据采集时间
    location string,                 #  数据采集地点
USING iceberg
PARTITIONED BY (data_name);""")


# 数据标注类别简要分类表
spark.sql("""CREATE TABLE if not exists nessie.db_lowaltitude_vision.categories (
    category_id INT,          #  数据标注id
    name STRING,              #  标注名字（做到本数据集统一）
    supercategory STRING,     #  标注类别超类
    domain STRING,            #  标注类别领域
)
USING iceberg
PARTITIONED BY (domain);""")


spark.sql("""CREATE TABLE my_catalog.db.annotations (
    annotation_id STRING,               #  使用UUID创建
    split_name string,                  #  在原始数据集中的分类类型（train/val/test/undef）
    asset_id BIGINT,                    #  对应资产id
    annotation_type STRING,             #  标注类型（如bbox2d/seg_poly/bbox3d等）
    category_id INT,                    #  标注对象所属类别
    category_ids ARRAY<INT>,            #  用于多标签组
    
    geometry STRUCT<                    #  标注具体信息， 因不同的标注类型而异
        bbox2d STRUCT<
            x DOUBLE,                   # xmin
            y DOUBLE,                   # ymin（正常就是标注框左上点的坐标）
            width DOUBLE,               # 标注框宽
            height DOUBLE,              # 标注框高
            coord_sys STRING            #  "pixel"/"relative"前者表示以上坐标以像素单位给出；后者则表示使用相对坐标
        >,
        bbox3d STRUCT<                  # 下面的几个类型，根据所遇到的数据集进行分析，如果遇到新的类型请修改表结构增加字段
            center ARRAY<DOUBLE>,
            size ARRAY<DOUBLE>,
            rotation ARRAY<DOUBLE>,
            coord_sys STRING
        >,
        polygon STRUCT<
            points ARRAY<ARRAY<DOUBLE>>,
            coord_sys STRING
        >,
        keypoints STRUCT<
            points ARRAY<DOUBLE>,
            num_joints INT,
            coord_sys STRING
        >
    >
)
USING iceberg
PARTITIONED BY (annotation_type);""")


#  标注分组描述表
spark.sql("""CREATE TABLE nessie.db_lowaltitude_vision.splits (
    dataset_id INT,             #  数据集id
    split_name STRING,          #  分组名字（train, val, test）
    annotation_id ARRAY<INT>    #  标注id列表
)
USING iceberg
PARTITIONED BY (dataset_id);""")


#  标注用户信息表
spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.db_lowaltitude_vision.annotate_user (
    user_id STRING,          -- id 
    username STRING,         -- 账户名
    role STRING,             -- 管理员 / 标注员 / 审核员
    organization STRING      -- 所属单位或团队
)
USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS nessie.db_lowaltitude_vision.import_user (
    user_id STRING,          -- id 
    username STRING,         -- 姓名
    import_time timestamp        -- 导入数据集时间
    last_updated_time timestamp  -- 最近更新时间
    data_name string            -- 导入数据集名字
)
USING iceberg
""")

