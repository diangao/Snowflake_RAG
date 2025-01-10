-- 使用数据库 ANIMAL_DATA
USE animal_data;

-- 修改 text_chunker 函数以根据文件类型进行不同处理
-- 修改 text_chunker 函数
CREATE OR REPLACE FUNCTION text_chunker(pdf_text STRING, file_type STRING)
RETURNS TABLE (
    main_heading VARCHAR,
    sub_heading VARCHAR,
    chunk VARCHAR
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'text_chunker'
PACKAGES = ('langchain', 'pandas')
AS
$$
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd

class text_chunker:

    def __init__(self):
        self.base_font_size = 12.0  # Default font size
        self.base_font_color = (0, 0, 0)  # Default black font color

    def process(self, pdf_text: str, file_type: str):
        # 添加调试日志
        print(f"Processing file with type: {file_type}")
        
        # 统一转换为小写并去除空格，使判断更健壮
        file_type = file_type.lower().strip()
        
        if file_type == "paragraph":
            return self.process_paragraph_file(pdf_text)
        elif file_type == "table":
            return self.process_table_file(pdf_text)
        elif file_type == "unknown":
            # 对于未知类型，默认使用段落处理方式
            print("Unknown file type, defaulting to paragraph processing")
            return self.process_paragraph_file(pdf_text)
        else:
            print(f"Unsupported file type: {file_type}")
            # 对于不支持的类型，默认使用段落处理方式
            return self.process_paragraph_file(pdf_text)

    def process_paragraph_file(self, pdf_text: str):
        # 提取标题和块
        headings, chunks = self.extract_headings_and_chunks(pdf_text)
        data = []

        current_main_heading = "No Main Heading"
        current_sub_heading = "No Sub Heading"

        for chunk in chunks:
            for heading in headings:
                if heading in chunk:
                    if self.is_main_heading(heading):
                        current_main_heading = heading
                    elif self.is_sub_heading(heading):
                        current_sub_heading = heading

            # 仅返回基础字段
            data.append((current_main_heading, current_sub_heading, chunk))

        df = pd.DataFrame(data, columns=['main_heading', 'sub_heading', 'chunk'])
        yield from df.itertuples(index=False, name=None)

    def extract_headings_and_chunks(self, pdf_text: str):
        # 提取标题和块
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1512, chunk_overlap=256, length_function=len)
        chunks = text_splitter.split_text(pdf_text)

        headings = []
        for line in pdf_text.split("\n"):
            if self.is_heading(line):
                headings.append(line.strip())

        return headings, chunks

    def is_heading(self, line: str, font_size: float = None, is_bold: bool = False, font_color: str = None) -> bool:
    # 判断是否为标题
        if font_size and font_size > self.base_font_size * 1.2:  # 字体大小超过 20%
            return True
        if is_bold:  # 加粗
            return True
        if font_color and font_color != self.base_font_color:  # 字体颜色不同于默认颜色
            return True
        return False


    def is_main_heading(self, heading: str, font_size: float = None) -> bool:
        if font_size and font_size > self.base_font_size * 1.5:  # Font size larger by 50%
            return True
        return False

    def is_sub_heading(self, heading: str, font_size: float = None) -> bool:
        if font_size and self.base_font_size * 1.2 <= font_size <= self.base_font_size * 1.5:
            return True
        return False
        
    def extract_headings_and_chunks(self, pdf_text: str):
        # 使用 RecursiveCharacterTextSplitter 拆分文本
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1512, chunk_overlap=256, length_function=len)
        chunks = text_splitter.split_text(pdf_text)

        headings = []
        for line in pdf_text.split("\n"):
            if self.is_heading(line):  # 使用类中已有的 is_heading 方法判断是否是标题
                headings.append(line.strip())

        return headings, chunks

    def assign_pet_type(self, chunk: str) -> str:
        # Use LLM to determine pet type
        prompt = f"""
        Given the following text, identify if it refers to a large cat, small cat, large dog, or small dog.
        You must ONLY respond with one of these five options (exactly as written):
        - 'Large Cat'
        - 'Small Cat'
        - 'Large Dog'
        - 'Small Dog'
        - 'Undefined'

        Choose 'Undefined' if the text cannot be clearly classified into the other four categories.

        Text:
        {chunk}
        """
        pet_type = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", params=['mistral-large', prompt]).collect()[0][0]
        
        # Validate response and default to 'Undefined' if not in expected values
        valid_types = {'Large Cat', 'Small Cat', 'Large Dog', 'Small Dog', 'Undefined'}
        return pet_type if pet_type in valid_types else 'Undefined'

    def summarize_condition(self, chunk: str, heading: str) -> str:
        # Use LLM to summarize condition
        prompt = f"""
        Based on the following text and its related heading, summarize the main condition described.

        Heading: {heading}
        Text: {chunk}
        """

        # from snowflake.snowpark import Session
        # session = Session.builder.configs({...}).create()  # 初始化 session
        # condition = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", params=['mistral-large', prompt]).collect()[0][0]
        condition = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", params=['mistral-large', prompt]).collect()[0][0]

        return condition

    def process_table_file(self, pdf_text: str):
        """处理表格类型的文件"""
        # 将表格文本按行分割
        rows = pdf_text.split('\n')
        data = []
        
        current_main_heading = "Table Content"
        current_sub_heading = "Table Row"
        
        # 使用文本分割器处理每一行
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1512,
            chunk_overlap=256,
            length_function=len
        )
        
        # 处理每一行作为独立的块
        for row in rows:
            if row.strip():  # 忽略空行
                chunks = text_splitter.split_text(row)
                for chunk in chunks:
                    data.append((current_main_heading, current_sub_heading, chunk))
        
        df = pd.DataFrame(data, columns=['main_heading', 'sub_heading', 'chunk'])
        yield from df.itertuples(index=False, name=None)
$$;

-- 创建 stage
CREATE OR REPLACE STAGE docs_processed
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
DIRECTORY = (ENABLE = true);

-- 授予权限给角色
GRANT READ ON STAGE docs TO ROLE ACCOUNTADMIN;
GRANT READ ON STAGE docs_processed TO ROLE ACCOUNTADMIN;
GRANT WRITE ON STAGE docs_processed TO ROLE ACCOUNTADMIN;

-- 检查 stage 中的文件
LS @docs_processed;


-- 更新 DOCS_CHUNKS_TABLE 增加新的字段
CREATE OR REPLACE TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216), -- PDF 文件路径
    SIZE NUMBER(38,0),              -- 文件大小
    FILE_URL VARCHAR(16777216),     -- 文件 URL
    SCOPED_FILE_URL VARCHAR(16777216), -- 受限 URL
    MAIN_HEADING VARCHAR(16777216), -- 主标题
    SUB_HEADING VARCHAR(16777216),  -- 子标题
    CHUNK VARCHAR(16777216),        -- 文本块
    PET_TYPE VARCHAR(50),           -- 宠物类别（增强字段）
    CONDITION TEXT,                 -- 症状总结（增强字段）
    CATEGORY VARCHAR(16777216)      -- 文档类别
);

SHOW STAGES IN SCHEMA ANIMAL_DATA.PUBLIC;


-- 插入解析后的数据
-- 增强并插入解析后的数据
INSERT INTO docs_chunks_table (
    relative_path, 
    size, 
    file_url, 
    scoped_file_url, 
    main_heading, 
    sub_heading, 
    chunk, 
    pet_type, 
    condition
)
-- 使用 WITH 生成增强结果集并直接插入数据
WITH chunk_results AS (
    -- 调用 text_chunker 提取基本字段
    SELECT 
        d.relative_path, 
        d.size, 
        d.file_url, 
        build_scoped_file_url(@docs, d.relative_path) AS scoped_file_url,
        c.main_heading,
        c.sub_heading,
        c.chunk
    FROM 
        directory(@docs) d,
        TABLE(text_chunker(
            pdf_text => TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@docs, d.relative_path, {'mode': 'LAYOUT'})), 
            file_type => CASE 
                            WHEN LOWER(d.relative_path) LIKE '%cat_paragraphs%' THEN 'paragraph'
                            WHEN LOWER(d.relative_path) LIKE '%handbook%' THEN 'table'
                            ELSE 'paragraph'
                         END
        )) c
),
enhanced_results AS (
    -- 增强数据
    SELECT 
        relative_path,
        size,
        file_url,
        scoped_file_url,
        main_heading,
        sub_heading,
        chunk,
        CASE 
            WHEN NOT REGEXP_LIKE(
                SNOWFLAKE.CORTEX.COMPLETE(
                    'mistral-large',
                    'Given the following text, identify if it refers to a large cat, small cat, large dog, or small dog.
                    You must ONLY respond with one of these five options (exactly as written):
                    - ''Large Cat''
                    - ''Small Cat''
                    - ''Large Dog''
                    - ''Small Dog''
                    - ''Undefined''

                    Choose ''Undefined'' if the text cannot be clearly classified into the other four categories.

                    Text: ' || chunk
                ),
                '^(Large Cat|Small Cat|Large Dog|Small Dog|Undefined)$'
            ) THEN 'Undefined'
            ELSE SNOWFLAKE.CORTEX.COMPLETE(
                'mistral-large',
                'Given the following text, identify if it refers to a large cat, small cat, large dog, or small dog.
                You must ONLY respond with one of these five options (exactly as written):
                - ''Large Cat''
                - ''Small Cat''
                - ''Large Dog''
                - ''Small Dog''
                - ''Undefined''

                Choose ''Undefined'' if the text cannot be clearly classified into the other four categories.

                Text: ' || chunk
            )
        END AS pet_type,
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large',
            'Based on the following text and its related heading, summarize the main condition described.\n\nHeading: ' || main_heading || '\nText: ' || chunk
        ) AS condition
    FROM chunk_results
)
SELECT * FROM enhanced_results;

-- 检查表内容
SELECT * FROM docs_chunks_table;

-- 创建 Cortex Search Service for 精确种类搜索
CREATE OR REPLACE CORTEX SEARCH SERVICE EXACT_TYPE_SEARCH
ON pet_type
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
    SELECT 
        pet_type, 
        chunk, 
        relative_path, 
        file_url,
        CASE 
            WHEN pet_type = 'Undefined' THEN 0.5  -- 降低 Undefined 条目的权重
            ELSE 1.0                              -- 保持其他分类的正常权重
        END as relevance_score
    FROM docs_chunks_table
);

-- 创建 Cortex Search Service for 模糊条件匹配
CREATE OR REPLACE CORTEX SEARCH SERVICE CONDITION_MATCH_SEARCH
ON condition  -- 按照 condition 字段进行相似性搜索
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 hour'
AS (
    SELECT condition, chunk, relative_path, file_url FROM docs_chunks_table
);


DESCRIBE TABLE docs_chunks_table;

SELECT * FROM docs_chunks_table;

SELECT pet_type, chunk, relative_path, file_url
FROM docs_chunks_table;
SHOW FUNCTIONS LIKE 'EXACT_TYPE_SEARCH';


-- 使用 EXACT_TYPE_SEARCH 执行精确种类搜索
SELECT *
FROM TABLE(EXACT_TYPE_SEARCH(
    query => 'Large Cat',  -- 示例：用户查询为"大型猫"
    limit => 10  -- 返回最多 10 条结果
));

-- 使用 CONDITION_MATCH_SEARCH 执行模糊条件匹配搜索
SELECT *
FROM TABLE(CONDITION_MATCH_SEARCH(
    query => 'Electric Shock Treatment',  -- 示例：用户查询为电击治疗
    limit => 10  -- 返回最多 10 条结果
));

-- 检查 INFORMATION_SCHEMA 的表和权限
SHOW TABLES IN SCHEMA ANIMAL_DATA.INFORMATION_SCHEMA;
SHOW GRANTS ON SCHEMA ANIMAL_DATA.INFORMATION_SCHEMA;

-- -- 查看当前会话信息
-- SELECT SESSION_ID, USER_NAME, CLIENT_APPLICATION, START_TIME
-- FROM INFORMATION_SCHEMA.SESSIONS
-- WHERE IS_CURRENT = 'TRUE';