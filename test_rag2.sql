-- 使用数据库
USE animal_data;

-----------------
-- 基础数据检查
-----------------

-- 1. 数据量检查
SELECT COUNT(*) as total_records FROM docs_chunks_table;

-- 2. 数据完整性检查
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN pet_type IS NULL THEN 1 END) as null_pet_types,
    COUNT(CASE WHEN condition IS NULL THEN 1 END) as null_conditions,
    COUNT(CASE WHEN chunk IS NULL THEN 1 END) as null_chunks
FROM docs_chunks_table;

-- 3. 示例数据预览
SELECT 
    main_heading,
    sub_heading,
    LEFT(chunk, 100) as chunk_preview,
    pet_type,
    LEFT(condition, 100) as condition_preview,
    relative_path
FROM docs_chunks_table
LIMIT 5;

-----------------
-- 宠物类型分类检查
-----------------

-- 1. 类型分布统计
SELECT 
    pet_type,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM docs_chunks_table
GROUP BY pet_type
ORDER BY count DESC;

-- 2. Undefined 案例分析
SELECT 
    LEFT(chunk, 200) as chunk_preview,
    pet_type
FROM docs_chunks_table
WHERE pet_type = 'Undefined'
LIMIT 5;

-----------------
-- 症状提取检查
-----------------

-- 1. 症状频率分析
SELECT DISTINCT
    LEFT(condition, 100) as condition_preview,
    COUNT(*) OVER (PARTITION BY condition) as frequency
FROM docs_chunks_table
WHERE condition IS NOT NULL
ORDER BY frequency DESC
LIMIT 10;

-- 2. 症状-宠物类型关联分析
SELECT 
    pet_type,
    LEFT(condition, 100) as condition_preview,
    COUNT(*) as frequency
FROM docs_chunks_table
WHERE condition IS NOT NULL
GROUP BY pet_type, condition
ORDER BY frequency DESC
LIMIT 10;

-----------------
-- 搜索功能测试
-----------------

-- 1. 宠物类型搜索测试
SELECT 
    pet_type,
    LEFT(chunk, 200) as chunk_preview,
    SIMILARITY_SCORE() as score
FROM docs_chunks_table
WHERE MATCH_SEARCH(
    pet_type,
    'Large Cat',  -- 测试用例 1
    USING exact_type_search
)
ORDER BY score DESC
LIMIT 3;

SELECT 
    pet_type,
    LEFT(chunk, 200) as chunk_preview,
    SIMILARITY_SCORE() as score
FROM docs_chunks_table
WHERE MATCH_SEARCH(
    pet_type,
    'Small Dog',  -- 测试用例 2
    USING exact_type_search
)
ORDER BY score DESC
LIMIT 3;

-- 2. 症状搜索测试
SELECT 
    LEFT(condition, 100) as condition_preview,
    LEFT(chunk, 200) as chunk_preview,
    SYSTEM$SIMILARITY_SCORE() as score
FROM docs_chunks_table
WHERE CONTAINS(condition, '"vomiting and diarrhea"')
ORDER BY score DESC
LIMIT 3;

SELECT 
    LEFT(condition, 100) as condition_preview,
    LEFT(chunk, 200) as chunk_preview,
    SIMILARITY_SCORE() as score
FROM docs_chunks_table
WHERE MATCH_SEARCH(
    condition,
    'skin irritation and scratching',  -- 测试用例 2
    USING condition_match_search
)
ORDER BY score DESC
LIMIT 3;

-----------------
-- 文档覆盖率检查
-----------------

-- 检查每个源文档的处理情况
SELECT 
    relative_path,
    COUNT(*) as chunk_count,
    COUNT(DISTINCT pet_type) as unique_pet_types,
    COUNT(DISTINCT condition) as unique_conditions
FROM docs_chunks_table
GROUP BY relative_path
ORDER BY chunk_count DESC; 