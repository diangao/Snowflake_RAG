-- Purpose: Create a Snowflake database and schema for the animal data project.
USE animal_data;

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
        """
        A class for processing and chunking text documents with heading extraction.
        
        Attributes:
            base_font_size (float): Reference font size for heading detection
            base_font_color (tuple): Reference color for heading detection
        """
    def __init__(self):
        self.base_font_size = 12.0  # Default font size
        self.base_font_color = (0, 0, 0)  # Default black font color

    def process(self, pdf_text: str, file_type: str):
        """
        Process the text document based on the file type.
        
        Parameters:
            pdf_text (str): The raw text content from the document
            file_type (str): Document type ('paragraph', 'table', or 'unknown')
        
        Returns:
            Generator: A generator of tuples containing main_heading, sub_heading, and chunk
        """
        print(f"Processing file with type: {file_type}")
        
        file_type = file_type.lower().strip()
        
        if file_type == "paragraph":
            return self.process_paragraph_file(pdf_text)
        elif file_type == "table":
            return self.process_table_file(pdf_text)
        elif file_type == "unknown":
            print("Unknown file type, defaulting to paragraph processing")
            return self.process_paragraph_file(pdf_text)
        else:
            print(f"Unsupported file type: {file_type}")
            return self.process_paragraph_file(pdf_text)

    def process_paragraph_file(self, pdf_text: str):
        """
        Process documents with paragraph-based structure.
        
        Args:
            pdf_text (str): Raw document text
            
        Returns:
            Generator yielding (main_heading, sub_heading, chunk) tuples
        """
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

            data.append((current_main_heading, current_sub_heading, chunk))

        df = pd.DataFrame(data, columns=['main_heading', 'sub_heading', 'chunk'])
        yield from df.itertuples(index=False, name=None)

    def extract_headings_and_chunks(self, pdf_text: str):
        """
        Extract headings and split text into manageable chunks.
        
        Args:
            pdf_text (str): Raw document text
            
        Returns:
            tuple: (list of headings, list of text chunks)
        """
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1512, chunk_overlap=256, length_function=len)
        chunks = text_splitter.split_text(pdf_text)

        headings = []
        for line in pdf_text.split("\n"):
            if self.is_heading(line):
                headings.append(line.strip())

        return headings, chunks

    def is_heading(self, line: str, font_size: float = None, is_bold: bool = False, font_color: str = None) -> bool:
        """
        Determine if a line is a heading based on font size, boldness, and color.
        
        Args:
            line (str): The line of text to check
            font_size (float): Font size of the line
            is_bold (bool): Whether the line is bold
            font_color (str): Color of the line
            
        Returns:
            bool: True if the line is a heading, False otherwise
        """
        if font_size and font_size > self.base_font_size * 1.2:
            return True
        if is_bold:
            return True
        if font_color and font_color != self.base_font_color:
            return True
        return False


    def is_main_heading(self, heading: str, font_size: float = None) -> bool:
        """
        Check if heading is a main/primary heading.
        
        Criteria:
        - Font size > 50% larger than base
        
        Args:
            heading (str): Heading text
            font_size (float, optional): Heading font size
            
        Returns:
            bool: True if heading is main heading
        """
        if font_size and font_size > self.base_font_size * 1.5:  # Font size larger by 50%
            return True
        return False

    def is_sub_heading(self, heading: str, font_size: float = None) -> bool:
        """
        Check if heading is a sub/secondary heading.
        
        Criteria:
        - Font size between 20% and 50% larger than base
        
        Args:
            heading (str): Heading text
            font_size (float, optional): Heading font size
            
        Returns:
            bool: True if heading is sub heading
        """
        if font_size and self.base_font_size * 1.2 <= font_size <= self.base_font_size * 1.5:
            return True
        return False
        
    def extract_headings_and_chunks(self, pdf_text: str):
        """
        Extract headings and split text into manageable chunks.
        
        Args:
            pdf_text (str): Raw document text
            
        Returns:
            tuple: (list of headings, list of text chunks)
        """
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1512, chunk_overlap=256, length_function=len)
        chunks = text_splitter.split_text(pdf_text)

        headings = []
        for line in pdf_text.split("\n"):
            if self.is_heading(line):
                headings.append(line.strip())

        return headings, chunks

    def assign_pet_type(self, chunk: str) -> str:
        """
        Determine the pet type based on the text content.
        
        Args:
            chunk (str): The text content to classify
            
        Returns:
            str: The pet type ('Large Cat', 'Small Cat', 'Large Dog', 'Small Dog', or 'Undefined')
        """
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
        """
        Summarize the main condition described in the text based on the provided heading.
        
        Args:
            chunk (str): The text content to summarize
            heading (str): The related heading to the text
            
        Returns:
            str: The summarized condition
        """
        prompt = f"""
        Based on the following text and its related heading, summarize the main condition described.

        Heading: {heading}
        Text: {chunk}
        """

        condition = session.sql("SELECT SNOWFLAKE.CORTEX.COMPLETE(?, ?)", params=['mistral-large', prompt]).collect()[0][0]

        return condition

    def process_table_file(self, pdf_text: str):
        """
        Process documents with table-based structure.
        
        Args:
            pdf_text (str): Raw document text
            
        Returns:
            Generator yielding (main_heading, sub_heading, chunk) tuples
        """
        rows = pdf_text.split('\n')
        data = []
        
        current_main_heading = "Table Content"
        current_sub_heading = "Table Row"
        
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1512,
            chunk_overlap=256,
            length_function=len
        )
        
        for row in rows:
            if row.strip():
                chunks = text_splitter.split_text(row)
                for chunk in chunks:
                    data.append((current_main_heading, current_sub_heading, chunk))
        
        df = pd.DataFrame(data, columns=['main_heading', 'sub_heading', 'chunk'])
        yield from df.itertuples(index=False, name=None)
$$;

-- create stage
CREATE OR REPLACE STAGE docs_processed
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
DIRECTORY = (ENABLE = true);

-- grant permission
GRANT READ ON STAGE docs TO ROLE ACCOUNTADMIN;
GRANT READ ON STAGE docs_processed TO ROLE ACCOUNTADMIN;
GRANT WRITE ON STAGE docs_processed TO ROLE ACCOUNTADMIN;

-- create table
CREATE OR REPLACE TABLE DOCS_CHUNKS_TABLE ( 
    RELATIVE_PATH VARCHAR(16777216),
    SIZE NUMBER(38,0),
    FILE_URL VARCHAR(16777216),
    SCOPED_FILE_URL VARCHAR(16777216),
    MAIN_HEADING VARCHAR(16777216),
    SUB_HEADING VARCHAR(16777216),
    CHUNK VARCHAR(16777216),
    PET_TYPE VARCHAR(50),
    CONDITION TEXT,
    CATEGORY VARCHAR(16777216)
);

-- create procedure
INSERT INTO docs_chunks_table (
    relative_path, 
    size, 
    file_url, 
    scoped_file_url, 
    main_heading, 
    sub_heading, 
    chunk, 
    pet_type
)
WITH chunk_results AS (
    SELECT 
        d.relative_path, 
        d.size, 
        d.file_url, 
        build_scoped_file_url(@docs, d.relative_path) AS scoped_file_url,
        c.main_heading,
        c.sub_heading,
        c.chunk
    FROM directory(@docs) d
    CROSS JOIN TABLE(text_chunker(
        pdf_text => TO_VARCHAR(SNOWFLAKE.CORTEX.PARSE_DOCUMENT(@docs, d.relative_path, {'mode': 'LAYOUT'})), 
        file_type => CASE 
                        WHEN LOWER(d.relative_path) LIKE '%cat_paragraphs%' THEN 'paragraph'
                        WHEN LOWER(d.relative_path) LIKE '%handbook%' THEN 'table'
                        ELSE 'paragraph'
                     END
    )) c
)
SELECT 
    relative_path,
    size,
    file_url,
    scoped_file_url,
    main_heading,
    sub_heading,
    chunk,
    CAST(
        GET_PATH(
            PARSE_JSON(
                SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
                    COALESCE(chunk, ''),
                    ARRAY_CONSTRUCT('Large Cat', 'Small Cat', 'Large Dog', 'Small Dog', 'Undefined')
                )
            ),
            'label'
        ) AS STRING
    ) AS pet_type
FROM chunk_results;

-- update condition summary
UPDATE docs_chunks_table t
SET condition = SNOWFLAKE.CORTEX.SUMMARIZE(CONCAT(
    'Medical Condition Summary:\nHeading: ', 
    COALESCE(t.MAIN_HEADING, ''),
    '\nSubheading: ',
    COALESCE(t.SUB_HEADING, ''),
    '\nSymptoms and Conditions: ',
    t.CHUNK
))
WHERE t.category IS NULL;

SELECT * FROM docs_chunks_table LIMIT 20;
SELECT COUNT(*) AS total_chunks FROM docs_chunks_table;


USE SCHEMA ANIMAL_DATA.PUBLIC;
ALTER TABLE docs_chunks_table SET CHANGE_TRACKING = TRUE;

-- create exact type search service
CREATE OR REPLACE CORTEX SEARCH SERVICE exact_type_search
ON pet_type
ATTRIBUTES chunk, relative_path, file_url
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 day'
AS (
    SELECT 
        pet_type,
        chunk,
        relative_path,
        file_url
    FROM docs_chunks_table
);


-- create condition match search service
CREATE OR REPLACE CORTEX SEARCH SERVICE condition_match_search
ON condition
ATTRIBUTES chunk, relative_path, file_url, pet_type
WAREHOUSE = COMPUTE_WH
TARGET_LAG = '1 day'
AS (
    SELECT 
        condition,
        chunk,
        relative_path,
        file_url,
        pet_type
    FROM docs_chunks_table
);


-- permission check
SHOW GRANTS ON SCHEMA ANIMAL_DATA;

GRANT USAGE ON DATABASE ANIMAL_DATA TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA PUBLIC TO ROLE ACCOUNTADMIN;
GRANT USAGE ON CORTEX SEARCH SERVICE exact_type_search TO ROLE ACCOUNTADMIN;
GRANT USAGE ON CORTEX SEARCH SERVICE condition_match_search TO ROLE ACCOUNTADMIN;


-- DEBUG
-- SELECT PARSE_JSON(
--   SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
--     'ANIMAL_DATA.PUBLIC.CONDITION_MATCH_SEARCH',
--     '{
--       "query": "causes of loss of appetite, especially Large Dog",
--       "columns": ["chunk", "relative_path", "pet_type"],
--       "filter": {"@eq": {"pet_type": "Large Dog"} },
--       "limit": 5
--     }'
--   )
-- )['results'] as results;
