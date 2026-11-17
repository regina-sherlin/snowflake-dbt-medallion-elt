import logging
import snowflake.connector
from ingestion import config
import json
logger = logging.getLogger(__name__)

class SnowflakeClient:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            account=config.SF_ACCOUNT,
            user=config.SF_USER,
            password=config.SF_PASSWORD,
            role=config.SF_ROLE,
            warehouse=config.SF_WAREHOUSE
        )
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def setup_tables(self, path):
        with open(path, 'r', encoding='utf-8') as file:
            lines=[line for line in file if line.strip() and not line.startswith('--') ]

        for stmt in '\n'.join(lines).split(';'):
            if stmt.strip():
                self.cursor.execute(stmt.strip())
        logger.info('Snowflake schemas and tables are ready')
        
    
    def get_watermark(self, pipeline_name):
        sql = """
            SELECT LAST_RUN_TIMESTAMP
            FROM SEISMIC_ELT_DB.META.PIPELINE_WATERMARKS
            WHERE PIPELINE_NAME = %s
        """
        self.cursor.execute(sql, (pipeline_name,))
        row = self.cursor.fetchone()
        return row[0] if row else None 
        

    def load_into_bronze(self, batch_id, events):
        if not events:
            return 0
        
        sql = """
            INSERT INTO SEISMIC_ELT_DB.BRONZE.RAW_EARTHQUAKE_EVENTS
                (BATCH_ID, SOURCE_EVENT_ID, RAW_PAYLOAD)
            SELECT %s, f.value:id::VARCHAR, f.value
            FROM TABLE(FLATTEN(INPUT => PARSE_JSON(%s))) f
        """
        self.cursor.execute(sql, (batch_id, json.dumps(events)))

        logger.info(f"Inserted {len(events)} rows into bronze (batch_id={batch_id})")
        return len(events)

    def update_watermark(self, pipeline_name, new_watermark):
        sql = """
        MERGE INTO SEISMIC_ELT_DB.META.PIPELINE_WATERMARKS AS target
        USING (SELECT %s AS PIPELINE_NAME, %s AS LAST_RUN_TIMESTAMP) AS src
        ON target.PIPELINE_NAME = src.PIPELINE_NAME
        WHEN MATCHED THEN
            UPDATE SET
                target.LAST_RUN_TIMESTAMP = src.LAST_RUN_TIMESTAMP,
                target.UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (PIPELINE_NAME, LAST_RUN_TIMESTAMP)
            VALUES (src.PIPELINE_NAME, src.LAST_RUN_TIMESTAMP)
        """
        self.cursor.execute(sql, (pipeline_name, new_watermark.isoformat()))
        self.conn.commit()
        logger.info(f"Updated watermark to {new_watermark.isoformat()}")
