import os
import subprocess
import time
from datetime import datetime
import snowflake.connector
from dotenv import load_dotenv
import logging
from mdp_mssql_mine.defs.config import Config, generate_snowflake_ddl 
import os
import subprocess
import time
from pathlib import Path
import logging
from dotenv import load_dotenv
from typing import Optional, Tuple, List

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


#####  CRÉATION SNOWFLAKE (File Format, Stage, Table)==============
def get_snowflake_connection(database: str = Config.SF_DATABASE, schema: str = Config.SF_SCHEMA):
    """Créer connexion Snowflake"""
    return snowflake.connector.connect(
        account=Config.SF_ACCOUNT,
        user=Config.SF_USER,
        password=Config.SF_PASSWORD,
        warehouse=Config.SF_WAREHOUSE,
        database=database,
        schema=schema,
        role=Config.SF_ROLE,
    )


def create_file_format(
    cursor,
    logger,
    database: str = Config.SF_DATABASE, 
    schema: str = Config.SF_SCHEMA, 
):
    """
    Créer le format de fichier CSV s'il n'existe pas déjà --- on pourrait aussi utiliser parquet à la place
    Équivalent: CREATE OR REPLACE FILE FORMAT...
    """
    
    logger.info("🔧 Création du file format CSV...")
    
    sql = f"""
    CREATE FILE FORMAT IF NOT EXISTS {database}.{schema}.{Config.FILE_FORMAT_NAME}
        TYPE = CSV
        FIELD_DELIMITER = '|'
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', '')
        EMPTY_FIELD_AS_NULL = TRUE
        error_on_column_count_mismatch = false
    """
    
    cursor.execute(sql)
    logger.info(f"✅ File format {Config.FILE_FORMAT_NAME} créé")


def create_stage(
    cursor, 
    logger,
    database: str = Config.SF_DATABASE, 
    schema: str = Config.SF_SCHEMA, 
):
    """
    Créer le stage interne s'il n'existe pas déjà dans le schéma
    Équivalent: CREATE OR REPLACE STAGE...
    """
    
    logger.info("🔧 Création du stage...")
    
    sql = f"""
    CREATE STAGE IF NOT EXISTS {database}.{schema}.{Config.STAGE_NAME}
        FILE_FORMAT = {Config.FILE_FORMAT_NAME}
    """
    
    cursor.execute(sql)
    logger.info(f"✅ Stage {Config.STAGE_NAME} créé")


def create_snowflake_table(
    cursor,
    mssql_table_name: str,
    snowflake_table_name: str, 
    logger,
    database: str = Config.SF_DATABASE, 
    schema: str = Config.SF_SCHEMA, 
) -> None:
    """
    Créer une table Snowflake avec schéma personnalisable
    
    Args:
        cursor: Snowflake cursor
        database: Nom de la base de données
        schema: Nom du schéma
        mssql_table_name: Nom de la table mssql
        snowflake_table_name : Nom de la table snowflake
    
    Examples:
        # Utilisation avec schéma par défaut        
        create_snowflake_table(
            cursor, 
            "NEEMBA", 
            "EQUIPEMENT", 
            "mssql_table_name",
            "snowflake_table_name"
        )
    """
    
    logger.info(f"🔧 Création de la table {database}.{schema}.{mssql_table_name}...")
    # Construire la requête DDL
    sql_ddl = generate_snowflake_ddl(
        mssql_table_name = mssql_table_name,
        snowflake_table_name = snowflake_table_name,
        snowflake_database = database,
        snowflake_schema = schema
    )
    # Exécuter
    cursor.execute(sql_ddl)
    
    logger.info(f"✅ Table {mssql_table_name} créée avec succès")
    logger.info(f"   Localisation: {database}.{schema}.{mssql_table_name}")


### Créer format de fichier, stage et table dans snowflake==============
def setup_snowflake(
    mssql_table_name: str, 
    snowflake_table_name: str, 
    logger,
    snowflake_database: str = Config.SF_DATABASE,
    snowflake_schema: str = Config.SF_SCHEMA, 
):
    """
    Setup des objets Snowflake
    """
    
    logger.info("=" * 80)
    logger.info("🔧 Setup Snowflake")
    logger.info("=" * 80)
    
    conn = get_snowflake_connection(database = snowflake_database, schema = snowflake_schema)
    cursor = conn.cursor()
    
    try:
    
        create_file_format(cursor,database = snowflake_database, schema = snowflake_schema, logger=logger)
    
        create_stage(cursor,database = snowflake_database, schema = snowflake_schema, logger=logger)
        create_snowflake_table(
            cursor = cursor, 
            database = snowflake_database, #"NEEMBA",
            schema = snowflake_schema, #"EQUIPEMENT",          
            mssql_table_name = mssql_table_name,
            snowflake_table_name = snowflake_table_name, 
            logger = logger
        )
        
    finally:
        cursor.close()
        conn.close()

# Upload du fichier dans le stage de snowflake avec la commande PUT==============

def upload_to_stage(cursor, logger):
    """
    Upload du fichier vers le stage
    Équivalent: PUT file://... @STAGE AUTO_COMPRESS=TRUE
    """
    
    logger.info("=" * 80)
    logger.info("📤 Upload vers Snowflake Stage")
    logger.info("=" * 80)
        
    try:
        # PUT command (utiliser forward slashes)
        file_path = str(Config.OUTPUT_PATH).replace("\\", "/")
        
        sql_put = f"""
        PUT file://{file_path} @{Config.STAGE_NAME}
        AUTO_COMPRESS=TRUE
        OVERWRITE=TRUE
        """
        
        logger.info(f"🔄 Upload de {Config.OUTPUT_PATH.name}...")
        start_time = time.time()
        
        cursor.execute(sql_put)
        
        duration = time.time() - start_time
        logger.info(f"✅ Upload terminé en {duration:.2f}s")
        
        # Lister les fichiers dans le stage
        cursor.execute(f"LIST @{Config.STAGE_NAME}")
        files = cursor.fetchall()
        logger.info(f"📁 Fichiers dans le stage: {len(files)}")
        
    finally:
        logger.info(f"Connexion fermées dans la fonction qui appelle")
    #    cursor.close()
       # conn.close()


# COPY INTO des données dans la table finale==============

def copy_into_table(cursor, snowflake_table_name: str, logger):
    """
    Chargement final avec COPY INTO
    Équivalent: COPY INTO table FROM @STAGE...
    """
    
    logger.info("=" * 80)
    logger.info("📥 COPY INTO Snowflake")
    logger.info("=" * 80)
    
    try:
        sql_truncate = f"Truncate table {snowflake_table_name}"
        sql_copy = f"""
        COPY INTO {snowflake_table_name}
        FROM @{Config.STAGE_NAME}
        FILE_FORMAT = (FORMAT_NAME = {Config.FILE_FORMAT_NAME})
        --ON_ERROR = 'ABORT_STATEMENT'
        ON_ERROR = 'CONTINUE' 
        PURGE = TRUE
        """
        
        logger.info(f"🔄 Chargement dans {snowflake_table_name}...")
        start_time = time.time()
        
        cursor.execute(sql_copy)
        results = cursor.fetchall()
        
        duration = time.time() - start_time
        
        # Analyser les résultats
        total_rows = 0
        total_errors = 0
        
        for row in results:
            file_name = row[0]
            rows_loaded = row[2]
            errors = row[5]
            
            total_rows += rows_loaded
            total_errors += errors
            
            logger.info(f"   ✓ {file_name}: {rows_loaded:,} lignes")
        
        logger.info(f"✅ COPY INTO terminé en {duration:.2f}s")
        logger.info(f"📊✅ Nombre de lignes chargées : {total_rows:,}")
        logger.info(f"📊❌ Nombre d'erreurs : {total_errors:,}")
        
        # Vérification finale
        cursor.execute(f"SELECT COUNT(*) FROM {snowflake_table_name}")
        final_count = cursor.fetchone()[0]
        logger.info(f"📊 Total dans la table {snowflake_table_name}: {final_count:,}")
        
        return {
            'rows_loaded': total_rows,
            'errors': total_errors,
            'duration': duration
        }
        
    finally: 
        logger.info(f"Connexion fermées dans la fonction qui appelle")
    #    cursor.close()
        #conn.close()


### Créer format de fichier, stage et table dans snowflake==============
def upload_to_snowflake(
    snowflake_table_name: str, 
    logger,
    snowflake_database: str = Config.SF_DATABASE,
    snowflake_schema: str = Config.SF_SCHEMA, 
):
    """
    Upload + copy into
    """
    
    logger.info("=" * 80)
    logger.info("🔧 Upload + copy into Snowflake")
    logger.info("=" * 80)
    
    conn = get_snowflake_connection(database = snowflake_database, schema = snowflake_schema)
    cursor = conn.cursor()
    
    try:
        upload_to_stage(cursor, logger)
        result = copy_into_table(cursor = cursor, snowflake_table_name = snowflake_table_name, logger = logger)
        return result
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    setup_snowflake(
        snowflake_database = "NEEMBA",
        snowflake_schema = "EQUIPEMENT", 
        mssql_table_name = "V_Inventory_Parts_Ops",
        snowflake_table_name = "AI_V_Inventory_Parts_Ops",
        logger = logger
    )
    upload_to_snowflake(
        snowflake_database = "NEEMBA",
        snowflake_schema = "EQUIPEMENT", 
        snowflake_table_name = "AI_V_Inventory_Parts_Ops",
        logger = logger
    )
