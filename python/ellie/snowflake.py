"""
Snowflake to Ellie Transfer - Snowflake Module

This module handles the connection to Snowflake and the extraction of schema information
including tables, views, columns, primary keys, and foreign key relationships.

The extracted data is formatted to match Ellie's API requirements for model import.
"""

import snowflake.connector
import uuid
import re

### SNOWFLAKE
snowflake_connection = None

def extract_account_from_url(url):
    """
    Extract the account identifier from a Snowflake URL.
    
    Args:
        url (str): Snowflake URL (e.g., https://nn73358.eu-north-1.aws.snowflakecomputing.com)
        
    Returns:
        str: Account identifier (e.g., nn73358.eu-north-1.aws)
    """
    # First, check if it's a URL format
    url_pattern = r'https?://([^\.]+\.[^\.]+\.[^\.]+)\.snowflakecomputing\.com'
    match = re.match(url_pattern, url)
    if match:
        return match.group(1)
    
    # If not a URL format, return as is (could be just the account ID)
    return url

def snowflake_connect(settings):
    """
    Make a new connection to Snowflake.
    
    The connection is established globally and used by other functions in this module.
    
    Args:
        settings (dict): Dictionary containing Snowflake connection parameters:
            - user: Snowflake username
            - password: Snowflake password
            - account: Snowflake account identifier (standard mode) or custom_url (privatelink mode)
            - warehouse: Snowflake warehouse to use
            - database: Snowflake database to connect to
            - connection_mode: 'standard' or 'privatelink'
            - custom_url: Optional, URL for privatelink connections
            
    Returns:
        None
    
    Raises:
        Exception: If connection fails
    """
    global snowflake_connection
    
    # Determine which account parameter to use based on connection mode
    if settings.get('connection_mode') == 'privatelink' and settings.get('custom_url'):
        account_param = settings['custom_url']
    else:
        # For standard connections, the account might be a full URL
        account_param = extract_account_from_url(settings['account'])
    
    # Connect to Snowflake
    snowflake_connection = snowflake.connector.connect(
        user=settings['user'],
        password=settings['password'],
        account=account_param,
        warehouse=settings['warehouse'],
        database=settings['database'],
        schema="INFORMATION_SCHEMA"
    )
    
def snowflake_export(schemas = ['PUBLIC'], include_views = True):
    """
    Export schema metadata from Snowflake and format it for Ellie import.
    
    This function queries Snowflake for table/view definitions, column information,
    primary keys, and foreign key relationships, then formats the data into Ellie's
    expected API structure.
    
    Args:
        schemas (list): List of schema names to export. Default: ['PUBLIC']
        include_views (bool): Whether to include views in addition to tables. Default: True
        
    Returns:
        dict: Model data in Ellie API format with entities and relationships
        
    Raises:
        Exception: If Snowflake queries fail or data processing encounters errors
    """
    grouped_rows = {}
    relationships = []
    
    # Dictionary to store entity IDs
    entity_ids = {}
    
    for schema in schemas:
        print(f"Processing schema: {schema}")
        
        # First, get all table and view names in this schema
        table_types = _get_tables_and_views(schema, include_views)
        print(f"Found {len(table_types)} tables/views in schema {schema}")
        
        # Skip empty schemas
        if not table_types:
            continue
            
        # Extract just the table names for further processing
        tables = [row['TABLE_NAME'] for row in table_types]
        
        # First, get all foreign key constraints for this schema
        foreign_keys = _get_foreign_keys(schema)
        print(f"Found {len(foreign_keys)} foreign key constraints in schema {schema}")
        
        # Get table metadata including primary keys
        schema_data = _query_schema_data(schema)
        
        # Process columns and build entities
        for row in schema_data:
            table_schema = row[0]
            table_name = row[1]
            
            # Skip this table/view if it's not in our filtered list
            if table_name not in tables:
                continue
                
            column_name = row[2]
            is_pk = row[3]
            data_type = row[4]
            
            # Create attribute for this column
            attribute = {
                "name": column_name,
                "metadata": {
                    "PK": is_pk,
                    "FK": False,  # We'll set this based on foreign key constraints later
                    "DATA TYPE": data_type
                }
            }
            
            # Add to the grouped rows by table name
            if table_name not in grouped_rows:
                grouped_rows[table_name] = {"attributes": []}
                entity_ids[table_name] = str(uuid.uuid4())  # Generate a unique ID for each entity
            
            grouped_rows[table_name]["attributes"].append(attribute)
        
        # Create relationships from foreign key constraints
        for fk in foreign_keys:
            try:
                # Extract information from the foreign key constraint
                fk_schema = fk['fk_schema_name']
                fk_table = fk['fk_table_name']
                fk_column = fk['fk_column_name']
                pk_schema = fk['pk_schema_name']
                pk_table = fk['pk_table_name']
                pk_column = fk['pk_column_name']
                
                # Skip if either table is not in our filtered list
                if fk_table not in tables or pk_table not in tables:
                    continue
                
                # Mark the foreign key column as FK=True in its attribute
                if fk_table in grouped_rows:
                    for attr in grouped_rows[fk_table]["attributes"]:
                        if attr["name"] == fk_column:
                            attr["metadata"]["FK"] = True
                
                # Create a relationship following Ellie's expected format
                if pk_table in entity_ids and fk_table in entity_ids:
                    relationship = {
                        "sourceEntity": {
                            "id": entity_ids[pk_table],
                            "name": pk_table,
                            "startType": "one",
                            "attributeNames": [pk_column]
                        },
                        "targetEntity": {
                            "id": entity_ids[fk_table],
                            "name": fk_table,
                            "endType": "many",
                            "attributeNames": [fk_column]
                        },
                        "description": []
                    }
                    
                    # Add the relationship if both tables are in our data
                    if pk_table in grouped_rows and fk_table in grouped_rows:
                        relationships.append(relationship)
                        print(f"Added relationship: {pk_table}.{pk_column} -> {fk_table}.{fk_column}")
            except Exception as e:
                print(f"Error processing foreign key constraint: {str(e)}")

    # Create an entity for each table
    entities = [{
        "id": entity_ids[name],
        "name": name,
        "attributes": grouped_rows[name]["attributes"]
    } for name in grouped_rows.keys()]

    print(f"Created {len(entities)} entities and {len(relationships)} relationships")
    
    # Generate the JSON schema
    model = {
        "model": {
            "level": "conceptual",
            "entities": entities,
            "relationships": relationships
        }
    }
    return model

def _get_tables_and_views(schema_name, include_views=True):
    """
    Get a list of all tables and optionally views in the specified schema.
    
    Args:
        schema_name (str): Name of the schema to query
        include_views (bool): Whether to include views in addition to tables
    
    Returns:
        list: List of dictionaries containing table/view information
        
    Raises:
        Exception: If Snowflake queries fail
    """
    global snowflake_connection
    
    with snowflake_connection.cursor(snowflake.connector.DictCursor) as cur:
        try:
            # Build SQL query based on whether to include views
            if include_views:
                table_type_filter = "('BASE TABLE', 'VIEW')"
            else:
                table_type_filter = "('BASE TABLE')"
                
            # Query INFORMATION_SCHEMA.TABLES
            cur.execute(f'''
                SELECT 
                    TABLE_NAME,
                    TABLE_TYPE
                FROM 
                    INFORMATION_SCHEMA.TABLES
                WHERE 
                    TABLE_SCHEMA = '{schema_name}'
                    AND TABLE_TYPE IN {table_type_filter}
            ''')
            tables = cur.fetchall()
            
            # If no results, try fallback method with SHOW TABLES
            if not tables:
                try:
                    # SHOW TABLES doesn't have a filter for table type,
                    # so we'll need to filter the results afterwards
                    cur.execute(f'''SHOW TABLES IN SCHEMA {schema_name}''')
                    all_tables = cur.fetchall()
                    
                    # Filter based on kind column (TABLE or VIEW)
                    if include_views:
                        tables = all_tables
                    else:
                        tables = [t for t in all_tables if t.get('kind', '').upper() == 'TABLE']
                        
                except Exception as e:
                    print(f"Error fetching tables with SHOW TABLES: {str(e)}")
            
            return tables
            
        except Exception as e:
            print(f"Error fetching tables: {str(e)}")
            return []

def _query_tables(schemas):
    """
    Query all tables in the given schemas.
    
    Args:
        schemas (list): List of schema names to query
        
    Returns:
        list: List of tuples containing (TABLE_SCHEMA, TABLE_NAME)
    """
    global snowflake_connection
    
    joined_schemas = _join_schemas(schemas)

    with snowflake_connection.cursor() as cur:
        cur.execute(f'''
            SELECT TABLE_SCHEMA,
                TABLE_NAME
            FROM TABLES
            WHERE TABLE_SCHEMA IN ({joined_schemas})
            ORDER BY TABLE_NAME;
        ''')
        tables = cur.fetchall()
    return tables 

def _get_foreign_keys(schema_name):
    """
    Query all foreign key constraints in a schema from Snowflake's metadata.
    
    This function tries multiple methods to retrieve foreign key information
    since different Snowflake versions and editions support different metadata views.
    
    Args:
        schema_name (str): Name of the schema to query foreign keys for
        
    Returns:
        list: List of dictionaries containing foreign key constraint information
        
    Raises:
        Exception: If all foreign key query methods fail
    """
    global snowflake_connection
    
    foreign_keys = []
    
    # Try different approaches to get foreign key information
    methods = [
        # Method 1: REFERENTIAL_CONSTRAINTS view (standard INFORMATION_SCHEMA)
        f'''
        SELECT 
            rc.CONSTRAINT_NAME,
            ccu.TABLE_SCHEMA as PK_SCHEMA_NAME,
            ccu.TABLE_NAME as PK_TABLE_NAME,
            ccu.COLUMN_NAME as PK_COLUMN_NAME,
            kcu.TABLE_SCHEMA as FK_SCHEMA_NAME,
            kcu.TABLE_NAME as FK_TABLE_NAME,
            kcu.COLUMN_NAME as FK_COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        JOIN 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME 
                AND rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
        JOIN 
            INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
                ON rc.UNIQUE_CONSTRAINT_NAME = ccu.CONSTRAINT_NAME 
                AND rc.UNIQUE_CONSTRAINT_SCHEMA = ccu.CONSTRAINT_SCHEMA
        WHERE 
            kcu.TABLE_SCHEMA = '{schema_name}'
        ''',
        
        # Method 2: Direct query to the Snowflake-specific system tables
        f'''
        SELECT 
            c.constraint_name,
            c.constraint_type,
            t1.TABLE_SCHEMA as FK_SCHEMA_NAME,
            t1.TABLE_NAME as FK_TABLE_NAME,
            k1.COLUMN_NAME as FK_COLUMN_NAME,
            t2.TABLE_SCHEMA as PK_SCHEMA_NAME,
            t2.TABLE_NAME as PK_TABLE_NAME,
            k2.COLUMN_NAME as PK_COLUMN_NAME
        FROM 
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
        JOIN 
            INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE t1 
                ON c.constraint_name = t1.constraint_name
        JOIN 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE k1 
                ON c.constraint_name = k1.constraint_name
        JOIN 
            INFORMATION_SCHEMA.CONSTRAINT_TABLE_USAGE t2 
                ON c.constraint_name = t2.constraint_name 
                AND t2.TABLE_NAME != t1.TABLE_NAME
        JOIN 
            INFORMATION_SCHEMA.KEY_COLUMN_USAGE k2 
                ON t2.constraint_name = k2.constraint_name 
                AND t2.TABLE_NAME = k2.TABLE_NAME
        WHERE 
            c.constraint_type = 'FOREIGN KEY' 
            AND t1.TABLE_SCHEMA = '{schema_name}'
        ''',
        
        # Method 3: Use SHOW IMPORTED KEYS command
        f'''SHOW IMPORTED KEYS IN {schema_name}.*;'''
    ]
    
    # Try each method until we get results
    for i, query in enumerate(methods):
        try:
            print(f"Trying foreign key query method {i+1}...")
            with snowflake_connection.cursor(snowflake.connector.DictCursor) as cur:
                cur.execute(query)
                results = cur.fetchall()
                
                if results:
                    print(f"Method {i+1} successful, found {len(results)} foreign keys.")
                    
                    # Handle SHOW IMPORTED KEYS special case (different column names)
                    if i == 2:  # Method 3
                        # Map the column names from SHOW IMPORTED KEYS to our standard format
                        foreign_keys = [{
                            'fk_schema_name': row.get('FK_DATABASE_NAME', row.get('fk_schema_name', '')),
                            'fk_table_name': row.get('FK_TABLE_NAME', row.get('fk_table_name', '')),
                            'fk_column_name': row.get('FK_COLUMN_NAME', row.get('fk_column_name', '')),
                            'pk_schema_name': row.get('PK_DATABASE_NAME', row.get('pk_schema_name', '')),
                            'pk_table_name': row.get('PK_TABLE_NAME', row.get('pk_table_name', '')),
                            'pk_column_name': row.get('PK_COLUMN_NAME', row.get('pk_column_name', ''))
                        } for row in results]
                    else:
                        foreign_keys = results
                    
                    # We found results, no need to try other methods
                    break
        except Exception as e:
            print(f"Foreign key query method {i+1} failed: {str(e)}")
    
    return foreign_keys

def _query_schema_data(schema_name):
    """
    Query schema data including tables, columns, and primary keys.
    
    Args:
        schema_name (str): Name of the schema to query
        
    Returns:
        list: List of tuples containing (schema_name, table_name, column_name, is_pk, data_type)
        
    Raises:
        Exception: If Snowflake queries fail
    """
    global snowflake_connection
    
    # Query primary keys
    primary_keys = {}
    
    try:
        print(f"Fetching primary keys for schema: {schema_name}")
        with snowflake_connection.cursor(snowflake.connector.DictCursor) as cur:
            # Method 1: Use KEY_COLUMN_USAGE
            try:
                cur.execute(f'''
                    SELECT 
                        TABLE_SCHEMA, 
                        TABLE_NAME, 
                        COLUMN_NAME
                    FROM 
                        INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE 
                        TABLE_SCHEMA = '{schema_name}'
                        AND CONSTRAINT_NAME LIKE 'PRIMARY%'
                ''')
                for row in cur.fetchall():
                    key = (row['TABLE_SCHEMA'], row['TABLE_NAME'], row['COLUMN_NAME'])
                    primary_keys[key] = True
            except Exception as e:
                print(f"Primary key query with KEY_COLUMN_USAGE failed: {str(e)}")
            
            # If we didn't get any results, try Method 2: SHOW PRIMARY KEYS
            if not primary_keys:
                try:
                    cur.execute(f'''SHOW PRIMARY KEYS IN {schema_name}.*;''')
                    for row in cur.fetchall():
                        pk_schema = row.get('schema_name', '')
                        pk_table = row.get('table_name', '')
                        pk_column = row.get('column_name', '')
                        key = (pk_schema, pk_table, pk_column)
                        primary_keys[key] = True
                except Exception as e:
                    print(f"Primary key query with SHOW PRIMARY KEYS failed: {str(e)}")
    except Exception as e:
        print(f"Error fetching primary keys: {str(e)}")
    
    print(f"Found {len(primary_keys)} primary keys")
    
    # Query all columns and tag as primary key if applicable
    result = []
    with snowflake_connection.cursor() as cur:
        cur.execute(f'''
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE
            FROM 
                INFORMATION_SCHEMA.COLUMNS
            WHERE 
                TABLE_SCHEMA = '{schema_name}'
            ORDER BY 
                TABLE_NAME, ORDINAL_POSITION;
        ''')
        
        for row in cur.fetchall():
            table_schema = row[0]
            table_name = row[1]
            column_name = row[2]
            data_type = row[3]
            
            # Check if this column is a primary key
            is_pk = primary_keys.get((table_schema, table_name, column_name), False)
            
            # Build the result row
            result_row = [
                table_schema,
                table_name,
                column_name,
                is_pk,
                data_type
            ]
            
            result.append(result_row)
    
    print(f"Total columns: {len(result)}")
    print(f"Total PK columns: {sum(1 for r in result if r[3])}")
    
    return result

def _join_schemas(schemas):
    """
    Join schema names for use in SQL IN clause.
    
    Args:
        schemas (list): List of schema names
        
    Returns:
        str: Comma-separated, quoted schema names for SQL
    """
    return ', '.join(f"'{k}'" for k in schemas)
