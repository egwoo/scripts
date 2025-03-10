import json
import sqlite3
from typing import Dict, Any, Set, List
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class JsonToSqlite:
    def __init__(self, db_path: str, root_table: str = 'root'):
        """Initialize the converter with database path and root table name."""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        self.known_tables: Dict[str, Set[str]] = {}
        self.root_table = self._sanitize_name(root_table)
        
    def _sanitize_name(self, name: str) -> str:
        """Convert a JSON key to a valid SQLite column name.
        Also handles SQLite reserved keywords by prefixing them with an underscore."""
        # List of SQLite reserved keywords that could appear as column names
        reserved_keywords = {
            'abort', 'action', 'add', 'after', 'all', 'alter', 'analyze', 'and', 'as', 'asc',
            'attach', 'autoincrement', 'before', 'begin', 'between', 'by', 'cascade', 'case',
            'cast', 'check', 'collate', 'column', 'commit', 'conflict', 'constraint', 'create',
            'cross', 'current', 'current_date', 'current_time', 'current_timestamp', 'database',
            'default', 'deferrable', 'deferred', 'delete', 'desc', 'detach', 'distinct', 'drop',
            'each', 'else', 'end', 'escape', 'except', 'exclusive', 'exists', 'explain', 'fail',
            'for', 'foreign', 'from', 'full', 'glob', 'group', 'having', 'if', 'ignore',
            'immediate', 'in', 'index', 'indexed', 'initially', 'inner', 'insert', 'instead',
            'intersect', 'into', 'is', 'isnull', 'join', 'key', 'left', 'like', 'limit', 'match',
            'natural', 'no', 'not', 'notnull', 'null', 'of', 'offset', 'on', 'or', 'order',
            'outer', 'plan', 'pragma', 'primary', 'query', 'raise', 'recursive', 'references',
            'regexp', 'reindex', 'release', 'rename', 'replace', 'restrict', 'right', 'rollback',
            'row', 'savepoint', 'select', 'set', 'table', 'temp', 'temporary', 'then', 'to',
            'transaction', 'trigger', 'type', 'union', 'unique', 'update', 'using', 'vacuum',
            'values', 'view', 'virtual', 'when', 'where', 'with', 'without'
        }
        
        # First sanitize the name to remove invalid characters
        sanitized = ''.join(c if c.isalnum() else '_' for c in name)
        
        # Then check if it's a reserved keyword (case-insensitive check)
        if sanitized.lower() in reserved_keywords:
            sanitized = f"_{sanitized}"
            
        return sanitized

    def _get_sqlite_type(self, value: Any) -> str:
        """Determine SQLite type from Python value."""
        if isinstance(value, bool):
            return 'BOOLEAN'
        elif isinstance(value, int):
            return 'INTEGER'
        elif isinstance(value, float):
            return 'REAL'
        elif isinstance(value, (dict, list)):
            return 'REFERENCE'  # This indicates we need a separate table
        else:
            return 'TEXT'

    def _create_table_if_not_exists(self, table_name: str, data: Dict[str, Any], parent_table: str = None) -> None:
        """Create a table if it doesn't exist, or alter it if it needs new columns."""
        table_name = self._sanitize_name(table_name)
        
        # Initialize table in known_tables if not present
        if table_name not in self.known_tables:
            self.known_tables[table_name] = set()
            
            # Create table with id and parent reference if needed
            columns = ['row__id INTEGER PRIMARY KEY AUTOINCREMENT']
            if parent_table:
                parent_ref = f'{parent_table}_id INTEGER'
                columns.append(parent_ref)
                
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(columns)}
                )
            """)
        
        # Process each field in the data
        for key, value in data.items():
            column_name = self._sanitize_name(key)
            
            if column_name not in self.known_tables[table_name]:
                sqlite_type = self._get_sqlite_type(value)
                
                if sqlite_type == 'REFERENCE':
                    # Handle nested objects and arrays
                    if isinstance(value, dict):
                        self._create_table_if_not_exists(
                            f"{table_name}__{column_name}",
                            value,
                            table_name
                        )
                    elif isinstance(value, list) and value and isinstance(value[0], dict):
                        self._create_table_if_not_exists(
                            f"{table_name}__{column_name}",
                            value[0],
                            table_name
                        )
                else:
                    # Add new column to existing table
                    try:
                        self.cursor.execute(f"""
                            ALTER TABLE {table_name}
                            ADD COLUMN {column_name} {sqlite_type}
                        """)
                        self.known_tables[table_name].add(column_name)
                    except sqlite3.OperationalError as e:
                        if "duplicate column name" not in str(e):
                            raise

    def _insert_data(self, table_name: str, data: Dict[str, Any], parent_id: int = None) -> int:
        """Insert data into a table and return the inserted row's ID."""
        try:
            table_name = self._sanitize_name(table_name)
            simple_data = {}
            nested_data = {}
            
            # Separate simple values from nested objects/arrays
            for key, value in data.items():
                column_name = self._sanitize_name(key)
                if isinstance(value, (dict, list)):
                    nested_data[column_name] = value
                else:
                    simple_data[column_name] = value
            
            # Insert simple data
            if simple_data:
                columns = list(simple_data.keys())
                values = list(simple_data.values())
                placeholders = ['?' for _ in values]
                
                if parent_id is not None:
                    # Get parent table name by splitting on double underscore
                    parent_table = '__'.join(table_name.split('__')[:-1])
                    parent_ref_col = f"{parent_table}_id"
                    columns.append(parent_ref_col)
                    values.append(parent_id)
                    placeholders.append('?')
                
                query = f"""
                    INSERT INTO {table_name} ({', '.join(columns)})
                    VALUES ({', '.join(placeholders)})
                """
                self.cursor.execute(query, values)
                row__id = self.cursor.lastrowid
            else:
                # If there's no simple data but we need a row for nested data
                if parent_id is not None:
                    parent_table = '__'.join(table_name.split('__')[:-1])
                    parent_ref_col = f"{parent_table}_id"
                    query = f"""
                        INSERT INTO {table_name} ({parent_ref_col})
                        VALUES (?)
                    """
                    self.cursor.execute(query, [parent_id])
                    row__id = self.cursor.lastrowid
                else:
                    query = f"INSERT INTO {table_name} DEFAULT VALUES"
                    self.cursor.execute(query)
                    row__id = self.cursor.lastrowid
            
            # Handle nested data
            for key, value in nested_data.items():
                nested_table = f"{table_name}__{key}"
                if isinstance(value, dict):
                    self._insert_data(nested_table, value, row__id)
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    for item in value:
                        self._insert_data(nested_table, item, row__id)
            
            return row__id
        except Exception as e:
            logger.error(f"Error inserting data: {table_name} {parent_id} {e}")
            raise e

    def process_file(self, file_path: str) -> None:
        """Process either a JSON Lines file or a regular JSON file and load it into SQLite."""
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        logger.info(f"Processing file: {file_path}")
        # Try to detect file format and process accordingly
        with open(file_path, 'r') as f:
            first_char = f.read(1)
            f.seek(0)  # Reset file pointer
            
            if first_char == '[':  # Regular JSON with array
                self._process_json_file(file_path)
            else:  # Assume JSON Lines
                self._process_jsonlines_file(file_path)

    def _process_json_file(self, file_path: Path) -> None:
        """Process a regular JSON file with a top-level array."""
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                
            if not isinstance(data, list):
                raise ValueError("Expected a top-level array in JSON file")
                
            # Process schema
            for item in data:
                self._create_table_if_not_exists(self.root_table, item)
            
            # Insert data
            for item_num, item in enumerate(data, 1):
                try:
                    self._insert_data(self.root_table, item)
                except Exception as e:
                    logger.error(f"Error processing item {item_num}: {e}")
                
                if item_num % 1000 == 0:
                    logger.info(f"Processed {item_num} items...")
                    self.conn.commit()
                    
            self.conn.commit()
            logger.info("File processing completed")
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON file: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing JSON file: {e}")
            raise

    def _process_jsonlines_file(self, file_path: Path) -> None:
        """Process a JSON Lines file."""
        # Process schema
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line)
                    self._create_table_if_not_exists(self.root_table, data)
                except json.JSONDecodeError as e:
                    logger.error(f"Analyze Schema - Error decoding JSON on line {line_num}: {e}")
                except Exception as e:
                    logger.error(f"Analyze Schema - Error processing line {line_num}: {e}")

        # Insert data
        with open(file_path, 'r') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line)
                    self._insert_data(self.root_table, data)
                except json.JSONDecodeError as e:
                    logger.error(f"Insert Data - Error decoding JSON on line {line_num}: {e}")
                except Exception as e:
                    logger.error(f"Insert Data - Error processing line {line_num}: {e}")
                
                if line_num % 1000 == 0:
                    logger.info(f"Processed {line_num} lines...")
                    self.conn.commit()

        self.conn.commit()
        logger.info("File processing completed")

    def close(self):
        """Close the database connection."""
        self.conn.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Convert JSON Lines file to SQLite database')
    parser.add_argument('input_file', help='Input JSON Lines file')
    parser.add_argument('--db', default='output.db', help='Output SQLite database file')
    parser.add_argument('--root-table', default='root', help='Name of the root table (default: root)')
    
    args = parser.parse_args()
    
    converter = JsonToSqlite(args.db, args.root_table)
    try:
        converter.process_file(args.input_file)
    finally:
        converter.close()

if __name__ == '__main__':
    main() 
