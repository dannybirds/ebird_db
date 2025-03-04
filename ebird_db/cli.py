"""
Command-line interface for ebird_db package.
"""
import os
import getpass
from datetime import datetime

from .utils.logging import setup_logging
from .utils.progress import ImportStats, stage_context
from .db.connection import open_connection

def interactive_setup():
    """Run an interactive setup to guide users through the process."""
    logger = setup_logging()
    logger.info("Starting interactive setup")
    
    print("\n====== eBird DB Interactive Setup ======\n")
    
    # Database configuration
    print("\n--- Database Configuration ---")
    db_name = input("Database name [ebird]: ").strip() or "ebird"
    db_user = input("PostgreSQL username: ").strip()
    db_pwd = getpass.getpass("PostgreSQL password: ")
    
    # API key
    print("\n--- API Configuration ---")
    api_key = input("eBird API key: ").strip()
    
    # File location
    print("\n--- Data file ---")
    ebird_file = input("Path to eBird data file (.tar or .zip): ").strip()
    
    # Optional filters
    print("\n--- Optional filters (press Enter to skip) ---")
    start_date_str = input("Start date (YYYY-MM-DD): ").strip()
    end_date_str = input("End date (YYYY-MM-DD): ").strip()
    state_code = input("State code (e.g. US-NY): ").strip()
    
    # Convert dates
    start_date = None
    end_date = None
    try:
        if start_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        if end_date_str:
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    except ValueError as e:
        print(f"Error parsing date: {e}")
        print("Using no date filter instead.")
        
    # Set environment variables for this session
    os.environ['POSTGRES_USER'] = db_user
    os.environ['POSTGRES_PWD'] = db_pwd
    os.environ['EBIRD_API_KEY'] = api_key
    os.environ['DB_NAME'] = db_name
    
    # Run the import
    print("\n--- Running eBird data import ---")
    try:
        # Dynamically import to avoid circular imports
        from .db.importers import (
            make_temp_sampling_table,
            create_and_fill_locality_table,
            create_and_fill_checklist_table,
            create_and_fill_species_table,
            create_and_fill_observations_table
        )
        
        stats = ImportStats()
        
        with stage_context(stats, "Copying sampling data", 6):
            make_temp_sampling_table(ebird_file)
        
        with stage_context(stats, "Creating localities table", 6):
            create_and_fill_locality_table()
        
        with stage_context(stats, "Creating checklists table", 6):
            create_and_fill_checklist_table()
        
        with stage_context(stats, "Dropping temporary tables", 6):
            with open_connection(autocommit=True) as conn:
                conn.execute('DROP TABLE IF EXISTS tmp_sampling_table')
        
        with stage_context(stats, "Creating species table", 6):
            create_and_fill_species_table()
        
        with stage_context(stats, "Creating observations table", 6):
            create_and_fill_observations_table(ebird_file, start_date, end_date, state_code)
        
        print("\n=== Import completed successfully! ===")
        stats.summary()
            
    except Exception as e:
        print(f"\n!!! Import failed: {e}")
        logger.error(f"Interactive import failed: {e}", exc_info=True)
        return False
    
    return True

def validate_archive_file(file_path:str) -> bool:
    """
    Validate that the specified file exists and is a valid archive.
    
    Args:
        file_path: Path to the archive file
        
    Returns:
        bool: True if valid, False otherwise
    """
    import os.path
    
    # Check if file exists
    if not os.path.isfile(file_path):
        print(f"Error: File '{file_path}' does not exist.")
        return False
    
    # Check file extension
    if not (file_path.endswith('.tar') or file_path.endswith('.zip')):
        print(f"Error: File '{file_path}' is not a .tar or .zip archive.")
        return False
    
    # Basic check to see if it's a valid archive
    try:
        if file_path.endswith('.tar'):
            import tarfile
            with tarfile.open(file_path, 'r') as tar:
                # Just try to list contents
                tar.getnames()
        else:  # .zip
            import zipfile
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                # Just try to list contents
                zip_file.namelist()
    except Exception as e:
        print(f"Error: '{file_path}' is not a valid archive: {e}")
        return False
    
    return True