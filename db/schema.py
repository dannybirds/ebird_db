"""
Database schema definitions for ebird_db.
"""
from typing import LiteralString, OrderedDict
import logging

import psycopg

from db import (
    LOCALITIES_TABLE, 
    CHECKLISTS_TABLE, 
    SPECIES_TABLE, 
    OBSERVATIONS_TABLE
)

logger = logging.getLogger('ebird_db')

# Define column definitions for each table
locality_columns: dict[LiteralString, LiteralString] = OrderedDict({
    'locality_id': 'text',
    'name': 'text',
    'type': 'text',
    'latitude': 'float',
    'longitude': 'float'
})

checklist_columns: dict[LiteralString, LiteralString] = OrderedDict({
    'sampling_event_id': 'text',
    'last_edited_date': 'timestamptz',
    'country': 'text',
    'country_code': 'text',
    'state': 'text',
    'state_code': 'text',
    'county': 'text',
    'county_code': 'text',
    'iba_code': 'text',  # important bird area
    'bcr_code': 'text',  # bird conservation region
    'usfws_code': 'text',  # US fish and wildlife service
    'atlas_block': 'text',
    'observation_date': 'date',
    'time_started': 'time',
    'observer_id': 'text',
    'protocol_type': 'text',  # incidental, stationary, traveling
    'protocol_code': 'text',
    'project_code': 'text',  # ebird, atlas, etc
    'duration_minutes': 'int',
    'effort_distance_km': 'float',
    'effort_area_ha': 'float',
    'number_observers': 'int',
    'all_species_reported': 'bool',
    'group_identifier': 'text',
    'trip_comments': 'text'
})

species_columns: dict[LiteralString, LiteralString] = OrderedDict({
    'species_code': 'text',
    'common_name': 'text',
    'scientific_name': 'text',
    'category': 'text',  # species, hybrid, etc
    'taxon_order': 'int',
    'banding_codes': 'text[]',
    'common_name_codes': 'text[]',
    'scientific_name_codes': 'text[]',
    'order_name': 'text',
    'family_code': 'text',
    'family_common_name': 'text',
    'family_scientific_name': 'text'
})

observation_columns: dict[LiteralString, LiteralString] = OrderedDict({
    'global_unique_identifier': 'text',
    'sampling_event_id': 'text',
    'species_code': 'text',
    'sub_species_code': 'text',
    'exotic_code': 'text',
    'observation_count': 'int',  
    'breeding_code': 'text',
    'breeding_category': 'text',
    'behavior_code': 'text',
    'age_sex_code': 'text',
    'species_comments': 'text',
    'has_media': 'bool',
    'approved': 'bool',
    'reviewed': 'bool',
    'reason': 'text'
})

def get_create_table_statement(table_name: LiteralString, columns: dict[LiteralString, LiteralString], 
                              primary_key: str|None = None, references: dict[LiteralString, LiteralString]|None = None) -> LiteralString:
    """
    Generate a CREATE TABLE SQL statement.
    
    Args:
        table_name: Name of the table to create
        columns: Dictionary mapping column names to their SQL types
        primary_key: Name of the primary key column (optional)
        references: Dictionary mapping column names to referenced tables/columns (optional)
        
    Returns:
        SQL statement for creating the table
    """
    # Make a copy of the columns dictionary to modify
    cols = columns.copy()
    
    # Set primary key constraint if specified
    if primary_key and primary_key in cols:
        cols[primary_key] = f"{cols[primary_key]} PRIMARY KEY"
    
    # Add foreign key references if specified
    if references:
        for col, ref in references.items():
            if col in cols:
                cols[col] = f"{cols[col]} REFERENCES {ref}"
    
    # Build the column definitions string
    column_defs = ", ".join([f"{name} {type}" for name, type in cols.items()])
    
    # Return the complete CREATE TABLE statement
    return f"CREATE TABLE IF NOT EXISTS {table_name} ({column_defs});"

def create_tables(conn: psycopg.Connection) -> None:
    """
    Create all database tables if they don't exist.
    
    Args:
        conn: Database connection object
    """
    logger.info("Creating tables if they don't exist")
    
    # Create localities table
    locality_create = get_create_table_statement(
        LOCALITIES_TABLE,
        locality_columns,
        primary_key='locality_id'
    )
    
    # Create checklists table
    checklist_create = get_create_table_statement(
        CHECKLISTS_TABLE,
        checklist_columns,
        primary_key='sampling_event_id',
        references={'locality_id': f"{LOCALITIES_TABLE}(locality_id)"}
    )
    
    # Create species table
    species_create = get_create_table_statement(
        SPECIES_TABLE,
        species_columns,
        primary_key='species_code'
    )
    
    # Create observations table
    observation_create = get_create_table_statement(
        OBSERVATIONS_TABLE,
        observation_columns,
        primary_key='global_unique_identifier',
        references={
            'sampling_event_id': f"{CHECKLISTS_TABLE}(sampling_event_id)",
            'species_code': f"{SPECIES_TABLE}(species_code)",
            'sub_species_code': f"{SPECIES_TABLE}(species_code)"
        }
    )
    
    # Execute all create statements
    with conn.cursor() as cur:
        logger.debug(f"Creating localities table: {locality_create}")
        cur.execute(locality_create)
        
        logger.debug(f"Creating checklists table: {checklist_create}")
        cur.execute(checklist_create)
        
        logger.debug(f"Creating species table: {species_create}")
        cur.execute(species_create)
        
        logger.debug(f"Creating observations table: {observation_create}")
        cur.execute(observation_create)
        
        conn.commit()