"""
Data import functions for ebird_db.
"""
from functools import lru_cache
import os
import json
from ebird_db.utils import logging as ul
import urllib.request
from datetime import datetime

import psycopg
from tqdm import tqdm

from . import (
    TMP_SAMPLING_TABLE,
    LOCALITIES_TABLE,
    CHECKLISTS_TABLE,
    SPECIES_TABLE,
    OBSERVATIONS_TABLE
)
from .connection import open_connection, vacuum
from .schema import (
    locality_columns,
    checklist_columns
)
from .. import archive_readers as ar

logger = ul.setup_logging()

def copy_sampling_file_to_temp_table(conn: psycopg.Connection, reader: ar.ArchiveMemberReader) -> None:
    """
    Copy data from a sampling file to a temporary table.
    
    Args:
        conn: Database connection
        reader: Archive reader for the sampling file
    """
    # Create the temporary table
    columns = ", ".join([f'{name} {type}' for name, type in locality_columns.items()])
    columns += ", "
    columns += ", ".join([f'{name} {type}' for name, type in checklist_columns.items()])
    create_query = f"CREATE TABLE IF NOT EXISTS {TMP_SAMPLING_TABLE} ({columns});"
    
    logger.info(f"Creating temporary table: {TMP_SAMPLING_TABLE}")
    logger.debug(f"Creating temp table with query: {create_query}")
    
    conn.execute(create_query)
    conn.commit()
    
    # Set up COPY command
    copy_cmd = f"""
    COPY {TMP_SAMPLING_TABLE} (
        locality_id, name, type, latitude, longitude, 
        sampling_event_id, last_edited_date, country, country_code, 
        state, state_code, county, county_code, iba_code, bcr_code, 
        usfws_code, atlas_block, observation_date, time_started, 
        observer_id, protocol_type, protocol_code, project_code, 
        duration_minutes, effort_distance_km, effort_area_ha, 
        number_observers, all_species_reported, group_identifier, 
        trip_comments
    ) FROM STDIN
    """
    
    # Copy data from file to database
    with conn.cursor() as cur:
        with cur.copy(copy_cmd) as copy:
            logger.info(f"Copying data from {reader.file_name} to {TMP_SAMPLING_TABLE}")
            logger.info(f"Total size is {reader.file_size} bytes")
            
            bytes_pbar = tqdm(desc='Bytes read', unit='B', total=reader.file_size, unit_scale=True)
            num_added = 0
            added_pbar = tqdm(desc='Checklists added')
            
            for line in tqdm(reader.lines(), desc='Lines read'):
                # Handle inconsistent column names
                if 'COUNTRY' not in line:
                    line['COUNTRY'] = line['country']
                    
                bytes_pbar.update(reader.last_bytes_read)
                
                # Write row to database
                copy.write_row((
                    line['LOCALITY ID'],
                    line['LOCALITY'],
                    line['LOCALITY TYPE'],
                    line['LATITUDE'],
                    line['LONGITUDE'],
                    line['SAMPLING EVENT IDENTIFIER'],
                    line['LAST EDITED DATE'],
                    line['COUNTRY'],
                    line['COUNTRY CODE'],
                    line['STATE'],
                    line['STATE CODE'],
                    line['COUNTY'],
                    line['COUNTY CODE'],
                    line['IBA CODE'],
                    line['BCR CODE'],
                    line['USFWS CODE'],
                    line['ATLAS BLOCK'],
                    line['OBSERVATION DATE'],
                    line['TIME OBSERVATIONS STARTED'],
                    line['OBSERVER ID'],
                    line['PROTOCOL TYPE'],
                    line['PROTOCOL CODE'],
                    line['PROJECT CODE'],
                    line['DURATION MINUTES'],
                    line['EFFORT DISTANCE KM'],
                    line['EFFORT AREA HA'],
                    line['NUMBER OBSERVERS'],
                    line['ALL SPECIES REPORTED'],
                    line['GROUP IDENTIFIER'],
                    line['TRIP COMMENTS']
                ))
                num_added += 1
                added_pbar.update(1)
                
        conn.commit()
        
    logger.info(f"Added {num_added} checklists to {TMP_SAMPLING_TABLE}")

def make_temp_sampling_table(ebird_file: str):
    """
    Create a temporary table with data from the sampling file.
    
    Args:
        ebird_file: Path to the eBird archive file
    """
    logger.info(f"Creating temporary sampling table from {ebird_file}")
    
    with open_connection() as conn:
        with ar.get_sampling_file_archive_member_reader(ebird_file) as reader:
            copy_sampling_file_to_temp_table(conn, reader)
            
    # Clean up after inserting
    vacuum(TMP_SAMPLING_TABLE)

def create_and_fill_locality_table():
    """Create and populate the localities table from the temporary table."""
    logger.info("Creating and populating localities table")
    
    # Prepare columns with primary key
    col_dict = locality_columns.copy()
    col_dict['locality_id'] = 'text primary key'
    
    # Create localities table
    columns = ", ".join([f'{name} {type}' for name, type in col_dict.items()])
    create_query = f"CREATE TABLE IF NOT EXISTS {LOCALITIES_TABLE} ({columns});"
    
    # Insert data from temporary table
    insert_query = f"""
    INSERT INTO {LOCALITIES_TABLE} (locality_id, name, type, latitude, longitude) 
    SELECT DISTINCT ON (locality_id) locality_id, name, type, latitude, longitude
    FROM {TMP_SAMPLING_TABLE}
    ON CONFLICT (locality_id) DO NOTHING;
    """
    
    logger.debug(f"Creating localities table with query: {create_query}")
    logger.debug(f"Populating localities table with query: {insert_query}")
    
    # Execute queries
    with open_connection() as conn:
        conn.execute(create_query)
        conn.commit()
        
        with conn.cursor() as cur:
            logger.info("Inserting data into localities table")
            cur.execute(insert_query)
            logger.info(f"Inserted {cur.rowcount} rows into {LOCALITIES_TABLE}")
            conn.commit()
            
    # Clean up
    vacuum(LOCALITIES_TABLE)

def create_and_fill_checklist_table():
    """Create and populate the checklists table from the temporary table."""
    logger.info("Creating and populating checklists table")
    
    # Prepare columns with primary key and foreign key
    col_dict = checklist_columns.copy()
    col_dict['sampling_event_id'] = 'text primary key'
    col_dict['locality_id'] = f'text references {LOCALITIES_TABLE}(locality_id)'
    
    # Create checklists table
    columns = ", ".join([f'{name} {type}' for name, type in col_dict.items()])
    create_query = f"CREATE TABLE IF NOT EXISTS {CHECKLISTS_TABLE} ({columns});"
    
    # Insert data from temporary table
    insert_query = f"""
    INSERT INTO {CHECKLISTS_TABLE} (
        sampling_event_id, last_edited_date, country, country_code,
        state, state_code, county, county_code, iba_code, bcr_code,
        usfws_code, atlas_block, observation_date, time_started,
        observer_id, protocol_type, protocol_code, project_code,
        duration_minutes, effort_distance_km, effort_area_ha,
        number_observers, all_species_reported, group_identifier,
        trip_comments, locality_id
    )
    SELECT DISTINCT ON (sampling_event_id)
        sampling_event_id, last_edited_date, country, country_code,
        state, state_code, county, county_code, iba_code, bcr_code,
        usfws_code, atlas_block, observation_date, time_started,
        observer_id, protocol_type, protocol_code, project_code,
        duration_minutes, effort_distance_km, effort_area_ha,
        number_observers, all_species_reported, group_identifier,
        trip_comments, locality_id
    FROM {TMP_SAMPLING_TABLE}
    ON CONFLICT (sampling_event_id) DO NOTHING;
    """
    
    logger.debug(f"Creating checklists table with query: {create_query}")
    logger.debug(f"Populating checklists table with query: {insert_query}")
    
    # Execute queries
    with open_connection() as conn:
        conn.execute(create_query)
        conn.commit()
        
        with conn.cursor() as cur:
            logger.info("Inserting data into checklists table")
            cur.execute(insert_query)
            logger.info(f"Inserted {cur.rowcount} rows into {CHECKLISTS_TABLE}")
            conn.commit()
            
    # Clean up
    vacuum(CHECKLISTS_TABLE)

def create_and_fill_species_table():
    """Create and populate the species table from the eBird API."""
    logger.info("Creating and populating species table")
    
    # Get API key
    api_key = os.environ.get("EBIRD_API_KEY")
    if api_key is None:
        raise ValueError("EBIRD_API_KEY environment variable not set")
    
    # Fetch species data from API
    logger.info("Fetching species data from eBird API")
    req = urllib.request.Request(
        "https://api.ebird.org/v2/ref/taxonomy/ebird?fmt=json", 
        headers={"X-eBirdApiToken": api_key}
    )
    
    species_json = []
    try:
        with urllib.request.urlopen(req) as response:
            if response.status == 200:
                species_json = json.loads(response.read().decode())
                logger.info(f"Retrieved {len(species_json)} species from API")
            else:
                raise ValueError(f"API request failed with status {response.status}")
    except Exception as e:
        logger.error(f"Failed to fetch species data: {e}")
        raise
        
    # Fill in missing keys
    for species in species_json:
        if 'order' not in species:
            species['order'] = None
        if 'familyCode' not in species:
            species['familyCode'] = None
        if 'familyComName' not in species:
            species['familyComName'] = None
        if 'familySciName' not in species:
            species['familySciName'] = None
    
    # Create species table
    create_species_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SPECIES_TABLE} (
        species_code            text primary key,
        common_name             text,
        scientific_name         text,
        category                text,
        taxon_order             int,
        banding_codes           text[],
        common_name_codes       text[],
        scientific_name_codes   text[],
        order_name              text,
        family_code             text,
        family_common_name      text,
        family_scientific_name  text
    )
    """
    
    # Insert species data
    with open_connection() as conn:
        logger.debug(f"Creating species table with query: {create_species_table_query}")
        conn.execute(create_species_table_query)
        conn.commit()
        
        with conn.cursor() as cur:
            logger.info(f"Inserting {len(species_json)} species into {SPECIES_TABLE}")
            
            insert_query = f"""
            INSERT INTO {SPECIES_TABLE} (
                species_code,
                common_name,
                scientific_name,
                category,
                taxon_order,
                banding_codes,
                common_name_codes,
                scientific_name_codes,
                order_name,
                family_code,
                family_common_name,
                family_scientific_name
            )
            VALUES (
                %(speciesCode)s,
                %(comName)s,
                %(sciName)s,
                %(category)s,
                %(taxonOrder)s,
                %(bandingCodes)s,
                %(comNameCodes)s,
                %(sciNameCodes)s,
                %(order)s,
                %(familyCode)s,
                %(familyComName)s,
                %(familySciName)s
            )
            ON CONFLICT (species_code) DO NOTHING
            """
            
            cur.executemany(insert_query, species_json)
            logger.info(f"Inserted {cur.rowcount} species into {SPECIES_TABLE}")
            conn.commit()
            
    # Clean up
    vacuum(SPECIES_TABLE)

@lru_cache(maxsize=1)
def make_species_code_map() -> dict[str, str]:
    """
    Create a mapping from scientific names to species codes.
    
    Returns:
        Dictionary mapping scientific names to species codes
    """
    logger.info("Creating species code mapping")
    
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT scientific_name, species_code FROM {SPECIES_TABLE}")
            species_map = {row[0]: row[1] for row in cur.fetchall()}
            
    logger.info(f"Created mapping for {len(species_map)} species")
    return species_map

def create_observations_table():
    """Create the observations table."""
    logger.info("Creating observations table")
    
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {OBSERVATIONS_TABLE} (
        global_unique_identifier    text primary key,
        sampling_event_id           text references {CHECKLISTS_TABLE}(sampling_event_id),
        species_code                text references {SPECIES_TABLE}(species_code),
        sub_species_code            text references {SPECIES_TABLE}(species_code),
        exotic_code                 text,
        observation_count           int,
        breeding_code               text,
        breeding_category           text,
        behavior_code               text,
        age_sex_code                text,
        species_comments            text,
        has_media                   bool,
        approved                    bool,
        reviewed                    bool,
        reason                      text
    )
    """
    
    with open_connection() as conn:
        logger.debug(f"Creating observations table with query: {create_query}")
        conn.execute(create_query)
        conn.commit()

def copy_observations_to_observations_table(
        conn: psycopg.Connection,
        reader: ar.ArchiveMemberReader,
        species_code_map: dict[str, str],
        start_date: datetime|None = None,
        end_date: datetime|None = None,
        state_code: str|None = None
    ) -> None:
    """
    Copy observation data to the observations table.
    
    Args:
        conn: Database connection
        reader: Archive reader for the observations file
        species_code_map: Mapping from scientific names to species codes
        start_date: Only include observations after this date
        end_date: Only include observations before this date
        state_code: Only include observations from this state
    """
    # Set up COPY command
    copy_cmd = f"""
    COPY {OBSERVATIONS_TABLE} (
        global_unique_identifier, sampling_event_id, species_code, 
        sub_species_code, exotic_code, observation_count, breeding_code, 
        breeding_category, behavior_code, age_sex_code, species_comments, 
        has_media, approved, reviewed, reason
    ) FROM STDIN
    """
    
    with conn.cursor() as cur:
        with cur.copy(copy_cmd) as copy:
            logger.info(f"Copying observations from {reader.file_name} to {OBSERVATIONS_TABLE}")
            logger.info(f"Total size is {reader.file_size} bytes")
            
            bytes_pbar = tqdm(desc='Bytes read', unit='B', total=reader.file_size, unit_scale=True)
            num_added = 0
            added_pbar = tqdm(desc='Observations added')
            num_skipped = 0
            skipped_pbar = tqdm(desc='Observations skipped')
            
            for line in tqdm(reader.lines(), desc='Lines read'):
                bytes_pbar.update(reader.last_bytes_read)
                
                # Apply filters
                if state_code and line['STATE CODE'] != state_code:
                    num_skipped += 1
                    skipped_pbar.update(1)
                    continue
                    
                if ((start_date or end_date) and line['OBSERVATION DATE']):
                    obs_date = datetime.strptime(line['OBSERVATION DATE'], '%Y-%m-%d')
                    if start_date and obs_date < start_date:
                        num_skipped += 1
                        skipped_pbar.update(1)
                        continue
                    if end_date and obs_date > end_date:
                        num_skipped += 1
                        skipped_pbar.update(1)
                        continue
                
                # Ensure species is in our map
                if line['SCIENTIFIC NAME'] not in species_code_map:
                    logger.warning(f"Species {line['SCIENTIFIC NAME']} not found in species table")
                    num_skipped += 1
                    skipped_pbar.update(1)
                    continue
                
                # Map scientific names to species codes
                line['species_code'] = species_code_map[line['SCIENTIFIC NAME']]
                
                # Handle subspecies
                if line['SUBSPECIES SCIENTIFIC NAME'] and line['SUBSPECIES SCIENTIFIC NAME'] in species_code_map:
                    line['sub_species_code'] = species_code_map[line['SUBSPECIES SCIENTIFIC NAME']]
                else:
                    line['sub_species_code'] = None
                
                # Handle count
                if line['OBSERVATION COUNT'] == 'X':
                    line['OBSERVATION COUNT'] = None
                
                # Write to database
                copy.write_row((
                    line['GLOBAL UNIQUE IDENTIFIER'],
                    line['SAMPLING EVENT IDENTIFIER'],
                    line['species_code'],
                    line['sub_species_code'],
                    line['EXOTIC CODE'],
                    line['OBSERVATION COUNT'],
                    line['BREEDING CODE'],
                    line['BREEDING CATEGORY'],
                    line['BEHAVIOR CODE'],
                    line['AGE/SEX'],
                    line['SPECIES COMMENTS'],
                    line['HAS MEDIA'],
                    line['APPROVED'],
                    line['REVIEWED'],
                    line['REASON']
                ))
                
                num_added += 1
                added_pbar.update(1)
                
        conn.commit()
        
    logger.info(f"Added {num_added} observations to {OBSERVATIONS_TABLE}, skipped {num_skipped}")

def create_and_fill_observations_table(
        ebird_file: str, 
        start_date: datetime|None = None, 
        end_date: datetime|None = None, 
        state_code: str|None = None
    ):
    """
    Create and populate the observations table.
    
    Args:
        ebird_file: Path to the eBird archive file
        start_date: Only include observations after this date
        end_date: Only include observations before this date
        state_code: Only include observations from this state
    """
    logger.info("Creating and populating observations table")
    
    # Create species code mapping
    species_code_map = make_species_code_map()
    
    # Create table
    create_observations_table()
    
    # Import data
    with ar.get_observations_file_archive_member_reader(ebird_file) as reader:
        with open_connection() as conn:
            copy_observations_to_observations_table(conn, reader, species_code_map, start_date, end_date, state_code)
            
    # Clean up
    vacuum(OBSERVATIONS_TABLE)