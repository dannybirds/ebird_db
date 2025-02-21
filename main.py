from datetime import datetime
import json
import os
from typing import LiteralString, OrderedDict
import psycopg
from psycopg.types.string import StrDumper
import argparse
from tqdm import tqdm
import pprint

from archive_readers import ArchiveMemberReader, get_observations_file_archive_member_reader, get_sampling_file_archive_member_reader

DB_NAME = "ebird_us"
TMP_SAMPLING_TABLE = "tmp_sampling_table"
LOCALITIES_TABLE = "localities"
CHECKLISTS_TABLE = "checklists"
SPECIES_TABLE = "species"
OBSERVATIONS_TABLE = "observations"


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

class NullStrDumper(StrDumper):
    def dump(self, obj: str):
        if not obj or obj.isspace():
            return None
        return super().dump(obj)

def open_connection(autocommit: bool=False) -> psycopg.Connection:
    conn = psycopg.connect(f"dbname={DB_NAME} user={os.getenv("POSTGRES_USER")} password={os.getenv("POSTGRES_PWD")}", autocommit=autocommit)
    conn.adapters.register_dumper(str, NullStrDumper)
    return conn

def vacuum(table: LiteralString):
    with open_connection(autocommit=True) as conn:
        conn.execute(f'VACUUM {table}')

def copy_sampling_file_to_temp_table(conn: psycopg.Connection, reader: ArchiveMemberReader) -> None:
    columns = ", ".join([f'{name} {type}' for name, type in locality_columns.items()])
    columns += ", "
    columns += ", ".join([f'{name} {type}' for name, type in checklist_columns.items()])
    qs = f"CREATE TABLE IF NOT EXISTS {TMP_SAMPLING_TABLE} ({columns});"

    pprint.pp(qs)
    conn.execute(qs)
    conn.commit()
    copy_cmd = f"COPY {TMP_SAMPLING_TABLE} (locality_id, name, type, latitude, longitude, sampling_event_id, last_edited_date, country, country_code, state, state_code, county, county_code, iba_code, bcr_code, usfws_code, atlas_block, observation_date, time_started, observer_id, protocol_type, protocol_code, project_code, duration_minutes, effort_distance_km, effort_area_ha, number_observers, all_species_reported, group_identifier, trip_comments) FROM STDIN"
    with conn.cursor() as cur:
        with cur.copy(copy_cmd) as copy:
            pprint.pp(f"Copying observations from {reader.file_name} to observations table.")
            pprint.pp(f"Total size is {reader.file_size} bytes.")
            bytes_pbar = tqdm(desc='bytes read', unit='B', total=reader.file_size, unit_scale=True)
            num_added: int = 0;
            added_pbar = tqdm(desc='checklists added')
            for line in tqdm(reader.lines(), desc='lines read'):
                if 'COUNTRY' not in line:
                    line['COUNTRY']= line['country']
                bytes_pbar.update(reader.last_bytes_read)
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
    pprint.pp(f"Wrote {num_added} checklists to tmp_sampling_table.")



def make_temp_sampling_table(ebird_file: str):
    with open_connection() as conn:
        # Open the tar file and iterate over the lines in the sampling files.
        with get_sampling_file_archive_member_reader(ebird_file) as reader:
            copy_sampling_file_to_temp_table(conn, reader)
    # Clean up after inserting so many rows.
    vacuum(TMP_SAMPLING_TABLE)

def create_and_fill_locality_table():
    col_dict = locality_columns.copy()
    col_dict['locality_id'] = 'text primary key'

    columns = ", ".join([f'{name} {type}' for name, type in col_dict.items()])
    create_q = f"CREATE TABLE IF NOT EXISTS {LOCALITIES_TABLE} ({columns});"
    print("Create localities table query:")
    print(create_q)
    
    insert_q = f"""
    INSERT INTO {LOCALITIES_TABLE} (locality_id, name, type, latitude, longitude) 
    SELECT DISTINCT ON (locality_id) locality_id, name, type, latitude, longitude
    FROM tmp_sampling_table
    ON CONFLICT (locality_id) DO NOTHING;
    """
    print("Fill localities table query:")
    print(insert_q)

    with open_connection() as conn:
        conn.execute(create_q)
        conn.commit()
        conn.execute(insert_q)
        conn.commit()
    vacuum(LOCALITIES_TABLE)

def create_and_fill_checklist_table():
    col_dict = checklist_columns.copy()
    col_dict['sampling_event_id'] = 'text primary key'
    col_dict['locality_id'] = f'text references {LOCALITIES_TABLE}(locality_id)'

    columns = ", ".join([f'{name} {type}' for name, type in col_dict.items()])
    create_q = f"CREATE TABLE IF NOT EXISTS checklists ({columns});"

    # This would probable be faster by re-COPYing the data from the TAR file. Will maaaaaybe investigate later.
    insert_q = f"""
    INSERT INTO {CHECKLISTS_TABLE} (
        sampling_event_id,
        last_edited_date,
        country,
        country_code,
        state,
        state_code,
        county,
        county_code,
        iba_code,
        bcr_code,
        usfws_code,
        atlas_block,
        observation_date,
        time_started,
        observer_id,
        protocol_type,
        protocol_code,
        project_code,
        duration_minutes,
        effort_distance_km,
        effort_area_ha,
        number_observers,
        all_species_reported,
        group_identifier,
        trip_comments,
        locality_id
    )
    SELECT
        DISTINCT ON (sampling_event_id)
        sampling_event_id,
        last_edited_date,
        country,
        country_code,
        state,
        state_code,
        county,
        county_code,
        iba_code,
        bcr_code,
        usfws_code,
        atlas_block,
        observation_date,
        time_started,
        observer_id,
        protocol_type,
        protocol_code,
        project_code,
        duration_minutes,
        effort_distance_km,
        effort_area_ha,
        number_observers,
        all_species_reported,
        group_identifier,
        trip_comments,
        locality_id
    FROM {TMP_SAMPLING_TABLE}
    ON CONFLICT (sampling_event_id) DO NOTHING;
    """

    print("Create checklists table query:")
    print(create_q)
    print("Fill checklists table query:")
    print(insert_q)

    with open_connection() as conn:
        conn.execute(create_q)
        conn.commit()
        conn.execute(insert_q)
        conn.commit()
    vacuum('checklists')


def create_and_fill_species_table():
    import urllib.request
    api_key = os.environ.get("EBIRD_API_KEY")
    if api_key is None:
        raise Exception("EBIRD_API_KEY environment variable not set")
    req = urllib.request.Request("https://api.ebird.org/v2/ref/taxonomy/ebird?fmt=json", headers={"X-eBirdApiToken": api_key})
    species_json = []
    with urllib.request.urlopen(req) as response:
        if response.status == 200:
            species_json = json.loads(response.read().decode())
        else:
            raise Exception(f"Error: {response.status}")
        
    # Fill in any missing keys.
    for species in species_json:
        if 'order' not in species:
            species['order'] = None
        if 'familyCode' not in species:
            species['familyCode'] = None
        if 'familyComName' not in species:
            species['familyComName'] = None
        if 'familySciName' not in species:
            species['familySciName'] = None
    
    # We've got the species data, so create the table.
    create_species_table_q = f"""
    CREATE TABLE IF NOT EXISTS {SPECIES_TABLE} (
        species_code            text primary key,
        common_name             text,
        scientific_name         text,
        category                text, -- species, hybrid, etc
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
    with open_connection() as conn:
        conn.execute(create_species_table_q)
        conn.commit()
        with conn.cursor() as cur:
            cur.executemany(f"""
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
            """,
            species_json)
            cur.connection.commit()
    vacuum(SPECIES_TABLE)


def make_species_code_map() -> dict[str, str]:
    with open_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT scientific_name, species_code FROM {SPECIES_TABLE}")
            return {row[0]: row[1] for row in cur.fetchall()}

def create_observations_table():
    create_observations_table = f"""
    CREATE TABLE IF NOT EXISTS {OBSERVATIONS_TABLE} (
        global_unique_identifier    text primary key,
        sampling_event_id           text references checklists(sampling_event_id),
        species_code                text references species(species_code),
        sub_species_code            text references species(species_code),
        exotic_code                 text,
        observation_count           int, -- negative 1 indicates "X"
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
        with conn.cursor() as cur:
            cur.execute(create_observations_table)
            conn.commit()

def copy_observations_to_observations_table(
        conn: psycopg.Connection,
        reader: ArchiveMemberReader,
        species_code_map: dict[str,str],
        start_date: datetime|None=None,
        end_date: datetime|None=None,
        state_code: str|None=None
    ) -> None:
    copy_cmd = f"COPY {OBSERVATIONS_TABLE} (global_unique_identifier, sampling_event_id, species_code, sub_species_code, exotic_code, observation_count, breeding_code, breeding_category, behavior_code, age_sex_code, species_comments, has_media, approved, reviewed, reason) FROM STDIN"
    with conn.cursor() as cur:
        with cur.copy(copy_cmd) as copy:
            pprint.pp(f"Copying observations from {reader.file_name} to observations table.")
            pprint.pp(f"Effective size is {reader.file_size} bytes.")
            bytes_pbar = tqdm(desc='bytes read', unit='B', total=reader.file_size, unit_scale=True)
            num_added: int = 0;
            added_pbar = tqdm(desc='observations added')
            num_skipped: int = 0;
            skipped_pbar = tqdm(desc='observations skipped')
            for line in tqdm(reader.lines(), desc='lines read'):
                bytes_pbar.update(reader.last_bytes_read)
                if(state_code and line['STATE CODE'] != state_code):
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

                assert line['SCIENTIFIC NAME'] in species_code_map, f"Species {line['SCIENTIFIC NAME']} not found in species table"
                assert line['SUBSPECIES SCIENTIFIC NAME'] is not None
                
                line['species_code'] = species_code_map[line['SCIENTIFIC NAME']]
                line['sub_species_code'] = species_code_map.get(line['SUBSPECIES SCIENTIFIC NAME'], None)
                if line['OBSERVATION COUNT'] == 'X':
                    line['OBSERVATION COUNT'] = None
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
    pprint.pp(f"Wrote {num_added} observations to observations table, skipped {num_skipped}.")

def create_and_fill_observations_table(ebird_file: str, start_date: datetime|None=None, end_date: datetime|None=None, state_code: str|None=None):
    # Make a map from scientific name to species code.
    species_code_map = make_species_code_map()
    # Create the table
    create_observations_table()
    with get_observations_file_archive_member_reader(ebird_file) as reader:
        with open_connection() as conn:
            copy_observations_to_observations_table(conn, reader, species_code_map, start_date, end_date, state_code)
    # Clean up after inserting so many rows.
    vacuum(OBSERVATIONS_TABLE)


def main():
    parser = argparse.ArgumentParser(description="Process eBird data.")
    parser.add_argument("--ebird_file", type=str, help="The tar file containing eBird data")
    parser.add_argument("--stage",
                        type=str,
                        help="Which stage of the process to run",
                        choices=["copy_sampling", "localities", "checklists", "drop_sampling", "species", "observations"])
    parser.add_argument("--obs_start_date",
                        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="Only observations after this date will be processed (format: YYYY-MM-DD)")
    parser.add_argument("--obs_end_date",
                        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                        help="Only observations before this date will be processed (format: YYYY-MM-DD)")
    parser.add_argument("--obs_state_code", action="store_true", help="Only observations with this state code will be processed")
    args = parser.parse_args()

    # Read in raw sampling data.
    if (args.stage == "copy_sampling"):
        make_temp_sampling_table(args.ebird_file)

    # Then copy the sampling data to the localities and checklists tables.    
    if (args.stage == "localities"):    
        create_and_fill_locality_table()
    if (args.stage == "checklists"):
        create_and_fill_checklist_table()

    if (args.stage == "drop_sampling"):    
        # And delete the temp table and vacuum.
        with psycopg.connect(f"dbname={DB_NAME} user={os.getenv("POSTGRES_USER")} password={os.getenv("POSTGRES_PWD")}", autocommit=True) as conn:
            conn.execute(f'DROP TABLE {TMP_SAMPLING_TABLE}')

    # Make table for species.
    if (args.stage == "species"):
        create_and_fill_species_table()

    # Make the big table of all the observations.
    if (args.stage == "observations"):
        create_and_fill_observations_table(args.ebird_file, args.obs_start_date, args.obs_end_date, args.obs_state_code)

if __name__ == "__main__":
    main()