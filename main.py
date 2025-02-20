import csv
import json
import os
from typing import Any, Generator, LiteralString, OrderedDict
import psycopg
import tarfile
import gzip
import io
import argparse
from tqdm import tqdm
from psycopg.types.string import StrDumper

DB_NAME = "ebird"
TMP_SAMPLING_TABLE = "tmp_sampling_table"

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

def create_tables(conn: psycopg.Connection):
    create_sightings_table = """
    CREATE TABLE IF NOT EXISTS sightings (
        global_unique_identifier    text primary key,
        sampling_event_id           text references checklists(sampling_event_id),
        species_code                text references species(species_code),
        sub_species_code            text references species(species_code),
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

    with conn.cursor() as cur:
        cur.execute(create_sightings_table)
        conn.commit()


def lines_from_tar_member_with_suffix(tar: tarfile.TarFile, suffix: str) -> Generator[dict[str|Any, str|Any], None, None]:
    for member in tar.getmembers():
        if member.name.endswith(suffix):
            f = tar.extractfile(member)
            if f is not None:
                yield from csv.DictReader(io.TextIOWrapper(gzip.GzipFile(fileobj=f)), delimiter='\t')

class NullStrDumper(StrDumper):
    def dump(self, obj: str):
        if not obj or obj.isspace():
            return None
        return super().dump(obj)

def copy_sampling_file_to_temp_table(conn: psycopg.Connection, lines: Generator[dict[str, Any]]) -> None:
    columns = ", ".join([f'{name} {type}' for name, type in locality_columns.items()])
    columns += ", "
    columns += ", ".join([f'{name} {type}' for name, type in checklist_columns.items()])
    qs = f"CREATE TABLE IF NOT EXISTS {TMP_SAMPLING_TABLE} ({columns});"

    print(qs)
    conn.execute(qs)
    conn.commit()
    copy_cmd = f"COPY {TMP_SAMPLING_TABLE} (locality_id, name, type, latitude, longitude, sampling_event_id, last_edited_date, country, country_code, state, state_code, county, county_code, iba_code, bcr_code, usfws_code, atlas_block, observation_date, time_started, observer_id, protocol_type, protocol_code, project_code, duration_minutes, effort_distance_km, effort_area_ha, number_observers, all_species_reported, group_identifier, trip_comments) FROM STDIN"
    with conn.cursor() as cur:
        with cur.copy(copy_cmd) as copy:
            num: int = 0;
            for line in tqdm(lines):
                copy.write_row((
                    line['LOCALITY ID'],
                    line['LOCALITY'],
                    line['LOCALITY TYPE'],
                    line['LATITUDE'],
                    line['LONGITUDE'],
                    line['SAMPLING EVENT IDENTIFIER'],
                    line['LAST EDITED DATE'],
                    line['country'],
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
                num += 1
                #if num > 100000:
                #    break
        conn.commit()
    print(f"Wrote {num} checklists to tmp_sampling_table.")


def open_connection(autocommit: bool=False) -> psycopg.Connection:
    conn = psycopg.connect(f"dbname={DB_NAME} user={os.getenv("POSTGRES_USER")} password={os.getenv("POSTGRES_PWD")}", autocommit=autocommit)
    conn.adapters.register_dumper(str, NullStrDumper)
    return conn

def vacuum():
    with open_connection(autocommit=True) as conn:
        conn.execute('VACUUM FULL')

def make_temp_sampling_table(ebird_file: str):
    with open_connection() as conn:
        # Open the tar file and iterate over the lines in the sampling files.
        with tarfile.open(ebird_file, "r") as tar:
            # Copy the sampling file to a temp table.
            sampling_file_lines = lines_from_tar_member_with_suffix(tar, '_sampling.txt.gz')
            copy_sampling_file_to_temp_table(conn, sampling_file_lines)
    # Clean up after inserting so many rows.
    vacuum()

def create_and_fill_locality_table():
    col_dict = locality_columns.copy()
    col_dict['locality_id'] = 'text primary key'

    columns = ", ".join([f'{name} {type}' for name, type in col_dict.items()])
    create_q = f"CREATE TABLE IF NOT EXISTS localities ({columns});"
    print("Create localities table query:")
    print(create_q)
    
    insert_q = """
    INSERT INTO localities (locality_id, name, type, latitude, longitude) 
    SELECT DISTINCT ON (locality_id) locality_id, name, type, latitude, longitude
    FROM tmp_sampling_table;
    """
    print("Fill localities table query:")
    print(insert_q)

    with open_connection() as conn:
        conn.execute(create_q)
        conn.commit()
        conn.execute(insert_q)
        conn.commit()
    vacuum()

def create_and_fill_checklist_table():
    col_dict = checklist_columns.copy()
    col_dict['sampling_event_id'] = 'text primary key'
    col_dict['locality_id'] = 'text references localities(locality_id)'

    columns = ", ".join([f'{name} {type}' for name, type in col_dict.items()])
    create_q = f"CREATE TABLE IF NOT EXISTS checklists ({columns});"

    # This would probable be faster by re-COPYing the data from the TAR file. Will maaaaaybe investigate later.
    insert_q = """
    INSERT INTO checklists (
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
    FROM tmp_sampling_table;
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
    vacuum()


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
    create_species_table_q = """
    CREATE TABLE IF NOT EXISTS species (
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
            cur.executemany("""
                        INSERT INTO species (
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
    vacuum()


def main():
    parser = argparse.ArgumentParser(description="Process eBird data.")
    parser.add_argument("--ebird_file", type=str, help="The tar file containing eBird data")
    parser.add_argument("--stage",
                        type=str,
                        help="Which stage of the process to run",
                        choices=["copy_sampling", "localities", "checklists", "drop_sampling", "species", "observations"])
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
            conn.execute('VACUUM FULL')

    # Make table for species.
    if (args.stage == "species"):
        create_and_fill_species_table()

    # Make the big table of all the observations.
    if (args.stage == "observations"):
        pass

if __name__ == "__main__":
    main()