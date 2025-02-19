import csv
import os
from typing import Any, Generator
import psycopg
import tarfile
import gzip
import io
import argparse
from tqdm import tqdm
from psycopg.types.string import StrDumper

DB_NAME = "ebird"

def create_tables(conn: psycopg.Connection):

    create_localities_table = """
    CREATE TABLE IF NOT EXISTS localities (
        locality_id     text primary key,
        name            text,
        type            text, -- H for hidden, P for public
        lat_lng         geography(Point, 4326)
    )
    """

    create_checklists_table = """
    CREATE TABLE IF NOT EXISTS checklists (
        sampling_event_id       text primary key,
        locality_id             text references localities(locality_id),
        last_edited_date        timestamptz,
        country                 text,
        country_code            text,
        state                   text,
        state_code              text,
        county                  text,
        county_code             text,
        iba_code                text, -- important bird area
        bcr_code                text, -- bird conservation region
        usfws_code              text, -- US fish and wildlife service
        atlas_block             text,
        observation_date        date,
        time_started            time,
        observer_id             text,
        protocol_type           text, -- incidental, stationary, traveling
        protocol_code           text,
        project_code            text, -- ebird, atlas, etc
        duration_minutes        int,
        effort_distance_km      float,
        effort_area_ha          float,
        number_observers        int,
        all_species_reported    bool,
        group_identifier        text,
        trip_comments           text
    )
    """

    create_species_table = """
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
        family_scientific_name  text,
        taxon_concept_id        text,
        exotic_code             text
    )
    """

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
        cur.execute(create_localities_table)
        cur.execute(create_species_table)
        cur.execute(create_checklists_table)
        cur.execute(create_sightings_table)
        conn.commit()


def fmt_value(line: dict[str, Any], key: str, add_quotes: bool=True) -> str:
    v = line.get(key) or None
    if not v:
        return 'NULL'
    if add_quotes:
        return f"'{v.replace("'", "''")}'"
    else:
        return v



def insert_checklist(conn: psycopg.Connection, checklist_line: dict[str, Any]) -> None:

    insert_locality_query = """
    INSERT INTO localities (locality_id, name, type, lat_lng)
    VALUES (%(LOCALITY ID)s, %(LOCALITY)s, %(LOCALITY TYPE)s, ST_SetSRID(ST_MakePoint(%(LATITUDE)s::float, %(LONGITUDE)s::float), 4326))
    ON CONFLICT (locality_id) DO NOTHING
    """

    insert_checklist_query = """
    INSERT INTO checklists (sampling_event_id, locality_id, last_edited_date, country, country_code, state, state_code, county, county_code, iba_code, bcr_code, usfws_code, atlas_block, observation_date, time_started, observer_id, protocol_type, protocol_code, project_code, duration_minutes, effort_distance_km, effort_area_ha, number_observers, all_species_reported, group_identifier, trip_comments)
    VALUES (
        %(SAMPLING EVENT IDENTIFIER)s,
        %(LOCALITY ID)s,
        %(LAST EDITED DATE)s::timestamptz,
        %(country)s,
        %(COUNTRY CODE)s,
        %(STATE)s,
        %(STATE CODE)s,
        %(COUNTY)s,
        %(COUNTY CODE)s,
        %(IBA CODE)s,
        %(BCR CODE)s,
        %(USFWS CODE)s,
        %(ATLAS BLOCK)s,
        %(OBSERVATION DATE)s::date,
        %(TIME OBSERVATIONS STARTED)s::time,
        %(OBSERVER ID)s,
        %(PROTOCOL TYPE)s,
        %(PROTOCOL CODE)s,
        %(PROJECT CODE)s,
        %(DURATION MINUTES)s::int,
        %(EFFORT DISTANCE KM)s::float,
        %(EFFORT AREA HA)s::float,
        %(NUMBER OBSERVERS)s::int,
        %(ALL SPECIES REPORTED)s::bool,
        %(GROUP IDENTIFIER)s,
        %(TRIP COMMENTS)s
    )
    ON CONFLICT (sampling_event_id) DO NOTHING
    """

    with conn.cursor() as cur:
        cur.execute(insert_locality_query, checklist_line)
        cur.execute(insert_checklist_query, checklist_line)
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

def copy_localities(conn: psycopg.Connection, lines: Generator[dict[str, Any]]) -> None:
    with conn.cursor() as cur:
        with cur.copy("COPY localities (locality_id, name, type) FROM STDIN") as copy:
            num: int = 0;
            for line in tqdm(lines):
                #p = f"POINT({line['LATITUDE']}, {line['LONGITUDE']})"
                copy.write_row( (line['LOCALITY ID'], line['LOCALITY'], line['LOCALITY TYPE']))
                num += 1
    print(f"Wrote {num} localities.")


def main():
    parser = argparse.ArgumentParser(description="Process eBird data.")
    parser.add_argument("--ebird_file", type=str, help="The tar file containing eBird data")
    args = parser.parse_args()

    # Connect to the database.
    conn = psycopg.connect(f"dbname={DB_NAME} user={os.getenv("POSTGRES_USER")} password={os.getenv("POSTGRES_PWD")}")
    conn.adapters.register_dumper(str, NullStrDumper)

    # First, create the tables if they don't already exist.
    create_tables(conn)
    
    # Open the tar file and iterate over the lines in the sampling files.
    tar = tarfile.open(args.ebird_file, "r")
    sampling_file_lines = lines_from_tar_member_with_suffix(tar, '_sampling.txt.gz')
    copy_localities(conn, sampling_file_lines)
    # num_checklists_inserted: int = 0
    # for line in tqdm(sampling_file_lines):
    #     insert_checklist(conn, line)
    #     num_checklists_inserted += 1
    #     if num_checklists_inserted % 100000 == 0:
    #         print('.')
    
    # Close the tar file.
    tar.close()
    # Close the database connection.
    conn.close()


if __name__ == "__main__":
    main()