import os
import psycopg2

DB_NAME = "ebird"

def create_tables():

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
        has_media               bool,
        approved                bool,
        reviewed                bool,
        reason                  text,
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
        species_comments            text
    )
    """

    
    with psycopg2.connect(f"dbname={DB_NAME} user={os.getenv("POSTGRES_USER")} password={os.getenv("POSTGRES_PWD")}") as conn:
        with conn.cursor() as cur:
            cur.execute(create_localities_table)
            cur.execute(create_species_table)
            cur.execute(create_checklists_table)
            cur.execute(create_sightings_table)


def main():
    create_tables()
    with psycopg2.connect(f"dbname={DB_NAME} user={os.getenv("POSTGRES_USER")} password={os.getenv("POSTGRES_PWD")}") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM pg_catalog.pg_tables;")
            rows = cur.fetchall()
            for row in rows:
                print(row)


if __name__ == "__main__":
    main()