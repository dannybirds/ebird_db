# ebird_db

`ebird_db` is a script that reads data from an eBird TAR file into a postgres database.

It's very far turnkey, but with some babysitting (and maybe commenting/un-commenting of code) you'll get it to work. You got this.

The TAR file must contain the sampling (checklist metadata) file in addition to the observations file.

## Setting things up

### Python
Packages required:
* [psycopg](https://www.psycopg.org/)
* [tqdm](https://github.com/tqdm/tqdm)

### Postgres
Of course, you'll need postgres installed and running. Create a database called `ebird` before trying to run anything.
```
$ psql --user=your_postgres_username
your_postgres_username=# CREATE DATABASE ebird;
```

### Enviroment vars
Set the below three enviroment variables with your sensitive data. Species info is read from the [eBird API](https://documenter.getpostman.com/view/664302/S1ENwy59), not the TAR file, so you'll need an eBird API key.
```bash
$ export POSTGRES_USER=your_postgres_username
$ export POSTGRES_PWD=your_postgres_password
$ export EBIRD_API_KEY=your_api_key
```

## Importing eBird data.

The script works in a few stages. Each stage has a `stage` command line argument that will cause it to run. You'll want to run them in this order! And you'll probably re-run them (maybe dropping a table in between) a few times.
1. `copy_sampling`: Copies all data from the sampling file into a `tmp_sampling_data` table.
2. `localities`: Makes a `localities` table and populates it with hotspot info from `tmp_sampling_data`.
3. `checklists`: Makes a `checklists` table and populates it with hotspot info from `tmp_sampling_data`.
4. `drop_sampling`: Drops the `tmp_sampling_table`. Optional if you want to save space.
6. `species`: Makes a `species` table with info about all species in the eBird taxonomy. Uses the eBird API.
7. `observations`: Still in progress!


## Example Queries


### How many checklists were submitted in Brooklyn, NY each year over the last 10 years?
```sql
SELECT 
    EXTRACT(year FROM observation_date) AS year,
    COUNT(*) AS count
FROM checklists
WHERE
    county_code = 'US-NY-047'
    AND observation_date > now() - interval '10 year'
GROUP BY year
ORDER BY year DESC;
```
```
 year | count 
------+-------
 2025 |  2484
 2024 | 34639
 2023 | 32036
 2022 | 26193
 2021 | 26903
 2020 | 20017
 2019 | 12604
 2018 | 10263
 2017 |  8820
 2016 |  7326
 2015 |  6868
(11 rows)
```