# ebird_db

`ebird_db` is a script that reads data from an eBird tar or zip file into a postgres database.

For large files, it's very far from turnkey, but with some babysitting (and maybe commenting/un-commenting of code) you'll get it to work. You got this.

The archive file must contain the sampling (checklist metadata) file in addition to the observations file.

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
3. `checklists`: Makes a `checklists` table with one row per checklist in `tmp_sampling_data`. Hotspot info is not copied, instead it contains the `locality_id` as a foreign key to the `localities` table.
4. `drop_sampling`: Drops the `tmp_sampling_table`. Optional if you want to save space.
6. `species`: Makes a `species` table with info about all species in the eBird taxonomy. Uses the eBird API.
7. `observations`: Makes an `observations` table with one row per observation. Species linked to the `species` table via `species_code`, and checklists to the `checklists` table via `sampling_event_id`.

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


### What was the average number of Blue Jays seen at each hotspot on Christmas day?
```sql
SELECT
    loc.name,
    observation_date AS date, 
    ANY_VALUE(common_name) AS species,
    ROUND(AVG(observation_count),2) AS avg_count
FROM observations
JOIN checklists USING (sampling_event_id)
JOIN localities loc USING (locality_id)
JOIN species USING (species_code) 
WHERE
    EXTRACT(MONTH FROM observation_date) = 12
    AND EXTRACT(DAY FROM observation_date) = 25
    AND species_code = 'blujay'
GROUP BY 1, 2
ORDER BY avg_count DESC;
```
```
                                        name                                        |    date    | species  | avg_count 
------------------------------------------------------------------------------------+------------+----------+-----------
 Green-Wood Cemetery, Brooklyn                                                      | 2022-12-25 | Blue Jay |      9.75
 Prospect Park                                                                      | 2022-12-25 | Blue Jay |      7.17
 Bedford- Stuyvesant                                                                | 2021-12-25 | Blue Jay |      7.00
 Green-Wood Cemetery, Brooklyn                                                      | 2023-12-25 | Blue Jay |      6.00
 Green-Wood Cemetery, Brooklyn                                                      | 2024-12-25 | Blue Jay |      5.50
 Green-Wood Cemetery, Brooklyn                                                      | 2021-12-25 | Blue Jay |      5.50
 Prospect Park                                                                      | 2020-12-25 | Blue Jay |      5.50
 Prospect Park                                                                      | 2024-12-25 | Blue Jay |      5.33
 702 Monroe Street                                                                  | 2021-12-25 | Blue Jay |      5.00
 702 Monroe Street                                                                  | 2022-12-25 | Blue Jay |      5.00
 Sunset Park (5th-7th Ave.; 41st-44th St.)                                          | 2022-12-25 | Blue Jay |      5.00
 Prospect Park--Prospect Lake                                                       | 2023-12-25 | Blue Jay |      4.00
 Green-Wood Cemetery, Brooklyn                                                      | 2020-12-25 | Blue Jay |      4.00
 Prospect Park--Prospect Lake                                                       | 2021-12-25 | Blue Jay |      4.00
 Prospect Park                                                                      | 2023-12-25 | Blue Jay |      3.25
 Salt Marsh                                                                         | 2021-12-25 | Blue Jay |      3.00
 702 Monroe Street                                                                  | 2023-12-25 | Blue Jay |      3.00
 Owls Head Park                                                                     | 2020-12-25 | Blue Jay |      3.00
 83 Midwood St, New York US-NY 40.65987, -73.95799                                  | 2024-12-25 | Blue Jay |      3.00
 Prospect Park, New York US-NY (40.6544,-73.9715)                                   | 2020-12-25 | Blue Jay |      3.00
 Brooklyn Bridge Park                                                               | 2022-12-25 | Blue Jay |      3.00
 10th St, Park Slope                                                                | 2020-12-25 | Blue Jay |      3.00
 Herbert Von King Park                                                              | 2024-12-25 | Blue Jay |      3.00
 Prospect Park                                                                      | 2021-12-25 | Blue Jay |      2.80
 Greenwood Heights, Brooklyn (4th Ave.-Prospect Park West; Prospect Expy.-25th St.) | 2022-12-25 | Blue Jay |      2.00
 287 16th St, New York US-NY (40.6638,-73.9865)                                     | 2022-12-25 | Blue Jay |      2.00
 702 Monroe Street                                                                  | 2020-12-25 | Blue Jay |      2.00
 702 Monroe Street                                                                  | 2024-12-25 | Blue Jay |      2.00
 Bay Ridge - 86th Street                                                            | 2022-12-25 | Blue Jay |      2.00
 Bedford- Stuyvesant                                                                | 2022-12-25 | Blue Jay |      2.00
 Brooklyn Bridge Park                                                               | 2021-12-25 | Blue Jay |      2.00
 Brooklyn Bridge Park                                                               | 2024-12-25 | Blue Jay |      2.00
 Brooklyn Bridge Walkway, Brooklyn                                                  | 2022-12-25 | Blue Jay |      2.00
 Greenwood Heights, Brooklyn (4th Ave.-Prospect Park West; Prospect Expy.-25th St.) | 2021-12-25 | Blue Jay |      2.00
 Hello MODO                                                                         | 2024-12-25 | Blue Jay |      2.00
 Prospect Park--Prospect Lake                                                       | 2020-12-25 | Blue Jay |      2.00
 Prospect Park--Prospect Lake                                                       | 2022-12-25 | Blue Jay |      2.00
 Salt Marsh Nature Center at Marine Park                                            | 2024-12-25 | Blue Jay |      2.00
 Sunset Park (5th-7th Ave.; 41st-44th St.)                                          | 2023-12-25 | Blue Jay |      2.00
 Sutton Backyard                                                                    | 2023-12-25 | Blue Jay |      2.00
 287 16th St, New York US-NY 40.66380, -73.98655                                    | 2022-12-25 | Blue Jay |      1.50
 Dyker Beach Park                                                                   | 2022-12-25 | Blue Jay |      1.00
 Crown Heights - Mittens Feeder                                                     | 2024-12-25 | Blue Jay |      1.00
 gowanus waterfront                                                                 | 2024-12-25 | Blue Jay |      1.00
 Brooklyn Bridge Park                                                               | 2023-12-25 | Blue Jay |      1.00
 Brooklyn Bridge Park                                                               | 2020-12-25 | Blue Jay |      1.00
 Putnam and Franklin                                                                | 2023-12-25 | Blue Jay |      1.00
 Bay Ridge - 86th Street                                                            | 2024-12-25 | Blue Jay |      1.00
 Park Place (Backyard list), Brooklyn                                               | 2024-12-25 | Blue Jay |      1.00
 Pratt Institute, Brooklyn                                                          | 2023-12-25 | Blue Jay |      1.00
 My block                                                                           | 2020-12-25 | Blue Jay |      1.00
 McGolrick Park                                                                     | 2023-12-25 | Blue Jay |      1.00
 Greenwood Heights, Brooklyn (4th Ave.-Prospect Park West; Prospect Expy.-25th St.) | 2023-12-25 | Blue Jay |      1.00
 Home                                                                               | 2024-12-25 | Blue Jay |      1.00
(54 rows)
```