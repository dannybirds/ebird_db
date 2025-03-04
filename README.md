# eBird DB

`ebird_db` is a Python package that imports data from eBird archive files (tar or zip) into a PostgreSQL database for analysis and querying.

The archive file must contain the sampling (checklist metadata) file in addition to the observations file.

For large files, it's very far from turnkey, but with some babysitting (and maybe commenting/un-commenting of code) you'll get it to work. You got this.

## Features

- Import eBird data from tar or zip archives
- Support for filtering by date range and state
- Interactive setup mode
- Detailed progress reporting

## Installation

### Requirements

- Python 3.7+
- PostgreSQL database
- eBird API key (for species data)
- [psycopg](https://www.psycopg.org/)
- [tqdm](https://github.com/tqdm/tqdm)

### Installing the package

```bash
# Install from PyPI
pip install ebird-db

# Or install from source
git clone https://github.com/yourusername/ebird_db.git
cd ebird_db
pip install -e .
```

### Database setup

Create a PostgreSQL database before running:

```bash
$ psql --user=your_postgres_username
your_postgres_username=# CREATE DATABASE ebird;
```

## Configuration

You can configure `ebird_db` in several ways:

### Environment variables

```bash
export POSTGRES_USER=your_postgres_username
export POSTGRES_PWD=your_postgres_password
export EBIRD_API_KEY=your_ebird_api_key
```

### Configuration file

Create a file at `~/.ebird_db/config.ini`:

```ini
[database]
name = ebird_us
user = your_postgres_username
password = your_postgres_password

[api]
ebird_key = your_ebird_api_key
```

### Command-line options

```bash
ebird-db --config path/to/config.ini --ebird_file data.tar
```

## Usage

### Interactive mode

Interactive mode can guide you through the process:

```bash
ebird-db --interactive
```

### Importing all data at once

```bash
ebird-db --ebird_file path/to/ebird_data.tar --stage full
```

### Import specific stages

```bash
# Import only the sampling data
ebird-db --ebird_file path/to/ebird_data.tar --stage copy_sampling

# Create the species table
ebird-db --stage species
```

### Filter data during import

```bash
# Import only data from 2020-2022
ebird-db --ebird_file path/to/ebird_data.tar --stage full \
  --obs_start_date 2020-01-