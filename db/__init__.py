"""
Database module for ebird_db package.

This module contains functionality for connecting to the database,
defining the schema, and importing data.
"""

# Define constants for table names that will be used throughout the package
DB_NAME = "ebird_us"
TMP_SAMPLING_TABLE = "tmp_sampling_table"
LOCALITIES_TABLE = "localities"
CHECKLISTS_TABLE = "checklists"
SPECIES_TABLE = "species"
OBSERVATIONS_TABLE = "observations"