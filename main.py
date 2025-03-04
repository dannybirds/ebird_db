import argparse
import sys
from datetime import datetime

# Import modules (assuming new package structure)
from utils.logging import setup_logging
from utils.progress import ImportStats, stage_context
from db.importers import (
    make_temp_sampling_table,
    create_and_fill_locality_table,
    create_and_fill_checklist_table,
    create_and_fill_species_table,
    create_and_fill_observations_table
)
from db.connection import open_connection

def run_all_stages(stats: ImportStats, ebird_file: str, start_date: datetime | None = None, end_date: datetime | None = None, state_code: str | None = None):
    """Run all import stages with statistics tracking."""
    logger = setup_logging()
    logger.info(f"Starting import of {ebird_file}")
    
    total_stages = 6
    
    with stage_context(stats, "Copying sampling data", total_stages):
        make_temp_sampling_table(ebird_file)
    
    with stage_context(stats, "Creating localities table", total_stages):
        create_and_fill_locality_table()
    
    with stage_context(stats, "Creating checklists table", total_stages):
        create_and_fill_checklist_table()
    
    with stage_context(stats, "Dropping temporary tables", total_stages):
        with open_connection(autocommit=True) as conn:
            conn.execute('DROP TABLE IF EXISTS tmp_sampling_table')
    
    with stage_context(stats, "Creating species table", total_stages):
        create_and_fill_species_table()
    
    with stage_context(stats, "Creating observations table", total_stages):
        create_and_fill_observations_table(ebird_file, start_date, end_date, state_code)
    
    logger.info("Import completed successfully!")

def interactive_mode():
    """Run the script in interactive mode."""
    from cli import interactive_setup
    interactive_setup()

def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(description="Import eBird data into PostgreSQL")
    
    # Main options
    parser.add_argument("--ebird_file", type=str, help="The tar/zip file containing eBird data")
    parser.add_argument("--interactive", action="store_true", help="Run in interactive mode")
    parser.add_argument("--config", type=str, help="Path to configuration file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    parser.add_argument("--log-file", type=str, help="Write logs to this file")
    
    # Import options
    parser.add_argument("--stage", type=str, 
                      choices=["copy_sampling", "localities", "checklists", 
                               "drop_sampling", "species", "observations", "full"],
                      help="Stage to run (or 'full' for all stages)")
    parser.add_argument("--obs_start_date", type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                      help="Only observations after this date (YYYY-MM-DD)")
    parser.add_argument("--obs_end_date", type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
                      help="Only observations before this date (YYYY-MM-DD)")
    parser.add_argument("--obs_state_code", type=str, help="Only observations with this state code")
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging(verbose=args.verbose, log_file=args.log_file)
    
    try:        
        # Interactive mode
        if args.interactive:
            interactive_mode()
            return
                    
        # Import mode
        if not args.ebird_file and not (args.stage == "species"):
            parser.error("--ebird_file is required unless only --stage=species is specified")
            
        # Initialize stats
        stats = ImportStats()
        
        if args.stage == "full":
            run_all_stages(stats, args.ebird_file, args.obs_start_date, 
                         args.obs_end_date, args.obs_state_code)
        elif args.stage == "copy_sampling":
            with stage_context(stats, "Copying sampling data"):
                make_temp_sampling_table(args.ebird_file)
        elif args.stage == "localities":
            with stage_context(stats, "Creating localities table"):
                create_and_fill_locality_table()
        elif args.stage == "checklists":
            with stage_context(stats, "Creating checklists table"):
                create_and_fill_checklist_table()
        elif args.stage == "drop_sampling":
            with stage_context(stats, "Dropping temporary tables"):
                with open_connection(autocommit=True) as conn:
                    conn.execute('DROP TABLE IF EXISTS tmp_sampling_table')
        elif args.stage == "species":
            with stage_context(stats, "Creating species table"):
                create_and_fill_species_table()
        elif args.stage == "observations":
            with stage_context(stats, "Creating observations table"):
                create_and_fill_observations_table(args.ebird_file, args.obs_start_date, 
                                                args.obs_end_date, args.obs_state_code)
        
        # Print summary
        stats.summary()
            
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=args.verbose)
        sys.exit(1)

if __name__ == "__main__":
    main()