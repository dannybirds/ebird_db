import time
from contextlib import contextmanager

class ImportStats:
    """Track statistics for the import process."""
    def __init__(self):
        self.start_time = time.time()
        self.current_stage:str|None = None
        self.total_stages:int = 0
        self.current_stage_start = None
        self.stage_times: dict[str, float] = {}
    
    def start_stage(self, name:str, total_stages:int|None=None):
        """Start tracking a new stage."""
        self.current_stage = name
        self.current_stage_start = time.time()
        if total_stages:
            self.total_stages = total_stages
        print(f"\n=== Stage: {name} ===")
    
    def end_stage(self):
        """End the current stage and record statistics."""
        if self.current_stage and self.current_stage_start:
            duration = time.time() - self.current_stage_start
            self.stage_times[self.current_stage] = duration
            print(f"=== Completed in {duration:.1f} seconds ===\n")
            self.current_stage = None
            self.current_stage_start = None
    
    def summary(self):
        """Print a summary of the import process."""
        total_time = time.time() - self.start_time
        print("\n=== Import Summary ===")
        print(f"Total time: {total_time:.1f} seconds")
        for stage, duration in self.stage_times.items():
            print(f"{stage}: {duration:.1f} seconds ({duration/total_time*100:.1f}%)")

@contextmanager
def stage_context(stats:ImportStats, name:str, total_stages:int|None = None):
    """Context manager for import stages."""
    try:
        stats.start_stage(name, total_stages)
        yield
    except Exception as e:
        print(f"\n!!! Error in stage {name}: {e}")
        raise
    finally:
        stats.end_stage()