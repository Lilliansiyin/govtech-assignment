#!/usr/bin/env python3
import sys
import os
import argparse
import logging
from pathlib import Path
from datetime import datetime
# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipeline.orchestrator import DQOrchestrator
from src.utils.logger import setup_logger, flush_all_handlers


# Parse arguments first to get output directory
parser = argparse.ArgumentParser(description="Data Quality Monitoring Analysis")
parser.add_argument(
    "--data",
    type=str,
    default="test_data/grant_applications.csv",
    help="Path to input CSV file"
)
parser.add_argument(
    "--output",
    type=str,
    default="output",
    help="Output directory for results"
)
parser.add_argument(
    "--config",
    type=str,
    default=None,
    help="Path to configuration file (optional)"
)


def main():
    # Parse arguments FIRST to determine output directory
    args = parser.parse_args()
    
    # Resolve paths relative to script location
    script_dir = Path(__file__).parent.parent
    data_path = script_dir / args.data
    output_dir = script_dir / args.output
    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup logging with file output (configures root logger for all modules)
    log_level = getattr(logging, "INFO")
    log_file = output_dir / f"dq_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = setup_logger(__name__, log_level, str(log_file))
    logger.info(f"Logging configured. Log file: {log_file}")

    
    if not data_path.exists():
        logger.error(f"Data file not found: {data_path}")
        sys.exit(1)
    
    # Initialize orchestrator
    orchestrator = DQOrchestrator(config_path=args.config)
    
    try:
        # Setup
        orchestrator.setup()
        
        # Run analysis
        results = orchestrator.run_analysis(
            data_path=str(data_path),
            output_dir=str(output_dir)
        )
        
        # Print results
        logger.info("\n" + "="*50)
        logger.info("DATA QUALITY DIMENSION SCORES")
        logger.info("="*50)
        dimension_scores_df = results["dimension_scores_df"]
        dimension_scores_df.select("dimension_name", "score").show(truncate=False)
        logger.info("="*50)
        logger.info(f"Execution ID: {results['execution_id']}")
        logger.info(f"Analysis completed. Output directory: {output_dir.absolute()}")
        logger.info(f"Log file: {log_file}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        flush_all_handlers()
        sys.exit(1)
    finally:
        # Ensure all logs are written to disk before spark interferes with file handles
        root_logger = logging.getLogger()
        file_handlers = []
        for handler in root_logger.handlers:
            if isinstance(handler, logging.FileHandler):
                file_handlers.append(handler)
                try:
                    handler.flush()
                    if hasattr(handler, 'stream') and handler.stream:
                        handler.stream.flush()
                        os.fsync(handler.stream.fileno())
                except Exception:
                    pass
        
        try:
            orchestrator.cleanup()
            logger.info("DQ analysis completed successfully")
        except Exception as cleanup_error:
            try:
                logger.warning(f"Error during cleanup: {cleanup_error}")
            except:
                print(f"Error during cleanup: {cleanup_error}")
        
        # Final flush of all file handlers
        for handler in file_handlers:
            try:
                if hasattr(handler, 'stream') and handler.stream and not handler.stream.closed:
                    handler.flush()
                    handler.stream.flush()
                    os.fsync(handler.stream.fileno())
            except Exception:
                pass


if __name__ == "__main__":
    main()

