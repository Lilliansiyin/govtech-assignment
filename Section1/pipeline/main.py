import sys
import os
import argparse
import zipfile
import tempfile
import glob

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.orchestration.pipeline_orchestrator import PipelineOrchestrator


def main():
    parser = argparse.ArgumentParser(description="Tax Data Pipeline")
    parser.add_argument("--csv-path", required=True, help="Path to input CSV file")
    
    # Use parse_known_args() to ignore Glue's internal arguments
    args, unknown = parser.parse_known_args()
    
    # Log unknown arguments for debugging (Glue passes many internal args we don't need)
    if unknown:
        print(f"Ignoring unknown arguments passed by Glue: {unknown}")
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    
    # Build list of possible base paths to search
    search_paths = [script_dir, cwd]
    # Also check sys.path locations (where Glue extracts dependencies.zip)
    for path in sys.path:
        if path and os.path.isdir(path):
            search_paths.append(path)
    
    # Search in /tmp subdirectories (Glue might extract to /tmp/glue-* or similar)
    tmp_search_paths = []
    if os.path.exists("/tmp"):
        for item in os.listdir("/tmp"):
            item_path = os.path.join("/tmp", item)
            if os.path.isdir(item_path) and ("glue" in item.lower() or "dependencies" in item.lower()):
                tmp_search_paths.append(item_path)
        # Also check common Glue extraction patterns
        for pattern in ["/tmp/*glue*", "/tmp/*dependencies*", "/tmp/glue-*"]:
            tmp_search_paths.extend(glob.glob(pattern))
    search_paths.extend(tmp_search_paths)
    
    # Try multiple possible locations for config files
    possible_config_paths = []
    possible_validation_paths = []
    possible_postal_paths = []
    
    for base_path in search_paths:
        if os.path.isdir(base_path):
            possible_config_paths.append(os.path.join(base_path, "config", "pipeline_config.yaml"))
            possible_validation_paths.append(os.path.join(base_path, "config", "validation_rules.yaml"))
            possible_postal_paths.append(os.path.join(base_path, "src", "utils", "postal_mapping.json"))
    
    # Also try relative paths
    possible_config_paths.extend(["config/pipeline_config.yaml", "./config/pipeline_config.yaml"])
    possible_validation_paths.extend(["config/validation_rules.yaml", "./config/validation_rules.yaml"])
    possible_postal_paths.extend(["src/utils/postal_mapping.json", "./src/utils/postal_mapping.json"])
    
    # Find the first existing path for each config file
    config_path = next((p for p in possible_config_paths if os.path.exists(p)), None)
    validation_rules_path = next((p for p in possible_validation_paths if os.path.exists(p)), None)
    postal_mapping_path = next((p for p in possible_postal_paths if os.path.exists(p)), None)
    
    # If files still not found, try to find and extract dependencies.zip
    if not all([config_path, validation_rules_path, postal_mapping_path]):
        print("Config files not found in standard locations, searching for dependencies.zip...")
        zip_paths = []
        # Search for dependencies.zip in common locations
        for search_dir in ["/tmp", cwd, script_dir] + [p for p in sys.path if p and os.path.isdir(p)]:
            zip_candidates = [
                os.path.join(search_dir, "dependencies.zip"),
                os.path.join(search_dir, "*dependencies*.zip"),
            ]
            for pattern in zip_candidates:
                zip_paths.extend(glob.glob(pattern))
        
        # Also search recursively in /tmp
        if os.path.exists("/tmp"):
            for root, dirs, files in os.walk("/tmp"):
                if "dependencies.zip" in files:
                    zip_paths.append(os.path.join(root, "dependencies.zip"))
        
        # Try to extract and use files from zip
        for zip_path in zip_paths:
            if os.path.exists(zip_path):
                try:
                    print(f"Found dependencies.zip at: {zip_path}")
                    # Extract to a temporary directory
                    extract_dir = tempfile.mkdtemp(prefix="glue_deps_")
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                    
                    # Check if config files are in extracted zip
                    extracted_config = os.path.join(extract_dir, "config", "pipeline_config.yaml")
                    extracted_validation = os.path.join(extract_dir, "config", "validation_rules.yaml")
                    extracted_postal = os.path.join(extract_dir, "src", "utils", "postal_mapping.json")
                    
                    if os.path.exists(extracted_config):
                        config_path = extracted_config
                    if os.path.exists(extracted_validation):
                        validation_rules_path = extracted_validation
                    if os.path.exists(extracted_postal):
                        postal_mapping_path = extracted_postal
                    
                    if all([config_path, validation_rules_path, postal_mapping_path]):
                        print(f"Successfully extracted and found config files from {zip_path}")
                        break
                except Exception as e:
                    print(f"Error extracting {zip_path}: {e}")
                    continue
    
    # Verify all config files were found
    if not config_path:
        raise FileNotFoundError(
            f"Config file not found. Tried: {possible_config_paths[:10]}... (showing first 10). "
            f"Current working directory: {os.getcwd()}, Script directory: {script_dir}. "
            f"Please ensure config/pipeline_config.yaml is included in dependencies.zip"
        )
    if not validation_rules_path:
        raise FileNotFoundError(
            f"Validation rules file not found. Tried: {possible_validation_paths[:10]}... "
            f"Please ensure config/validation_rules.yaml is included in dependencies.zip"
        )
    if not postal_mapping_path:
        raise FileNotFoundError(
            f"Postal mapping file not found. Tried: {possible_postal_paths[:10]}... "
            f"Please ensure src/utils/postal_mapping.json is included in dependencies.zip"
        )
    
    print(f"Using config_path: {config_path}")
    print(f"Using validation_rules_path: {validation_rules_path}")
    print(f"Using postal_mapping_path: {postal_mapping_path}")
    
    orchestrator = PipelineOrchestrator(
        config_path=config_path,
        validation_rules_path=validation_rules_path,
        postal_mapping_path=postal_mapping_path
    )
    
    results = orchestrator.run_full_pipeline(args.csv_path)
    
    print("\n" + "="*50)
    print("Pipeline Execution Summary")
    print("="*50)
    print(f"Bronze records: {results['bronze_count']}")
    print(f"Silver records: {results['silver_count']}")
    print(f"Rejected records: {results['rejected_count']}")
    print(f"Gold tables created: {', '.join(results['gold_tables'])}")
    print("="*50)


if __name__ == "__main__":
    main()

