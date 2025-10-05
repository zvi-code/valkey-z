#!/usr/bin/env python3
"""Configuration utility for Valkey Search Delete Stress Test."""

import argparse
import sys
import yaml
from pathlib import Path
from typing import Dict, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from core.config import Config, ConfigValidator


def generate_example_config(output_path: Path) -> None:
    """Generate an example configuration file."""
    config = Config()
    config_dict = config.to_dict()
    
    # Add comments to make the config more understandable
    example_config = {
        "# Valkey connection settings": None,
        "valkey": config_dict["valkey"],
        
        "# Vector index configuration": None,
        "index": config_dict["index"],
        
        "# Workload execution settings": None,
        "workload": config_dict["workload"],
        
        "# Monitoring and metrics collection": None,
        "monitoring": config_dict["monitoring"],
        
        "# Output and logging settings": None,
        "output": config_dict["output"],
        
        "# S3 configuration for dataset storage": None,
        "s3": config_dict["s3"],
        
        "# Dataset preparation configuration": None,
        "datasets": config_dict["datasets"],
    }
    
    # Write to file
    with open(output_path, 'w') as f:
        for key, value in example_config.items():
            if key.startswith("#"):
                f.write(f"{key}\n")
            elif value is not None:
                yaml.dump({key: value}, f, default_flow_style=False)
                f.write("\n")
    
    print(f"Example configuration written to: {output_path}")


def validate_config_file(config_path: Path) -> bool:
    """Validate a configuration file."""
    try:
        config = Config(config_path)
        print(f"✓ Configuration file '{config_path}' is valid")
        return True
    except Exception as e:
        print(f"✗ Configuration file '{config_path}' is invalid: {e}")
        return False


def show_config_info(config_path: Path = None) -> None:
    """Show configuration information."""
    try:
        config = Config(config_path) if config_path else Config()
        
        print("Current Configuration:")
        print("=" * 50)
        
        print(f"Valkey:")
        print(f"  Host: {config.valkey.host}:{config.valkey.port}")
        print(f"  Database: {config.valkey.db}")
        print(f"  Max Connections: {config.valkey.max_connections}")
        
        print(f"\nVector Index:")
        print(f"  Algorithm: {config.index.algorithm}")
        print(f"  Dimensions: {config.index.dimensions}")
        print(f"  Distance Metric: {config.index.distance_metric}")
        if config.index.algorithm == "HNSW":
            print(f"  M: {config.index.m}")
            print(f"  EF Construction: {config.index.ef_construction}")
            print(f"  EF Runtime: {config.index.ef_runtime}")
        
        print(f"\nWorkload:")
        print(f"  Threads: {config.workload.n_threads}")
        print(f"  Clients: {config.workload.n_clients}")
        print(f"  Batch Size: {config.workload.batch_size}")
        print(f"  Query K: {config.workload.query_k}")
        
        print(f"\nS3 Storage:")
        print(f"  Bucket: {config.s3.bucket_name}")
        print(f"  Region: {config.s3.region}")
        print(f"  Multipart Threshold: {config.s3.multipart_threshold / (1024*1024):.0f}MB")
        print(f"  Max Concurrency: {config.s3.max_concurrency}")
        print(f"  Download Threads: {config.s3.download_threads}")
        
        print(f"\nDataset Preparation:")
        print(f"  Default Compression: {config.datasets.default_compression}")
        print(f"  Valkey Host: {config.datasets.valkey_host}:{config.datasets.valkey_port}")
        print(f"  Batch Size: {config.datasets.batch_size}")
        print(f"  Max Vector Dimensions: {config.datasets.max_vector_dimensions}")
        print(f"  Max Dataset Size: {config.datasets.max_dataset_size_gb}GB")
        print(f"  Processing Timeout: {config.datasets.processing_timeout_minutes} minutes")
        
        print(f"\nOutput:")
        print(f"  CSV Path: {config.output.csv_path}")
        print(f"  Summary Path: {config.output.summary_path}")
        print(f"  Log Level: {config.output.log_level}")
    
    except Exception as e:
        print(f"Error loading configuration: {e}")


def show_env_vars() -> None:
    """Show available environment variables."""
    print("Available Environment Variables:")
    print("=" * 50)
    
    print("Valkey:")
    print("  VALKEY_HOST - Valkey host (default: localhost)")
    print("  VALKEY_PORT - Valkey port (default: 6379)")
    print("  VALKEY_PASSWORD - Valkey password (default: none)")
    
    print("\nOutput:")
    print("  VST_OUTPUT_DIR - Output directory path")
    print("  VST_LOG_LEVEL - Log level (DEBUG, INFO, WARNING, ERROR)")
    
    print("\nS3 Configuration:")
    print("  AWS_S3_BUCKET - S3 bucket name")
    print("  AWS_DEFAULT_REGION - AWS region")
    print("  AWS_ACCESS_KEY_ID - AWS access key ID")
    print("  AWS_SECRET_ACCESS_KEY - AWS secret access key")
    print("  AWS_SESSION_TOKEN - AWS session token (for temporary credentials)")
    print("  VST_S3_MULTIPART_THRESHOLD - Multipart upload threshold in bytes")
    print("  VST_S3_MAX_CONCURRENCY - Maximum concurrent uploads/downloads")
    print("  VST_S3_DOWNLOAD_THREADS - Number of download threads")
    
    print("\nDataset Preparation:")
    print("  VST_VALKEY_HOST - Valkey host for RDB generation")
    print("  VST_VALKEY_PORT - Valkey port for RDB generation")
    print("  VST_VALKEY_PASSWORD - Valkey password for RDB generation")
    print("  VST_MEMORY_LIMIT_GB - Memory limit in GB for RDB generation")
    print("  VST_DEFAULT_COMPRESSION - Default compression (none, zstd, lz4)")
    print("  VST_DATASET_BATCH_SIZE - Batch size for dataset processing")
    print("  VST_PROCESSING_TIMEOUT_MINUTES - Processing timeout in minutes")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Configuration utility for Valkey Search Delete Stress Test",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Generate example config
    gen_parser = subparsers.add_parser("generate", help="Generate example configuration file")
    gen_parser.add_argument("output", type=Path, help="Output path for example config")
    
    # Validate config
    val_parser = subparsers.add_parser("validate", help="Validate configuration file")
    val_parser.add_argument("config", type=Path, help="Configuration file to validate")
    
    # Show config info
    info_parser = subparsers.add_parser("info", help="Show configuration information")
    info_parser.add_argument("--config", type=Path, help="Configuration file to read (optional)")
    
    # Show environment variables
    subparsers.add_parser("env-vars", help="Show available environment variables")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == "generate":
            generate_example_config(args.output)
        elif args.command == "validate":
            valid = validate_config_file(args.config)
            sys.exit(0 if valid else 1)
        elif args.command == "info":
            show_config_info(args.config)
        elif args.command == "env-vars":
            show_env_vars()
    
    except KeyboardInterrupt:
        print("\nOperation cancelled.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
