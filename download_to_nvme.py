#!/usr/bin/env python3
"""
Download datasets directly to NVMe (/mnt/data/datasets/)
by overriding the DATASET_LOCAL_DIR configuration
"""

import os
from vectordb_bench import config
from vectordb_bench.backend.dataset import Dataset

# Override the default dataset directory to use NVMe
config.DATASET_LOCAL_DIR = "/mnt/data/datasets"

print(f"Dataset directory configured to: {config.DATASET_LOCAL_DIR}")
print(f"Downloads will go directly to NVMe drive\n")

# Example: Download COHERE 1M dataset
print("=" * 80)
print("Example: Downloading COHERE 1M dataset...")
print("=" * 80)

data = Dataset.COHERE.manager(size=1_000_000)
print(f"Dataset: {data.data.name}")
print(f"Size: {data.data.size:,} vectors")
print(f"Dimensions: {data.data.dim}")
print(f"Metric: {data.data.metric_type}")
print(f"\nStarting download...")

data.prepare()

print("\n✓ Download complete!")
print(f"Dataset location: {config.DATASET_LOCAL_DIR}/cohere/")
