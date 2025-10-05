#!/bin/bash
# setup_datasets.sh - Quick setup script for downloading datasets

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATASET_DIR="${SCRIPT_DIR}/_datasets_downloads"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

check_requirements() {
    print_header "Checking Requirements"
    
    # Check available disk space (require at least 50GB for full setup)
    available_space=$(df "$SCRIPT_DIR" | awk 'NR==2 {print $4}')
    required_space=52428800  # 50GB in KB
    
    if [ "$available_space" -lt "$required_space" ]; then
        print_warning "Available disk space: $(($available_space / 1024 / 1024))GB"
        print_warning "Recommended: 50GB+ for full dataset collection"
        echo
    fi
    
    # Check if wget is available
    if ! command -v wget &> /dev/null; then
        print_error "wget is required but not installed"
        echo "Install wget: brew install wget (macOS) or apt-get install wget (Ubuntu)"
        exit 1
    fi
    
    # Check if Python is available
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is required but not installed"
        exit 1
    fi
    
    print_success "Requirements check passed"
    echo
}

create_directory() {
    print_header "Setting up directories"
    mkdir -p "$DATASET_DIR"
    print_success "Created dataset directory: $DATASET_DIR"
    echo
}

download_openai_datasets() {
    print_header "OpenAI Embedding Datasets"
    echo "These datasets contain high-quality text embeddings from OpenAI"
    echo
    
    # OpenAI-100K (small, for testing)
    echo -e "${BLUE}OpenAI-100K Dataset${NC} (~600MB)"
    echo "Recommended for: Quick testing, development"
    read -p "Download OpenAI-100K? [Y/n] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Skipping OpenAI-100K"
    else
        if [ ! -f "$DATASET_DIR/openai-100k.hdf5" ]; then
            echo "Downloading OpenAI-100K..."
            wget -O "$DATASET_DIR/openai-100k.hdf5" \
                "https://huggingface.co/_datasets_downloads/Qdrant/ann-benchmarks-openai/resolve/main/openai-100k.hdf5" || {
                print_error "Failed to download OpenAI-100K"
                return 1
            }
            print_success "OpenAI-100K downloaded"
        else
            print_success "OpenAI-100K already exists"
        fi
    fi
    echo
    
    # OpenAI-1M (medium, for development)
    echo -e "${BLUE}OpenAI-1M Dataset${NC} (~6GB)"
    echo "Recommended for: Development, medium-scale testing"
    read -p "Download OpenAI-1M? [Y/n] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Nn]$ ]]; then
        echo "Skipping OpenAI-1M"
    else
        if [ ! -f "$DATASET_DIR/openai-1m.hdf5" ]; then
            echo "Downloading OpenAI-1M (this may take a while)..."
            wget -O "$DATASET_DIR/openai-1m.hdf5" \
                "https://huggingface.co/_datasets_downloads/Qdrant/ann-benchmarks-openai/resolve/main/openai-1m.hdf5" || {
                print_error "Failed to download OpenAI-1M"
                return 1
            }
            print_success "OpenAI-1M downloaded"
        else
            print_success "OpenAI-1M already exists"
        fi
    fi
    echo
    
    # OpenAI-5M (large, for production testing)
    echo -e "${BLUE}OpenAI-5M Dataset${NC} (~30GB)"
    echo "Recommended for: Production testing, large-scale evaluation"
    print_warning "This is a large download and requires significant disk space"
    read -p "Download OpenAI-5M? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if [ ! -f "$DATASET_DIR/openai-5m.hdf5" ]; then
            echo "Downloading OpenAI-5M (this will take a long time)..."
            wget -O "$DATASET_DIR/openai-5m.hdf5" \
                "https://huggingface.co/_datasets_downloads/Qdrant/ann-benchmarks-openai/resolve/main/openai-5m.hdf5" || {
                print_error "Failed to download OpenAI-5M"
                return 1
            }
            print_success "OpenAI-5M downloaded"
        else
            print_success "OpenAI-5M already exists"
        fi
    else
        echo "Skipping OpenAI-5M"
    fi
    echo
}

download_sift_datasets() {
    print_header "SIFT Computer Vision Datasets"
    echo "Classic computer vision benchmarks with SIFT descriptors"
    echo
    
    echo -e "${BLUE}SIFT-1M Dataset${NC} (~500MB)"
    echo "Recommended for: Computer vision workloads, lower-dimensional testing"
    read -p "Download SIFT-1M? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if [ ! -f "$DATASET_DIR/sift-128d.hdf5" ]; then
            echo "Downloading and converting SIFT-1M..."
            cd "$DATASET_DIR"
            
            # Download original SIFT data
            wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz || {
                print_error "Failed to download SIFT dataset"
                return 1
            }
            
            tar -xzf sift.tar.gz
            rm sift.tar.gz
            
            # Note: Would need conversion tool here
            print_warning "SIFT data downloaded - conversion to HDF5 requires additional tools"
            print_success "SIFT-1M raw data available in $DATASET_DIR/sift/"
        else
            print_success "SIFT-1M already exists"
        fi
    else
        echo "Skipping SIFT-1M"
    fi
    echo
}

create_synthetic_datasets() {
    print_header "Synthetic Datasets"
    echo "Generate synthetic datasets for testing specific scenarios"
    echo
    
    read -p "Create synthetic datasets? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Creating synthetic datasets..."
        
        # Create a simple Python script for generating synthetic data
        cat > "$DATASET_DIR/create_synthetic.py" << 'EOF'
#!/usr/bin/env python3
"""Generate synthetic datasets for testing."""

import numpy as np
import h5py
from pathlib import Path

def create_synthetic_dataset(name, num_vectors, dimensions, distribution='gaussian'):
    """Create synthetic dataset."""
    print(f"Creating {name}: {num_vectors} vectors, {dimensions} dimensions, {distribution} distribution")
    
    if distribution == 'gaussian':
        vectors = np.random.normal(0, 1, (num_vectors, dimensions))
    elif distribution == 'uniform':
        vectors = np.random.uniform(-1, 1, (num_vectors, dimensions))
    elif distribution == 'clustered':
        # Create 10 clusters
        num_clusters = 10
        cluster_size = num_vectors // num_clusters
        vectors = []
        
        for i in range(num_clusters):
            center = np.random.normal(0, 5, dimensions)
            cluster_vectors = np.random.normal(center, 0.5, (cluster_size, dimensions))
            vectors.append(cluster_vectors)
        
        vectors = np.vstack(vectors)
        # Handle remainder
        if len(vectors) < num_vectors:
            remainder = num_vectors - len(vectors)
            extra = np.random.normal(0, 1, (remainder, dimensions))
            vectors = np.vstack([vectors, extra])
    
    # Convert to float32 and normalize
    vectors = vectors.astype(np.float32)
    norms = np.linalg.norm(vectors, axis=1, keepdims=True)
    vectors = vectors / (norms + 1e-8)
    
    # Create queries (subset of vectors)
    num_queries = min(10000, num_vectors // 10)
    query_indices = np.random.choice(num_vectors, num_queries, replace=False)
    queries = vectors[query_indices]
    
    # Save as HDF5
    output_path = f"{name}.hdf5"
    with h5py.File(output_path, 'w') as f:
        f.create_dataset('train', data=vectors, compression='gzip')
        f.create_dataset('test', data=queries, compression='gzip')
        f.attrs['distribution'] = distribution
        f.attrs['dimensions'] = dimensions
        f.attrs['num_vectors'] = num_vectors
    
    print(f"Created: {output_path}")

if __name__ == "__main__":
    # Create various synthetic datasets
    datasets = [
        ("synthetic-small", 10000, 128, "gaussian"),
        ("synthetic-medium", 100000, 512, "clustered"),
        ("synthetic-large", 1000000, 1536, "gaussian"),
    ]
    
    for name, vectors, dims, dist in datasets:
        create_synthetic_dataset(name, vectors, dims, dist)
EOF

        cd "$DATASET_DIR"
        python3 create_synthetic.py || {
            print_error "Failed to create synthetic datasets"
            return 1
        }
        
        rm create_synthetic.py
        print_success "Synthetic datasets created"
    else
        echo "Skipping synthetic datasets"
    fi
    echo
}

verify_datasets() {
    print_header "Verifying Downloaded Datasets"
    
    dataset_count=0
    
    for dataset in "$DATASET_DIR"/*.hdf5; do
        if [ -f "$dataset" ]; then
            filename=$(basename "$dataset")
            size=$(du -h "$dataset" | cut -f1)
            echo "  $filename ($size)"
            dataset_count=$((dataset_count + 1))
        fi
    done
    
    if [ $dataset_count -eq 0 ]; then
        print_warning "No datasets found"
    else
        print_success "Found $dataset_count dataset(s)"
    fi
    echo
    
    # Show total disk usage
    if [ -d "$DATASET_DIR" ]; then
        total_size=$(du -sh "$DATASET_DIR" | cut -f1)
        echo "Total dataset storage: $total_size"
    fi
    echo
}

create_dataset_config() {
    print_header "Creating Dataset Configuration"
    
    cat > "$DATASET_DIR/datasets.yaml" << 'EOF'
# Dataset configuration for Valkey Search Delete Stress Test
# This file maps dataset names to their file paths and metadata

datasets:
  # OpenAI embedding datasets
  openai-100k:
    path: _datasets_downloads/openai-100k.hdf5
    type: hdf5
    dimensions: 1536
    size: 100000
    description: "100K OpenAI text embeddings for quick testing"
    
  openai-1m:
    path: _datasets_downloads/openai-1m.hdf5
    type: hdf5
    dimensions: 1536
    size: 1000000
    description: "1M OpenAI text embeddings for development"
    
  openai-5m:
    path: _datasets_downloads/openai-5m.hdf5
    type: hdf5
    dimensions: 1536
    size: 5000000
    description: "5M OpenAI text embeddings for production testing"
  
  # SIFT computer vision datasets  
  sift-128d:
    path: _datasets_downloads/sift-128d.hdf5
    type: hdf5
    dimensions: 128
    size: 1000000
    description: "1M SIFT descriptors for computer vision workloads"
  
  # Synthetic datasets
  synthetic-small:
    path: _datasets_downloads/synthetic-small.hdf5
    type: hdf5
    dimensions: 128
    size: 10000
    description: "Small synthetic dataset for quick tests"
    
  synthetic-medium:
    path: _datasets_downloads/synthetic-medium.hdf5
    type: hdf5
    dimensions: 512
    size: 100000
    description: "Medium synthetic dataset with clustered data"
    
  synthetic-large:
    path: _datasets_downloads/synthetic-large.hdf5
    type: hdf5
    dimensions: 1536
    size: 1000000
    description: "Large synthetic dataset for stress testing"

# Default dataset for quick tests
default_dataset: synthetic-small
EOF
    
    print_success "Dataset configuration created: $DATASET_DIR/datasets.yaml"
    echo
}

show_next_steps() {
    print_header "Next Steps"
    echo "Your datasets are ready! Here's how to use them:"
    echo
    echo "1. Verify your setup:"
    echo "   vst info system"
    echo "   vst dataset list"
    echo
    echo "2. Run a quick test:"
    echo "   vst run quick --dataset synthetic-small"
    echo
    echo "3. Try a scenario with real data:"
    echo "   vst run --scenario config/scenarios/continuous_growth.yaml --dataset openai-100k"
    echo
    echo "4. Create custom scenarios:"
    echo "   cp config/scenarios/continuous_growth.yaml my_scenario.yaml"
    echo "   # Edit my_scenario.yaml"
    echo "   vst run --scenario my_scenario.yaml"
    echo
    echo "5. Read the documentation:"
    echo "   - Configuration: docs/CONFIGURATION.md"
    echo "   - Scenarios: docs/SCENARIOS.md"
    echo "   - Datasets: docs/DATASETS.md"
    echo
    print_success "Setup complete! Happy testing!"
}

main() {
    clear
    print_header "Search Module Delete Test Test - Dataset Setup"
    echo "This script will help you download and set up datasets for testing."
    echo
    
    check_requirements
    create_directory
    
    echo "Available dataset categories:"
    echo "1. OpenAI Embeddings (high-quality text embeddings)"
    echo "2. SIFT Computer Vision (image feature descriptors)"  
    echo "3. Synthetic Data (generated test data)"
    echo
    
    download_openai_datasets
    download_sift_datasets
    create_synthetic_datasets
    
    verify_datasets
    create_dataset_config
    show_next_steps
}

# Run main function
main
