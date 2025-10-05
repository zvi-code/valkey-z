# Dataset Guide

This comprehensive guide covers working with datasets in the Search Module Delete Test Testing tool, including public datasets, custom datasets, and data preparation techniques.

## Table of Contents

- [Overview](#overview)
- [Public Datasets](#public-datasets)
- [Dataset Formats](#dataset-formats)
- [Downloading Datasets](#downloading-datasets)
- [Dataset Preparation](#dataset-preparation)
- [Custom Datasets](#custom-datasets)
- [Dataset Configuration](#dataset-configuration)
- [Performance Optimization](#performance-optimization)
- [Validation and Quality](#validation-and-quality)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

## Overview

The Search Module Delete Test Testing tool supports various vector datasets for testing different scenarios:

- **Public Datasets**: Well-known benchmark datasets from research community
- **Synthetic Datasets**: Artificially generated data with specific characteristics
- **Custom Datasets**: User-provided data in supported formats
- **Streaming Datasets**: Large datasets loaded incrementally

### Supported Formats

- **HDF5**: Primary format for large-scale datasets
- **NumPy**: Binary arrays (.npy, .npz files)
- **CSV/TSV**: Text-based formats with configurable delimiters
- **FVECS/BVECS**: Standard vector formats from INRIA
- **Parquet**: Columnar format for efficient storage

## Public Datasets

### OpenAI Embeddings Datasets

The OpenAI embeddings datasets are high-quality, real-world vector embeddings commonly used for benchmarking.

#### OpenAI-5M Dataset

**Description**: 5 million OpenAI text embeddings (1536 dimensions)
**Size**: ~30 GB
**Use Case**: Large-scale memory testing, production simulation

```bash
# Download OpenAI-5M dataset
wget https://huggingface.co/datasets/Qdrant/ann-benchmarks-openai/resolve/main/openai-5m.hdf5

# Verify download
vst dataset info --dataset openai-5m.hdf5
```

**Dataset Structure**:
```
openai-5m.hdf5
├── train: (5000000, 1536) float32 - Training vectors
├── test: (10000, 1536) float32 - Query vectors  
└── neighbors: (10000, 100) int32 - Ground truth neighbors
```

#### OpenAI-1M Dataset

**Description**: 1 million OpenAI text embeddings (1536 dimensions)
**Size**: ~6 GB
**Use Case**: Medium-scale testing, development

```bash
# Download OpenAI-1M dataset
wget https://huggingface.co/datasets/Qdrant/ann-benchmarks-openai/resolve/main/openai-1m.hdf5

# Alternative: Use dataset downloader
vst dataset download openai-1m --output _datasets_downloads/
```

#### OpenAI-100K Dataset

**Description**: 100K OpenAI text embeddings (1536 dimensions)
**Size**: ~600 MB
**Use Case**: Quick testing, CI/CD pipelines

```bash
# Download smaller dataset for testing
vst dataset download openai-100k --output _datasets_downloads/
```

### SIFT Datasets

SIFT (Scale-Invariant Feature Transform) datasets are classic computer vision benchmarks.

#### SIFT-1M Dataset

**Description**: 1 million SIFT descriptors (128 dimensions)
**Size**: ~500 MB
**Use Case**: Computer vision workloads, lower-dimensional testing

```bash
# Download SIFT-1M
wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
tar -xzf sift.tar.gz

# Convert to HDF5 format
vst dataset convert --input sift/sift_base.fvecs --output sift-128d.hdf5 --format fvecs
```

**Dataset Structure**:
```
sift-128d/
├── sift_base.fvecs - 1M base vectors (128D)
├── sift_query.fvecs - 10K query vectors (128D)
└── sift_groundtruth.ivecs - Ground truth neighbors
```

#### SIFT-10M Dataset

**Description**: 10 million SIFT descriptors (128 dimensions)
**Size**: ~5 GB
**Use Case**: Large-scale testing with lower dimensions

```bash
# Download SIFT-10M (larger dataset)
wget ftp://ftp.irisa.fr/local/texmex/corpus/bigann_base.bvecs
wget ftp://ftp.irisa.fr/local/texmex/corpus/bigann_query.bvecs
wget ftp://ftp.irisa.fr/local/texmex/corpus/bigann_groundtruth.ivecs

# Convert to HDF5
vst dataset convert --input bigann_base.bvecs --output sift-10m.hdf5 --format bvecs
```

### GIST Datasets

GIST descriptors for global image features.

#### GIST-1M Dataset

**Description**: 1 million GIST descriptors (960 dimensions)
**Size**: ~4 GB
**Use Case**: High-dimensional testing, image similarity

```bash
# Download GIST-1M
wget ftp://ftp.irisa.fr/local/texmex/corpus/gist.tar.gz
tar -xzf gist.tar.gz

# Convert to HDF5
vst dataset convert --input gist/gist_base.fvecs --output gist-1m.hdf5 --format fvecs
```

### Deep Learning Embeddings

#### Deep1B Dataset

**Description**: 1 billion deep learning features (96 dimensions)
**Size**: ~360 GB
**Use Case**: Extreme scale testing

```bash
# Download Deep1B (very large - ensure sufficient storage)
wget ftp://ftp.irisa.fr/local/texmex/corpus/deep1B_base.bvecs
wget ftp://ftp.irisa.fr/local/texmex/corpus/deep1B_query.bvecs

# Note: This is a very large dataset - consider using subsets
vst dataset subset --input deep1B_base.bvecs --output deep100m.hdf5 --count 100000000
```

### Specialized Datasets

#### GloVe Word Embeddings

**Description**: Pre-trained word vectors from Stanford
**Dimensions**: 50, 100, 200, 300
**Use Case**: NLP applications, text similarity

```bash
# Download GloVe embeddings
wget https://nlp.stanford.edu/data/glove.6B.zip
unzip glove.6B.zip

# Convert to HDF5
vst dataset convert --input glove.6B.300d.txt --output glove-300d.hdf5 --format glove
```

#### Sentence Transformers

**Description**: Modern sentence embeddings
**Dimensions**: 384, 768, 1024
**Use Case**: Semantic search, modern NLP

```bash
# Generate sentence embeddings dataset
python scripts/create_sentence_embeddings.py --model all-MiniLM-L6-v2 --output sentence-embeddings.hdf5
```

## Dataset Formats

### HDF5 Format (Recommended)

HDF5 is the preferred format for large-scale datasets due to efficient storage and access patterns.

#### Standard Structure

```python
# HDF5 dataset structure
dataset.hdf5
├── train: (N, D) float32          # Training/base vectors
├── test: (M, D) float32           # Query vectors (optional)
├── neighbors: (M, K) int32        # Ground truth neighbors (optional)
└── metadata/                      # Optional metadata
    ├── dataset_name: string
    ├── dimensions: int32
    ├── distance_metric: string
    └── creation_date: string
```

#### Creating HDF5 Datasets

```python
import h5py
import numpy as np

# Create HDF5 dataset
def create_hdf5_dataset(vectors, queries=None, neighbors=None, output_path="dataset.hdf5"):
    with h5py.File(output_path, 'w') as f:
        # Store training vectors
        f.create_dataset('train', data=vectors, compression='gzip', compression_opts=9)
        
        # Store query vectors (optional)
        if queries is not None:
            f.create_dataset('test', data=queries, compression='gzip', compression_opts=9)
        
        # Store ground truth (optional)
        if neighbors is not None:
            f.create_dataset('neighbors', data=neighbors, compression='gzip', compression_opts=9)
        
        # Add metadata
        f.attrs['dimensions'] = vectors.shape[1]
        f.attrs['num_vectors'] = vectors.shape[0]
        f.attrs['dtype'] = str(vectors.dtype)

# Example usage
vectors = np.random.random((1000000, 1536)).astype(np.float32)
create_hdf5_dataset(vectors, output_path="my_dataset.hdf5")
```

### FVECS/BVECS Format

Binary vector format commonly used in research.

#### FVECS (Float Vectors)

```python
def read_fvecs(filename):
    """Read FVECS format file."""
    vectors = []
    with open(filename, 'rb') as f:
        while True:
            # Read dimension
            dim_bytes = f.read(4)
            if not dim_bytes:
                break
            dim = struct.unpack('i', dim_bytes)[0]
            
            # Read vector
            vector_bytes = f.read(dim * 4)
            vector = struct.unpack('f' * dim, vector_bytes)
            vectors.append(vector)
    
    return np.array(vectors)

def write_fvecs(vectors, filename):
    """Write vectors in FVECS format."""
    with open(filename, 'wb') as f:
        for vector in vectors:
            # Write dimension
            f.write(struct.pack('i', len(vector)))
            # Write vector
            f.write(struct.pack('f' * len(vector), *vector))
```

#### BVECS (Byte Vectors)

```python
def read_bvecs(filename):
    """Read BVECS format file."""
    vectors = []
    with open(filename, 'rb') as f:
        while True:
            dim_bytes = f.read(4)
            if not dim_bytes:
                break
            dim = struct.unpack('i', dim_bytes)[0]
            
            vector_bytes = f.read(dim)
            vector = struct.unpack('B' * dim, vector_bytes)
            vectors.append(vector)
    
    return np.array(vectors, dtype=np.uint8)
```

## Downloading Datasets

### Using the Built-in Downloader

```bash
# List available datasets
vst dataset list

# Download specific dataset
vst dataset download openai-5m --output ./_datasets_downloads/

# Download with verification
vst dataset download openai-1m --output ./_datasets_downloads/ --verify-checksum

# Download and convert format
vst dataset download sift-128d --output ./_datasets_downloads/ --format hdf5
```

### Manual Download Scripts

#### OpenAI Datasets

```bash
#!/bin/bash
# download_openai_datasets.sh

DATASET_DIR="./_datasets_downloads"
mkdir -p $DATASET_DIR

echo "Downloading OpenAI embedding datasets..."

# OpenAI-100K (small, for testing)
echo "Downloading OpenAI-100K..."
wget -O "$DATASET_DIR/openai-100k.hdf5" \
    "https://huggingface.co/datasets/Qdrant/ann-benchmarks-openai/resolve/main/openai-100k.hdf5"

# OpenAI-1M (medium size)
echo "Downloading OpenAI-1M..."
wget -O "$DATASET_DIR/openai-1m.hdf5" \
    "https://huggingface.co/datasets/Qdrant/ann-benchmarks-openai/resolve/main/openai-1m.hdf5"

# OpenAI-5M (large size - optional)
read -p "Download OpenAI-5M (30GB)? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Downloading OpenAI-5M..."
    wget -O "$DATASET_DIR/openai-5m.hdf5" \
        "https://huggingface.co/datasets/Qdrant/ann-benchmarks-openai/resolve/main/openai-5m.hdf5"
fi

echo "Dataset download complete!"
```

#### SIFT Datasets

```bash
#!/bin/bash
# download_sift_datasets.sh

DATASET_DIR="./_datasets_downloads"
mkdir -p $DATASET_DIR

echo "Downloading SIFT datasets..."

# SIFT-1M
echo "Downloading SIFT-1M..."
cd $DATASET_DIR
wget ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
tar -xzf sift.tar.gz
rm sift.tar.gz

# Convert to HDF5
echo "Converting SIFT-1M to HDF5..."
vst dataset convert --input sift/sift_base.fvecs --output sift-128d.hdf5 --format fvecs

echo "SIFT dataset download complete!"
```

### Automated Download with Validation

```python
# scripts/download_datasets.py
import hashlib
import requests
from pathlib import Path

DATASETS = {
    "openai-100k": {
        "url": "https://huggingface.co/datasets/Qdrant/ann-benchmarks-openai/resolve/main/openai-100k.hdf5",
        "size_mb": 600,
        "md5": "abc123...",  # Add actual checksums
        "description": "100K OpenAI embeddings for quick testing"
    },
    "openai-1m": {
        "url": "https://huggingface.co/datasets/Qdrant/ann-benchmarks-openai/resolve/main/openai-1m.hdf5", 
        "size_mb": 6000,
        "md5": "def456...",
        "description": "1M OpenAI embeddings for development"
    },
    "openai-5m": {
        "url": "https://huggingface.co/datasets/Qdrant/ann-benchmarks-openai/resolve/main/openai-5m.hdf5",
        "size_mb": 30000,
        "md5": "ghi789...",
        "description": "5M OpenAI embeddings for production testing"
    }
}

def download_dataset(name, output_dir="./_datasets_downloads", verify=True):
    """Download and optionally verify dataset."""
    if name not in DATASETS:
        raise ValueError(f"Unknown dataset: {name}")
    
    info = DATASETS[name]
    output_path = Path(output_dir) / f"{name}.hdf5"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"Downloading {name} ({info['size_mb']} MB)...")
    print(f"Description: {info['description']}")
    
    # Download with progress
    response = requests.get(info['url'], stream=True)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    print(f"\rProgress: {percent:.1f%}", end='', flush=True)
    
    print(f"\nDownload complete: {output_path}")
    
    # Verify checksum
    if verify and 'md5' in info:
        print("Verifying checksum...")
        with open(output_path, 'rb') as f:
            file_hash = hashlib.md5(f.read()).hexdigest()
        
        if file_hash == info['md5']:
            print("✓ Checksum verified")
        else:
            print("✗ Checksum mismatch!")
            return False
    
    return True

# Example usage
if __name__ == "__main__":
    download_dataset("openai-100k")
```

## Dataset Preparation

### Data Preprocessing

#### Normalization

```python
import numpy as np

def normalize_vectors(vectors, method='l2'):
    """Normalize vectors using specified method."""
    if method == 'l2':
        # L2 normalization
        norms = np.linalg.norm(vectors, axis=1, keepdims=True)
        return vectors / (norms + 1e-8)
    
    elif method == 'min_max':
        # Min-max normalization
        min_vals = np.min(vectors, axis=1, keepdims=True)
        max_vals = np.max(vectors, axis=1, keepdims=True)
        return (vectors - min_vals) / (max_vals - min_vals + 1e-8)
    
    elif method == 'standard':
        # Standard normalization (z-score)
        mean = np.mean(vectors, axis=1, keepdims=True)
        std = np.std(vectors, axis=1, keepdims=True)
        return (vectors - mean) / (std + 1e-8)
    
    else:
        raise ValueError(f"Unknown normalization method: {method}")

# Example usage
vectors = np.random.random((100000, 1536)).astype(np.float32)
normalized_vectors = normalize_vectors(vectors, method='l2')
```

#### Dimensionality Reduction

```python
from sklearn.decomposition import PCA
from sklearn.random_projection import GaussianRandomProjection

def reduce_dimensions(vectors, target_dim, method='pca'):
    """Reduce vector dimensions."""
    if method == 'pca':
        reducer = PCA(n_components=target_dim)
        return reducer.fit_transform(vectors)
    
    elif method == 'random_projection':
        reducer = GaussianRandomProjection(n_components=target_dim)
        return reducer.fit_transform(vectors)
    
    else:
        raise ValueError(f"Unknown reduction method: {method}")

# Example: Reduce from 1536 to 768 dimensions
reduced_vectors = reduce_dimensions(vectors, target_dim=768, method='pca')
```

### Data Augmentation

#### Adding Noise

```python
def add_noise(vectors, noise_level=0.01, noise_type='gaussian'):
    """Add noise to vectors for robustness testing."""
    if noise_type == 'gaussian':
        noise = np.random.normal(0, noise_level, vectors.shape)
    elif noise_type == 'uniform':
        noise = np.random.uniform(-noise_level, noise_level, vectors.shape)
    else:
        raise ValueError(f"Unknown noise type: {noise_type}")
    
    return vectors + noise.astype(vectors.dtype)

# Example usage
noisy_vectors = add_noise(vectors, noise_level=0.05)
```

#### Creating Variants

```python
def create_variants(vectors, num_variants=3, variation_strength=0.1):
    """Create vector variants for testing."""
    variants = []
    
    for i in range(num_variants):
        # Apply random transformation
        noise = np.random.normal(0, variation_strength, vectors.shape)
        variant = vectors + noise.astype(vectors.dtype)
        
        # Optionally normalize
        variant = normalize_vectors(variant, method='l2')
        variants.append(variant)
    
    return variants
```

### Dataset Splitting

```python
def split_dataset(vectors, train_ratio=0.8, test_ratio=0.1, val_ratio=0.1):
    """Split dataset into train/test/validation sets."""
    assert train_ratio + test_ratio + val_ratio == 1.0
    
    n_vectors = len(vectors)
    indices = np.random.permutation(n_vectors)
    
    train_end = int(n_vectors * train_ratio)
    test_end = train_end + int(n_vectors * test_ratio)
    
    train_indices = indices[:train_end]
    test_indices = indices[train_end:test_end]
    val_indices = indices[test_end:]
    
    return {
        'train': vectors[train_indices],
        'test': vectors[test_indices],
        'validation': vectors[val_indices]
    }

# Example usage
splits = split_dataset(vectors, train_ratio=0.8, test_ratio=0.1, val_ratio=0.1)
```

## Custom Datasets

### Creating from Text Embeddings

```python
# scripts/create_text_embeddings.py
from sentence_transformers import SentenceTransformer
import h5py
import numpy as np

def create_text_embedding_dataset(texts, model_name='all-MiniLM-L6-v2', output_path='text_embeddings.hdf5'):
    """Create vector dataset from text corpus."""
    
    # Load embedding model
    model = SentenceTransformer(model_name)
    
    # Generate embeddings
    print(f"Generating embeddings for {len(texts)} texts...")
    embeddings = model.encode(texts, show_progress_bar=True)
    
    # Create HDF5 dataset
    with h5py.File(output_path, 'w') as f:
        f.create_dataset('train', data=embeddings, compression='gzip')
        f.attrs['model_name'] = model_name
        f.attrs['dimensions'] = embeddings.shape[1]
        f.attrs['num_vectors'] = embeddings.shape[0]
    
    print(f"Dataset created: {output_path}")
    return output_path

# Example usage
texts = [
    "Machine learning is a subset of artificial intelligence.",
    "Deep learning uses neural networks with multiple layers.",
    "Vector databases store and search high-dimensional vectors.",
    # ... more texts
]

dataset_path = create_text_embedding_dataset(texts)
```

### Creating from Images

```python
# scripts/create_image_embeddings.py
import torch
import torchvision.transforms as transforms
from torchvision.models import resnet50
from PIL import Image
import h5py
import numpy as np

def create_image_embedding_dataset(image_paths, output_path='image_embeddings.hdf5'):
    """Create vector dataset from images."""
    
    # Load pre-trained model
    model = resnet50(pretrained=True)
    model.eval()
    
    # Remove final classification layer to get features
    model = torch.nn.Sequential(*list(model.children())[:-1])
    
    # Image preprocessing
    preprocess = transforms.Compose([
        transforms.Resize(256),
        transforms.CenterCrop(224),
        transforms.ToTensor(),
        transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
    ])
    
    embeddings = []
    
    print(f"Processing {len(image_paths)} images...")
    for i, img_path in enumerate(image_paths):
        # Load and preprocess image
        image = Image.open(img_path).convert('RGB')
        input_tensor = preprocess(image).unsqueeze(0)
        
        # Generate embedding
        with torch.no_grad():
            embedding = model(input_tensor).squeeze().numpy()
        
        embeddings.append(embedding)
        
        if (i + 1) % 100 == 0:
            print(f"Processed {i + 1}/{len(image_paths)} images")
    
    embeddings = np.array(embeddings)
    
    # Create HDF5 dataset
    with h5py.File(output_path, 'w') as f:
        f.create_dataset('train', data=embeddings, compression='gzip')
        f.attrs['model'] = 'resnet50'
        f.attrs['dimensions'] = embeddings.shape[1]
        f.attrs['num_vectors'] = embeddings.shape[0]
    
    print(f"Dataset created: {output_path}")
    return output_path
```

### Synthetic Dataset Generation

```python
def generate_synthetic_dataset(num_vectors, dimensions, distribution='gaussian', output_path='synthetic.hdf5'):
    """Generate synthetic vector dataset."""
    
    print(f"Generating {num_vectors} {dimensions}D vectors with {distribution} distribution...")
    
    if distribution == 'gaussian':
        vectors = np.random.normal(0, 1, (num_vectors, dimensions))
    elif distribution == 'uniform':
        vectors = np.random.uniform(-1, 1, (num_vectors, dimensions))
    elif distribution == 'clustered':
        # Create clustered data
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
            extra_vectors = np.random.normal(0, 1, (remainder, dimensions))
            vectors = np.vstack([vectors, extra_vectors])
    
    else:
        raise ValueError(f"Unknown distribution: {distribution}")
    
    # Convert to float32 for efficiency
    vectors = vectors.astype(np.float32)
    
    # Normalize vectors
    vectors = normalize_vectors(vectors, method='l2')
    
    # Generate query vectors (subset of training vectors)
    query_indices = np.random.choice(num_vectors, size=min(10000, num_vectors // 10), replace=False)
    queries = vectors[query_indices]
    
    # Create HDF5 dataset
    with h5py.File(output_path, 'w') as f:
        f.create_dataset('train', data=vectors, compression='gzip')
        f.create_dataset('test', data=queries, compression='gzip')
        f.attrs['distribution'] = distribution
        f.attrs['dimensions'] = dimensions
        f.attrs['num_vectors'] = num_vectors
    
    print(f"Synthetic dataset created: {output_path}")
    return output_path

# Example usage
generate_synthetic_dataset(1000000, 1536, distribution='clustered')
```

## Dataset Configuration

### Dataset Registry

Configure datasets in the tool's registry:

```yaml
# config/datasets.yaml
datasets:
  openai-5m:
    path: openai-5m.hdf5
    type: hdf5
    dimensions: 1536
    size: 5000000
    description: "5M OpenAI text embeddings"
    
  openai-1m:
    path: openai-1m.hdf5
    type: hdf5
    dimensions: 1536
    size: 1000000
    description: "1M OpenAI text embeddings"
    
  sift-128d:
    path: sift-128d.hdf5
    type: hdf5
    dimensions: 128
    size: 1000000
    description: "1M SIFT descriptors"
    
  custom-embeddings:
    path: custom-embeddings.hdf5
    type: hdf5
    dimensions: 768
    size: 500000
    description: "Custom text embeddings"
    preprocessing:
      normalize: l2
      center: true
```

### Dynamic Dataset Loading

```yaml
# Scenario with dataset configuration
name: multi_dataset_test
description: Test with multiple datasets

steps:
  - name: test_openai
    type: workload
    workload: ingest
    dataset: openai-1m
    parameters:
      target_vectors: 500000
      
  - name: test_sift
    type: workload
    workload: ingest
    dataset: sift-128d
    parameters:
      target_vectors: 500000
      index_suffix: "_sift"
```

### Custom Dataset Loading

```python
# Custom dataset loader
class CustomDatasetLoader:
    def __init__(self, config):
        self.config = config
        
    def load(self, dataset_name):
        if dataset_name.startswith('synthetic_'):
            # Generate synthetic data
            params = self.parse_synthetic_params(dataset_name)
            return self.generate_synthetic(**params)
        elif dataset_name.startswith('url:'):
            # Download from URL
            url = dataset_name[4:]
            return self.download_and_cache(url)
        else:
            # Load from file
            return self.load_from_file(dataset_name)
    
    def parse_synthetic_params(self, name):
        # Parse parameters from name like "synthetic_1M_1536D_gaussian"
        parts = name.split('_')
        size = self.parse_size(parts[1])  # "1M" -> 1000000
        dims = int(parts[2][:-1])  # "1536D" -> 1536
        dist = parts[3] if len(parts) > 3 else 'gaussian'
        return {'num_vectors': size, 'dimensions': dims, 'distribution': dist}
```

## Performance Optimization

### Memory-Efficient Loading

```python
class StreamingDatasetLoader:
    def __init__(self, dataset_path, chunk_size=10000):
        self.dataset_path = dataset_path
        self.chunk_size = chunk_size
        self._file = None
        self._dataset = None
        
    def __enter__(self):
        self._file = h5py.File(self.dataset_path, 'r')
        self._dataset = self._file['train']
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            self._file.close()
    
    def iter_chunks(self):
        """Iterate over dataset in chunks."""
        total_vectors = self._dataset.shape[0]
        
        for start_idx in range(0, total_vectors, self.chunk_size):
            end_idx = min(start_idx + self.chunk_size, total_vectors)
            chunk = self._dataset[start_idx:end_idx]
            yield start_idx, chunk

# Usage
with StreamingDatasetLoader('large_dataset.hdf5', chunk_size=50000) as loader:
    for start_idx, chunk in loader.iter_chunks():
        # Process chunk
        process_vectors(chunk)
```

### Parallel Loading

```python
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

def load_dataset_parallel(dataset_path, num_workers=4):
    """Load dataset using multiple processes."""
    
    with h5py.File(dataset_path, 'r') as f:
        dataset_size = f['train'].shape[0]
        dimensions = f['train'].shape[1]
    
    # Calculate chunks for each worker
    chunk_size = dataset_size // num_workers
    chunks = []
    
    for i in range(num_workers):
        start_idx = i * chunk_size
        end_idx = min((i + 1) * chunk_size, dataset_size)
        chunks.append((dataset_path, start_idx, end_idx))
    
    # Load chunks in parallel
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        results = list(executor.map(load_chunk, chunks))
    
    # Combine results
    vectors = np.vstack(results)
    return vectors

def load_chunk(args):
    dataset_path, start_idx, end_idx = args
    with h5py.File(dataset_path, 'r') as f:
        return f['train'][start_idx:end_idx]
```

### Caching and Preprocessing

```python
import joblib
from pathlib import Path

class CachedDatasetLoader:
    def __init__(self, cache_dir='./dataset_cache'):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
    
    def load_with_cache(self, dataset_path, preprocessing=None):
        """Load dataset with caching of preprocessed data."""
        
        # Generate cache key
        cache_key = self.get_cache_key(dataset_path, preprocessing)
        cache_path = self.cache_dir / f"{cache_key}.pkl"
        
        # Check if cached version exists
        if cache_path.exists():
            print(f"Loading from cache: {cache_path}")
            return joblib.load(cache_path)
        
        # Load and preprocess dataset
        print(f"Loading dataset: {dataset_path}")
        with h5py.File(dataset_path, 'r') as f:
            vectors = f['train'][:]
        
        if preprocessing:
            vectors = self.apply_preprocessing(vectors, preprocessing)
        
        # Cache preprocessed data
        print(f"Caching to: {cache_path}")
        joblib.dump(vectors, cache_path)
        
        return vectors
    
    def get_cache_key(self, dataset_path, preprocessing):
        """Generate unique cache key."""
        import hashlib
        
        key_data = str(dataset_path)
        if preprocessing:
            key_data += str(preprocessing)
        
        return hashlib.md5(key_data.encode()).hexdigest()[:16]
```

## Validation and Quality

### Dataset Validation

```python
def validate_dataset(dataset_path):
    """Validate dataset format and content."""
    
    errors = []
    warnings = []
    
    try:
        with h5py.File(dataset_path, 'r') as f:
            # Check required groups
            if 'train' not in f:
                errors.append("Missing 'train' dataset")
            else:
                train_data = f['train']
                
                # Check data type
                if train_data.dtype not in [np.float32, np.float64]:
                    warnings.append(f"Unexpected dtype: {train_data.dtype}")
                
                # Check shape
                if len(train_data.shape) != 2:
                    errors.append(f"Invalid shape: {train_data.shape}, expected 2D")
                
                # Check for NaN/Inf values
                sample = train_data[:1000]
                if np.any(np.isnan(sample)) or np.any(np.isinf(sample)):
                    errors.append("Dataset contains NaN or Inf values")
                
                # Check vector norms
                norms = np.linalg.norm(sample, axis=1)
                if np.any(norms == 0):
                    warnings.append("Dataset contains zero vectors")
            
            # Check optional groups
            if 'test' in f:
                test_data = f['test']
                if test_data.shape[1] != train_data.shape[1]:
                    errors.append("Train and test vectors have different dimensions")
            
            if 'neighbors' in f:
                neighbors_data = f['neighbors']
                if 'test' not in f:
                    errors.append("Ground truth provided but no test vectors")
    
    except Exception as e:
        errors.append(f"Failed to read dataset: {e}")
    
    return {
        'valid': len(errors) == 0,
        'errors': errors,
        'warnings': warnings
    }

# Usage
result = validate_dataset('my_dataset.hdf5')
if not result['valid']:
    print("Dataset validation failed:")
    for error in result['errors']:
        print(f"  ERROR: {error}")
```

### Quality Metrics

```python
def analyze_dataset_quality(vectors):
    """Analyze dataset quality metrics."""
    
    # Basic statistics
    stats = {
        'num_vectors': len(vectors),
        'dimensions': vectors.shape[1],
        'dtype': str(vectors.dtype),
        'memory_mb': vectors.nbytes / (1024 * 1024)
    }
    
    # Vector norms
    norms = np.linalg.norm(vectors, axis=1)
    stats.update({
        'norm_mean': float(np.mean(norms)),
        'norm_std': float(np.std(norms)),
        'norm_min': float(np.min(norms)),
        'norm_max': float(np.max(norms))
    })
    
    # Dimension-wise statistics
    stats.update({
        'value_mean': float(np.mean(vectors)),
        'value_std': float(np.std(vectors)),
        'value_min': float(np.min(vectors)),
        'value_max': float(np.max(vectors))
    })
    
    # Sparsity
    zero_count = np.sum(vectors == 0)
    total_elements = vectors.size
    stats['sparsity'] = float(zero_count / total_elements)
    
    # Diversity (approximate)
    sample_size = min(10000, len(vectors))
    sample_indices = np.random.choice(len(vectors), sample_size, replace=False)
    sample_vectors = vectors[sample_indices]
    
    # Pairwise distances (sample)
    from sklearn.metrics.pairwise import pairwise_distances
    distances = pairwise_distances(sample_vectors[:1000], metric='euclidean')
    stats.update({
        'distance_mean': float(np.mean(distances)),
        'distance_std': float(np.std(distances))
    })
    
    return stats
```

## Best Practices

### Dataset Selection

1. **Choose Appropriate Size**
   ```python
   # Development: Use small datasets
   dataset_sizes = {
       'development': 'openai-100k',    # 100K vectors
       'testing': 'openai-1m',         # 1M vectors  
       'production': 'openai-5m'       # 5M vectors
   }
   ```

2. **Match Use Case**
   ```yaml
   # For text/NLP applications
   dataset: openai-5m  # High-dimensional text embeddings
   
   # For computer vision
   dataset: sift-128d    # Lower-dimensional image features
   
   # For general testing
   dataset: synthetic_1M_512D_clustered  # Synthetic data
   ```

3. **Consider Memory Constraints**
   ```python
   def estimate_memory_usage(num_vectors, dimensions, dtype=np.float32):
       bytes_per_element = np.dtype(dtype).itemsize
       total_bytes = num_vectors * dimensions * bytes_per_element
       
       # Add overhead for index structures (approximate)
       overhead_factor = 1.5  # 50% overhead
       total_bytes *= overhead_factor
       
       return total_bytes / (1024 ** 3)  # GB
   
   # Example
   memory_gb = estimate_memory_usage(5000000, 1536)  # ~45 GB
   ```

### Data Preprocessing

1. **Normalize Consistently**
   ```python
   # Always use the same normalization
   vectors = normalize_vectors(vectors, method='l2')
   ```

2. **Handle Missing Data**
   ```python
   # Check for and handle NaN values
   if np.any(np.isnan(vectors)):
       vectors = np.nan_to_num(vectors, nan=0.0)
   ```

3. **Validate Data Quality**
   ```python
   # Regular quality checks
   quality_report = analyze_dataset_quality(vectors)
   if quality_report['sparsity'] > 0.9:
       print("Warning: Dataset is very sparse")
   ```

### Storage and Management

1. **Use Compression**
   ```python
   # Enable compression for HDF5
   f.create_dataset('train', data=vectors, compression='gzip', compression_opts=9)
   ```

2. **Organize Datasets**
   ```
   _datasets_downloads/
   ├── openai/
   │   ├── openai-100k.hdf5
   │   ├── openai-1m.hdf5
   │   └── openai-5m.hdf5
   ├── sift/
   │   ├── sift-128d.hdf5
   │   └── sift-10m.hdf5
   └── custom/
       └── my-embeddings.hdf5
   ```

3. **Version Control Metadata**
   ```yaml
   # datasets/metadata.yaml
   _datasets_downloads:
     openai-5m:
       version: "1.0"
       created: "2024-01-15"
       checksum: "abc123..."
       source: "https://huggingface.co/..."
   ```

## Troubleshooting

### Common Issues

#### File Format Errors

**Error: Unable to open file (file signature not found)**
```bash
# Check file format
file dataset.hdf5

# Validate HDF5 structure
h5dump -H dataset.hdf5

# Re-download if corrupted
rm dataset.hdf5
vst dataset download openai-1m
```

#### Memory Errors

**Error: Cannot allocate memory for array**
```python
# Use streaming loading
with StreamingDatasetLoader('large_dataset.hdf5') as loader:
    for start_idx, chunk in loader.iter_chunks():
        process_chunk(chunk)

# Or reduce dataset size
vst dataset subset --input large_dataset.hdf5 --output small_dataset.hdf5 --count 100000
```

#### Dimension Mismatches

**Error: Vector dimensions don't match index**
```python
# Check dataset dimensions
vst dataset info --dataset my_dataset.hdf5

# Reshape or transform if needed
vectors = reduce_dimensions(vectors, target_dim=1536)
```

### Performance Issues

#### Slow Loading

```python
# Enable parallel loading
vectors = load_dataset_parallel('dataset.hdf5', num_workers=4)

# Use SSD storage
# Move datasets to SSD for faster I/O

# Enable caching
loader = CachedDatasetLoader()
vectors = loader.load_with_cache('dataset.hdf5')
```

#### High Memory Usage

```python
# Use memory mapping
import h5py
f = h5py.File('dataset.hdf5', 'r')
vectors = f['train']  # Memory-mapped access

# Process in chunks
for i in range(0, len(vectors), chunk_size):
    chunk = vectors[i:i+chunk_size]
    process_chunk(chunk)
```

### Data Quality Issues

#### Invalid Values

```python
# Check for invalid values
def check_data_quality(vectors):
    issues = []
    
    if np.any(np.isnan(vectors)):
        issues.append("Contains NaN values")
    
    if np.any(np.isinf(vectors)):
        issues.append("Contains Inf values")
    
    norms = np.linalg.norm(vectors, axis=1)
    if np.any(norms == 0):
        issues.append("Contains zero vectors")
    
    return issues

# Fix common issues
vectors = np.nan_to_num(vectors, nan=0.0, posinf=1.0, neginf=-1.0)
```

### Getting Help

For additional help with datasets:

1. **Check Dataset Info**
   ```bash
   vst dataset info --dataset my_dataset.hdf5
   vst dataset validate --dataset my_dataset.hdf5
   ```

2. **List Available Datasets**
   ```bash
   vst dataset list
   vst dataset search --query "openai"
   ```

3. **Dataset Documentation**
   ```bash
   vst dataset docs --dataset openai-5m
   ```

4. **Community Resources**
   - [ANN Benchmarks](http://ann-benchmarks.com/)
   - [Hugging Face Datasets](https://huggingface.co/datasets)
   - [INRIA TEXMEX](http://corpus-texmex.irisa.fr/)
