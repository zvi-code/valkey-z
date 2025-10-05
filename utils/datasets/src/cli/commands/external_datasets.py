"""Dataset management commands - simplified version."""

import typer
import h5py
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple, Union
import urllib.request
import logging
import zipfile
import tarfile
import gzip
import tempfile
import shutil
import struct
import json
import re
from enum import Enum

from core.dataset import Dataset

# Optional dependencies
try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

try:
    import scipy.sparse
    HAS_SCIPY = True
except ImportError:
    HAS_SCIPY = False

app = typer.Typer()
logger = logging.getLogger(__name__)


class DatasetFormat(str, Enum):
    """Supported dataset formats."""
    HDF5 = "hdf5"
    NUMPY = "npy"
    PARQUET = "parquet"
    FBIN = "fbin"
    IBIN = "ibin" 
    U8BIN = "u8bin"
    BVECS = "bvecs"
    FVECS = "fvecs"
    CSV = "csv"
    CSR_GZ = "csr.gz"
    UNKNOWN = "unknown"

DATASETS = {
    "Text-to-Image-1B": {
        "url": "https://storage.yandexcloud.net/yandex-research/ann-datasets/T2I/",
        "description": "Text-to-Image cross-modal embeddings",
        "format": "fbin",
        "dimensions": 200,
        "size_mb": 8000,  # ~8GB
        "distance": "inner_product",
        "train_size": 10000000,
        "test_size": 10000,
        "note": "Cross-modal dataset with different query/base distributions",
        "files": {
            # "base1b": "base.1B.fbin",
            "base1m": "base.1M.fbin",
            "base10m": "base.10M.fbin",
            "queries": "query.public.100K.fbin",
            "ground_truth": "groundtruth.public.100K.ibin",
            "query_learn": "query.learn.50M.fbin"
        }
    },
    "Deep1B": {
        "url": "https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/",
        "description": "DEEP1B image descriptors",
        "format": "fbin",
        "dimensions": 96,
        "size_mb": 358000,  # ~358GB for 1B vectors
        "distance": "euclidean",
        "train_size": 1000000000,
        "test_size": 10000,
        "note": "Deep image descriptors from Yandex",
        "files": {
            # "base1b": "base.1B.fbin",
            "base10m": "base.10M.fbin",
            "learn350m": "learn.350M.fbin",
            "queries": "query.public.10K.fbin",
            "ground_truth": "groundtruth.public.10K.ibin"
        }
    },
    # Your existing datasets
    "openai-5m": {
        "url": "https://cdn.openai.com/API/examples/data/vector_database_wikipedia_articles_embedded.zip",
        "description": "OpenAI 5M vectors from Wikipedia articles",
        "format": "zip",
        "dimensions": 1536,
        "size_mb": 2000,
    },
    
    # VectorDBBench-compatible datasets (S3 direct access)
    "openai_small_50k": {
        "s3_base": "https://vectordb-bench-datasets.s3.amazonaws.com/",
        "description": "OpenAI embeddings 50k vectors",
        "format": "parquet",
        "dimensions": 1536,
        "size_mb": 77,
        "distance": "cosine",
        "train_size": 50000,
        "test_size": 1000,
        "files": ["train.parquet", "test.parquet"],
        "note": "Direct S3 access, no vectordb-bench installation required. Note: S3 URL is hypothetical - replace with actual bucket URL",
    },
    "sift_small_500k": {
        "s3_base": "https://vectordb-bench-datasets.s3.amazonaws.com/",
        "description": "SIFT 500k vectors for benchmarking",
        "format": "parquet",
        "dimensions": 128,
        "size_mb": 64,
        "distance": "euclidean",
        "train_size": 500000,
        "test_size": 1000,
        "files": ["train.parquet", "test.parquet"],
        "note": "Direct S3 access, no vectordb-bench installation required. Note: S3 URL is hypothetical - replace with actual bucket URL",
    },
    
    # Big-ANN-Benchmarks Datasets (Billion-Scale)
    "bigann-10m": {
        "url": "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/",
        "description": "BIGANN SIFT descriptors - 10M subset",
        "format": "u8bin",
        "dimensions": 128,
        "size_mb": 1280,  # ~1.3GB
        "distance": "euclidean",
        "train_size": 10000000,
        "test_size": 10000,
        "note": "Binary format: base.10M.u8bin, query.public.10K.u8bin",
        "files": {
            "base": "base.10M.u8bin",
            "queries": "query.public.10K.u8bin"
        }
    },
    "bigann-100m": {
        "url": "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/",
        "description": "BIGANN SIFT descriptors - 100M subset",
        "format": "u8bin",
        "dimensions": 128,
        "size_mb": 12800,  # ~12.8GB
        "distance": "euclidean",
        "train_size": 100000000,
        "test_size": 10000,
        "note": "Binary format: base.100M.u8bin, query.public.10K.u8bin",
        "files": {
            "base": "base.100M.u8bin",
            "queries": "query.public.10K.u8bin"
        }
    },
    "bigann-1b": {
        "url": "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/",
        "description": "BIGANN SIFT descriptors - 1 billion vectors",
        "format": "u8bin",
        "dimensions": 128,
        "size_mb": 128000,  # ~128GB
        "distance": "euclidean",
        "train_size": 1000000000,
        "test_size": 10000,
        "note": "Binary format: base.1B.u8bin, query.public.10K.u8bin",
        "files": {
            "base": "base.1B.u8bin",
            "queries": "query.public.10K.u8bin"
        }
    },
    "deep1b": {
        "url": "https://research.yandex.com/blog/benchmarks-for-billion-scale-similarity-search",
        "description": "Yandex DEEP1B image descriptors",
        "format": "fbin",
        "dimensions": 96,
        "size_mb": 358000,  # ~358GB for 1B vectors
        "distance": "euclidean",
        "train_size": 1000000000,
        "test_size": 10000,
        "note": "Deep image descriptors from Yandex",
    },
    "text2image-10m": {
        "url": "https://research.yandex.com/blog/benchmarks-for-billion-scale-similarity-search",
        "description": "Yandex Text-to-Image cross-modal embeddings",
        "format": "fbin",
        "dimensions": 200,
        "size_mb": 8000,  # ~8GB
        "distance": "euclidean",
        "train_size": 10000000,
        "test_size": 10000,
        "note": "Cross-modal dataset with different query/base distributions",
    },
    "msturing-10m": {
        "url": "neurips-competition",
        "description": "Microsoft Turing query embeddings - 10M subset",
        "format": "fbin",
        "dimensions": 100,
        "size_mb": 4000,  # ~4GB
        "distance": "euclidean",
        "train_size": 10000000,
        "test_size": 100000,
        "note": "Available through NeurIPS Big-ANN competition",
    },
    "msturing-30m": {
        "url": "neurips-competition",
        "description": "Microsoft Turing query embeddings - 30M clustered",
        "format": "fbin",
        "dimensions": 100,
        "size_mb": 12000,  # ~12GB
        "distance": "euclidean",
        "train_size": 30000000,
        "test_size": 100000,
        "note": "Available through NeurIPS Big-ANN competition",
    },
    "msturing-1b": {
        "url": "neurips-competition",
        "description": "Microsoft Turing-ANNS-1B full dataset",
        "format": "custom",
        "dimensions": 100,
        "size_mb": 400000,  # ~400GB
        "distance": "euclidean",
        "train_size": 1000000000,
        "test_size": 100000,
        "note": "Available through NeurIPS Big-ANN competition",
    },
    "msspacev-1b": {
        "url": "neurips-competition",
        "description": "Microsoft SPACEV-1B web search dataset",
        "format": "custom",
        "dimensions": 100,
        "size_mb": 400000,  # ~400GB
        "distance": "euclidean",
        "train_size": 1000000000,
        "test_size": 100000,
        "note": "Available through NeurIPS Big-ANN competition",
    },
    "fb-ssnpp": {
        "url": "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/",
        "description": "Facebook SimSearchNet++ image copy detection",
        "format": "u8bin",
        "dimensions": 256,
        "size_mb": 300000,  # ~300GB for 1.17B vectors
        "distance": "euclidean",
        "train_size": 1170000000,  # 1.17B after deduplication  
        "test_size": 100000,
        "note": "DB: FB_ssnpp_database.u8bin, Queries: FB_ssnpp_public_queries.u8bin",
        "threshold": 96237,  # squared L2 threshold for range search
        "license": "CC-BY-NC",
        "task": "range_search",  # Not k-NN!
        "files": {
            "database": "FB_ssnpp_database.u8bin",
            "queries": "FB_ssnpp_public_queries.u8bin",
            "ground_truth": "FB_ssnpp_public_queries.100.rangeres"
        }
    },
 
    "msmarco-sparse": {
        "url": "https://storage.googleapis.com/ann-challenge-sparse-vectors/csr/",
        "description": "MS MARCO passage retrieval sparse vectors",
        "format": "csr.gz",
        "dimensions": "sparse (variable)",
        "size_mb": 5500,  # 5.5GB for full dataset
        "distance": "dot_product",
        "train_size": 8841823,
        "test_size": 6980,
        "note": "SPLADE sparse embeddings, avg ~130 non-zeros per vector, max dim ~30k",
        "files": {
            "full": "base_full.csr.gz",
            "1M": "base_1M.csr.gz",
            "small": "base_small.csr.gz",
            "queries": "queries.dev.csr.gz",
            "gt_full": "base_full.gt",
            "gt_1M": "base_1M.gt",
            "gt_small": "base_small.gt",
        }
    },
    "yfcc-10m": {
        "url": "neurips-competition",
        "description": "YFCC100M CLIP embeddings with metadata filtering",
        "format": "u8bin + sparse metadata",
        "dimensions": 192,
        "size_mb": 7680,  # ~7.7GB
        "distance": "euclidean",
        "train_size": 10000000,
        "test_size": 100000,
        "note": "Big-ANN filtered search task; includes 200k vocabulary sparse metadata",
        "metadata_vocab": 200386,  # vocabulary size for tags
    },
    # ANN-Benchmarks datasets (HDF5 format with query vectors and ground truth)
    "gist-960d": {
        "url": "https://ann-benchmarks.com/gist-960-euclidean.hdf5",
        "description": "GIST 1M vectors with 960 dimensions",
        "format": "hdf5",
        "dimensions": 960,
        "size_mb": 3600,
        "distance": "euclidean",
        "train_size": 1000000,
        "test_size": 1000,
    },
    "sift-128d": {
        "url": "https://ann-benchmarks.com/sift-128-euclidean.hdf5",
        "description": "SIFT 1M vectors with 128 dimensions",
        "format": "hdf5",
        "dimensions": 128,
        "size_mb": 136,
        "distance": "euclidean",
        "train_size": 1000000,
        "test_size": 10000,
    },
    "deep1b-96d": {
        "url": "https://ann-benchmarks.com/deep-image-96-angular.hdf5",
        "description": "DEEP1B subset with 96 dimensions",
        "format": "hdf5",
        "dimensions": 96,
        "size_mb": 3600,
        "distance": "angular",
        "train_size": 9990000,
        "test_size": 10000,
    },
    "fashion-mnist": {
        "url": "https://ann-benchmarks.com/fashion-mnist-784-euclidean.hdf5",
        "description": "Fashion-MNIST 784-dimensional vectors",
        "format": "hdf5",
        "dimensions": 784,
        "size_mb": 217,
        "distance": "euclidean",
        "train_size": 60000,
        "test_size": 10000,
    },
    "mnist": {
        "url": "https://ann-benchmarks.com/mnist-784-euclidean.hdf5",
        "description": "MNIST 784-dimensional vectors",
        "format": "hdf5",
        "dimensions": 784,
        "size_mb": 217,
        "distance": "euclidean",
        "train_size": 60000,
        "test_size": 10000,
    },
    "glove-25": {
        "url": "https://ann-benchmarks.com/glove-25-angular.hdf5",
        "description": "GloVe word embeddings 25 dimensions",
        "format": "hdf5",
        "dimensions": 25,
        "size_mb": 121,
        "distance": "angular",
        "train_size": 1183514,
        "test_size": 10000,
    },
    "glove-50": {
        "url": "https://ann-benchmarks.com/glove-50-angular.hdf5",
        "description": "GloVe word embeddings 50 dimensions",
        "format": "hdf5",
        "dimensions": 50,
        "size_mb": 235,
        "distance": "angular",
        "train_size": 1183514,
        "test_size": 10000,
    },
    "glove-100": {
        "url": "https://ann-benchmarks.com/glove-100-angular.hdf5",
        "description": "GloVe word embeddings 100 dimensions",
        "format": "hdf5",
        "dimensions": 100,
        "size_mb": 463,
        "distance": "angular",
        "train_size": 1183514,
        "test_size": 10000,
    },
    "glove-200": {
        "url": "https://ann-benchmarks.com/glove-200-angular.hdf5",
        "description": "GloVe word embeddings 200 dimensions",
        "format": "hdf5",
        "dimensions": 200,
        "size_mb": 918,
        "distance": "angular",
        "train_size": 1183514,
        "test_size": 10000,
    },
    "nytimes": {
        "url": "https://ann-benchmarks.com/nytimes-256-angular.hdf5",
        "description": "NYTimes 256-dimensional article vectors",
        "format": "hdf5",
        "dimensions": 256,
        "size_mb": 301,
        "distance": "angular",
        "train_size": 290000,
        "test_size": 10000,
    },
    "kosarak": {
        "url": "https://ann-benchmarks.com/kosarak-jaccard.hdf5",
        "description": "Kosarak sparse dataset for Jaccard distance",
        "format": "hdf5",
        "dimensions": 27983,
        "size_mb": 33,
        "distance": "jaccard",
        "train_size": 74962,
        "test_size": 500,
    },
    "movielens-10m": {
        "url": "https://ann-benchmarks.com/movielens10m-jaccard.hdf5",
        "description": "MovieLens-10M sparse dataset",
        "format": "hdf5",
        "dimensions": 65134,
        "size_mb": 63,
        "distance": "jaccard",
        "train_size": 69363,
        "test_size": 500,
    },
    
    # Billion-scale datasets from Big-ANN-Benchmarks
    "sift-1b": {
        "url": "http://corpus-texmex.irisa.fr/",
        "description": "SIFT 1 billion vectors (requires manual download)",
        "format": "bvecs",
        "dimensions": 128,
        "size_mb": 120000,  # ~120GB
        "note": "ANN_SIFT1B dataset from corpus-texmex",
    },
    
    # Additional large-scale datasets used in VectorDBBench
    "laion-100m": {
        "s3_base": "https://vectordb-bench-datasets.s3.amazonaws.com/laion_large_100m/",
        "description": "LAION 100M CLIP image embeddings",
        "format": "parquet",
        "dimensions": 768,
        "size_mb": 230000,  # ~230GB
        "note": "Direct S3 access, 100 parquet files",
        "train_size": 100000000,
        "file_count": 100,
        "files": ["train.parquet", "test.parquet"],
    },
    "cohere-10m": {
        "s3_base": "https://vectordb-bench-datasets.s3.amazonaws.com/cohere_medium_10m/", 
        "description": "Cohere 10M multilingual embeddings (768d)",
        "format": "parquet",
        "dimensions": 768,
        "size_mb": 23000,  # ~23GB
        "note": "Direct S3 access, no vectordb-bench installation required",
        "train_size": 10000000,
        "test_size": 1000,
        "files": ["train.parquet", "test.parquet"],
    },
    "cohere-1m": {
        "s3_base": "https://vectordb-bench-datasets.s3.amazonaws.com/cohere_small_1m/",
        "description": "Cohere 1M multilingual embeddings (768d)",
        "format": "parquet", 
        "dimensions": 768,
        "size_mb": 2300,
        "note": "Direct S3 access, no vectordb-bench installation required",
        "train_size": 1000000,
        "test_size": 1000,
        "files": ["train.parquet", "test.parquet"],
    },
}


class DatasetLoader:
    """Unified dataset loader for all formats."""
    
    @staticmethod
    def detect_format(file_path: Path) -> DatasetFormat:
        """Detect dataset format from file content or extension."""
        # Try content-based detection first
        try:
            # Check for NumPy format
            with open(file_path, 'rb') as f:
                magic = f.read(6)
                if magic == b'\x93NUMPY':
                    return DatasetFormat.NUMPY
        except:
            pass
        
        try:
            # Check for HDF5 format
            with h5py.File(file_path, 'r') as f:
                return DatasetFormat.HDF5
        except:
            pass
        
        try:
            # Check for Parquet format
            if HAS_PYARROW:
                with pq.ParquetFile(file_path) as pf:
                    return DatasetFormat.PARQUET
        except:
            pass
        
        # Fall back to extension-based detection
        suffix = file_path.suffix.lower()
        name_lower = file_path.name.lower()
        
        if suffix in ['.h5', '.hdf5']:
            return DatasetFormat.HDF5
        elif suffix == '.npy':
            return DatasetFormat.NUMPY
        elif suffix == '.parquet':
            return DatasetFormat.PARQUET
        elif suffix == '.fbin':
            return DatasetFormat.FBIN
        elif suffix == '.ibin':
            return DatasetFormat.IBIN
        elif suffix == '.u8bin':
            return DatasetFormat.U8BIN
        elif suffix == '.bvecs':
            return DatasetFormat.BVECS
        elif suffix == '.fvecs':
            return DatasetFormat.FVECS
        elif suffix == '.csv':
            return DatasetFormat.CSV
        elif name_lower.endswith('.csr.gz'):
            return DatasetFormat.CSR_GZ
        
        return DatasetFormat.UNKNOWN
    
    @staticmethod
    def load(file_path: Path, format: Optional[DatasetFormat] = None) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load dataset from file, auto-detecting format if not specified.
        
        Returns:
            Tuple of (vectors, metadata)
        """
        if format is None:
            format = DatasetLoader.detect_format(file_path)
        
        if format == DatasetFormat.HDF5:
            return DatasetLoader._load_hdf5(file_path)
        elif format == DatasetFormat.NUMPY:
            return DatasetLoader._load_numpy(file_path)
        elif format == DatasetFormat.PARQUET:
            return DatasetLoader._load_parquet(file_path)
        elif format == DatasetFormat.FBIN:
            return DatasetLoader._load_fbin(file_path)
        elif format == DatasetFormat.IBIN:
            return DatasetLoader._load_ibin(file_path)
        elif format == DatasetFormat.U8BIN:
            return DatasetLoader._load_u8bin(file_path)
        elif format == DatasetFormat.BVECS:
            return DatasetLoader._load_bvecs(file_path)
        elif format == DatasetFormat.FVECS:
            return DatasetLoader._load_fvecs(file_path)
        elif format == DatasetFormat.CSV:
            return DatasetLoader._load_csv(file_path)
        elif format == DatasetFormat.CSR_GZ:
            return DatasetLoader._load_sparse(file_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
    
    @staticmethod
    def _load_hdf5(file_path: Path) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load HDF5 dataset."""
        with h5py.File(file_path, 'r') as f:
            metadata = dict(f.attrs)
            
            # Look for standard dataset names
            if 'train' in f:
                vectors = np.array(f['train'])
                metadata['source'] = 'train'
            elif 'vectors' in f:
                vectors = np.array(f['vectors'])
                metadata['source'] = 'vectors'
            else:
                # Use the first dataset found
                datasets = list(f.keys())
                if datasets:
                    vectors = np.array(f[datasets[0]])
                    metadata['source'] = datasets[0]
                else:
                    raise ValueError("No datasets found in HDF5 file")
            
            # Add additional datasets to metadata
            metadata['datasets'] = list(f.keys())
            
            # Load test vectors and ground truth if available
            if 'test' in f:
                metadata['test_vectors'] = np.array(f['test'])
            if 'neighbors' in f:
                metadata['neighbors'] = np.array(f['neighbors'])
            if 'distances' in f:
                metadata['distances'] = np.array(f['distances'])
                
        return vectors, metadata
    
    @staticmethod
    def _load_numpy(file_path: Path) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load NumPy dataset."""
        vectors = np.load(file_path, allow_pickle=False)
        metadata = {
            'format': 'numpy',
            'dtype': str(vectors.dtype),
            'shape': vectors.shape
        }
        return vectors, metadata
    
    @staticmethod 
    def _load_parquet(file_path: Path) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load Parquet dataset."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required to load Parquet files")
        
        df = pd.read_parquet(file_path)
        metadata = {'format': 'parquet', 'columns': list(df.columns)}
        
        # Check for VectorDBBench format
        if 'id' in df.columns and 'emb' in df.columns:
            vectors = np.array(df['emb'].tolist())
            metadata['ids'] = df['id'].values
            metadata['vectordb_bench_format'] = True
        else:
            # Try to find vector columns
            vector_cols = [col for col in df.columns if 'vector' in col.lower() or 'embedding' in col.lower()]
            if vector_cols:
                vectors = np.array(df[vector_cols[0]].tolist())
                metadata['vector_column'] = vector_cols[0]
            else:
                raise ValueError("No vector columns found in Parquet file")
        
        return vectors, metadata
    
    @staticmethod
    def _load_binary_format(file_path: Path, dtype: np.dtype) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load binary format (fbin, ibin, u8bin)."""
        with open(file_path, 'rb') as f:
            # Read header
            header = f.read(8)
            if len(header) != 8:
                raise ValueError("Invalid binary file: cannot read header")
            
            num_vectors, dimension = struct.unpack('<II', header)
            
            # Read vectors
            expected_bytes = num_vectors * dimension * dtype.itemsize
            data_bytes = f.read(expected_bytes)
            
            if len(data_bytes) != expected_bytes:
                raise ValueError(f"File size mismatch: expected {expected_bytes}, got {len(data_bytes)}")
            
            vectors = np.frombuffer(data_bytes, dtype=dtype).reshape(num_vectors, dimension)
            
        metadata = {
            'format': file_path.suffix[1:],
            'num_vectors': num_vectors,
            'dimension': dimension,
            'dtype': str(dtype)
        }
        
        return vectors, metadata
    
    @staticmethod
    def _load_fbin(file_path: Path) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load .fbin format (float32)."""
        return DatasetLoader._load_binary_format(file_path, np.float32)
    
    @staticmethod
    def _load_ibin(file_path: Path) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load .ibin format (int32)."""
        return DatasetLoader._load_binary_format(file_path, np.int32)
    
    @staticmethod
    def _load_u8bin(file_path: Path) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load .u8bin format (uint8)."""
        return DatasetLoader._load_binary_format(file_path, np.uint8)
    
    @staticmethod
    def _load_vecs_format(file_path: Path, dtype: np.dtype) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load BVECS/FVECS format."""
        vectors = []
        
        with open(file_path, 'rb') as f:
            while True:
                # Read dimension
                dim_bytes = f.read(4)
                if len(dim_bytes) == 0:
                    break
                if len(dim_bytes) != 4:
                    raise ValueError("Invalid VECS file")
                
                dimension = struct.unpack('<I', dim_bytes)[0]
                
                # Read vector
                vector_bytes = f.read(dimension * dtype.itemsize)
                if len(vector_bytes) != dimension * dtype.itemsize:
                    raise ValueError("Incomplete vector in file")
                
                vector = np.frombuffer(vector_bytes, dtype=dtype)
                vectors.append(vector)
        
        vectors = np.array(vectors)
        metadata = {
            'format': file_path.suffix[1:],
            'num_vectors': len(vectors),
            'dimension': vectors.shape[1] if len(vectors) > 0 else 0,
            'dtype': str(dtype)
        }
        
        return vectors, metadata
    
    @staticmethod
    def _load_bvecs(file_path: Path) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load .bvecs format (uint8)."""
        return DatasetLoader._load_vecs_format(file_path, np.uint8)
    
    @staticmethod
    def _load_fvecs(file_path: Path) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load .fvecs format (float32)."""
        return DatasetLoader._load_vecs_format(file_path, np.float32)
    
    @staticmethod
    def _load_csv(file_path: Path) -> Tuple[np.ndarray, Dict[str, Any]]:
        """Load CSV dataset."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required to load CSV files")
        
        df = pd.read_csv(file_path)
        metadata = {'format': 'csv', 'columns': list(df.columns)}
        
        # Find vector columns
        vector_cols = [col for col in df.columns if 'vector' in col.lower() or 'embedding' in col.lower()]
        
        if not vector_cols:
            raise ValueError("No vector columns found in CSV")
        
        vector_col = vector_cols[0]
        sample_val = df[vector_col].iloc[0]
        
        # Parse string representations of vectors
        if isinstance(sample_val, str):
            import ast
            vectors = np.array([ast.literal_eval(vec) for vec in df[vector_col]])
        else:
            vectors = np.array(df[vector_col].tolist())
        
        metadata['vector_column'] = vector_col
        return vectors, metadata
    
    @staticmethod
    def _load_sparse(file_path: Path) -> Tuple[Any, Dict[str, Any]]:
        """Load sparse matrix format."""
        if not HAS_SCIPY:
            raise ImportError("scipy is required for sparse matrix loading")
        
        with gzip.open(file_path, 'rb') as f:
            matrix = scipy.sparse.load_npz(f)
        
        metadata = {
            'format': 'sparse_csr',
            'shape': matrix.shape,
            'nnz': matrix.nnz,
            'sparsity': 100 * (1 - matrix.nnz / (matrix.shape[0] * matrix.shape[1]))
        }
        
        return matrix, metadata


class DatasetDownloader:
    """Unified dataset downloader."""
    
    @staticmethod
    def download(dataset_name: str, output_dir: Path, force: bool = False) -> bool:
        """Download a dataset by name.
        
        Returns:
            True if successful, False otherwise
        """
        if dataset_name not in DATASETS:
            typer.echo(f"❌ Unknown dataset: {dataset_name}")
            return False
        
        dataset_info = DATASETS[dataset_name]
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Handle different URL types
        url = dataset_info.get("url", "")
        
        if not url or url in ["custom_download_required", "neurips-competition"]:
            typer.echo(f"❌ Dataset '{dataset_name}' requires manual download")
            typer.echo(f"  Description: {dataset_info['description']}")
            typer.echo(f"  Note: {dataset_info.get('note', 'Contact dataset provider')}")
            return False
        
        # Download files
        files = dataset_info.get("files", {})
        
        if files:
            # Multiple files to download
            return DatasetDownloader._download_multiple_files(url, files, output_dir, force)
        else:
            # Single file download
            return DatasetDownloader._download_single_file(url, output_dir, force)
    
    @staticmethod
    def _download_multiple_files(base_url: str, files: Dict[str, str], output_dir: Path, force: bool) -> bool:
        """Download multiple files for a dataset."""
        success_count = 0
        file_list = files.values() if isinstance(files, dict) else files
        
        for file_name in file_list:
            file_url = f"{base_url.rstrip('/')}/{file_name}"
            output_path = output_dir / file_name
            
            if output_path.exists() and not force:
                typer.echo(f"  ✅ {file_name} already exists")
                success_count += 1
                continue
            
            try:
                typer.echo(f"  ⬇️  Downloading {file_name}...")
                DatasetDownloader._download_with_progress(file_url, output_path)
                
                if output_path.exists() and output_path.stat().st_size > 0:
                    success_count += 1
                    typer.echo(f"  ✅ {file_name} downloaded")
                else:
                    typer.echo(f"  ❌ {file_name} download failed")
                    
            except Exception as e:
                typer.echo(f"  ❌ Error downloading {file_name}: {e}")
                if output_path.exists():
                    output_path.unlink()
        
        return success_count == len(file_list)
    
    @staticmethod
    def _download_single_file(url: str, output_dir: Path, force: bool) -> bool:
        """Download a single file."""
        filename = Path(url).name
        output_path = output_dir / filename
        
        if output_path.exists() and not force:
            typer.echo(f"✅ File already exists: {output_path}")
            return True
        
        try:
            typer.echo(f"⬇️ Downloading {filename} url {url}...")
            DatasetDownloader._download_with_progress(url, output_path)
            
            if output_path.exists() and output_path.stat().st_size > 0:
                typer.echo(f"✅ Downloaded: {output_path}")
                
                # Handle compressed files
                if DatasetDownloader._is_compressed(output_path):
                    typer.echo(f"📦 Extracting {filename}...")
                    DatasetDownloader._extract_compressed(output_path, output_dir)
                
                return True
            else:
                typer.echo(f"❌ Download failed: empty file")
                if output_path.exists():
                    output_path.unlink()
                return False
                
        except Exception as e:
            typer.echo(f"❌ Download error: {e}")
            if output_path.exists():
                output_path.unlink()
            return False
    
    @staticmethod
    def _download_with_progress(url: str, output_path: Path) -> None:
        """Download with progress bar."""
        import sys
        import time
        
        last_update = 0
        
        def progress_hook(block_num, block_size, total_size):
            nonlocal last_update
            current_time = time.time()
            
            # Only update every 0.1 seconds to avoid terminal spam
            if current_time - last_update < 0.1 and block_num > 0:
                return
            last_update = current_time
            
            downloaded = block_num * block_size
            if total_size > 0:
                percent = min(100, (downloaded / total_size) * 100)
                mb_downloaded = downloaded / (1024 * 1024)
                mb_total = total_size / (1024 * 1024)
                
                # Create a progress bar
                bar_length = 30  # Shorter bar
                filled_length = int(bar_length * percent / 100)
                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                
                # Clear the line and write new progress
                progress_text = f"    Progress: {percent:5.1f}% |{bar}| {mb_downloaded:6.1f}/{mb_total:6.1f} MB"
                # Pad with spaces to clear any remaining characters
                sys.stdout.write(f"\r{progress_text:<80}")
                sys.stdout.flush()
            else:
                # Unknown size, just show downloaded amount
                mb_downloaded = downloaded / (1024 * 1024)
                progress_text = f"    Downloaded: {mb_downloaded:6.1f} MB"
                sys.stdout.write(f"\r{progress_text:<80}")
                sys.stdout.flush()
        
        # Set up proper headers to avoid 403 errors
        opener = urllib.request.build_opener()
        opener.addheaders = [
            ('User-Agent', 'Mozilla/5.0 (Compatible VST Dataset Downloader)')
        ]
        urllib.request.install_opener(opener)
        
        urllib.request.urlretrieve(url, output_path, reporthook=progress_hook)
        sys.stdout.write("\n")  # New line after progress
        sys.stdout.flush()
    
    @staticmethod
    def _is_compressed(file_path: Path) -> bool:
        """Check if file is compressed."""
        suffix = file_path.suffix.lower()
        name = file_path.name.lower()
        return (suffix in ['.zip', '.gz', '.bz2', '.xz'] or 
                any(name.endswith(ext) for ext in ['.tar.gz', '.tar.bz2', '.tgz']))
    
    @staticmethod
    def _extract_compressed(file_path: Path, output_dir: Path) -> None:
        """Extract compressed file."""
        suffix = file_path.suffix.lower()
        name = file_path.name.lower()
        
        if suffix == '.zip':
            with zipfile.ZipFile(file_path, 'r') as zf:
                zf.extractall(output_dir)
        elif name.endswith(('.tar.gz', '.tgz', '.tar.bz2', '.tar.xz')):
            with tarfile.open(file_path, 'r:*') as tf:
                tf.extractall(output_dir)
        elif suffix == '.gz' and not name.endswith('.tar.gz'):
            # Simple gzip file
            output_name = file_path.stem
            with gzip.open(file_path, 'rb') as f_in:
                with open(output_dir / output_name, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)


class DatasetConverter:
    """Convert between dataset formats."""
    
    @staticmethod
    def convert(input_path: Path, output_path: Path, output_format: str, sample_size: Optional[int] = None) -> None:
        """Convert dataset to specified format."""
        # Load the dataset
        typer.echo(f"Loading {input_path}...")
        vectors, metadata = DatasetLoader.load(input_path)
        
        # Handle sampling
        if sample_size and sample_size < len(vectors):
            typer.echo(f"Sampling {sample_size} vectors from {len(vectors)}")
            indices = np.random.choice(len(vectors), sample_size, replace=False)
            vectors = vectors[indices]
        
        typer.echo(f"Converting to {output_format} format...")
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if output_format.lower() == "hdf5":
            DatasetConverter._save_hdf5(vectors, metadata, output_path)
        elif output_format.lower() == "npy":
            DatasetConverter._save_numpy(vectors, output_path)
        elif output_format.lower() == "parquet":
            DatasetConverter._save_parquet(vectors, metadata, output_path)
        elif output_format.lower() == "fbin":
            DatasetConverter._save_binary(vectors.astype(np.float32), output_path)
        elif output_format.lower() == "u8bin":
            DatasetConverter._save_binary(vectors.astype(np.uint8), output_path)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        
        typer.echo(f"✅ Saved {len(vectors)} vectors to {output_path}")
    
    @staticmethod
    def _save_hdf5(vectors: np.ndarray, metadata: Dict[str, Any], output_path: Path) -> None:
        """Save as HDF5."""
        with h5py.File(output_path, 'w') as f:
            f.create_dataset('train', data=vectors, compression='gzip', compression_opts=6)
            
            # Save metadata as attributes
            f.attrs['dimensions'] = vectors.shape[1]
            f.attrs['num_vectors'] = vectors.shape[0]
            f.attrs['dtype'] = str(vectors.dtype)
            
            # Save additional datasets if present in metadata
            if 'test_vectors' in metadata:
                f.create_dataset('test', data=metadata['test_vectors'])
            if 'neighbors' in metadata:
                f.create_dataset('neighbors', data=metadata['neighbors'])
            if 'distances' in metadata:
                f.create_dataset('distances', data=metadata['distances'])
    
    @staticmethod
    def _save_numpy(vectors: np.ndarray, output_path: Path) -> None:
        """Save as NumPy."""
        np.save(output_path, vectors)
    
    @staticmethod
    def _save_parquet(vectors: np.ndarray, metadata: Dict[str, Any], output_path: Path) -> None:
        """Save as Parquet."""
        if not HAS_PANDAS:
            raise ImportError("pandas is required to save Parquet files")
        
        # Create DataFrame in VectorDBBench format
        df = pd.DataFrame({
            'id': np.arange(len(vectors)),
            'emb': list(vectors.astype(np.float32))
        })
        df.to_parquet(output_path, index=False)
    
    @staticmethod
    def _save_binary(vectors: np.ndarray, output_path: Path) -> None:
        """Save in binary format (fbin/u8bin)."""
        with open(output_path, 'wb') as f:
            # Write header
            num_vectors, dimension = vectors.shape
            f.write(struct.pack('<II', num_vectors, dimension))
            # Write vectors
            vectors.flatten().tofile(f)


# CLI Commands

@app.command()
def download(
    name: str = typer.Argument(..., help="Dataset name"),
    output_dir: Optional[Path] = typer.Option(None, "--output", "-o", help="Output directory"),
    force: bool = typer.Option(False, "--force", "-f", help="Overwrite existing files"),
):
    """Download a dataset."""
    if name not in DATASETS:
        available = ", ".join(list(DATASETS.keys())[:5]) + "..."
        typer.echo(f"❌ Unknown dataset: {name}")
        typer.echo(f"Available datasets: {available}")
        typer.echo("Use 'vst dataset list' to see all datasets")
        raise typer.Exit(1)
    
    dataset_info = DATASETS[name]
    
    if output_dir is None:
        output_dir = Path(f"./_datasets_downloads/{name}")
    
    typer.echo(f"📦 Downloading: {name}")
    typer.echo(f"📝 Description: {dataset_info['description']}")
    typer.echo(f"💾 Expected size: {dataset_info.get('size_mb', 'Unknown')} MB")
    typer.echo(f"📁 Output: {output_dir}")
    typer.echo()
    
    success = DatasetDownloader.download(name, output_dir, force)
    
    if success:
        typer.echo(f"\n✅ Download completed: {name}")
        typer.echo(f"📁 Files saved in: {output_dir}")
        
        # List the actual dataset files (non-compressed)
        dataset_files = []
        if output_dir.exists():
            for file_path in output_dir.iterdir():
                if file_path.is_file() and not DatasetDownloader._is_compressed(file_path) and not file_path.name.startswith('.'):
                    dataset_files.append(file_path.name)
        
        if dataset_files:
            typer.echo(f"\n📊 Dataset files:")
            for file_name in sorted(dataset_files):
                typer.echo(f"  • {file_name}")
            
            # Show usage hint with first dataset file
            first_file = sorted(dataset_files)[0]
            typer.echo("\n🔧 Next steps:")
            typer.echo(f"  vst external-datasets info {output_dir}/{first_file}")
            if len(dataset_files) > 1:
                typer.echo(f"  # Or info on other files: {', '.join(sorted(dataset_files)[1:])}")
        else:
            typer.echo("\n🔧 Next steps:")
            typer.echo(f"  ls {output_dir}/  # List all files")
            typer.echo(f"  vst external-datasets info {output_dir}/<filename>")
    else:
        typer.echo(f"\n❌ Download failed: {name}")
        raise typer.Exit(1)


@app.command()
def info(dataset_path: Path = typer.Argument(..., help="Path to dataset file")):
    """Display information about a dataset."""
    if not dataset_path.exists():
        typer.echo(f"❌ File not found: {dataset_path}")
        raise typer.Exit(1)
    
    typer.echo(f"📊 Dataset Information: {dataset_path}")
    typer.echo("=" * 50)
    
    # File info
    file_size = dataset_path.stat().st_size
    typer.echo(f"📁 File size: {format_bytes(file_size)}")
    
    try:
        # Detect and load
        format = DatasetLoader.detect_format(dataset_path)
        typer.echo(f"📄 Format: {format.value}")
        
        if format == DatasetFormat.UNKNOWN:
            typer.echo("⚠️  Unknown format - cannot load dataset")
            return
        
        # Load dataset
        typer.echo("⏳ Loading dataset...")
        vectors, metadata = DatasetLoader.load(dataset_path, format)
        
        # Display info based on type
        if isinstance(vectors, np.ndarray):
            typer.echo(f"🔢 Vectors: {len(vectors):,}")
            typer.echo(f"📏 Dimensions: {vectors.shape[1] if len(vectors.shape) > 1 else 1}")
            typer.echo(f"📊 Data type: {vectors.dtype}")
            
            # Sample statistics
            if len(vectors) > 0:
                sample = vectors[:min(1000, len(vectors))]
                typer.echo(f"\n📈 Sample statistics (first 1000):")
                typer.echo(f"  Mean: {np.mean(sample):.4f}")
                typer.echo(f"  Std: {np.std(sample):.4f}")
                typer.echo(f"  Min: {np.min(sample):.4f}")
                typer.echo(f"  Max: {np.max(sample):.4f}")
        
        elif HAS_SCIPY and scipy.sparse.issparse(vectors):
            typer.echo(f"🔢 Sparse matrix: {vectors.shape}")
            typer.echo(f"📊 Non-zeros: {vectors.nnz:,}")
            typer.echo(f"📉 Sparsity: {metadata.get('sparsity', 'N/A'):.2f}%")
        
        # Show metadata
        if metadata:
            typer.echo(f"\n📋 Metadata:")
            for key, value in metadata.items():
                if key not in ['test_vectors', 'neighbors', 'distances']:  # Skip large arrays
                    typer.echo(f"  {key}: {value}")
            
            # Check for additional datasets
            if 'test_vectors' in metadata:
                typer.echo(f"  ✅ Test vectors available: {metadata['test_vectors'].shape}")
            if 'neighbors' in metadata:
                typer.echo(f"  ✅ Ground truth available: {metadata['neighbors'].shape}")
                
    except Exception as e:
        typer.echo(f"❌ Error loading dataset: {e}")
        raise typer.Exit(1)


@app.command()
def convert(
    input_path: Path = typer.Argument(..., help="Input dataset path"),
    output_path: Path = typer.Argument(..., help="Output path"),
    format: str = typer.Option("hdf5", "--format", "-f", help="Output format"),
    sample: Optional[int] = typer.Option(None, "--sample", "-s", help="Sample size"),
):
    """Convert dataset between formats."""
    if not input_path.exists():
        typer.echo(f"❌ Input file not found: {input_path}")
        raise typer.Exit(1)
    
    typer.echo(f"🔄 Converting dataset")
    typer.echo(f"📥 Input: {input_path}")
    typer.echo(f"📤 Output: {output_path}")
    typer.echo(f"📄 Format: {format}")
    
    if sample:
        typer.echo(f"📊 Sample size: {sample:,}")
    
    try:
        DatasetConverter.convert(input_path, output_path, format, sample)
        typer.echo(f"\n✅ Conversion completed!")
        
        # Verify output
        if output_path.exists():
            size = output_path.stat().st_size
            typer.echo(f"📁 Output size: {format_bytes(size)}")
            
    except Exception as e:
        typer.echo(f"❌ Conversion failed: {e}")
        raise typer.Exit(1)


@app.command(name="list")
def list_datasets():
    """List available datasets."""
    typer.echo("📚 Available Datasets")
    typer.echo("=" * 60)
    
    # Group by format
    by_format = {}
    for name, info in DATASETS.items():
        fmt = info.get('format', 'unknown')
        if fmt not in by_format:
            by_format[fmt] = []
        by_format[fmt].append((name, info))
    
    # Display by format
    for fmt, datasets in sorted(by_format.items()):
        typer.echo(f"\n📄 {fmt.upper()} Format:")
        typer.echo("-" * 40)
        
        for name, info in sorted(datasets):
            typer.echo(f"  {name}")
            typer.echo(f"    📝 {info['description']}")
            typer.echo(f"    📏 Dimensions: {info.get('dimensions', 'N/A')}")
            typer.echo(f"    💾 Size: {info.get('size_mb', 'N/A')} MB")
            
            if 'note' in info:
                typer.echo(f"    ℹ️  {info['note']}")
            
            typer.echo()
    
    # Show local files
    typer.echo("\n📁 Local Dataset Files:")
    typer.echo("-" * 40)
    
    local_dirs = [Path("./_datasets_downloads"), Path("./_output"), Path(".")]
    found_files = []
    
    for dir_path in local_dirs:
        if dir_path.exists():
            for file_path in dir_path.rglob('*'):
                if file_path.is_file() and DatasetLoader.detect_format(file_path) != DatasetFormat.UNKNOWN:
                    found_files.append(file_path)
    
    if found_files:
        for file_path in sorted(set(found_files)):
            try:
                rel_path = file_path.relative_to(Path.cwd())
                size = format_bytes(file_path.stat().st_size)
                typer.echo(f"  {rel_path} ({size})")
            except:
                pass
    else:
        typer.echo("  No local dataset files found")


@app.command()
def validate(dataset_path: Path = typer.Argument(..., help="Dataset path")):
    """Validate dataset integrity."""
    if not dataset_path.exists():
        typer.echo(f"❌ Path not found: {dataset_path}")
        raise typer.Exit(1)
    
    typer.echo(f"🔍 Validating: {dataset_path}")
    typer.echo("=" * 50)
    
    try:
        # Try to load the dataset
        format = DatasetLoader.detect_format(dataset_path)
        typer.echo(f"✅ Format detected: {format.value}")
        
        if format == DatasetFormat.UNKNOWN:
            typer.echo("❌ Unknown format")
            raise typer.Exit(1)
        
        vectors, metadata = DatasetLoader.load(dataset_path, format)
        
        # Basic validation
        issues = []
        
        if isinstance(vectors, np.ndarray):
            typer.echo(f"✅ Loaded successfully: {len(vectors):,} vectors")
            
            # Check for NaN/Inf
            if np.any(np.isnan(vectors)):
                issues.append("Contains NaN values")
            if np.any(np.isinf(vectors)):
                issues.append("Contains infinite values")
            
            # Check dimensions consistency
            if len(vectors.shape) != 2:
                issues.append(f"Expected 2D array, got {len(vectors.shape)}D")
            
            # Check if empty
            if len(vectors) == 0:
                issues.append("No vectors found")
        
        elif HAS_SCIPY and scipy.sparse.issparse(vectors):
            typer.echo(f"✅ Loaded sparse matrix: {vectors.shape}")
        
        # Report issues
        if issues:
            typer.echo("\n⚠️  Issues found:")
            for issue in issues:
                typer.echo(f"  - {issue}")
        else:
            typer.echo("\n✅ Validation passed - no issues found")
            
    except Exception as e:
        typer.echo(f"❌ Validation failed: {e}")
        raise typer.Exit(1)


@app.command()
def quick_setup(
    size: str = typer.Argument("small", help="Dataset size (small, medium, large)"),
    output_dir: Optional[Path] = typer.Option(None, "--output", "-o"),
):
    """Quick setup with recommended datasets."""
    recommendations = {
        "small": ("sift-128d", "Good for testing, includes ground truth"),
        "medium": ("bigann-10m", "Production scale testing"), 
        "large": ("deep1b", "Large scale, 96-dimensional"),
    }
    
    if size not in recommendations:
        typer.echo(f"❌ Invalid size. Choose from: {', '.join(recommendations.keys())}")
        raise typer.Exit(1)
    
    dataset_name, description = recommendations[size]
    
    typer.echo(f"🎯 Quick Setup: {size.upper()}")
    typer.echo(f"📦 Dataset: {dataset_name}")
    typer.echo(f"📝 Why: {description}")
    typer.echo()
    
    # Download the dataset
    download(dataset_name, output_dir, force=False)


# Utility functions

def format_bytes(size: int) -> str:
    """Format bytes to human readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"


if __name__ == "__main__":
    app()