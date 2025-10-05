"""
Dataset loading and management for OpenAI vector datasets.

This module handles loading vectors from HDF5 files and provides
interfaces for accessing training vectors, test vectors, and ground truth
neighbor information.
"""

from __future__ import annotations

import h5py
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Generator
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class DatasetInfo:
    """Metadata about the loaded dataset."""
    name: str
    dimensions: int
    train_size: int
    test_size: int
    has_ground_truth: bool
    dtype: np.dtype
    max_norm: float


class Dataset:
    """
    Handles loading and accessing OpenAI dataset from HDF5 files.
    
    The OpenAI dataset format typically contains:
    - 'train': Training vectors
    - 'test': Query vectors for testing
    - 'neighbors': Ground truth nearest neighbors for test vectors
    """
    
    def __init__(self, dataset_path: Union[str, Path]):
        """
        Initialize the dataset from an HDF5 file.
        
        Args:
            dataset_path: Path to the HDF5 dataset file
        """
        self.dataset_path = Path(dataset_path)
        self._file: Optional[h5py.File] = None
        self._train_vectors: Optional[np.ndarray] = None
        self._test_vectors: Optional[np.ndarray] = None
        self._ground_truth: Optional[np.ndarray] = None
        self._info: Optional[DatasetInfo] = None
        self._max_norm: Optional[float] = None
        
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset file not found: {self.dataset_path}")
        
    def load(self) -> DatasetInfo:
        """
        Load the dataset into memory.
        
        Returns:
            DatasetInfo object containing dataset metadata
            
        Raises:
            ValueError: If dataset format is invalid
            IOError: If file cannot be read
        """
        logger.info(f"Loading dataset from {self.dataset_path}")
        
        try:
            self._file = h5py.File(self.dataset_path, 'r')
            
            # Load train vectors
            if 'train' in self._file:
                self._train_vectors = np.array(self._file['train'], dtype=np.float32)
                logger.info(f"Loaded {len(self._train_vectors)} training vectors")
            else:
                raise ValueError("Dataset missing 'train' group")
            
            # Load test vectors
            if 'test' in self._file:
                self._test_vectors = np.array(self._file['test'], dtype=np.float32)
                logger.info(f"Loaded {len(self._test_vectors)} test vectors")
            else:
                self._test_vectors = np.array([])
                logger.warning("Dataset missing 'test' group")
            
            # Load ground truth
            has_ground_truth = False
            if 'neighbors' in self._file:
                self._ground_truth = np.array(self._file['neighbors'], dtype=np.int32)
                has_ground_truth = True
                logger.info(f"Loaded ground truth for {len(self._ground_truth)} queries")
            else:
                logger.warning("Dataset missing 'neighbors' group")
            
            # Calculate max norm
            self._calculate_max_norm()
            
            # Create DatasetInfo
            self._info = DatasetInfo(
                name=self.dataset_path.stem,
                dimensions=self._train_vectors.shape[1],
                train_size=len(self._train_vectors),
                test_size=len(self._test_vectors) if self._test_vectors is not None else 0,
                has_ground_truth=has_ground_truth,
                dtype=self._train_vectors.dtype,
                max_norm=self._max_norm
            )
            
            logger.info(f"Dataset loaded successfully: {self._info}")
            return self._info
            
        except Exception as e:
            logger.error(f"Failed to load dataset: {e}")
            self.close()
            raise
        
    def _calculate_max_norm(self) -> None:
        """Calculate the maximum L2 norm across all vectors."""
        max_norm_train = 0.0
        
        # Process train vectors in chunks to save memory
        chunk_size = 10000
        for i in range(0, len(self._train_vectors), chunk_size):
            chunk = self._train_vectors[i:i + chunk_size]
            norms = np.linalg.norm(chunk, axis=1)
            chunk_max = np.max(norms)
            max_norm_train = max(max_norm_train, chunk_max)
        
        # Check test vectors if available
        max_norm_test = 0.0
        if self._test_vectors is not None and len(self._test_vectors) > 0:
            test_norms = np.linalg.norm(self._test_vectors, axis=1)
            max_norm_test = np.max(test_norms)
        
        self._max_norm = max(max_norm_train, max_norm_test)
        logger.info(f"Maximum L2 norm in dataset: {self._max_norm:.4f}")
                
    def get_train_vectors(self, 
                         indices: Optional[List[int]] = None) -> np.ndarray:
        """
        Get training vectors, optionally by indices.
        
        Args:
            indices: Specific indices to retrieve. If None, returns all.
            
        Returns:
            numpy array of shape (n, dimensions) containing vectors
        """
        if self._train_vectors is None:
            raise RuntimeError("Dataset not loaded. Call load() first.")
            
        if indices is not None:
            return self._train_vectors[indices]
        else:
            return self._train_vectors
    
    def get_train_vectors_batched(self, 
                                 batch_size: int,
                                 indices: Optional[List[int]] = None):
        """
        Get training vectors in batches.
        
        Args:
            batch_size: Size of each batch
            indices: Specific indices to retrieve. If None, returns all.
            
        Yields:
            numpy arrays of shape (batch_size, dimensions) containing vectors
        """
        if self._train_vectors is None:
            raise RuntimeError("Dataset not loaded. Call load() first.")
            
        if indices is not None:
            vectors = self._train_vectors[indices]
        else:
            vectors = self._train_vectors

        for i in range(0, len(vectors), batch_size):
            yield vectors[i:i + batch_size]
        
        
    def get_test_vectors(self, 
                        indices: Optional[List[int]] = None) -> np.ndarray:
        """
        Get test/query vectors.
        
        Args:
            indices: Specific indices to retrieve. If None, returns all.
            
        Returns:
            numpy array of shape (n, dimensions) containing query vectors
        """
        if self._test_vectors is None:
            raise RuntimeError("Dataset not loaded. Call load() first.")

        if indices is not None:
            return self._test_vectors[indices]
        else:
            return self._test_vectors
        
    def get_ground_truth(self, 
                        query_indices: Optional[List[int]] = None,
                        k: int = 10) -> np.ndarray:
        """
        Get ground truth nearest neighbors for query vectors.
        
        Args:
            query_indices: Indices of query vectors. If None, returns all.
            k: Number of nearest neighbors to return
            
        Returns:
            numpy array of shape (n_queries, k) containing neighbor indices
        """
        if self._ground_truth is None:
            raise RuntimeError("Ground truth not available in this dataset")
        
        if query_indices is not None:
            gt = self._ground_truth[query_indices]
        else:
            gt = self._ground_truth
        
        # Return only top k neighbors
        if gt.shape[1] > k:
            return gt[:, :k]
        else:
            return gt
        
    def get_vector_by_key(self, key: str) -> Optional[np.ndarray]:
        """
        Retrieve a specific vector by its key/identifier.
        
        Args:
            key: Vector identifier (e.g., "train_0", "test_100")
            
        Returns:
            Vector as numpy array or None if not found
        """
        if self._train_vectors is None:
            raise RuntimeError("Dataset not loaded. Call load() first.")
        
        # Parse key format: "train_123" or "test_45"
        parts = key.split('_')
        if len(parts) != 2:
            logger.error(f"Invalid key format: {key}")
            return None
        
        dataset_type, idx_str = parts
        try:
            idx = int(idx_str)
        except ValueError:
            logger.error(f"Invalid index in key: {key}")
            return None
        
        if dataset_type == 'train' and 0 <= idx < len(self._train_vectors):
            return self._train_vectors[idx]
        elif dataset_type == 'test' and self._test_vectors is not None and 0 <= idx < len(self._test_vectors):
            return self._test_vectors[idx]
        else:
            logger.error(f"Key out of range: {key}")
            return None
        
    def get_max_norm(self) -> float:
        """
        Get the maximum L2 norm across all vectors in the dataset.
        
        Returns:
            Maximum L2 norm value
        """
        if self._max_norm is None:
            raise RuntimeError("Dataset not loaded. Call load() first.")
        return self._max_norm
        
    def iterate_batches(self, 
                       batch_size: int = 1000,
                       shuffle: bool = False) -> Generator[Tuple[np.ndarray, List[str]], None, None]:
        """
        Iterate over dataset in batches.
        
        Args:
            batch_size: Number of vectors per batch
            shuffle: Whether to shuffle the order
            
        Yields:
            Tuple of (vectors, keys) for each batch
        """
        if self._train_vectors is None:
            raise RuntimeError("Dataset not loaded. Call load() first.")
        
        n_vectors = len(self._train_vectors)
        indices = np.arange(n_vectors)
        
        if shuffle:
            np.random.shuffle(indices)
        
        for i in range(0, n_vectors, batch_size):
            batch_indices = indices[i:i + batch_size]
            batch_vectors = self._train_vectors[batch_indices]
            
            # Generate keys for this batch
            batch_keys = [f"train_{idx}" for idx in batch_indices]
            
            yield batch_vectors, batch_keys
        
    def get_sample(self, n: int = 100, seed: Optional[int] = None) -> np.ndarray:
        """
        Get a random sample of vectors for testing.
        
        Args:
            n: Number of vectors to sample
            seed: Random seed for reproducibility
            
        Returns:
            numpy array of sampled vectors
        """
        if self._train_vectors is None:
            raise RuntimeError("Dataset not loaded. Call load() first.")
        
        rng = np.random.default_rng(seed)
        
        if n >= len(self._train_vectors):
            return self._train_vectors
        
        indices = rng.choice(len(self._train_vectors), size=n, replace=False)
        return self._train_vectors[indices]
        
    def close(self):
        """Close the HDF5 file if open."""
        if self._file is not None:
            self._file.close()
            self._file = None
            logger.debug("Dataset file closed")
            
    def __enter__(self):
        """Context manager support."""
        self.load()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager cleanup."""
        self.close()
        
    @property
    def info(self) -> DatasetInfo:
        """Get dataset information."""
        if self._info is None:
            raise RuntimeError("Dataset not loaded. Call load() first.")
        return self._info


class DatasetManager:
    """
    Manages multiple datasets and provides unified access.
    Future extension point for handling multiple dataset types.
    """
    
    def __init__(self):
        """Initialize the dataset manager."""
        self.datasets: Dict[str, Dataset] = {}
        self._active_dataset: Optional[str] = None
        
    def register_dataset(self, name: str, path: Union[str, Path]):
        """
        Register a dataset for use.
        
        Args:
            name: Identifier for the dataset
            path: Path to the dataset file
        """
        path = Path(path)
        
        if not path.exists():
            raise FileNotFoundError(f"Dataset file not found: {path}")
        
        # Create dataset instance but don't load yet
        dataset = Dataset(path)
        
        # Validate it's a valid HDF5 file
        try:
            with h5py.File(path, 'r') as f:
                if 'train' not in f:
                    raise ValueError(f"Dataset {path} missing required 'train' group")
        except Exception as e:
            raise ValueError(f"Invalid dataset file {path}: {e}")
        
        self.datasets[name] = dataset
        logger.info(f"Registered dataset '{name}' from {path}")
        
    def get_dataset(self, name: str) -> Dataset:
        """
        Retrieve a registered dataset.
        
        Args:
            name: Dataset identifier
            
        Returns:
            Dataset instance
            
        Raises:
            KeyError: If dataset not found
        """
        if name not in self.datasets:
            raise KeyError(f"Dataset '{name}' not found. Available: {list(self.datasets.keys())}")
        
        dataset = self.datasets[name]
        
        # Load if not already loaded
        if dataset._info is None:
            dataset.load()
        
        self._active_dataset = name
        return dataset
    
    def list_datasets(self) -> List[str]:
        """List all registered dataset names."""
        return list(self.datasets.keys())
    
    def get_active_dataset(self) -> Optional[Dataset]:
        """Get the currently active dataset."""
        if self._active_dataset is None:
            return None
        return self.get_dataset(self._active_dataset)