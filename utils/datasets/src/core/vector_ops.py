# src/valkey_memory_stress/core/vector_ops.py
"""
Vector operations and expansion logic for dataset augmentation.

This module provides functions for:
- Generating vectors with specific norm constraints
- Expanding datasets by creating new vectors with ||V_n||₂ > NormMax
- Key generation for expanded vectors
- Vector arithmetic operations
"""

from __future__ import annotations

import numpy as np
from typing import Dict, List, Tuple, Optional, Union
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)


@dataclass
class VectorExpansionConfig:
    """Configuration for vector expansion operations."""
    target_norm_multiplier: float = 1.5  # How much larger than max_norm
    seed: Optional[int] = None  # For reproducible random generation
    dtype: np.dtype = np.float32


class VectorOperations:
    """
    Handles vector mathematical operations and dataset expansion.
    """
    
    def __init__(self, dimensions: int = 1536, dtype: np.dtype = np.float32):
        """
        Initialize vector operations handler.
        
        Args:
            dimensions: Vector dimensionality (default: 1536 for OpenAI)
            dtype: Data type for vectors (default: float32)
        """
        self.dimensions = dimensions
        self.dtype = dtype
        self._rng: Optional[np.random.Generator] = None
        
    def calculate_norm(self, vectors: np.ndarray) -> np.ndarray:
        """
        Calculate L2 norm for a batch of vectors.
        
        Args:
            vectors: numpy array of shape (n, dimensions) or (dimensions,)
            
        Returns:
            numpy array of shape (n,) containing L2 norms or scalar if single vector
        """
        if vectors.ndim == 1:
            # Single vector
            return np.linalg.norm(vectors, ord=2)
        else:
            # Batch of vectors
            return np.linalg.norm(vectors, ord=2, axis=1)
        
    def find_max_norm_vector(self, vectors: np.ndarray) -> Tuple[np.ndarray, float]:
        """
        Find the vector with maximum L2 norm in a dataset.
        
        Args:
            vectors: numpy array of shape (n, dimensions)
            
        Returns:
            Tuple of (max_norm_vector, max_norm_value)
        """
        norms = self.calculate_norm(vectors)
        max_idx = np.argmax(norms)
        max_norm = norms[max_idx]
        max_vector = vectors[max_idx].copy()
        
        logger.debug(f"Found max norm vector at index {max_idx} with norm {max_norm:.4f}")
        return max_vector, max_norm
        
    def generate_vector_with_norm(self, 
                                 target_norm: float,
                                 seed: Optional[int] = None) -> np.ndarray:
        """
        Generate a random vector with specified L2 norm.
        
        Args:
            target_norm: Desired L2 norm for the vector
            seed: Random seed for reproducibility
            
        Returns:
            numpy array of shape (dimensions,) with specified norm
        """
        if seed is not None:
            rng = np.random.default_rng(seed)
        else:
            if self._rng is None:
                self._rng = np.random.default_rng()
            rng = self._rng
            
        # Generate random unit vector
        # Using normal distribution ensures uniform distribution on unit sphere
        random_vector = rng.standard_normal(self.dimensions).astype(self.dtype)
        
        # Normalize to unit vector
        unit_vector = random_vector / np.linalg.norm(random_vector)
        
        # Scale to target norm
        scaled_vector = unit_vector * target_norm
        
        # Verify the norm (for debugging)
        actual_norm = np.linalg.norm(scaled_vector)
        if abs(actual_norm - target_norm) > 1e-5:
            logger.warning(f"Generated vector norm {actual_norm:.6f} differs from target {target_norm:.6f}")
            
        return scaled_vector.astype(self.dtype)
        
    def generate_expansion_vectors(self,
                                 max_norm: float,
                                 n_expansions: int,
                                 config: Optional[VectorExpansionConfig] = None) -> Dict[int, np.ndarray]:
        """
        Generate n vectors V_n where ||V_n||₂ > max_norm.
        
        Args:
            max_norm: Maximum norm from original dataset
            n_expansions: Number of expansion vectors to generate
            config: Expansion configuration
            
        Returns:
            Dictionary mapping expansion index to vector
        """
        if config is None:
            config = VectorExpansionConfig()
            
        expansion_vectors = {}
        base_seed = config.seed
        
        for n in range(1, n_expansions + 1):
            # Each expansion vector has progressively larger norm
            target_norm = max_norm * config.target_norm_multiplier * n
            
            # Use deterministic seed for each expansion if base seed provided
            if base_seed is not None:
                seed = base_seed + n
            else:
                seed = None
                
            expansion_vectors[n] = self.generate_vector_with_norm(
                target_norm=target_norm,
                seed=seed
            )
            
            logger.info(f"Generated expansion vector {n} with norm {target_norm:.4f}")
            
        return expansion_vectors
        
    def expand_vector(self, 
                     original_vector: np.ndarray,
                     expansion_vector: np.ndarray) -> np.ndarray:
        """
        Create expanded vector: V_expanded = V_original + V_n.
        
        Args:
            original_vector: Original dataset vector
            expansion_vector: Expansion vector V_n
            
        Returns:
            Expanded vector
        """
        if original_vector.shape != expansion_vector.shape:
            raise ValueError(f"Vector shapes must match: {original_vector.shape} vs {expansion_vector.shape}")
            
        expanded = original_vector + expansion_vector
        return expanded.astype(self.dtype)
        
    def generate_expanded_key(self, 
                            original_key: str,
                            expansion_index: int) -> str:
        """
        Generate key for expanded vector following pattern: expand_n_<original_key>.
        
        Args:
            original_key: Original vector key
            expansion_index: Index n of expansion vector
            
        Returns:
            Expanded vector key
        """
        return f"expand_{expansion_index}_{original_key}"
        
    def expand_dataset(self,
                      vectors: np.ndarray,
                      keys: List[str],
                      expansion_index: int,
                      expansion_vector: np.ndarray) -> Tuple[np.ndarray, List[str]]:
        """
        Expand entire dataset with a single expansion vector V_n.
        
        Args:
            vectors: Original dataset vectors of shape (n, dimensions)
            keys: Original vector keys
            expansion_index: Index n for this expansion
            expansion_vector: The expansion vector V_n
            
        Returns:
            Tuple of (expanded_vectors, expanded_keys)
        """
        if len(vectors) != len(keys):
            raise ValueError(f"Number of vectors {len(vectors)} must match number of keys {len(keys)}")
            
        # Broadcast addition: add expansion_vector to each row
        expanded_vectors = vectors + expansion_vector[np.newaxis, :]
        expanded_vectors = expanded_vectors.astype(self.dtype)
        
        # Generate new keys
        expanded_keys = [
            self.generate_expanded_key(key, expansion_index) 
            for key in keys
        ]
        
        logger.debug(f"Expanded {len(vectors)} vectors with expansion index {expansion_index}")
        
        return expanded_vectors, expanded_keys
        
    def expand_ground_truth(self,
                          original_ground_truth: np.ndarray,
                          original_size: int,
                          expansion_index: int) -> np.ndarray:
        """
        Adjust ground truth indices for expanded dataset.
        
        When we expand dataset by adding V_n to each vector, the ground truth
        nearest neighbors need to be adjusted to account for the new indices.
        
        Args:
            original_ground_truth: Original ground truth indices of shape (n_queries, k)
            original_size: Size of original dataset
            expansion_index: Current expansion index
            
        Returns:
            Adjusted ground truth indices for expanded dataset
        """
        # For expanded vectors, the nearest neighbors are the same original vectors
        # but shifted by the expansion offset
        # Since V_expanded = V_original + V_n, and V_n is the same for all vectors,
        # the relative distances remain the same within each expansion
        
        # The expanded ground truth is the original indices offset by
        # (expansion_index - 1) * original_size
        offset = (expansion_index - 1) * original_size
        expanded_ground_truth = original_ground_truth + offset
        
        return expanded_ground_truth
        
    def validate_expansion(self,
                         expansion_vectors: Dict[int, np.ndarray],
                         max_norm: float) -> bool:
        """
        Validate that all expansion vectors satisfy ||V_n||₂ > max_norm.
        
        Args:
            expansion_vectors: Dictionary of expansion vectors
            max_norm: Maximum norm from original dataset
            
        Returns:
            True if all vectors satisfy the constraint
        """
        all_valid = True
        
        for n, vector in expansion_vectors.items():
            norm = np.linalg.norm(vector)
            if norm <= max_norm:
                logger.error(f"Expansion vector {n} has norm {norm:.4f} <= max_norm {max_norm:.4f}")
                all_valid = False
            else:
                logger.debug(f"Expansion vector {n} has valid norm {norm:.4f} > {max_norm:.4f}")
                
        return all_valid


class VectorSampler:
    """
    Handles sampling strategies for vector selection during shrinking.
    """
    
    def __init__(self, seed: Optional[int] = None):
        """
        Initialize the sampler.
        
        Args:
            seed: Random seed for reproducibility
        """
        self._rng = np.random.default_rng(seed)
        
    def sample_for_deletion(self,
                          total_vectors: int,
                          shrink_ratio: float,
                          exclude_patterns: Optional[List[str]] = None) -> List[int]:
        """
        Sample vector indices for deletion based on shrink ratio.
        
        Args:
            total_vectors: Total number of vectors in index
            shrink_ratio: Fraction of vectors to delete (0.0 to 1.0)
            exclude_patterns: Key patterns to exclude from deletion (not implemented)
            
        Returns:
            List of indices to delete
        """
        if not 0.0 <= shrink_ratio <= 1.0:
            raise ValueError(f"Shrink ratio must be between 0.0 and 1.0, got {shrink_ratio}")
            
        n_to_delete = int(total_vectors * shrink_ratio)
        
        if n_to_delete == 0:
            logger.warning(f"Shrink ratio {shrink_ratio} on {total_vectors} vectors results in 0 deletions")
            return []
            
        # Random sample without replacement
        indices_to_delete = self._rng.choice(
            total_vectors, 
            size=n_to_delete, 
            replace=False
        ).tolist()
        
        logger.info(f"Sampled {n_to_delete} vectors for deletion out of {total_vectors}")
        
        return indices_to_delete
        
    def stratified_sample(self,
                         vector_groups: Dict[str, List[int]],
                         shrink_ratio: float) -> List[int]:
        """
        Sample vectors for deletion with stratification across groups.
        
        Ensures balanced deletion across original and expanded vectors.
        
        Args:
            vector_groups: Dictionary mapping group names to vector indices
            shrink_ratio: Fraction to delete from each group
            
        Returns:
            List of indices to delete
        """
        all_indices_to_delete = []
        
        for group_name, group_indices in vector_groups.items():
            n_in_group = len(group_indices)
            n_to_delete = int(n_in_group * shrink_ratio)
            
            if n_to_delete > 0:
                # Sample from this group
                sampled = self._rng.choice(
                    group_indices,
                    size=n_to_delete,
                    replace=False
                ).tolist()
                all_indices_to_delete.extend(sampled)
                
                logger.debug(f"Sampled {n_to_delete} vectors from group '{group_name}'")
                
        return all_indices_to_delete


def calculate_recall(retrieved_indices: np.ndarray,
                    ground_truth: np.ndarray,
                    k: int = 10) -> float:
    """
    Calculate recall@k for retrieved results against ground truth.
    
    Args:
        retrieved_indices: Retrieved neighbor indices from search, shape (n_queries, k)
        ground_truth: True nearest neighbor indices, shape (n_queries, k_truth)
        k: Number of neighbors to consider
        
    Returns:
        Recall@k score (0.0 to 1.0)
    """
    if retrieved_indices.ndim == 1:
        # Single query
        retrieved_indices = retrieved_indices.reshape(1, -1)
        ground_truth = ground_truth.reshape(1, -1)
        
    n_queries = retrieved_indices.shape[0]
    
    # Ensure we only look at top k results
    retrieved_k = retrieved_indices[:, :k]
    ground_truth_k = ground_truth[:, :k]
    
    total_recall = 0.0
    
    for i in range(n_queries):
        # Convert to sets for intersection
        retrieved_set = set(retrieved_k[i])
        truth_set = set(ground_truth_k[i])
        
        # Calculate intersection
        correct = len(retrieved_set.intersection(truth_set))
        
        # Recall for this query
        query_recall = correct / len(truth_set) if len(truth_set) > 0 else 0.0
        total_recall += query_recall
        
    # Average recall across all queries
    avg_recall = total_recall / n_queries if n_queries > 0 else 0.0
    
    return avg_recall


def generate_test_vectors(n_vectors: int = 100,
                         dimensions: int = 1536,
                         norm_range: Tuple[float, float] = (10.0, 100.0),
                         seed: Optional[int] = None) -> np.ndarray:
    """
    Generate test vectors for development and testing.
    
    Args:
        n_vectors: Number of vectors to generate
        dimensions: Vector dimensionality
        norm_range: Range of L2 norms for generated vectors
        seed: Random seed
        
    Returns:
        numpy array of test vectors
    """
    rng = np.random.default_rng(seed)
    vec_ops = VectorOperations(dimensions=dimensions)
    
    # Generate random norms within the specified range
    min_norm, max_norm = norm_range
    norms = rng.uniform(min_norm, max_norm, size=n_vectors)
    
    # Generate vectors with these norms
    vectors = []
    for i, target_norm in enumerate(norms):
        # Use seed for reproducibility if provided
        vector_seed = seed + i if seed is not None else None
        vector = vec_ops.generate_vector_with_norm(target_norm, seed=vector_seed)
        vectors.append(vector)
        
    return np.array(vectors, dtype=np.float32)