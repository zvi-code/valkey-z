"""Unit tests for vector operations."""

import pytest
import numpy as np
from core.vector_ops import VectorOperations


class TestVectorOperations:
    """Test vector mathematical operations."""
    
    def test_calculate_norm(self):
        """Test L2 norm calculation."""
        ops = VectorOperations()
        
        # Test single vector
        vector = np.array([3.0, 4.0])
        norm = ops.calculate_norm(vector)
        assert abs(norm - 5.0) < 1e-6  # sqrt(3^2 + 4^2) = 5
        
        # Test batch of vectors
        vectors = np.array([[3.0, 4.0], [5.0, 12.0]])
        norms = ops.calculate_norm(vectors)
        expected = [5.0, 13.0]  # sqrt(3^2+4^2)=5, sqrt(5^2+12^2)=13
        np.testing.assert_allclose(norms, expected, rtol=1e-6)
        
        # Test edge cases
        zero_vector = np.array([0.0, 0.0])
        assert ops.calculate_norm(zero_vector) == 0.0
        
    def test_generate_vector_with_norm(self):
        """Test vector generation with specific norm."""
        ops = VectorOperations(dimensions=128)  # Set dimensions in constructor
        
        # Test norm accuracy
        target_norm = 10.0
        vector = ops.generate_vector_with_norm(target_norm)  # Remove dimensions parameter
        
        actual_norm = np.linalg.norm(vector)
        assert abs(actual_norm - target_norm) < 1e-6
        assert vector.shape == (128,)  # Use the dimensions set in constructor
        
        # Test reproducibility with seed
        vector1 = ops.generate_vector_with_norm(5.0, seed=42)
        vector2 = ops.generate_vector_with_norm(5.0, seed=42)
        np.testing.assert_allclose(vector1, vector2)
        
    def test_expand_dataset(self):
        """Test dataset expansion."""
        ops = VectorOperations(dimensions=64)
        
        # Test vector addition
        original_vectors = np.random.randn(10, 64).astype(np.float32)
        original_keys = [f"vec_{i}" for i in range(10)]
        expansion_vector = np.random.randn(64).astype(np.float32)
        
        expanded_vectors, expanded_keys = ops.expand_dataset(
            vectors=original_vectors,
            keys=original_keys,
            expansion_index=1,
            expansion_vector=expansion_vector
        )
        
        # Verify expansion
        assert expanded_vectors.shape[0] == 10
        assert expanded_vectors.shape[1] == 64
        assert len(expanded_keys) == 10
        
        # Test key generation
        assert all(key.startswith("expand_1_vec_") for key in expanded_keys)
        assert len(set(expanded_keys)) == 10  # All keys unique
        
        # Test vector transformation
        expected_vectors = original_vectors + expansion_vector
        np.testing.assert_allclose(expanded_vectors, expected_vectors)
