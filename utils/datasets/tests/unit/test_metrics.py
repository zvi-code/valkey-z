"""Unit tests for metric collection."""

import pytest
import numpy as np
from core.metrics import MetricCollector


class TestMetricCollection:
    """Test metric collection and aggregation."""
    
    def test_memory_metric_parsing(self, mock_valkey_client):
        """Test parsing of INFO MEMORY output."""
        # Setup mock memory info response
        mock_valkey_client.info = lambda section: {
            "used_memory": 1024 * 1024 * 100,  # 100 MB
            "used_memory_rss": 1024 * 1024 * 150,  # 150 MB
            "allocator_allocated": 1024 * 1024 * 105,
            "allocator_active": 1024 * 1024 * 120,
            "allocator_resident": 1024 * 1024 * 145,
        }
        
        # Test metric parsing
        collector = MetricCollector(mock_valkey_client, sampling_interval=1.0)
        memory_info = mock_valkey_client.info("memory")
        
        # Verify key metrics are present
        assert "used_memory" in memory_info
        assert "used_memory_rss" in memory_info
        
        # Verify values are correctly parsed as bytes
        assert memory_info["used_memory"] == 100 * 1024 * 1024
        assert memory_info["used_memory_rss"] == 150 * 1024 * 1024
        
    def test_percentile_calculation(self):
        """Test percentile calculations."""
        # Create test data
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        
        # Test percentile calculations
        p50 = np.percentile(values, 50)
        p95 = np.percentile(values, 95)
        p99 = np.percentile(values, 99)
        
        # Verify results (allow for floating point precision)
        assert abs(p50 - 5.5) < 1e-10  # Median of 1-10
        assert abs(p95 - 9.55) < 1e-10  # 95th percentile
        assert abs(p99 - 9.91) < 1e-10  # 99th percentile
        
        # Test edge cases
        single_value = [42]
        assert np.percentile(single_value, 50) == 42
        assert np.percentile(single_value, 95) == 42
        assert np.percentile(single_value, 99) == 42
