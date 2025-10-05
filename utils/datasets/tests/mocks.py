# tests/mocks.py
"""Mock implementations for testing without Valkey."""

from typing import Dict, List, Any, Optional, Tuple, Set
import asyncio
import struct
import numpy as np
import re
from collections import defaultdict
import fnmatch


class MockValkeyClient:
    """Mock Valkey client for testing."""
    
    def __init__(self):
        self.data: Dict[str, Any] = {}
        self.hash_data: Dict[str, Dict[str, Any]] = defaultdict(dict)
        self.indices: Dict[str, MockIndex] = {}
        self.call_count: Dict[str, int] = defaultdict(int)
        self.latency_ms: float = 1.0  # Simulated latency
        self._closed = False
        
    async def ping(self) -> bool:
        """Mock ping command."""
        self.call_count["ping"] += 1
        await asyncio.sleep(self.latency_ms / 1000)
        return True
        
    async def set(self, key: str, value: Any) -> bool:
        """Mock SET command."""
        self.call_count["set"] += 1
        await asyncio.sleep(self.latency_ms / 1000)
        self.data[key] = value
        return True
        
    async def get(self, key: str) -> Optional[Any]:
        """Mock GET command."""
        self.call_count["get"] += 1
        await asyncio.sleep(self.latency_ms / 1000)
        return self.data.get(key)
        
    async def delete(self, *keys: str) -> int:
        """Mock DELETE command."""
        self.call_count["delete"] += 1
        await asyncio.sleep(self.latency_ms / 1000)
        deleted = 0
        for key in keys:
            if key in self.data:
                del self.data[key]
                deleted += 1
            if key in self.hash_data:
                del self.hash_data[key]
                deleted += 1
        return deleted
        
    async def hset(self, key: str, field: str, value: Any) -> int:
        """Mock HSET command."""
        self.call_count["hset"] += 1
        await asyncio.sleep(self.latency_ms / 1000)
        is_new = field not in self.hash_data[key]
        self.hash_data[key][field] = value
        return 1 if is_new else 0
        
    async def hget(self, key: str, field: str) -> Optional[Any]:
        """Mock HGET command."""
        self.call_count["hget"] += 1
        await asyncio.sleep(self.latency_ms / 1000)
        return self.hash_data.get(key, {}).get(field)
        
    async def scan(self, cursor: int, match: Optional[str] = None, count: int = 10) -> Tuple[int, List[bytes]]:
        """Mock SCAN command."""
        self.call_count["scan"] += 1
        await asyncio.sleep(self.latency_ms / 1000)
        
        # Get all keys from both data and hash_data
        all_keys = set()
        all_keys.update(self.data.keys())
        all_keys.update(self.hash_data.keys())
        all_keys = list(all_keys)
        
        # Filter by pattern if provided
        if match:
            import fnmatch
            filtered_keys = [k for k in all_keys if fnmatch.fnmatch(k, match)]
        else:
            filtered_keys = all_keys
        
        # Simulate cursor pagination
        start_idx = cursor
        end_idx = min(start_idx + count, len(filtered_keys))
        
        keys = filtered_keys[start_idx:end_idx]
        next_cursor = 0 if end_idx >= len(filtered_keys) else end_idx
        
        return next_cursor, [k.encode() if isinstance(k, str) else k for k in keys]
        
    async def info(self, section: str = "default") -> Dict[str, Any]:
        """Mock INFO command."""
        self.call_count["info"] += 1
        await asyncio.sleep(self.latency_ms / 1000)
        
        if section == "memory":
            return {
                "used_memory": 1024 * 1024 * 100,  # 100 MB
                "used_memory_rss": 1024 * 1024 * 150,  # 150 MB
                "allocator_allocated": 1024 * 1024 * 105,
                "allocator_active": 1024 * 1024 * 120,
                "allocator_resident": 1024 * 1024 * 145,
            }
        elif section == "server":
            return {
                "valkey_version": "7.0.0",
                "valkey_mode": "standalone",
            }
        elif section == "cluster":
            return {
                "cluster_enabled": 0,
            }
        else:
            return {}
            
    async def execute_command(self, *args) -> Any:
        """Mock for custom commands like FT.CREATE, FT.SEARCH."""
        self.call_count["execute_command"] += 1
        await asyncio.sleep(self.latency_ms / 1000)
        
        command = args[0].upper()
        
        if command == "FT.CREATE":
            # Mock index creation
            index_name = args[1]
            self.indices[index_name] = MockIndex(index_name)
            return "OK"
            
        elif command == "FT._LIST":
            # Return list of indices
            return [name.encode() for name in self.indices.keys()]
            
        elif command == "FT.INFO":
            # Mock index info
            index_name = args[1] if len(args) > 1 else "unknown"
            # Always return a valid index info to avoid "Unknown index" errors
            return [
                b"index_name", index_name.encode() if isinstance(index_name, str) else index_name,
                b"num_docs", 100,  # Return some documents
                b"num_records", 100,
                b"hash_indexing_failures", 0,
            ]
                
        elif command == "FT.SEARCH":
            # Mock vector search - always return some results
            index_name = args[1] if len(args) > 1 else "unknown"
            return self._mock_search_result(args)
                
        else:
            raise Exception(f"Unknown command: {command}")
            
    def _mock_search_result(self, args) -> List[Any]:
        """Generate mock search results."""
        # Extract K from query
        query = args[2]
        k_match = re.search(r'KNN (\d+)', query)
        k = int(k_match.group(1)) if k_match else 10
        
        # Generate mock results
        results = [k]  # Total results
        
        for i in range(k):
            key = f"train_{i}"
            score = 0.1 * (i + 1)  # Increasing distances
            results.extend([
                key.encode(),
                [b"__vector_score", str(score).encode()]
            ])
            
        return results
        
    async def close(self) -> None:
        """Mock close method."""
        self.call_count["close"] += 1
        self._closed = True
        
    async def pipeline(self, transaction: bool = True):
        """Mock pipeline."""
        return MockPipeline(self)


class MockIndex:
    """Mock vector index."""
    
    def __init__(self, name: str):
        self.name = name
        self.num_docs = 0
        self.vectors: Dict[str, np.ndarray] = {}
        
    def add_vector(self, key: str, vector: np.ndarray):
        """Add a vector to the index."""
        self.vectors[key] = vector
        self.num_docs = len(self.vectors)


class MockPipeline:
    """Mock Valkey pipeline."""
    
    def __init__(self, client: MockValkeyClient):
        self.client = client
        self.commands: List[Tuple[str, List[Any]]] = []
        
    def __getattr__(self, name: str):
        """Capture command calls."""
        def command(*args):
            self.commands.append((name, list(args)))
            return self
        return command
        
    async def execute(self) -> List[Any]:
        """Execute all commands in pipeline."""
        results = []
        for cmd_name, args in self.commands:
            # Execute each command on the client
            method = getattr(self.client, cmd_name)
            result = await method(*args)
            results.append(result)
        return results
        
    async def __aenter__(self):
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MockConnectionPool:
    """Mock connection pool."""
    
    def __init__(self, **kwargs):
        self.config = kwargs
        self._shared_client = MockValkeyClient()  # Share one client for consistency
        self._clients: List[MockValkeyClient] = []
        
    async def get_client(self) -> MockValkeyClient:
        """Get a mock client."""
        # Return the same shared client for data persistence
        return self._shared_client
        
    async def disconnect(self) -> None:
        """Disconnect all clients."""
        await self._shared_client.close()
        self._clients.clear()
        
    def get_pool_stats(self) -> Dict[str, int]:
        """Get pool statistics."""
        return {
            "created_connections": len(self._clients),
            "available_connections": 0,
            "in_use_connections": len(self._clients),
        }


class MockAsyncValkeyPool:
    """Mock for AsyncValkeyPool."""
    
    def __init__(self, config: Any):
        self.config = config
        self._pool = MockConnectionPool()
        self._initialized = False
        self._shared_client = self._pool._shared_client  # Use the pool's shared client
        
    async def initialize(self) -> None:
        """Initialize the pool."""
        self._initialized = True
        
    async def get_client(self) -> MockValkeyClient:
        """Get a client."""
        return self._shared_client  # Always return the shared client
        
    async def close(self) -> None:
        """Close the pool."""
        await self._pool.disconnect()
        self._initialized = False
        
    async def get_pool_stats(self) -> Dict[str, Any]:
        """Get pool stats."""
        return self._pool.get_pool_stats()


class MockDataset:
    """Mock dataset for testing."""
    
    def __init__(self, n_vectors: int = 1000, dimensions: int = 1536):
        self.n_vectors = n_vectors
        self.dimensions = dimensions
        self._train_vectors = np.random.randn(n_vectors, dimensions).astype(np.float32)
        self._test_vectors = np.random.randn(100, dimensions).astype(np.float32)
        self._ground_truth = np.random.randint(0, n_vectors, size=(100, 10))
        self._max_norm = 100.0
        
    def get_train_vectors(self, indices: Optional[List[int]] = None) -> np.ndarray:
        """Get training vectors."""
        if indices:
            return self._train_vectors[indices]
        return self._train_vectors
        
    def get_test_vectors(self, indices: Optional[List[int]] = None) -> np.ndarray:
        """Get test vectors."""
        if indices:
            return self._test_vectors[indices]
        return self._test_vectors
        
    def get_ground_truth(self, query_indices: Optional[List[int]] = None, k: int = 10) -> np.ndarray:
        """Get ground truth."""
        gt = self._ground_truth
        if query_indices:
            gt = gt[query_indices]
        return gt[:, :k]
        
    def get_max_norm(self) -> float:
        """Get max norm."""
        return self._max_norm
        
    def iterate_batches(self, batch_size: int = 1000, shuffle: bool = False):
        """Iterate over batches."""
        indices = np.arange(self.n_vectors)
        if shuffle:
            np.random.shuffle(indices)
            
        for i in range(0, self.n_vectors, batch_size):
            batch_indices = indices[i:i + batch_size]
            batch_vectors = self._train_vectors[batch_indices]
            batch_keys = [f"train_{idx}" for idx in batch_indices]
            yield batch_vectors, batch_keys


class MockConnectionManager:
    """Mock connection manager."""
    
    def __init__(self, config: Any, n_pools: int = 1):
        self.config = config
        self.n_pools = n_pools
        # Create pools that share the same underlying client
        self._shared_client = MockValkeyClient()
        self.pools = []
        for _ in range(n_pools):
            pool = MockAsyncValkeyPool(config)
            pool._shared_client = self._shared_client  # Share the client
            self.pools.append(pool)
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize all pools."""
        for pool in self.pools:
            await pool.initialize()
        self._initialized = True
        
    def get_pool(self, pool_index: int = 0) -> MockAsyncValkeyPool:
        """Get a pool."""
        return self.pools[pool_index]
        
    async def execute_pipeline(self, commands: List[Tuple[str, List[Any]]], pool_index: int = 0) -> List[Any]:
        """Execute pipeline commands."""
        pool = self.get_pool(pool_index)
        client = await pool.get_client()
        
        results = []
        for cmd_name, args in commands:
            if cmd_name.upper() == "SET":
                result = await client.set(args[0], args[1])
            elif cmd_name.upper() == "GET":
                result = await client.get(args[0])
            else:
                result = None
            results.append(result)
            
        await client.close()
        return results
        
    async def close_all(self) -> None:
        """Close all pools."""
        for pool in self.pools:
            await pool.close()
        self._initialized = False
        
    async def __aenter__(self):
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close_all()