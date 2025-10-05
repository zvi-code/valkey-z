# src/core/connection.py
"""Valkey connection management with async support."""

from __future__ import annotations

import asyncio
import valkey.asyncio as valkey
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass
import logging
import time

logger = logging.getLogger(__name__)


@dataclass
class ConnectionConfig:
    """Configuration for Valkey connections."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    max_connections: int = 1000
    socket_timeout: float = 30.0
    socket_connect_timeout: float = 10.0
    socket_keepalive: bool = True
    socket_keepalive_options: Optional[Dict[int, int]] = None
    decode_responses: bool = False  # Keep as bytes for vector data
    
    def __post_init__(self):
        """Set default keepalive options if not provided."""
        if self.socket_keepalive_options is None:
            # TCP keepalive settings
            self.socket_keepalive_options = {
                1: 1,  # TCP_KEEPIDLE
                2: 1,  # TCP_KEEPINTVL
                3: 3,  # TCP_KEEPCNT
            }


class AsyncValkeyPool:
    """Manages async Valkey connection pool."""
    
    def __init__(self, config: ConnectionConfig):
        """Initialize connection pool."""
        self.config = config
        self._pool: Optional[valkey.ConnectionPool] = None
        self._initialized = False
        
    async def initialize(self) -> None:
        """Initialize the connection pool."""
        if self._initialized:
            return
            
        try:
            # Create connection pool
            self._pool = valkey.ConnectionPool(
                host=self.config.host,
                port=self.config.port,
                db=self.config.db,
                password=self.config.password,
                max_connections=self.config.max_connections,
                socket_timeout=self.config.socket_timeout,
                socket_connect_timeout=self.config.socket_connect_timeout,
                socket_keepalive=self.config.socket_keepalive,
                socket_keepalive_options=self.config.socket_keepalive_options,
                decode_responses=self.config.decode_responses,
            )
            
            # Test connectivity using direct client creation
            test_client = valkey.Valkey(connection_pool=self._pool)
            await test_client.ping()
            await test_client.close()
            
            self._initialized = True
            logger.info(f"Initialized Valkey pool: {self.config.host}:{self.config.port} "
                       f"(max_connections={self.config.max_connections})")
            
        except Exception as e:
            logger.error(f"Failed to initialize Valkey pool: {e}")
            raise
        
    async def get_client(self) -> valkey.Valkey:
        """Get a Valkey client from the pool."""
        if not self._initialized:
            await self.initialize()
            
        return valkey.Valkey(connection_pool=self._pool)
        
    async def close(self) -> None:
        """Close all connections in the pool."""
        if self._pool:
            await self._pool.disconnect()
            self._initialized = False
            logger.info("Valkey pool closed")
            
    async def get_pool_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        if not self._pool:
            return {}
            
        return {
            "created_connections": len(self._pool._created_connections),
            "available_connections": len(self._pool._available_connections),
            "in_use_connections": len(self._pool._in_use_connections),
            "max_connections": self.config.max_connections,
        }


class ConnectionManager:
    """Manages multiple connection pools for different workloads."""
    
    def __init__(self, config: ConnectionConfig, n_pools: int = 1):
        """Initialize connection manager."""
        self.config = config
        self.n_pools = n_pools
        self.pools: List[AsyncValkeyPool] = []
        self._initialized = False
        self._cluster_mode = False
        
    async def initialize(self) -> None:
        """Initialize all connection pools."""
        if self._initialized:
            return
            
        logger.info(f"Initializing {self.n_pools} connection pools")
        
        # Create pools
        for i in range(self.n_pools):
            # Distribute connections across pools
            pool_config = ConnectionConfig(
                **{k: v for k, v in self.config.__dict__.items() if k != 'max_connections'},
                max_connections=self.config.max_connections // self.n_pools
            )
            pool = AsyncValkeyPool(pool_config)
            await pool.initialize()
            self.pools.append(pool)
        
        # Check for cluster mode
        await self._check_cluster_mode()
        
        self._initialized = True
        logger.info(f"Connection manager initialized with {self.n_pools} pools")
        
    async def _check_cluster_mode(self) -> None:
        """Check if Valkey is running in cluster mode."""
        try:
            client = await self.pools[0].get_client()
            info = await client.info("cluster")
            self._cluster_mode = info.get("cluster_enabled", 0) == 1
            await client.close()
            
            if self._cluster_mode:
                logger.info("Valkey cluster mode detected")
            else:
                logger.info("Valkey standalone mode detected")
        except Exception as e:
            logger.warning(f"Could not determine cluster mode: {e}")
            self._cluster_mode = False
        
    def get_pool(self, pool_index: int = 0) -> AsyncValkeyPool:
        """Get a specific connection pool."""
        if not self._initialized:
            raise RuntimeError("Connection manager not initialized. Call initialize() first.")
            
        if pool_index < 0 or pool_index >= len(self.pools):
            raise IndexError(f"Pool index {pool_index} out of range (0-{len(self.pools)-1})")
            
        return self.pools[pool_index]
    
    def get_pool_round_robin(self) -> AsyncValkeyPool:
        """Get a pool using round-robin selection."""
        if not hasattr(self, '_round_robin_index'):
            self._round_robin_index = 0
            
        pool = self.get_pool(self._round_robin_index)
        self._round_robin_index = (self._round_robin_index + 1) % self.n_pools
        return pool
        
    async def execute_pipeline(self, 
                             commands: List[Tuple[str, List[Any]]],
                             pool_index: int = 0) -> List[Any]:
        """Execute a batch of commands using pipeline."""
        pool = self.get_pool(pool_index)
        client = await pool.get_client()
        
        try:
            async with client.pipeline(transaction=False) as pipe:
                for cmd_name, cmd_args in commands:
                    # Get the command method
                    cmd_method = getattr(pipe, cmd_name.lower())
                    cmd_method(*cmd_args)
                
                results = await pipe.execute()
                return results
        finally:
            await client.close()
    
    async def execute_with_retry(self,
                               command: str,
                               args: List[Any],
                               max_retries: int = 3,
                               pool_index: int = 0) -> Any:
        """Execute a command with retry logic."""
        pool = self.get_pool(pool_index)
        
        for attempt in range(max_retries):
            try:
                client = await pool.get_client()
                try:
                    cmd_method = getattr(client, command.lower())
                    result = await cmd_method(*args)
                    return result
                finally:
                    await client.close()
                    
            except valkey.ConnectionError as e:
                if attempt == max_retries - 1:
                    logger.error(f"Command {command} failed after {max_retries} attempts: {e}")
                    raise
                else:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Retry {attempt + 1}/{max_retries} for {command} after {wait_time}s")
                    await asyncio.sleep(wait_time)
    
    async def health_check(self) -> Dict[str, Any]:
        """Check health of all connection pools."""
        health_status = {
            "healthy": True,
            "pools": [],
            "cluster_mode": self._cluster_mode,
        }
        
        for i, pool in enumerate(self.pools):
            pool_health = {
                "pool_index": i,
                "healthy": True,
                "error": None,
                "stats": {},
            }
            
            try:
                client = await pool.get_client()
                start_time = time.time()
                await client.ping()
                latency_ms = (time.time() - start_time) * 1000
                await client.close()
                
                pool_health["latency_ms"] = latency_ms
                pool_health["stats"] = await pool.get_pool_stats()
                
            except Exception as e:
                pool_health["healthy"] = False
                pool_health["error"] = str(e)
                health_status["healthy"] = False
                
            health_status["pools"].append(pool_health)
        
        return health_status
    
    async def close_all(self) -> None:
        """Close all connection pools."""
        logger.info("Closing all connection pools")
        
        close_tasks = [pool.close() for pool in self.pools]
        await asyncio.gather(*close_tasks, return_exceptions=True)
        
        self.pools.clear()
        self._initialized = False
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close_all()


class ClientPool:
    """Pool of Valkey clients for a single thread/worker."""
    
    def __init__(self, valkey_pool: AsyncValkeyPool, n_clients: int):
        """Initialize client pool."""
        self.valkey_pool = valkey_pool
        self.n_clients = n_clients
        self.clients: List[valkey.Valkey] = []
        self._initialized = False
        
    async def initialize(self) -> None:
        """Create all clients."""
        if self._initialized:
            return
            
        for _ in range(self.n_clients):
            client = await self.valkey_pool.get_client()
            self.clients.append(client)
            
        self._initialized = True
        logger.debug(f"Initialized client pool with {self.n_clients} clients")
        
    async def execute_concurrent(self,
                               command: str,
                               args_list: List[List[Any]]) -> List[Any]:
        """Execute commands concurrently across all clients."""
        if not self._initialized:
            await self.initialize()
            
        tasks = []
        for i, args in enumerate(args_list):
            client = self.clients[i % self.n_clients]
            cmd_method = getattr(client, command.lower())
            task = cmd_method(*args)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check for exceptions
        exceptions = [r for r in results if isinstance(r, Exception)]
        if exceptions:
            logger.error(f"Concurrent execution had {len(exceptions)} errors")
            
        return results
    
    async def close_all(self) -> None:
        """Close all clients."""
        close_tasks = [client.close() for client in self.clients]
        await asyncio.gather(*close_tasks, return_exceptions=True)
        self.clients.clear()
        self._initialized = False