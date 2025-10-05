# src/core/config.py
"""Configuration parsing and validation."""

from __future__ import annotations

import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
import logging
import os

logger = logging.getLogger(__name__)


@dataclass
class IndexConfig:
    """Configuration for vector index."""
    algorithm: str = "HNSW"
    m: int = 16
    ef_construction: int = 356
    ef_runtime: int = 200
    distance_metric: str = "L2"
    dimensions: int = 1536
    initial_cap: int = 10000
    
    def validate(self) -> None:
        """Validate index configuration."""
        if self.algorithm not in ["HNSW", "FLAT"]:
            raise ValueError(f"Invalid algorithm: {self.algorithm}. Must be HNSW or FLAT")
        
        if self.distance_metric not in ["L2", "IP", "COSINE"]:
            raise ValueError(f"Invalid distance metric: {self.distance_metric}")
        
        if self.dimensions <= 0:
            raise ValueError(f"Dimensions must be positive: {self.dimensions}")
        
        if self.algorithm == "HNSW":
            if not 1 <= self.m <= 512:
                raise ValueError(f"M must be between 1 and 512: {self.m}")
            if not 1 <= self.ef_construction <= 4096:
                raise ValueError(f"ef_construction must be between 1 and 4096: {self.ef_construction}")
            if not 1 <= self.ef_runtime <= 4096:
                raise ValueError(f"ef_runtime must be between 1 and 4096: {self.ef_runtime}")


@dataclass
class WorkloadConfig:
    """Configuration for workload execution."""
    n_threads: int = 8
    n_clients: int = 1000
    batch_size: int = 1000
    operation_timeout: float = 30.0
    query_k: int = 10
    
    def validate(self) -> None:
        """Validate workload configuration."""
        if self.n_threads <= 0:
            raise ValueError(f"n_threads must be positive: {self.n_threads}")
        
        if self.n_clients <= 0:
            raise ValueError(f"n_clients must be positive: {self.n_clients}")
        
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive: {self.batch_size}")
        
        if self.operation_timeout <= 0:
            raise ValueError(f"operation_timeout must be positive: {self.operation_timeout}")
        
        if self.query_k <= 0:
            raise ValueError(f"query_k must be positive: {self.query_k}")


@dataclass
class ValkeyConfig:
    """Valkey connection configuration."""
    host: str = "localhost"
    port: int = 6379
    db: int = 0
    password: Optional[str] = None
    max_connections: int = 1000
    socket_timeout: float = 30.0
    socket_connect_timeout: float = 10.0
    
    def validate(self) -> None:
        """Validate Valkey configuration."""
        if not self.host:
            raise ValueError("Valkey host cannot be empty")
        
        if not 1 <= self.port <= 65535:
            raise ValueError(f"Invalid port number: {self.port}")
        
        if self.db < 0:
            raise ValueError(f"Database number must be non-negative: {self.db}")
        
        if self.max_connections <= 0:
            raise ValueError(f"max_connections must be positive: {self.max_connections}")


@dataclass
class MonitoringConfig:
    """Monitoring configuration."""
    sampling_interval: float = 10.0
    memory_metrics: List[str] = field(default_factory=lambda: [
        "rss_mb", "active_mb", "resident_mb", "allocated_mb", "fragmentation_ratio"
    ])
    export_format: str = "csv"
    prometheus_pushgateway: Optional[str] = None
    
    def validate(self) -> None:
        """Validate monitoring configuration."""
        if self.sampling_interval <= 0:
            raise ValueError(f"sampling_interval must be positive: {self.sampling_interval}")
        
        if self.export_format not in ["csv", "prometheus", "both"]:
            raise ValueError(f"Invalid export format: {self.export_format}")


@dataclass
class OutputConfig:
    """Output configuration."""
    csv_path: Path = Path("output/metrics.csv")
    summary_path: Path = Path("output/summary.csv")
    log_level: str = "INFO"
    
    def validate(self) -> None:
        """Validate output configuration."""
        # Create output directories if they don't exist
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.summary_path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            raise ValueError(f"Invalid log level: {self.log_level}")


@dataclass
class S3Config:
    """S3 configuration for dataset storage."""
    bucket_name: str = "vss-datasets"
    region: str = "us-east-1"
    
    # AWS credentials (usually from environment/IAM)
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    
    # Upload settings
    multipart_threshold: int = 64 * 1024 * 1024  # 64MB
    max_concurrency: int = 10
    multipart_chunksize: int = 64 * 1024 * 1024  # 64MB
    
    # Download settings
    download_threads: int = 4
    
    # Retry settings
    max_retries: int = 3
    retry_delay: float = 1.0
    
    def validate(self) -> None:
        """Validate S3 configuration."""
        if not self.bucket_name:
            raise ValueError("S3 bucket name cannot be empty")
        
        if not self.region:
            raise ValueError("S3 region cannot be empty")
        
        if self.multipart_threshold <= 0:
            raise ValueError(f"multipart_threshold must be positive: {self.multipart_threshold}")
        
        if self.max_concurrency <= 0:
            raise ValueError(f"max_concurrency must be positive: {self.max_concurrency}")
        
        if self.download_threads <= 0:
            raise ValueError(f"download_threads must be positive: {self.download_threads}")


@dataclass
class DatasetPrepConfig:
    """Dataset preparation configuration."""
    # Default compression and processing settings
    default_compression: str = "zstd"
    default_block_size: int = 1000
    
    # RDB generation settings
    valkey_host: str = "localhost"
    valkey_port: int = 6379
    valkey_password: Optional[str] = None
    memory_limit_gb: Optional[float] = None
    batch_size: int = 1000
    
    # Index creation defaults
    create_index_by_default: bool = True
    default_index_algorithm: str = "HNSW"
    default_distance_metric: str = "COSINE"
    default_hnsw_m: int = 16
    default_hnsw_ef_construction: int = 200
    
    # Processing limits and timeouts
    max_vector_dimensions: int = 4096
    max_dataset_size_gb: float = 1000.0
    processing_timeout_minutes: int = 480  # 8 hours
    
    def validate(self) -> None:
        """Validate dataset preparation configuration."""
        if self.default_compression not in ["none", "zstd", "lz4"]:
            raise ValueError(f"Invalid compression type: {self.default_compression}")
        
        if self.default_block_size <= 0:
            raise ValueError(f"default_block_size must be positive: {self.default_block_size}")
        
        if not self.valkey_host:
            raise ValueError("Valkey host cannot be empty")
        
        if not 1 <= self.valkey_port <= 65535:
            raise ValueError(f"Invalid Valkey port: {self.valkey_port}")
        
        if self.memory_limit_gb is not None and self.memory_limit_gb <= 0:
            raise ValueError(f"memory_limit_gb must be positive: {self.memory_limit_gb}")
        
        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be positive: {self.batch_size}")
        
        if self.default_index_algorithm not in ["HNSW", "FLAT"]:
            raise ValueError(f"Invalid index algorithm: {self.default_index_algorithm}")
        
        if self.default_distance_metric not in ["L2", "IP", "COSINE"]:
            raise ValueError(f"Invalid distance metric: {self.default_distance_metric}")
        
        if not 1 <= self.default_hnsw_m <= 512:
            raise ValueError(f"default_hnsw_m must be between 1 and 512: {self.default_hnsw_m}")
        
        if not 1 <= self.default_hnsw_ef_construction <= 4096:
            raise ValueError(f"default_hnsw_ef_construction must be between 1 and 4096: {self.default_hnsw_ef_construction}")
        
        if self.max_vector_dimensions <= 0:
            raise ValueError(f"max_vector_dimensions must be positive: {self.max_vector_dimensions}")
        
        if self.max_dataset_size_gb <= 0:
            raise ValueError(f"max_dataset_size_gb must be positive: {self.max_dataset_size_gb}")
        
        if self.processing_timeout_minutes <= 0:
            raise ValueError(f"processing_timeout_minutes must be positive: {self.processing_timeout_minutes}")


class Config:
    """Main configuration container."""
    
    def __init__(self, config_path: Optional[Path] = None):
        """Initialize configuration."""
        self.config_path = config_path
        self.data: Dict[str, Any] = {}
        
        # Initialize with defaults
        self.valkey = ValkeyConfig()
        self.index = IndexConfig()
        self.workload = WorkloadConfig()
        self.monitoring = MonitoringConfig()
        self.output = OutputConfig()
        self.s3 = S3Config()
        self.datasets = DatasetPrepConfig()
        
        # Always merge environment variables
        self._merge_env_vars()
        self._update_from_dict()
        
        # Load from file if provided
        if config_path:
            self.load()
        else:
            # Validate even when no config file is provided
            self.validate()
    
    def load(self) -> None:
        """Load configuration from file."""
        if not self.config_path:
            raise ValueError("No configuration path specified")
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        logger.info(f"Loading configuration from {self.config_path}")
        
        try:
            with open(self.config_path, 'r') as f:
                self.data = yaml.safe_load(f) or {}
            
            # Merge with environment variables
            self._merge_env_vars()
            
            # Update configuration objects
            self._update_from_dict()
            
            # Validate all configurations
            self.validate()
            
            logger.info("Configuration loaded successfully")
            
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML configuration: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            raise
    
    def _merge_env_vars(self) -> None:
        """Merge environment variables into configuration."""
        # Valkey configuration from environment
        if "VALKEY_HOST" in os.environ:
            self.data.setdefault("valkey", {})["host"] = os.environ["VALKEY_HOST"]
        if "VALKEY_PORT" in os.environ:
            self.data.setdefault("valkey", {})["port"] = int(os.environ["VALKEY_PORT"])
        if "VALKEY_PASSWORD" in os.environ:
            self.data.setdefault("valkey", {})["password"] = os.environ["VALKEY_PASSWORD"]
        
        # Output configuration from environment
        if "VST_OUTPUT_DIR" in os.environ:
            output_dir = Path(os.environ["VST_OUTPUT_DIR"])
            self.data.setdefault("output", {})["csv_path"] = str(output_dir / "metrics.csv")
            self.data.setdefault("output", {})["summary_path"] = str(output_dir / "summary.csv")
        
        if "VST_LOG_LEVEL" in os.environ:
            self.data.setdefault("output", {})["log_level"] = os.environ["VST_LOG_LEVEL"]
        
        # S3 configuration from environment
        if "AWS_S3_BUCKET" in os.environ:
            self.data.setdefault("s3", {})["bucket_name"] = os.environ["AWS_S3_BUCKET"]
        if "AWS_DEFAULT_REGION" in os.environ:
            self.data.setdefault("s3", {})["region"] = os.environ["AWS_DEFAULT_REGION"]
        if "AWS_ACCESS_KEY_ID" in os.environ:
            self.data.setdefault("s3", {})["access_key_id"] = os.environ["AWS_ACCESS_KEY_ID"]
        if "AWS_SECRET_ACCESS_KEY" in os.environ:
            self.data.setdefault("s3", {})["secret_access_key"] = os.environ["AWS_SECRET_ACCESS_KEY"]
        if "AWS_SESSION_TOKEN" in os.environ:
            self.data.setdefault("s3", {})["session_token"] = os.environ["AWS_SESSION_TOKEN"]
        
        # S3 performance settings from environment
        if "VST_S3_MULTIPART_THRESHOLD" in os.environ:
            self.data.setdefault("s3", {})["multipart_threshold"] = int(os.environ["VST_S3_MULTIPART_THRESHOLD"])
        if "VST_S3_MAX_CONCURRENCY" in os.environ:
            self.data.setdefault("s3", {})["max_concurrency"] = int(os.environ["VST_S3_MAX_CONCURRENCY"])
        if "VST_S3_DOWNLOAD_THREADS" in os.environ:
            self.data.setdefault("s3", {})["download_threads"] = int(os.environ["VST_S3_DOWNLOAD_THREADS"])
        
        # Dataset preparation configuration from environment
        if "VST_VALKEY_HOST" in os.environ:
            self.data.setdefault("datasets", {})["valkey_host"] = os.environ["VST_VALKEY_HOST"]
        if "VST_VALKEY_PORT" in os.environ:
            self.data.setdefault("datasets", {})["valkey_port"] = int(os.environ["VST_VALKEY_PORT"])
        if "VST_VALKEY_PASSWORD" in os.environ:
            self.data.setdefault("datasets", {})["valkey_password"] = os.environ["VST_VALKEY_PASSWORD"]
        if "VST_MEMORY_LIMIT_GB" in os.environ:
            self.data.setdefault("datasets", {})["memory_limit_gb"] = float(os.environ["VST_MEMORY_LIMIT_GB"])
        if "VST_DEFAULT_COMPRESSION" in os.environ:
            self.data.setdefault("datasets", {})["default_compression"] = os.environ["VST_DEFAULT_COMPRESSION"]
        if "VST_DATASET_BATCH_SIZE" in os.environ:
            self.data.setdefault("datasets", {})["batch_size"] = int(os.environ["VST_DATASET_BATCH_SIZE"])
        if "VST_PROCESSING_TIMEOUT_MINUTES" in os.environ:
            self.data.setdefault("datasets", {})["processing_timeout_minutes"] = int(os.environ["VST_PROCESSING_TIMEOUT_MINUTES"])
    
    def _update_from_dict(self) -> None:
        """Update configuration objects from loaded data."""
        # Update Valkey config
        if "valkey" in self.data:
            valkey_data = self.data["valkey"]
            self.valkey = ValkeyConfig(
                host=valkey_data.get("host", self.valkey.host),
                port=valkey_data.get("port", self.valkey.port),
                db=valkey_data.get("db", self.valkey.db),
                password=valkey_data.get("password", self.valkey.password),
                max_connections=valkey_data.get("max_connections", self.valkey.max_connections),
                socket_timeout=valkey_data.get("socket_timeout", self.valkey.socket_timeout),
                socket_connect_timeout=valkey_data.get("socket_connect_timeout", self.valkey.socket_connect_timeout),
            )
        
        # Update Index config
        if "index" in self.data:
            index_data = self.data["index"]
            self.index = IndexConfig(
                algorithm=index_data.get("algorithm", self.index.algorithm),
                m=index_data.get("m", self.index.m),
                ef_construction=index_data.get("ef_construction", self.index.ef_construction),
                ef_runtime=index_data.get("ef_runtime", self.index.ef_runtime),
                distance_metric=index_data.get("distance_metric", self.index.distance_metric),
                dimensions=index_data.get("dimensions", self.index.dimensions),
                initial_cap=index_data.get("initial_cap", self.index.initial_cap),
            )
        
        # Update Workload config
        if "workload" in self.data:
            workload_data = self.data["workload"]
            self.workload = WorkloadConfig(
                n_threads=workload_data.get("n_threads", self.workload.n_threads),
                n_clients=workload_data.get("n_clients", self.workload.n_clients),
                batch_size=workload_data.get("batch_size", self.workload.batch_size),
                operation_timeout=workload_data.get("operation_timeout", self.workload.operation_timeout),
                query_k=workload_data.get("query_k", self.workload.query_k),
            )
        
        # Update Monitoring config
        if "monitoring" in self.data:
            monitoring_data = self.data["monitoring"]
            self.monitoring = MonitoringConfig(
                sampling_interval=monitoring_data.get("sampling_interval", self.monitoring.sampling_interval),
                memory_metrics=monitoring_data.get("memory_metrics", self.monitoring.memory_metrics),
                export_format=monitoring_data.get("export_format", self.monitoring.export_format),
                prometheus_pushgateway=monitoring_data.get("prometheus_pushgateway", self.monitoring.prometheus_pushgateway),
            )
        
        # Update Output config
        if "output" in self.data:
            output_data = self.data["output"]
            self.output = OutputConfig(
                csv_path=Path(output_data.get("csv_path", str(self.output.csv_path))),
                summary_path=Path(output_data.get("summary_path", str(self.output.summary_path))),
                log_level=output_data.get("log_level", self.output.log_level),
            )
        
        # Update S3 config
        if "s3" in self.data:
            s3_data = self.data["s3"]
            self.s3 = S3Config(
                bucket_name=s3_data.get("bucket_name", self.s3.bucket_name),
                region=s3_data.get("region", self.s3.region),
                access_key_id=s3_data.get("access_key_id", self.s3.access_key_id),
                secret_access_key=s3_data.get("secret_access_key", self.s3.secret_access_key),
                session_token=s3_data.get("session_token", self.s3.session_token),
                multipart_threshold=s3_data.get("multipart_threshold", self.s3.multipart_threshold),
                max_concurrency=s3_data.get("max_concurrency", self.s3.max_concurrency),
                multipart_chunksize=s3_data.get("multipart_chunksize", self.s3.multipart_chunksize),
                download_threads=s3_data.get("download_threads", self.s3.download_threads),
                max_retries=s3_data.get("max_retries", self.s3.max_retries),
                retry_delay=s3_data.get("retry_delay", self.s3.retry_delay),
            )
        
        # Update DatasetPrep config
        if "datasets" in self.data:
            datasets_data = self.data["datasets"]
            self.datasets = DatasetPrepConfig(
                default_compression=datasets_data.get("default_compression", self.datasets.default_compression),
                default_block_size=datasets_data.get("default_block_size", self.datasets.default_block_size),
                valkey_host=datasets_data.get("valkey_host", self.datasets.valkey_host),
                valkey_port=datasets_data.get("valkey_port", self.datasets.valkey_port),
                valkey_password=datasets_data.get("valkey_password", self.datasets.valkey_password),
                memory_limit_gb=datasets_data.get("memory_limit_gb", self.datasets.memory_limit_gb),
                batch_size=datasets_data.get("batch_size", self.datasets.batch_size),
                create_index_by_default=datasets_data.get("create_index_by_default", self.datasets.create_index_by_default),
                default_index_algorithm=datasets_data.get("default_index_algorithm", self.datasets.default_index_algorithm),
                default_distance_metric=datasets_data.get("default_distance_metric", self.datasets.default_distance_metric),
                default_hnsw_m=datasets_data.get("default_hnsw_m", self.datasets.default_hnsw_m),
                default_hnsw_ef_construction=datasets_data.get("default_hnsw_ef_construction", self.datasets.default_hnsw_ef_construction),
                max_vector_dimensions=datasets_data.get("max_vector_dimensions", self.datasets.max_vector_dimensions),
                max_dataset_size_gb=datasets_data.get("max_dataset_size_gb", self.datasets.max_dataset_size_gb),
                processing_timeout_minutes=datasets_data.get("processing_timeout_minutes", self.datasets.processing_timeout_minutes),
            )
    
    def validate(self) -> None:
        """Validate all configuration sections."""
        try:
            self.valkey.validate()
            self.index.validate()
            self.workload.validate()
            self.monitoring.validate()
            self.output.validate()
            self.s3.validate()
            self.datasets.validate()
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            raise
    
    def get_index_config(self) -> IndexConfig:
        """Get index configuration."""
        return self.index
        
    def get_workload_config(self) -> WorkloadConfig:
        """Get workload configuration."""
        return self.workload
    
    def get_valkey_config(self) -> ValkeyConfig:
        """Get Valkey configuration."""
        return self.valkey
    
    def get_monitoring_config(self) -> MonitoringConfig:
        """Get monitoring configuration."""
        return self.monitoring
    
    def get_output_config(self) -> OutputConfig:
        """Get output configuration."""
        return self.output
    
    def get_s3_config(self) -> S3Config:
        """Get S3 configuration."""
        return self.s3
    
    def get_datasets_config(self) -> DatasetPrepConfig:
        """Get dataset preparation configuration."""
        return self.datasets
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            "valkey": {
                "host": self.valkey.host,
                "port": self.valkey.port,
                "db": self.valkey.db,
                "max_connections": self.valkey.max_connections,
            },
            "index": {
                "algorithm": self.index.algorithm,
                "dimensions": self.index.dimensions,
                "distance_metric": self.index.distance_metric,
                "m": self.index.m,
                "ef_construction": self.index.ef_construction,
                "ef_runtime": self.index.ef_runtime,
            },
            "workload": {
                "n_threads": self.workload.n_threads,
                "n_clients": self.workload.n_clients,
                "batch_size": self.workload.batch_size,
                "operation_timeout": self.workload.operation_timeout,
                "query_k": self.workload.query_k,
            },
            "monitoring": {
                "sampling_interval": self.monitoring.sampling_interval,
                "memory_metrics": self.monitoring.memory_metrics,
                "export_format": self.monitoring.export_format,
            },
            "output": {
                "csv_path": str(self.output.csv_path),
                "summary_path": str(self.output.summary_path),
                "log_level": self.output.log_level,
            },
            "s3": {
                "bucket_name": self.s3.bucket_name,
                "region": self.s3.region,
                "access_key_id": self.s3.access_key_id,
                "secret_access_key": self.s3.secret_access_key,
                "session_token": self.s3.session_token,
                "multipart_threshold": self.s3.multipart_threshold,
                "max_concurrency": self.s3.max_concurrency,
                "multipart_chunksize": self.s3.multipart_chunksize,
                "download_threads": self.s3.download_threads,
                "max_retries": self.s3.max_retries,
                "retry_delay": self.s3.retry_delay,
            },
            "datasets": {
                "default_compression": self.datasets.default_compression,
                "default_block_size": self.datasets.default_block_size,
                "valkey_host": self.datasets.valkey_host,
                "valkey_port": self.datasets.valkey_port,
                "valkey_password": self.datasets.valkey_password,
                "memory_limit_gb": self.datasets.memory_limit_gb,
                "batch_size": self.datasets.batch_size,
                "create_index_by_default": self.datasets.create_index_by_default,
                "default_index_algorithm": self.datasets.default_index_algorithm,
                "default_distance_metric": self.datasets.default_distance_metric,
                "default_hnsw_m": self.datasets.default_hnsw_m,
                "default_hnsw_ef_construction": self.datasets.default_hnsw_ef_construction,
                "max_vector_dimensions": self.datasets.max_vector_dimensions,
                "max_dataset_size_gb": self.datasets.max_dataset_size_gb,
                "processing_timeout_minutes": self.datasets.processing_timeout_minutes,
            },
        }


class ConfigValidator:
    """Validates configuration values."""
    
    @staticmethod
    def validate(config: Dict[str, Any]) -> bool:
        """Validate configuration dictionary."""
        try:
            # Create a temporary Config object to validate
            temp_config = Config()
            temp_config.data = config
            temp_config._update_from_dict()
            temp_config.validate()
            return True
        except Exception as e:
            logger.error(f"Configuration validation failed: {e}")
            return False
    
    @staticmethod
    def validate_scenario_config(scenario_config: Dict[str, Any]) -> bool:
        """Validate scenario-specific configuration."""
        required_fields = ["name", "dataset", "steps"]
        
        # Check required fields
        for field in required_fields:
            if field not in scenario_config:
                logger.error(f"Missing required field in scenario: {field}")
                return False
        
        # Validate steps
        steps = scenario_config.get("steps", [])
        if not steps:
            logger.error("Scenario must have at least one step")
            return False
        
        for i, step in enumerate(steps):
            if "name" not in step:
                logger.error(f"Step {i} missing required field: name")
                return False
            if "type" not in step:
                logger.error(f"Step {i} missing required field: type")
                return False
            
            step_type = step["type"]
            if step_type not in ["workload", "wait", "checkpoint"]:
                logger.error(f"Step {i} has invalid type: {step_type}")
                return False
            
            if step_type == "workload" and "workload" not in step:
                logger.error(f"Step {i} of type 'workload' missing workload field")
                return False
        
        return True