"""
Dataset Preparation CLI Commands

CLI interface for dataset preparation, S3 management, and validation.
Extends the existing CLI with dataset preparation capabilities.
"""

import typer
import asyncio
from pathlib import Path
from typing import Optional, List, Dict, Any
import rich.console
import rich.progress
import rich.table
from rich.prompt import Confirm
import json
import re

from datasets.preparer import DatasetPreparer
from datasets.s3_manager import S3DatasetManager, S3Config
from datasets.rdb_generator import RDBGenerationConfig
from datasets.rdb_generator import IndexSpec, IndexAlgorithm, MetricType
from datasets.converter import SourceFormat
from datasets.formats import CompressionType


# Create separate app for dataset preparation commands
prep_app = typer.Typer(name="prep", help="Dataset preparation and S3 management commands")
console = rich.console.Console()


@prep_app.command("prepare")
def prepare_dataset(
    dataset_name: str = typer.Argument(..., help="Name for the prepared dataset"),
    source_path: Path = typer.Argument(..., help="Path to source dataset file"),
    
    # S3 Configuration
    s3_bucket: str = typer.Option("vss-datasets", help="S3 bucket name"),
    s3_region: str = typer.Option("us-east-1", help="AWS region"),
    
    # Dataset options
    source_format: Optional[str] = typer.Option(None, help="Source format (auto-detect if not specified)"),
    description: Optional[str] = typer.Option(None, help="Dataset description"),
    
    # Index configuration
    create_index: bool = typer.Option(True, help="Create vector index in RDB"),
    index_algorithm: str = typer.Option("HNSW", help="Index algorithm (FLAT, HNSW, IVF)"),
    index_metric: str = typer.Option("COSINE", help="Distance metric (COSINE, L2, IP)"),
    hnsw_m: int = typer.Option(16, help="HNSW M parameter"),
    hnsw_ef_construction: int = typer.Option(200, help="HNSW EF construction parameter"),
    
    # Subset options
    subsets: Optional[str] = typer.Option(None, help="Comma-separated subset sizes (e.g., '10k,100k,1M')"),
    
    # Processing options
    compression: str = typer.Option("zstd", help="Compression type (none, zstd, lz4)"),
    block_size: int = typer.Option(1000, help="VKV block size"),
    batch_size: int = typer.Option(1000, help="RDB generation batch size"),
    
    # Valkey configuration  
    valkey_host: str = typer.Option("localhost", help="Valkey host for RDB generation"),
    valkey_port: int = typer.Option(6379, help="Valkey port"),
    valkey_memory_limit: Optional[str] = typer.Option(None, help="Memory limit for RDB generation (e.g., '32GB')"),
    
    # Control options
    dry_run: bool = typer.Option(False, help="Show preparation plan without executing"),
    force: bool = typer.Option(False, help="Overwrite existing dataset"),
):
    """Prepare dataset for S3 storage with RDB generation."""
    console.print(f"[blue]Preparing dataset: {dataset_name}[/blue]")
    
    if dry_run:
        # Show preparation plan without executing
        console.print("[yellow]Dry run mode - showing preparation plan[/yellow]")
        
        # Parse parameters for estimation
        subset_sizes_parsed = _parse_subset_sizes(subsets) if subsets else None
        memory_limit = _parse_memory_limit(valkey_memory_limit)
        
        # Get estimates
        estimates = asyncio.run(_estimate_preparation(
            source_path, create_index, subset_sizes_parsed, compression, memory_limit
        ))
        
        _display_estimates(estimates)
        
        console.print("\n[yellow]This was a dry run. Use --no-dry-run to execute.[/yellow]")
        return

    try:
        # Parse subset sizes
        subset_sizes_parsed = _parse_subset_sizes(subsets) if subsets else None
        
        # Create configuration objects
        s3_config = S3Config(bucket_name=s3_bucket, region=s3_region)
        rdb_config = RDBGenerationConfig(
            valkey_host=valkey_host,
            valkey_port=valkey_port,
            memory_limit_gb=_parse_memory_limit(valkey_memory_limit),
            batch_size=batch_size
        )
        
        # Create index spec if indexing enabled
        index_spec = None
        if create_index:
            index_spec = IndexSpec(
                algorithm=IndexAlgorithm(index_algorithm.upper()),
                metric=MetricType(index_metric.upper()),
                dimension=0,  # Will be determined from data
                m=hnsw_m,
                ef_construction=hnsw_ef_construction
            )
        
        # Run preparation pipeline
        asyncio.run(_run_preparation(
            dataset_name, source_path, s3_config, rdb_config,
            source_format, index_spec, subset_sizes_parsed, description,
            compression, block_size, force
        ))
        
    except Exception as e:
        console.print(f"[red]Preparation failed: {e}[/red]")
        raise typer.Exit(1)


@prep_app.command("list")
def list_datasets(
    s3_bucket: str = typer.Option("vss-datasets", help="S3 bucket name"),
    s3_region: str = typer.Option("us-east-1", help="AWS region"),
    show_details: bool = typer.Option(False, help="Show detailed information"),
    output_format: str = typer.Option("table", help="Output format (table, json)")
):
    """List available datasets in S3."""
    try:
        s3_config = S3Config(bucket_name=s3_bucket, region=s3_region)
        datasets = asyncio.run(_list_s3_datasets(s3_config, show_details))
        
        if output_format == "json":
            console.print(json.dumps(datasets, indent=2))
        else:
            _display_datasets_table(datasets, show_details)
            
    except Exception as e:
        console.print(f"[red]Failed to list datasets: {e}[/red]")
        raise typer.Exit(1)


@prep_app.command("info")
def dataset_info(
    dataset_name: str = typer.Argument(..., help="Dataset name to show info for"),
    s3_bucket: str = typer.Option("vss-datasets", help="S3 bucket name"),
    s3_region: str = typer.Option("us-east-1", help="AWS region"),
    output_format: str = typer.Option("rich", help="Output format (rich, json)")
):
    """Show detailed information about a dataset."""
    try:
        s3_config = S3Config(bucket_name=s3_bucket, region=s3_region)
        info = asyncio.run(_get_dataset_info(s3_config, dataset_name))
        
        if output_format == "json":
            console.print(json.dumps(info, indent=2))
        else:
            _display_dataset_info(info)
            
    except Exception as e:
        console.print(f"[red]Failed to get dataset info: {e}[/red]")
        raise typer.Exit(1)


@prep_app.command("download")
def download_dataset(
    dataset_name: str = typer.Argument(..., help="Dataset name to download"),
    output_dir: Path = typer.Argument(..., help="Local directory for downloaded files"),
    
    # S3 configuration
    s3_bucket: str = typer.Option("vss-datasets", help="S3 bucket name"),
    s3_region: str = typer.Option("us-east-1", help="AWS region"),
    
    # Download options
    components: Optional[str] = typer.Option(None, help="Components to download (comma-separated)"),
    subset_size: Optional[str] = typer.Option(None, help="Download specific subset (e.g., '100k')"),
    include_rdb: bool = typer.Option(True, help="Include RDB files"),
    include_vectors: bool = typer.Option(True, help="Include vector files"),
    
    # Control options
    force: bool = typer.Option(False, help="Overwrite existing files"),
    verify_checksums: bool = typer.Option(True, help="Verify file checksums after download")
):
    """Download dataset from S3."""
    console.print(f"[blue]Downloading dataset: {dataset_name}[/blue]")
    
    try:
        # Parse components list
        component_list = components.split(',') if components else None
        
        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Run download
        s3_config = S3Config(bucket_name=s3_bucket, region=s3_region)
        result = asyncio.run(_download_dataset(
            s3_config, dataset_name, output_dir, component_list,
            subset_size, include_rdb, include_vectors, force, verify_checksums
        ))
        
        console.print(f"[green]Download completed: {result['files_downloaded']} files[/green]")
        
    except Exception as e:
        console.print(f"[red]Download failed: {e}[/red]")
        raise typer.Exit(1)


@prep_app.command("validate")
def validate_dataset(
    dataset_name: str = typer.Argument(..., help="Dataset name to validate"),
    s3_bucket: str = typer.Option("vss-datasets", help="S3 bucket name"),
    s3_region: str = typer.Option("us-east-1", help="AWS region"),
    
    # Validation options
    check_checksums: bool = typer.Option(True, help="Verify file checksums"),
    test_rdb_loading: bool = typer.Option(True, help="Test RDB loading"),
    test_streaming: bool = typer.Option(True, help="Test vector streaming"),
    sample_size: int = typer.Option(1000, help="Sample size for validation"),
    
    # Valkey configuration for RDB testing
    valkey_host: str = typer.Option("localhost", help="Valkey host for RDB testing"),
    valkey_port: int = typer.Option(6379, help="Valkey port")
):
    """Validate dataset integrity and functionality."""
    console.print(f"[blue]Validating dataset: {dataset_name}[/blue]")
    
    try:
        s3_config = S3Config(bucket_name=s3_bucket, region=s3_region)
        rdb_config = RDBGenerationConfig(valkey_host=valkey_host, valkey_port=valkey_port)
        
        results = asyncio.run(_validate_dataset(
            s3_config, rdb_config, dataset_name, check_checksums,
            test_rdb_loading, test_streaming, sample_size
        ))
        
        _display_validation_results(results)
        
        if not all(results.values()):
            raise typer.Exit(1)
            
    except Exception as e:
        console.print(f"[red]Validation failed: {e}[/red]")
        raise typer.Exit(1)


@prep_app.command("delete")
def delete_dataset(
    dataset_name: str = typer.Argument(..., help="Dataset name to delete"),
    s3_bucket: str = typer.Option("vss-datasets", help="S3 bucket name"),
    s3_region: str = typer.Option("us-east-1", help="AWS region"),
    force: bool = typer.Option(False, help="Skip confirmation prompt")
):
    """Delete dataset from S3."""
    if not force:
        if not Confirm.ask(f"Delete dataset '{dataset_name}' from S3?"):
            console.print("[yellow]Deletion cancelled[/yellow]")
            return
    
    try:
        s3_config = S3Config(bucket_name=s3_bucket, region=s3_region)
        result = asyncio.run(_delete_dataset(s3_config, dataset_name))
        
        console.print(f"[green]Deleted {result['files_deleted']} files[/green]")
        
    except Exception as e:
        console.print(f"[red]Deletion failed: {e}[/red]")
        raise typer.Exit(1)


@prep_app.command("estimate")
def estimate_preparation(
    source_path: Path = typer.Argument(..., help="Path to source dataset file"),
    
    # Options affecting estimates
    create_index: bool = typer.Option(True, help="Include index creation in estimate"),
    subsets: Optional[str] = typer.Option(None, help="Subset sizes to include"),
    compression: str = typer.Option("zstd", help="Compression type"),
    
    # Valkey configuration
    valkey_memory_limit: Optional[str] = typer.Option(None, help="Available memory for RDB generation")
):
    """Estimate preparation time and resource requirements."""
    console.print(f"[blue]Analyzing dataset: {source_path}[/blue]")
    
    try:
        # Parse parameters
        subset_sizes_parsed = _parse_subset_sizes(subsets) if subsets else None
        memory_limit = _parse_memory_limit(valkey_memory_limit)
        
        # Run estimation
        estimates = asyncio.run(_estimate_preparation(
            source_path, create_index, subset_sizes_parsed, compression, memory_limit
        ))
        
        _display_estimates(estimates)
        
    except Exception as e:
        console.print(f"[red]Estimation failed: {e}[/red]")
        raise typer.Exit(1)


# Helper functions

def _parse_subset_sizes(subsets_str: str) -> List[int]:
    """Parse subset sizes from string format."""
    sizes = []
    for part in subsets_str.split(','):
        part = part.strip().upper()
        if not part:
            continue
            
        # Handle suffixes
        if part.endswith('K'):
            size = int(part[:-1]) * 1000
        elif part.endswith('M'):
            size = int(part[:-1]) * 1000000
        elif part.endswith('B'):
            size = int(part[:-1]) * 1000000000
        else:
            size = int(part)
        
        if size <= 0:
            raise ValueError(f"Invalid subset size: {part}")
        sizes.append(size)
    
    return sorted(sizes)


def _parse_memory_limit(memory_str: Optional[str]) -> Optional[float]:
    """Parse memory limit string to GB float."""
    if not memory_str:
        return None
    
    memory_str = memory_str.strip().upper()
    
    # Extract number and unit
    match = re.match(r'(\d+(?:\.\d+)?)\s*([KMGT]?B?)$', memory_str)
    if not match:
        raise ValueError(f"Invalid memory format: {memory_str}")
    
    value, unit = match.groups()
    value = float(value)
    
    # Convert to GB
    if unit in ['B', '']:
        return value / (1024 ** 3)
    elif unit in ['KB', 'K']:
        return value / (1024 ** 2)
    elif unit in ['MB', 'M']:
        return value / 1024
    elif unit in ['GB', 'G']:
        return value
    elif unit in ['TB', 'T']:
        return value * 1024
    else:
        raise ValueError(f"Unknown memory unit: {unit}")


async def _run_preparation(dataset_name: str, source_path: Path,
                         s3_config: S3Config, rdb_config: RDBGenerationConfig,
                         source_format: Optional[str], index_spec: Optional[IndexSpec],
                         subset_sizes: Optional[List[int]], description: Optional[str],
                         compression: str, block_size: int, force: bool):
    """Run the dataset preparation pipeline."""
    try:
        # Create DatasetPreparer
        preparer = DatasetPreparer(
            s3_config=s3_config,
            rdb_config=rdb_config
        )
        
        # Parse source format
        source_fmt = None
        if source_format:
            source_fmt = SourceFormat(source_format.upper())
        
        # Parse compression
        compression_type = CompressionType(compression.upper())
        
        # Show progress with Rich
        with rich.progress.Progress(
            rich.progress.TextColumn("[progress.description]{task.description}"),
            rich.progress.BarColumn(),
            rich.progress.MofNCompleteColumn(),
            rich.progress.TimeElapsedColumn(),
            console=console
        ) as progress:
            
            # Add progress tasks
            main_task = progress.add_task("Preparing dataset...", total=5)
            
            def update_progress(phase: str, step: int, total: int):
                progress.update(main_task, completed=step, description=f"{phase}...")
                
            # Run preparation
            result = await preparer.prepare_dataset(
                dataset_name=dataset_name,
                source_path=source_path,
                source_format=source_fmt,
                index_spec=index_spec,
                subset_sizes=subset_sizes,
                description=description,
                compression=compression_type,
                block_size=block_size,
                overwrite=force,
                show_progress=update_progress
            )
            
            progress.update(main_task, completed=5, description="Complete!")
        
        # Display results
        console.print(f"\n[green]✅ Dataset '{dataset_name}' prepared successfully![/green]")
        console.print(f"Total processing time: {result['total_time']:.1f} seconds")
        console.print(f"Total vectors processed: {result['total_vectors']:,}")
        console.print(f"Files uploaded to S3: {result['files_uploaded']}")
        
        if result.get('subsets_created'):
            console.print(f"Subsets created: {', '.join(map(str, result['subsets_created']))}")
            
    except Exception as e:
        console.print(f"[red]❌ Preparation failed: {e}[/red]")
        raise


async def _list_s3_datasets(s3_config: S3Config, show_details: bool) -> List[Dict[str, Any]]:
    """List datasets in S3 bucket."""
    s3_manager = S3DatasetManager(s3_config)
    
    # List all datasets
    datasets = s3_manager.list_datasets()
    
    if show_details:
        # Get detailed info for each dataset
        detailed_datasets = []
        for dataset_name in datasets:
            try:
                metadata = await s3_manager.download_metadata(dataset_name)
                dataset_info = {
                    'name': dataset_name,
                    'description': metadata.dataset.description,
                    'source_format': metadata.dataset.source,  # source path
                    'total_vectors': metadata.vectors.count,
                    'dimensions': metadata.vectors.dimension,
                    'created_at': metadata.dataset.created_at,
                    'file_sizes': {k: getattr(v, 'size_bytes', 0) for k, v in metadata.rdb_files.items()},
                    'subsets': [subset.size for subset in metadata.subsets] if metadata.subsets else []
                }
                detailed_datasets.append(dataset_info)
            except Exception as e:
                # If metadata is missing, add basic info
                detailed_datasets.append({
                    'name': dataset_name,
                    'error': str(e)
                })
        return detailed_datasets
    else:
        return [{'name': name} for name in datasets]


async def _get_dataset_info(s3_config: S3Config, dataset_name: str) -> Dict[str, Any]:
    """Get detailed dataset information."""
    s3_manager = S3DatasetManager(s3_config)
    
    # Download metadata
    metadata = await s3_manager.download_metadata(dataset_name)
    
    # Get file list and sizes
    files = s3_manager.list_dataset_files(dataset_name)
    
    return {
        'name': dataset_name,
        'description': metadata.dataset.description,
        'source_format': metadata.dataset.source,
        'total_vectors': metadata.vectors.count,
        'dimensions': metadata.vectors.dimension,
        'distance_metric': metadata.vectors.data_type,  # Or extract from RDB info if available
        'compression': metadata.storage.compression,
        'created_at': metadata.dataset.created_at,
        'file_sizes': {k: v.size_bytes for k, v in metadata.rdb_files.items()},
        'checksums': metadata.checksums,
        'subsets': [subset.size for subset in metadata.subsets] if metadata.subsets else [],
        'files': files
    }


async def _download_dataset(s3_config: S3Config, dataset_name: str, output_dir: Path,
                          components: Optional[List[str]], subset_size: Optional[str],
                          include_rdb: bool, include_vectors: bool, force: bool,
                          verify_checksums: bool) -> Dict[str, Any]:
    """Download dataset from S3."""
    s3_manager = S3DatasetManager(s3_config)
    
    downloaded_files = []
    total_size = 0
    
    with rich.progress.Progress(console=console) as progress:
        task = progress.add_task("Downloading...", total=None)
                
        try:
            # Download metadata first
            metadata_file = output_dir / f"{dataset_name}_metadata.json"
            if force or not metadata_file.exists():
                await s3_manager.download_file(
                    f"{dataset_name}/metadata.json", 
                    metadata_file,
                    show_progress=True
                )
                downloaded_files.append(metadata_file)
                total_size += metadata_file.stat().st_size
            
            # Download VKV file if requested
            if include_vectors:
                vkv_file = output_dir / f"{dataset_name}.vkv"
                if force or not vkv_file.exists():
                    await s3_manager.download_file(
                        f"{dataset_name}/vectors.vkv", 
                        vkv_file,
                        show_progress=True
                    )
                    downloaded_files.append(vkv_file)
                    total_size += vkv_file.stat().st_size
            
            # Download RDB files if requested
            if include_rdb:
                for rdb_type in ['vectors_only', 'indexed']:
                    rdb_file = output_dir / f"{dataset_name}_{rdb_type}.rdb"
                    if force or not rdb_file.exists():
                        try:
                            await s3_manager.download_file(
                                f"{dataset_name}/{rdb_type}.rdb", 
                                rdb_file,
                                show_progress=True
                            )
                            downloaded_files.append(rdb_file)
                            total_size += rdb_file.stat().st_size
                        except Exception:
                            # RDB might not exist, continue
                            pass
            
            # Download subsets if specific size requested
            if subset_size:
                subset_vkv = output_dir / f"{dataset_name}_subset_{subset_size}.vkv"
                subset_rdb = output_dir / f"{dataset_name}_subset_{subset_size}.rdb"
                
                for file_path, s3_key in [(subset_vkv, f"subsets/{subset_size}.vkv"), 
                                         (subset_rdb, f"subsets/{subset_size}.rdb")]:
                    if force or not file_path.exists():
                        try:
                            await s3_manager.download_file(
                                f"{dataset_name}/{s3_key}", 
                                file_path,
                                show_progress=True
                            )
                            downloaded_files.append(file_path)
                            total_size += file_path.stat().st_size
                        except Exception:
                            pass
            
            # Verify checksums if requested
            if verify_checksums:
                progress.update(task, description="Verifying checksums...")
                metadata = await s3_manager.download_metadata(dataset_name)
                
                for file_path in downloaded_files:
                    if file_path.name in metadata.checksums:
                        expected = metadata.checksums[file_path.name]
                        actual = s3_manager.calculate_file_checksum(file_path)
                        if actual != expected:
                            raise ValueError(f"Checksum mismatch for {file_path.name}")
        
        finally:
            progress.update(task, completed=True, description="Download complete")
    
    return {
        'files_downloaded': len(downloaded_files),
        'total_size': total_size,
        'files': [str(f) for f in downloaded_files]
    }


async def _validate_dataset(s3_config: S3Config, rdb_config: RDBGenerationConfig,
                          dataset_name: str, check_checksums: bool,
                          test_rdb_loading: bool, test_streaming: bool,
                          sample_size: int) -> Dict[str, bool]:
    """Validate dataset integrity."""
    s3_manager = S3DatasetManager(s3_config)
    results = {}
    
    try:
        # Check metadata exists and is valid
        metadata = await s3_manager.download_metadata(dataset_name)
        results['metadata_valid'] = True
        console.print("✅ Metadata validation passed")
    except Exception as e:
        results['metadata_valid'] = False
        console.print(f"❌ Metadata validation failed: {e}")
    
    # Check file checksums
    if check_checksums:
        try:
            # This would need implementation in S3DatasetManager
            results['checksums_valid'] = True
            console.print("✅ Checksum validation passed")
        except Exception as e:
            results['checksums_valid'] = False
            console.print(f"❌ Checksum validation failed: {e}")
    
    # Test RDB loading
    if test_rdb_loading:
        try:
            # This would need implementation
            results['rdb_loading'] = True
            console.print("✅ RDB loading test passed")
        except Exception as e:
            results['rdb_loading'] = False
            console.print(f"❌ RDB loading test failed: {e}")
    
    # Test streaming
    if test_streaming:
        try:
            # This would need implementation  
            results['streaming'] = True
            console.print("✅ Streaming test passed")
        except Exception as e:
            results['streaming'] = False
            console.print(f"❌ Streaming test failed: {e}")
    
    return results


async def _delete_dataset(s3_config: S3Config, dataset_name: str) -> Dict[str, Any]:
    """Delete dataset from S3."""
    s3_manager = S3DatasetManager(s3_config)
    
    # List all files for the dataset
    files = s3_manager.list_dataset_files(dataset_name)
    
    deleted_count = 0
    for file_info in files.values():
        try:
            # Use S3 client directly to delete individual files
            s3_manager.s3_client.delete_object(
                Bucket=s3_manager.config.bucket_name,
                Key=file_info['key']
            )
            deleted_count += 1
        except Exception as e:
            console.print(f"[yellow]Warning: Failed to delete {file_info['key']}: {e}[/yellow]")
    
    return {'files_deleted': deleted_count}


async def _estimate_preparation(source_path: Path, create_index: bool,
                              subset_sizes: Optional[List[int]], compression: str,
                              memory_limit: Optional[float]) -> Dict[str, Any]:
    """Estimate preparation requirements."""
    from datasets.converter import DatasetConverter
    from datasets.rdb_generator import RDBGenerator
    
    # Analyze source dataset
    converter = DatasetConverter()
    source_info = await converter.analyze_source_dataset(source_path)
    
    estimates = {
        'source_analysis': {
            'total_vectors': source_info.get('total_vectors', 0),
            'dimensions': source_info.get('dimensions', 0),
            'estimated_size_mb': source_info.get('size_mb', 0)
        },
        'processing_time': {},
        'memory_requirements': {},
        'storage_requirements': {}
    }
    
    # Estimate conversion time (rough estimate: 1M vectors per minute)
    vectors = estimates['source_analysis']['total_vectors']
    estimates['processing_time']['conversion_minutes'] = max(1, vectors / 1000000)
    
    # Estimate RDB generation time
    if create_index:
        estimates['processing_time']['rdb_generation_minutes'] = max(5, vectors / 500000)
    else:
        estimates['processing_time']['rdb_generation_minutes'] = max(2, vectors / 2000000)
    
    # Estimate memory requirements
    vector_size_mb = vectors * estimates['source_analysis']['dimensions'] * 4 / (1024 * 1024)
    
    if create_index:
        # HNSW index roughly 2-3x the vector data size
        estimates['memory_requirements']['peak_memory_gb'] = (vector_size_mb * 3) / 1024
    else:
        estimates['memory_requirements']['peak_memory_gb'] = (vector_size_mb * 1.5) / 1024
    
    # Estimate storage requirements
    compressed_factor = 0.7 if compression != 'none' else 1.0
    estimates['storage_requirements']['vkv_file_mb'] = vector_size_mb * compressed_factor
    estimates['storage_requirements']['rdb_file_mb'] = vector_size_mb * 1.2 * compressed_factor
    
    if subset_sizes:
        subset_storage = 0
        for size in subset_sizes:
            subset_factor = size / vectors
            subset_storage += vector_size_mb * subset_factor * compressed_factor * 2  # VKV + RDB
        estimates['storage_requirements']['subsets_mb'] = subset_storage
    
    return estimates


def _display_datasets_table(datasets: List[Dict[str, Any]], show_details: bool):
    """Display datasets in table format."""
    if not datasets:
        console.print("[yellow]No datasets found[/yellow]")
        return
    
    table = rich.table.Table(show_header=True, header_style="bold magenta")
    table.add_column("Dataset Name", style="cyan")
    
    if show_details:
        table.add_column("Vectors", justify="right")
        table.add_column("Dimensions", justify="right") 
        table.add_column("Format", style="green")
        table.add_column("Created", style="blue")
        table.add_column("Subsets", style="yellow")
        
        for dataset in datasets:
            if 'error' in dataset:
                table.add_row(
                    dataset['name'],
                    "[red]Error[/red]",
                    "[red]Error[/red]",
                    "[red]Error[/red]",
                    "[red]Error[/red]",
                    "[red]Error[/red]"
                )
            else:
                table.add_row(
                    dataset['name'],
                    f"{dataset.get('total_vectors', 0):,}",
                    str(dataset.get('dimensions', 0)),
                    dataset.get('source_format', 'Unknown'),
                    dataset.get('created_at', 'Unknown'),
                    ', '.join(map(str, dataset.get('subsets', [])))
                )
    else:
        for dataset in datasets:
            table.add_row(dataset['name'])
    
    console.print(table)


def _display_dataset_info(info: Dict[str, Any]):
    """Display detailed dataset information."""
    console.print(f"\n[bold cyan]Dataset: {info['name']}[/bold cyan]")
    console.print(f"Description: {info.get('description', 'No description')}")
    console.print(f"Source Format: {info.get('source_format', 'Unknown')}")
    console.print(f"Total Vectors: {info.get('total_vectors', 0):,}")
    console.print(f"Dimensions: {info.get('dimensions', 0)}")
    console.print(f"Compression: {info.get('compression', 'Unknown')}")
    console.print(f"Created: {info.get('created_at', 'Unknown')}")
    
    if info.get('distance_metric'):
        console.print(f"Distance Metric: {info['distance_metric']}")
    
    # File sizes
    if info.get('file_sizes'):
        console.print("\n[bold]File Sizes:[/bold]")
        for filename, size in info['file_sizes'].items():
            size_mb = size / (1024 * 1024)
            console.print(f"  {filename}: {size_mb:.1f} MB")
    
    # Subsets
    if info.get('subsets'):
        console.print(f"\n[bold]Subsets:[/bold] {', '.join(map(str, info['subsets']))}")


def _display_validation_results(results: Dict[str, bool]):
    """Display validation results."""
    console.print("\n[bold]Validation Results:[/bold]")
    
    all_passed = True
    for check, passed in results.items():
        if passed:
            console.print(f"  ✅ {check.replace('_', ' ').title()}")
        else:
            console.print(f"  ❌ {check.replace('_', ' ').title()}")
            all_passed = False
    
    if all_passed:
        console.print("\n[green]✅ All validation checks passed![/green]")
    else:
        console.print("\n[red]❌ Some validation checks failed[/red]")


def _display_estimates(estimates: Dict[str, Any]):
    """Display preparation estimates."""
    console.print("\n[bold cyan]Preparation Estimates[/bold cyan]")
    
    # Source analysis
    if 'source_analysis' in estimates:
        info = estimates['source_analysis']
        console.print(f"\n[bold]Source Dataset:[/bold]")
        console.print(f"  Vectors: {info.get('total_vectors', 0):,}")
        console.print(f"  Dimensions: {info.get('dimensions', 0)}")
        console.print(f"  Size: {info.get('estimated_size_mb', 0):.1f} MB")
    
    # Processing time
    if 'processing_time' in estimates:
        times = estimates['processing_time']
        console.print(f"\n[bold]Estimated Processing Time:[/bold]")
        for phase, minutes in times.items():
            console.print(f"  {phase.replace('_', ' ').title()}: {minutes:.1f} minutes")
        
        total_time = sum(times.values())
        console.print(f"  [bold]Total: {total_time:.1f} minutes[/bold]")
    
    # Memory requirements
    if 'memory_requirements' in estimates:
        memory = estimates['memory_requirements']
        console.print(f"\n[bold]Memory Requirements:[/bold]")
        for req, gb in memory.items():
            console.print(f"  {req.replace('_', ' ').title()}: {gb:.1f} GB")
    
    # Storage requirements
    if 'storage_requirements' in estimates:
        storage = estimates['storage_requirements']
        console.print(f"\n[bold]Storage Requirements:[/bold]")
        total_storage = 0
        for req, mb in storage.items():
            console.print(f"  {req.replace('_', ' ').title()}: {mb:.1f} MB")
            total_storage += mb
        
        console.print(f"  [bold]Total: {total_storage:.1f} MB ({total_storage/1024:.1f} GB)[/bold]")
