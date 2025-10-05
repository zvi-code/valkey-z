"""
Dataset Lifecycle Management CLI Commands

Provides comprehensive dataset management using the Dataset class architecture
for full lifecycle tracking from preparation to deployment.
"""

import typer
import asyncio
from pathlib import Path
from typing import Optional, List
import rich.console
import rich.table
from rich.prompt import Confirm
import json

from datasets import (
    Dataset, DatasetState, DatasetSource, DatasetRegistry,
    DatasetPreparer, S3Config, S3DatasetManager
)
from datasets.rdb_generator import RDBGenerationConfig, IndexSpec, IndexAlgorithm, MetricType
from datasets.converter import SourceFormat
from datasets.metadata import DatasetMetadata
from .external_datasets import DATASETS as EXTERNAL_DATASETS

app = typer.Typer(name="dataset", help="Dataset management with new Dataset class")
console = rich.console.Console()

# Global registry path
REGISTRY_PATH = Path("./dataset_registry.json")

def get_registry() -> DatasetRegistry:
    """Get or create dataset registry."""
    if REGISTRY_PATH.exists():
        return DatasetRegistry.load(REGISTRY_PATH)
    else:
        return DatasetRegistry(registry_path=REGISTRY_PATH)


@app.command("list")
def list_datasets(
    state: Optional[str] = typer.Option(None, help="Filter by state"),
    ready_only: bool = typer.Option(False, help="Show only ready datasets"),
    tag: Optional[str] = typer.Option(None, help="Filter by tag"),
    show_details: bool = typer.Option(False, "--details", help="Show detailed information"),
    registry_only: bool = typer.Option(False, help="Show only registry datasets (exclude external)")
):
    """List all datasets (both registered and external)."""
    registry = get_registry()
    
    # Apply filters for registry datasets
    if state:
        try:
            state_enum = DatasetState(state.lower())
            registry_datasets = registry.list(state=state_enum)
        except ValueError:
            console.print(f"❌ Invalid state: {state}", style="red")
            return
    elif ready_only:
        registry_datasets = registry.list(ready_only=True)
    elif tag:
        registry_datasets = registry.find_by_tag(tag)
    else:
        registry_datasets = registry.list()
    
    # Get external datasets (unless filtered by registry-specific criteria)
    external_datasets = [] if (state or ready_only or tag or registry_only) else list(EXTERNAL_DATASETS.keys())
    
    if not registry_datasets and not external_datasets:
        console.print("No datasets found matching criteria.", style="yellow")
        return
    
    # Create table with appropriate title
    if registry_datasets and external_datasets:
        title = "All Available Datasets"
    elif registry_datasets:
        title = "Registered Datasets"
    else:
        title = "External Datasets"
    
    table = rich.table.Table(title=title)
    table.add_column("Name", style="cyan", no_wrap=True)
    table.add_column("Type", style="magenta", no_wrap=True)
    table.add_column("State/Format", style="green")
    table.add_column("Info", style="blue")
    
    if show_details:
        table.add_column("Dimensions", justify="right")
        table.add_column("Size", style="dim")
    
    # Add registry datasets
    for dataset in registry_datasets:
        s3_location = f"s3://{dataset.s3_bucket}/{dataset.s3_prefix}" if dataset.s3_bucket else "Not uploaded"
        
        row = [
            dataset.name,
            "Registry",
            dataset.state.value,
            s3_location
        ]
        
        if show_details:
            row.extend([
                f"{len(dataset.subsets)} subsets",
                dataset.created_at[:10] if dataset.created_at else "Unknown"
            ])
        
        table.add_row(*row)
    
    # Add external datasets
    for dataset_name in external_datasets:
        dataset_info = EXTERNAL_DATASETS[dataset_name]
        
        row = [
            dataset_name,
            "External",
            dataset_info.get("format", "unknown"),
            dataset_info.get("description", "No description")[:50] + ("..." if len(dataset_info.get("description", "")) > 50 else "")
        ]
        
        if show_details:
            row.extend([
                str(dataset_info.get("dimensions", "unknown")),
                f"{dataset_info.get('size_mb', 0)} MB"
            ])
        
        table.add_row(*row)
    
    console.print(table)
    
    # Show statistics
    stats = registry.get_statistics()
    total_count = stats['total_datasets'] + len(external_datasets)
    console.print(f"\n📊 Total: {total_count} datasets ({stats['total_datasets']} registered, {len(external_datasets)} external), {stats['ready_datasets']} ready")


@app.command("show")
def show_dataset(
    name: str = typer.Argument(..., help="Dataset name"),
    show_manifest: bool = typer.Option(False, "--manifest", help="Show S3 manifest")
):
    """Show detailed information about a dataset."""
    registry = get_registry()
    dataset = registry.get(name)
    
    if not dataset:
        console.print(f"❌ Dataset '{name}' not found", style="red")
        return
    
    # Basic information
    console.print(f"\n📊 Dataset: [cyan]{dataset.name}[/cyan]")
    console.print(f"State: [green]{dataset.state.value}[/green]")
    console.print(f"Created: {dataset.created_at}")
    console.print(f"Updated: {dataset.updated_at}")
    
    if dataset.tags:
        console.print(f"Tags: {', '.join(dataset.tags)}")
    
    # Source information
    if dataset.source:
        console.print(f"\n📁 Source:")
        console.print(f"  Type: {dataset.source.type}")
        console.print(f"  Location: {dataset.source.location}")
        console.print(f"  Format: {dataset.source.format}")
    
    # S3 information
    if dataset.s3_bucket:
        console.print(f"\n☁️  S3 Location:")
        console.print(f"  Bucket: {dataset.s3_bucket}")
        console.print(f"  Prefix: {dataset.s3_prefix}")
    
    # Components
    if dataset.components:
        console.print(f"\n📦 Components:")
        for comp_type, path in dataset.components.items():
            console.print(f"  {comp_type}: {path}")
    
    # RDB files
    if dataset.rdb_files:
        console.print(f"\n🗄️  RDB Files:")
        for rdb_type, rdb_info in dataset.rdb_files.items():
            console.print(f"  {rdb_type}: {rdb_info.filename} ({rdb_info.size_bytes:,} bytes)")
    
    # Subsets
    if dataset.subsets:
        console.print(f"\n📊 Subsets:")
        for size, subset in dataset.subsets.items():
            console.print(f"  {size:,} vectors: {subset.s3_prefix}")
    
    # State history
    if len(dataset.state_history) > 1:
        console.print(f"\n📈 State History:")
        for timestamp, state in dataset.state_history[-3:]:  # Show last 3
            console.print(f"  {timestamp[:19]}: {state}")
    
    # Error message if failed
    if dataset.error_message:
        console.print(f"\n❌ Error: [red]{dataset.error_message}[/red]")
    
    # Validation
    errors = dataset.validate()
    if errors:
        console.print(f"\n⚠️  Validation Issues:")
        for error in errors:
            console.print(f"  • {error}", style="yellow")
    else:
        console.print(f"\n✅ Dataset is valid")
    
    # S3 manifest
    if show_manifest and dataset.s3_bucket:
        console.print(f"\n🗂️  S3 Manifest:")
        manifest = dataset.get_s3_manifest()
        for category, files in manifest.items():
            if files:
                console.print(f"  {category}: {len(files) if isinstance(files, list) else len(files)} files")


@app.command("prepare")
def prepare_dataset(
    name: str = typer.Argument(..., help="Dataset name"),
    source_path: Path = typer.Argument(..., help="Path to source data"),
    
    # Source options
    source_format: Optional[str] = typer.Option(None, help="Source format (hdf5, parquet, etc.)"),
    description: Optional[str] = typer.Option(None, help="Dataset description"),
    
    # S3 options
    s3_bucket: str = typer.Option("vss-datasets", help="S3 bucket name"),
    s3_region: str = typer.Option("us-east-1", help="AWS region"),
    
    # Index options
    create_index: bool = typer.Option(True, help="Create vector index"),
    index_algorithm: str = typer.Option("HNSW", help="Index algorithm"),
    index_metric: str = typer.Option("L2", help="Distance metric"),
    dimension: int = typer.Option(1536, help="Vector dimension"),
    
    # Subset options
    subsets: Optional[str] = typer.Option(None, help="Subset sizes (comma-separated)"),
    
    # Tags
    tags: Optional[str] = typer.Option(None, help="Tags (comma-separated)")
):
    """Prepare a new dataset with full tracking."""
    
    if not source_path.exists():
        console.print(f"❌ Source path does not exist: {source_path}", style="red")
        return
    
    # Parse parameters
    source_format_enum = None
    if source_format:
        try:
            source_format_enum = SourceFormat(source_format.lower())
        except ValueError:
            console.print(f"❌ Invalid source format: {source_format}", style="red")
            return
    
    # Parse subsets
    subset_sizes = None
    if subsets:
        try:
            subset_sizes = [int(s.strip()) for s in subsets.split(",")]
        except ValueError:
            console.print(f"❌ Invalid subset sizes: {subsets}", style="red")
            return
    
    # Parse tags
    tag_list = []
    if tags:
        tag_list = [t.strip() for t in tags.split(",")]
    
    # Create index spec
    index_spec = None
    if create_index:
        try:
            index_spec = IndexSpec(
                algorithm=IndexAlgorithm(index_algorithm),
                metric=MetricType(index_metric),
                dimension=dimension
            )
        except ValueError as e:
            console.print(f"❌ Invalid index specification: {e}", style="red")
            return
    
    # Create dataset
    dataset = Dataset(name=name, tags=tag_list)
    
    console.print(f"🚀 Starting preparation of dataset: [cyan]{name}[/cyan]")
    
    async def prepare():
        # Create configurations
        s3_config = S3Config(bucket_name=s3_bucket, region=s3_region)
        rdb_config = RDBGenerationConfig()
        
        # Create registry for tracking
        registry = get_registry()
        
        try:
            async with DatasetPreparer(
                s3_config=s3_config, 
                rdb_config=rdb_config,
                dataset_registry=registry,
                use_dataset_tracking=True
            ) as preparer:
                result_dataset = await preparer.prepare_dataset_with_tracking(
                    dataset=dataset,
                    source_path=source_path,
                    source_format=source_format_enum,
                    index_spec=index_spec,
                    subset_sizes=subset_sizes,
                    description=description
                )
                
                # Registry updates are handled automatically by preparer
                registry.save()
                console.print(f"✅ Dataset preparation completed successfully!")
                console.print(f"   State: {result_dataset.state}")
                console.print(f"   Subsets: {result_dataset.list_subset_sizes()}")
                console.print(f"   RDB files: {list(result_dataset.rdb_files.keys())}")
                
                return result_dataset
                
        except Exception as e:
            console.print(f"❌ Preparation failed: {e}", style="red")
            # Error handling is done automatically by preparer
            registry.save()
            raise typer.Exit(1)
    
    # Run preparation
    try:
        asyncio.run(prepare())
    except KeyboardInterrupt:
        console.print("\n🛑 Preparation cancelled by user", style="yellow")
        dataset.mark_failed("Cancelled by user")
        registry = get_registry()
        registry.register(dataset)
        registry.save()


@app.command("register")
def register_existing_dataset(
    name: str = typer.Argument(..., help="Dataset name"),
    s3_bucket: str = typer.Argument(..., help="S3 bucket"),
    s3_prefix: str = typer.Argument(..., help="S3 prefix"),
    metadata_path: Optional[Path] = typer.Option(None, help="Path to metadata.json file"),
    tags: Optional[str] = typer.Option(None, help="Tags (comma-separated)")
):
    """Register an existing dataset in S3."""
    
    # Parse tags
    tag_list = []
    if tags:
        tag_list = [t.strip() for t in tags.split(",")]
    
    console.print(f"📝 Registering existing dataset: [cyan]{name}[/cyan]")
    
    try:
        if metadata_path and metadata_path.exists():
            # Load from local metadata file
            metadata = DatasetMetadata.from_file(metadata_path)
            
            dataset = Dataset(
                name=name,
                state=DatasetState.READY,
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                tags=tag_list
            )
            
            # Update from metadata
            dataset.update_from_metadata(metadata)
        else:
            # Create basic dataset object
            dataset = Dataset(
                name=name,
                state=DatasetState.READY,  # Assume existing dataset is ready
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                tags=tag_list
            )
            
            # Try to download metadata from S3
            async def download_metadata():
                s3_config = S3Config(bucket_name=s3_bucket)
                async with S3DatasetManager(s3_config) as s3_manager:
                    metadata = await s3_manager.download_metadata(name)
                    if metadata:
                        dataset.update_from_metadata(metadata)
                        console.print("✅ Downloaded metadata from S3")
                    else:
                        console.print("⚠️  No metadata found in S3", style="yellow")
            
            asyncio.run(download_metadata())
        
        dataset.tags.extend(tag_list)
        
        # Register dataset
        registry = get_registry()
        success = registry.register(dataset)
        
        if success:
            registry.save()
            console.print(f"✅ Dataset registered successfully!")
            console.print(f"   State: {dataset.state}")
            console.print(f"   S3: s3://{dataset.s3_bucket}/{dataset.s3_prefix}")
        else:
            console.print(f"❌ Registration failed", style="red")
            
    except Exception as e:
        console.print(f"❌ Registration failed: {e}", style="red")
        raise typer.Exit(1)


@app.command("remove")
def remove_dataset(
    name: str = typer.Argument(..., help="Dataset name"),
    force: bool = typer.Option(False, "--force", help="Force removal without confirmation")
):
    """Remove a dataset from the registry."""
    
    registry = get_registry()
    dataset = registry.get(name)
    
    if not dataset:
        console.print(f"❌ Dataset '{name}' not found", style="red")
        return
    
    # Show dataset info
    console.print(f"Dataset to remove: [cyan]{name}[/cyan]")
    console.print(f"State: {dataset.state.value}")
    console.print(f"S3: s3://{dataset.s3_bucket}/{dataset.s3_prefix}" if dataset.s3_bucket else "No S3 location")
    
    # Confirm removal
    if not force:
        if not Confirm.ask("Are you sure you want to remove this dataset from the registry?"):
            console.print("❌ Removal cancelled", style="yellow")
            return
    
    # Remove from registry
    success = registry.remove(name)
    
    if success:
        registry.save()
        console.print(f"✅ Dataset removed from registry")
        console.print("Note: S3 files were not deleted. Use 'dataset delete-s3' to remove S3 data.")
    else:
        console.print(f"❌ Failed to remove dataset", style="red")


@app.command("stats")
def show_statistics():
    """Show dataset registry statistics."""
    
    registry = get_registry()
    stats = registry.get_statistics()
    
    console.print("📊 Dataset Registry Statistics")
    console.print("=" * 30)
    
    console.print(f"Total datasets: {stats['total_datasets']}")
    console.print(f"Ready datasets: {stats['ready_datasets']}")
    console.print(f"Total subsets: {stats['total_subsets']}")
    console.print(f"Total RDB files: {stats['total_rdb_files']}")
    
    console.print("\nState distribution:")
    for state, count in stats['states'].items():
        console.print(f"  {state}: {count}")


@app.command("validate")
def validate_dataset(
    name: str = typer.Argument(..., help="Dataset name"),
    check_s3: bool = typer.Option(False, "--check-s3", help="Validate S3 files exist")
):
    """Validate a dataset."""
    
    registry = get_registry()
    dataset = registry.get(name)
    
    if not dataset:
        console.print(f"❌ Dataset '{name}' not found", style="red")
        return
    
    console.print(f"🔍 Validating dataset: [cyan]{name}[/cyan]")
    
    # Basic validation
    errors = dataset.validate()
    
    if errors:
        console.print("❌ Validation errors found:", style="red")
        for error in errors:
            console.print(f"  • {error}")
    else:
        console.print("✅ Basic validation passed")
    
    # S3 validation
    if check_s3 and dataset.s3_bucket:
        console.print("🔍 Checking S3 files...")
        
        async def check_s3_files():
            s3_config = S3Config(bucket_name=dataset.s3_bucket)
            async with S3DatasetManager(s3_config) as s3_manager:
                # Check if metadata exists
                metadata = await s3_manager.download_metadata(dataset.name)
                if metadata:
                    console.print("✅ Metadata found in S3")
                else:
                    console.print("❌ Metadata not found in S3", style="red")
                
                # Check file listing
                files = s3_manager.list_dataset_files(dataset.name)
                console.print(f"📁 Found {len(files)} files in S3")
        
        try:
            asyncio.run(check_s3_files())
        except Exception as e:
            console.print(f"❌ S3 validation failed: {e}", style="red")


if __name__ == "__main__":
    app()