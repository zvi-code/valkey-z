"""System information commands."""

import typer
import platform
import sys
import psutil
import asyncio
from pathlib import Path
from typing import Optional
import valkey

from workload.registry import WorkloadRegistry
from core.connection import ConnectionConfig

app = typer.Typer()


@app.command()
def system():
    """Display system information."""
    typer.echo("System Information:")
    typer.echo("=" * 50)
    
    # Python information
    typer.echo(f"Python Version: {sys.version}")
    typer.echo(f"Python Executable: {sys.executable}")
    
    # Platform information
    typer.echo(f"Platform: {platform.platform()}")
    typer.echo(f"Architecture: {platform.architecture()[0]}")
    typer.echo(f"Machine: {platform.machine()}")
    typer.echo(f"Processor: {platform.processor()}")
    
    # CPU information
    typer.echo(f"CPU Count: {psutil.cpu_count(logical=True)} (logical), {psutil.cpu_count(logical=False)} (physical)")
    cpu_freq = psutil.cpu_freq()
    if cpu_freq:
        typer.echo(f"CPU Frequency: {cpu_freq.current:.2f} MHz (max: {cpu_freq.max:.2f} MHz)")
    
    # Memory information
    memory = psutil.virtual_memory()
    typer.echo(f"Total Memory: {memory.total / (1024**3):.2f} GB")
    typer.echo(f"Available Memory: {memory.available / (1024**3):.2f} GB")
    typer.echo(f"Memory Usage: {memory.percent}%")
    
    # Package versions
    typer.echo("\nKey Package Versions:")
    try:
        import importlib.metadata
        packages = ['valkey', 'numpy', 'pandas', 'psutil', 'typer', 'pyyaml', 'h5py']
        for pkg in packages:
            try:
                version = importlib.metadata.version(pkg)
                typer.echo(f"  {pkg}: {version}")
            except importlib.metadata.PackageNotFoundError:
                typer.echo(f"  {pkg}: Not installed")
    except ImportError:
        typer.echo("  Package version information not available")


def _get_valkey_info(host: str, port: int, password: Optional[str] = None):
    """Get Valkey info synchronously."""
    try:
        client = valkey.Valkey(
            host=host,
            port=port,
            password=password,
            socket_connect_timeout=5.0,
            socket_timeout=5.0,
            decode_responses=True
        )
        
        # Test connection
        client.ping()
        
        # Get server info
        info = client.info()
        
        client.close()
        return info
        
    except Exception as e:
        return {"error": str(e)}


@app.command("valkey")
def valkey_info(
    host: str = typer.Option("localhost", "--host", "-h", help="Valkey host"),
    port: int = typer.Option(6379, "--port", "-p", help="Valkey port"),
    password: Optional[str] = typer.Option(None, "--password", "-a", help="Valkey password"),
):
    """Display Valkey server information."""
    typer.echo(f"Valkey Server Information ({host}:{port})")
    typer.echo("=" * 50)
    
    try:
        info = _get_valkey_info(host, port, password)
        
        if "error" in info:
            typer.echo(f"❌ Connection failed: {info['error']}", err=True)
            raise typer.Exit(1)
        
        # Display key information sections
        sections = {
            "Server": ["valkey_version", "valkey_mode", "os", "arch_bits", "multiplexing_api", "process_id"],
            "Memory": ["used_memory_human", "used_memory_peak_human", "used_memory_rss_human", 
                      "mem_fragmentation_ratio", "mem_allocator"],
            "Persistence": ["rdb_last_save_time", "rdb_changes_since_last_save", "aof_enabled"],
            "Stats": ["total_connections_received", "total_commands_processed", "keyspace_hits", 
                     "keyspace_misses", "evicted_keys"],
            "Replication": ["role", "connected_slaves"],
            "CPU": ["used_cpu_sys", "used_cpu_user"],
        }
        
        for section_name, keys in sections.items():
            typer.echo(f"\n{section_name}:")
            for key in keys:
                if key in info:
                    typer.echo(f"  {key}: {info[key]}")
        
        # Check for modules
        typer.echo("\nLoaded Modules:")
        modules = [key for key in info.keys() if key.startswith("module:")]
        if modules:
            for module in modules:
                typer.echo(f"  {module}: {info[module]}")
        else:
            typer.echo("  No modules loaded")
            
        # Search module check
        search_keys = [key for key in info.keys() if "search" in key.lower()]
        if search_keys:
            typer.echo("\nSearch Module Information:")
            for key in search_keys:
                typer.echo(f"  {key}: {info[key]}")
        else:
            typer.echo("\n⚠️  Valkey Search module not detected")
            
    except Exception as e:
        typer.echo(f"❌ Error getting Valkey info: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def workloads():
    """List available workloads."""
    from workload import register_builtin_workloads
    
    # Ensure built-in workloads are registered
    register_builtin_workloads()
    
    workload_info = WorkloadRegistry.get_workload_info()
    
    if not workload_info:
        typer.echo("No workloads registered")
        return
    
    typer.echo("Available Workloads:")
    typer.echo("=" * 50)
    
    for info in workload_info:
        typer.echo(f"Name: {info['name']}")
        typer.echo(f"  Class: {info['class']}")
        typer.echo(f"  Module: {info['module']}")
        typer.echo(f"  Description: {info['description']}")
        typer.echo()
    
    typer.echo(f"Total: {len(workload_info)} workloads available")
