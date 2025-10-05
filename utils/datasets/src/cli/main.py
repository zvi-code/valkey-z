"""
Search Module Delete Test Testing CLI

Main entry point for the vst command-line tool.
"""

import typer
from typing import Optional
from pathlib import Path
import logging
import sys

app = typer.Typer(
    name="vst",
    help="Search Module Delete Test Testing Tool",
    add_completion=True,
)

# Import and add command groups individually to avoid circular imports
def _add_commands():
    """Add command groups to the main app."""
    from .commands import run, dataset, validate, info, visualize, prep, external_datasets
    
    app.add_typer(run.app, name="run", help="Run stress test scenarios")
    app.add_typer(dataset.app, name="dataset", help="Dataset lifecycle management")
    app.add_typer(external_datasets.app, name="external-datasets", help="Download and manage external datasets")
    app.add_typer(validate.app, name="validate", help="Validate configurations")
    app.add_typer(info.app, name="info", help="Display system information")
    app.add_typer(visualize.app, name="visualize", help="Generate visualizations from scenario data")
    app.add_typer(prep.prep_app, name="prep", help="Dataset preparation and S3 management")

# Add commands when the module is imported
_add_commands()


def _configure_logging(verbose: bool, quiet: bool) -> None:
    """Configure logging based on verbosity flags."""
    if quiet:
        level = logging.ERROR
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stderr
    )
    
    # Reduce noise from third-party libraries
    if not verbose:
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('h5py').setLevel(logging.WARNING)
        logging.getLogger('valkey').setLevel(logging.WARNING)


@app.callback()
def main(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress non-error output"),
):
    """Search Module Delete Test Testing Tool."""
    # Configure logging
    _configure_logging(verbose, quiet)
    
    # Validate conflicting options
    if verbose and quiet:
        typer.echo("Error: Cannot use both --verbose and --quiet", err=True)
        raise typer.Exit(1)


def _get_version() -> str:
    """Get package version."""
    try:
        import importlib.metadata
        return importlib.metadata.version("search_delete_test")
    except Exception:
        # Fallback version if package metadata is not available
        return "0.1.0"


@app.command()
def version():
    """Display version information."""
    version_str = _get_version()
    typer.echo(f"search_delete_test version {version_str}")
    
    # Additional version information in verbose mode
    try:
        import importlib.metadata
        import sys
        
        typer.echo(f"Python {sys.version}")
        
        # Show key dependency versions
        deps = ['valkey', 'numpy', 'typer', 'pyyaml']
        typer.echo("\nKey dependencies:")
        for dep in deps:
            try:
                dep_version = importlib.metadata.version(dep)
                typer.echo(f"  {dep}: {dep_version}")
            except importlib.metadata.PackageNotFoundError:
                typer.echo(f"  {dep}: Not found")
                
    except ImportError:
        pass


if __name__ == "__main__":
    app()
