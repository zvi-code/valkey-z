"""Run command for executing scenarios."""

import typer
import asyncio
from pathlib import Path
from typing import Optional
import logging

app = typer.Typer()
logger = logging.getLogger(__name__)


class DatasetManager:
    """Simple dataset manager for CLI."""
    
    def __init__(self):
        self.datasets = {}
    
    def get_dataset(self, dataset_name: str):
        """Get dataset by name - lazy import to avoid circular imports."""
        from core import Dataset
        return Dataset
        """Get or load a dataset."""
        if dataset_name not in self.datasets:
            # For now, create a mock dataset - in real implementation this would load actual data
            from core import Dataset
            self.datasets[dataset_name] = Dataset(dataset_path=Path("mock_path"))
        return self.datasets[dataset_name]


async def run_scenario_async(
    scenario_path: Path,
    config_path: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    dry_run: bool = False
):
    """Async function to run a scenario."""
    # Lazy imports to avoid circular import issues
    from core import Config, ConnectionManager
    from scenarios import ScenarioLoader, ScenarioRunner
    from monitoring import AsyncMemoryCollector
    from workload import register_builtin_workloads
    
    # Register built-in workloads
    register_builtin_workloads()
    
    # Load configuration
    if config_path:
        config = Config(config_path=config_path)
        config.load()
    else:
        # Use default configuration
        config = Config()
        
    # Initialize components
    connection_manager = ConnectionManager(config.valkey)
    dataset_manager = DatasetManager()
    
    # Setup output directory
    if not output_dir:
        output_dir = Path("./output")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load scenario
    loader = ScenarioLoader()
    
    if scenario_path.name in ["continuous_growth", "grow_shrink_grow"]:
        # Built-in scenario
        scenario = loader.load_builtin_scenario(scenario_path.name)
    else:
        # Custom scenario file
        scenario = loader.load_scenario(scenario_path)
    
    typer.echo(f"Loaded scenario: {scenario.name}")
    typer.echo(f"Description: {scenario.description}")
    typer.echo(f"Steps: {len(scenario.steps)}")
    
    if dry_run:
        typer.echo("Dry run - scenario validation completed successfully")
        return

    # Initialize connection manager
    await connection_manager.initialize()

    # Initialize metric collector with Valkey client from connection manager
    valkey_client = await connection_manager.get_pool().get_client()
    metric_collector = AsyncMemoryCollector(
        valkey_client=valkey_client,
        interval_seconds=5.0
    )
    
    # Create scenario runner
    runner = ScenarioRunner(
        connection_manager=connection_manager,
        dataset_manager=dataset_manager,
        metric_collector=metric_collector,
        output_dir=output_dir
    )
    
    try:
        typer.echo("Starting scenario execution...")
        results = await runner.run_scenario(scenario)
        
        # Generate and display report
        report = runner.generate_report(results)
        typer.echo("\n" + report)
        
        # Save detailed results
        results_file = output_dir / f"{scenario.name}_results.yaml"
        import yaml
        with open(results_file, 'w') as f:
            yaml.dump(results, f, default_flow_style=False)
        
        typer.echo(f"\nDetailed results saved to: {results_file}")
        
    except Exception as e:
        typer.echo(f"Error executing scenario: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def scenario(
    scenario_file: Path = typer.Argument(..., help="Path to scenario YAML file or built-in scenario name"),
    config: Optional[Path] = typer.Option(None, "--config", "-c", help="Configuration file"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output directory"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Validate without executing"),
):
    """Run a stress test scenario."""
    try:
        asyncio.run(run_scenario_async(scenario_file, config, output, dry_run))
    except KeyboardInterrupt:
        typer.echo("\nScenario execution interrupted by user")
        raise typer.Exit(130)


@app.command()
def quick(
    dataset: str = typer.Option("openai-5m", "--dataset", "-d", help="Dataset to use"),
    workload: str = typer.Option("mixed", "--workload", "-w", help="Workload type"),
    duration: int = typer.Option(300, "--duration", "-t", help="Duration in seconds"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output directory"),
):
    """Run a quick stress test with default settings."""
    # Create a simple scenario dynamically
    import tempfile
    import yaml
    
    quick_scenario = {
        "name": "quick_test",
        "description": f"Quick {workload} workload test for {duration} seconds",
        "dataset": dataset,
        "global_config": {
            "n_threads": 4,
            "n_clients": 100,
            "batch_size": 100
        },
        "steps": []
    }
    
    # Add appropriate steps based on workload type
    if workload == "mixed":
        quick_scenario["steps"] = [
            {
                "name": "initial_ingest",
                "type": "workload",
                "workload": "ingest",
                "parameters": {"target_vectors": 10000}
            },
            {
                "name": "mixed_operations",
                "type": "workload", 
                "workload": "query",
                "duration_seconds": duration,
                "parameters": {"queries_per_second": 100}
            }
        ]
    elif workload == "ingest":
        quick_scenario["steps"] = [
            {
                "name": "ingest_test",
                "type": "workload",
                "workload": "ingest",
                "duration_seconds": duration,
                "parameters": {"target_vectors": 100000}
            }
        ]
    elif workload == "query":
        quick_scenario["steps"] = [
            {
                "name": "load_data",
                "type": "workload",
                "workload": "ingest", 
                "parameters": {"target_vectors": 10000}
            },
            {
                "name": "query_test",
                "type": "workload",
                "workload": "query",
                "duration_seconds": duration,
                "parameters": {"queries_per_second": 100}
            }
        ]
    else:
        typer.echo(f"Unknown workload type: {workload}")
        raise typer.Exit(1)
    
    # Write scenario to temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(quick_scenario, f, default_flow_style=False)
        temp_scenario_path = Path(f.name)
    
    try:
        typer.echo(f"Running quick test with {workload} workload for {duration}s")
        asyncio.run(run_scenario_async(temp_scenario_path, None, output, False))
    finally:
        # Clean up temporary file
        temp_scenario_path.unlink(missing_ok=True)


@app.command()
def list_scenarios():
    """List available built-in scenarios."""
    from scenarios import ScenarioLoader
    
    loader = ScenarioLoader()
    scenarios = loader.list_builtin_scenarios()
    
    if scenarios:
        typer.echo("Available built-in scenarios:")
        for scenario in scenarios:
            typer.echo(f"  - {scenario}")
    else:
        typer.echo("No built-in scenarios found")


@app.command()
def validate(
    scenario_file: Path = typer.Argument(..., help="Path to scenario YAML file"),
):
    """Validate a scenario file without executing it."""
    from scenarios import ScenarioLoader
    
    try:
        loader = ScenarioLoader()
        scenario = loader.load_scenario(scenario_file)
        
        typer.echo(f"✓ Scenario '{scenario.name}' is valid")
        typer.echo(f"  Description: {scenario.description}")
        typer.echo(f"  Dataset: {scenario.dataset}")
        typer.echo(f"  Steps: {len(scenario.steps)}")
        
        total_duration = scenario.get_total_duration()
        if total_duration:
            typer.echo(f"  Estimated duration: {total_duration:.1f} seconds")
        else:
            typer.echo("  Duration: Variable (contains wait conditions)")
            
    except Exception as e:
        typer.echo(f"✗ Scenario validation failed: {e}", err=True)
        raise typer.Exit(1)
