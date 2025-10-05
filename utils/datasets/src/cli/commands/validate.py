"""Configuration validation commands."""

import typer
import yaml
from pathlib import Path
from typing import Dict, Any

from core.config import Config, ConfigValidator
from scenarios.loader import ScenarioLoader

app = typer.Typer()


@app.command()
def scenario(
    scenario_file: Path = typer.Argument(..., help="Path to scenario YAML file"),
    strict: bool = typer.Option(False, "--strict", help="Enable strict validation"),
):
    """Validate a scenario configuration file."""
    typer.echo(f"Validating scenario: {scenario_file}")
    
    if not scenario_file.exists():
        typer.echo(f"❌ Scenario file not found: {scenario_file}", err=True)
        raise typer.Exit(1)
    
    try:
        # Load and validate the scenario
        loader = ScenarioLoader()
        scenario = loader.load_scenario(scenario_file)
        
        typer.echo("✅ Scenario validation successful!")
        typer.echo(f"  Name: {scenario.name}")
        typer.echo(f"  Description: {scenario.description}")
        typer.echo(f"  Dataset: {scenario.dataset}")
        typer.echo(f"  Steps: {len(scenario.steps)}")
        
        # Check global configuration
        if scenario.global_config:
            typer.echo("  Global Config:")
            for key, value in scenario.global_config.items():
                typer.echo(f"    {key}: {value}")
        
        # Validate each step
        typer.echo("  Step Validation:")
        for i, step in enumerate(scenario.steps):
            step_type = step.type.value if hasattr(step.type, 'value') else str(step.type)
            step_name = step.name
            typer.echo(f"    {i+1}. {step_name} ({step_type}): ✅")
            
            if strict:
                # In strict mode, check for additional requirements
                if step.type.value == "workload":
                    workload_name = step.workload
                    if not workload_name:
                        typer.echo(f"      ⚠️  Missing workload specification", err=True)
                    
                    parameters = step.parameters
                    if not parameters:
                        typer.echo(f"      ⚠️  No parameters specified")
                
                if step.duration_seconds is not None and step.wait_condition is not None:
                    typer.echo(f"      ⚠️  Both duration and wait condition specified")
        
        # Estimate total duration
        total_duration = scenario.get_total_duration()
        if total_duration:
            typer.echo(f"  Estimated Duration: {total_duration:.1f} seconds")
        else:
            typer.echo(f"  Duration: Variable (contains wait conditions)")
            
    except yaml.YAMLError as e:
        typer.echo(f"❌ YAML parsing error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"❌ Scenario validation failed: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def config(
    config_file: Path = typer.Argument(..., help="Path to configuration file"),
):
    """Validate a configuration file."""
    typer.echo(f"Validating config: {config_file}")
    
    if not config_file.exists():
        typer.echo(f"❌ Configuration file not found: {config_file}", err=True)
        raise typer.Exit(1)
    
    try:
        # Load the configuration file
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f) or {}
        
        # Validate using ConfigValidator
        if not ConfigValidator.validate(config_data):
            typer.echo("❌ Configuration validation failed", err=True)
            raise typer.Exit(1)
        
        # Try creating Config object
        config = Config(config_path=config_file)
        config.validate()
        
        typer.echo("✅ Configuration validation successful!")
        
        # Display configuration sections
        sections = ["valkey", "index", "workload", "monitoring", "output"]
        for section in sections:
            if section in config_data:
                typer.echo(f"  {section.title()} Config: ✅")
                section_data = config_data[section]
                for key, value in section_data.items():
                    typer.echo(f"    {key}: {value}")
            else:
                typer.echo(f"  {section.title()} Config: Using defaults")
        
        # Show effective configuration
        typer.echo("\nEffective Configuration:")
        effective_config = config.to_dict()
        for section_name, section_data in effective_config.items():
            typer.echo(f"  {section_name}:")
            for key, value in section_data.items():
                typer.echo(f"    {key}: {value}")
            
    except yaml.YAMLError as e:
        typer.echo(f"❌ YAML parsing error: {e}", err=True)
        raise typer.Exit(1)
    except Exception as e:
        typer.echo(f"❌ Configuration validation failed: {e}", err=True)
        raise typer.Exit(1)
