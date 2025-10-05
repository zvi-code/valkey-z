"""
Visualization commands for the Valkey Search Delete Stress Test CLI.

Provides commands to generate graphs and charts from scenario execution data.
"""

import typer
from typing import Optional, List
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

app = typer.Typer()


@app.command()
def generate(
    scenario_name: str = typer.Argument(..., help="Name of the scenario"),
    metrics_file: str = typer.Option(
        None, "--metrics", "-m", 
        help="Path to CSV metrics file (default: output/{scenario_name}_metrics.csv)"
    ),
    phases_file: str = typer.Option(
        None, "--phases", "-p",
        help="Path to JSON phases file (default: output/{scenario_name}_phases.json)"
    ),
    output_dir: str = typer.Option(
        "visualizations", "--output-dir", "-o",
        help="Output directory for generated visualizations"
    ),
    viz_type: List[str] = typer.Option(
        ["all"], "--type", "-t",
        help="Type of visualization (dashboard, memory, performance, heatmap, all)"
    ),
    format: str = typer.Option(
        "both", "--format", "-f",
        help="Output format (png, html, both)"
    ),
    interactive: bool = typer.Option(
        True, "--interactive/--static",
        help="Generate interactive (HTML) visualizations"
    ),
    high_quality: bool = typer.Option(
        False, "--high-quality", "-hq",
        help="Generate high-quality visualizations (larger file sizes)"
    )
):
    """Generate visualizations from scenario execution data."""
    
    try:
        from visualization import AdvancedVisualizer, MetricsData
    except ImportError as e:
        typer.echo(f"Error: Visualization dependencies not available: {e}", err=True)
        typer.echo("Install with: pip install 'search_delete_test[visualization]'", err=True)
        raise typer.Exit(1)
    
    # Determine file paths
    if not metrics_file:
        metrics_file = f"output/{scenario_name}_metrics.csv"
    if not phases_file:
        phases_file = f"output/{scenario_name}_phases.json"
    
    # Check if files exist
    metrics_path = Path(metrics_file)
    phases_path = Path(phases_file)
    
    if not metrics_path.exists():
        typer.echo(f"Error: Metrics file not found: {metrics_path}", err=True)
        raise typer.Exit(1)
    
    if not phases_path.exists():
        typer.echo(f"Warning: Phases file not found: {phases_path}", err=True)
        phases_file = None
    
    # Load data
    typer.echo(f"Loading metrics from {metrics_path}...")
    data = MetricsData(csv_file=str(metrics_path), json_file=phases_file)
    
    if data.memory_df.empty and data.performance_df.empty:
        typer.echo("Error: No valid metrics data found", err=True)
        raise typer.Exit(1)
    
    # Create visualizer
    visualizer = AdvancedVisualizer(output_dir=output_dir)
    
    # Generate visualizations
    viz_types = viz_type if "all" not in viz_type else ["dashboard", "memory", "performance", "heatmap"]
    
    for vtype in viz_types:
        typer.echo(f"Generating {vtype} visualization...")
        
        try:
            if vtype == "dashboard":
                fig = visualizer.create_comprehensive_dashboard(data, scenario_name)
                if fig and not interactive:
                    # Save static version too
                    static_path = Path(output_dir) / f"{scenario_name}_dashboard.png"
                    fig.write_image(str(static_path), width=1920, height=1080)
                    
            elif vtype == "memory":
                visualizer.create_memory_phase_analysis(data, scenario_name)
                
            elif vtype == "performance":
                visualizer.create_performance_correlation_analysis(data, scenario_name)
                
            elif vtype == "heatmap":
                visualizer.create_phase_comparison_heatmap(data, scenario_name)
                
        except Exception as e:
            typer.echo(f"Error generating {vtype} visualization: {e}", err=True)
            logger.exception(f"Visualization error for {vtype}")
    
    typer.echo(f"✓ Visualizations saved to {output_dir}/")


@app.command()
def list_scenarios(
    output_dir: str = typer.Option("output", "--output-dir", "-o", help="Output directory to scan")
):
    """List available scenarios with metrics data."""
    
    output_path = Path(output_dir)
    if not output_path.exists():
        typer.echo(f"Output directory not found: {output_path}")
        return
    
    scenarios = {}
    
    # Scan for metrics files
    for metrics_file in output_path.glob("*_metrics.csv"):
        scenario_name = metrics_file.stem.replace("_metrics", "")
        phases_file = output_path / f"{scenario_name}_phases.json"
        
        scenarios[scenario_name] = {
            "metrics": metrics_file,
            "phases": phases_file if phases_file.exists() else None,
            "size": metrics_file.stat().st_size
        }
    
    if not scenarios:
        typer.echo("No scenarios with metrics data found")
        return
    
    typer.echo("Available scenarios for visualization:")
    typer.echo("")
    
    for name, info in sorted(scenarios.items()):
        size_mb = info["size"] / (1024 * 1024)
        phases_status = "✓" if info["phases"] else "✗"
        typer.echo(f"  {name:30} {size_mb:6.1f} MB   Phases: {phases_status}")


@app.command()
def compare(
    scenario_names: List[str] = typer.Argument(..., help="Names of scenarios to compare"),
    output_dir: str = typer.Option("visualizations", "--output-dir", "-o", help="Output directory"),
    metric: str = typer.Option("rss_mb", "--metric", "-m", help="Metric to compare"),
    normalize: bool = typer.Option(False, "--normalize", help="Normalize metrics for comparison")
):
    """Compare metrics across multiple scenarios."""
    
    try:
        from visualization import AdvancedVisualizer, MetricsData
        import matplotlib.pyplot as plt
        import pandas as pd
    except ImportError as e:
        typer.echo(f"Error: Visualization dependencies not available: {e}", err=True)
        raise typer.Exit(1)
    
    if len(scenario_names) < 2:
        typer.echo("Error: At least 2 scenarios required for comparison", err=True)
        raise typer.Exit(1)
    
    # Load data for all scenarios
    scenario_data = {}
    for name in scenario_names:
        metrics_file = f"output/{name}_metrics.csv"
        if not Path(metrics_file).exists():
            typer.echo(f"Warning: Metrics file not found for {name}", err=True)
            continue
            
        data = MetricsData(csv_file=metrics_file)
        if not data.memory_df.empty:
            scenario_data[name] = data
    
    if len(scenario_data) < 2:
        typer.echo("Error: Not enough valid scenario data for comparison", err=True)
        raise typer.Exit(1)
    
    # Create comparison plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    for name, data in scenario_data.items():
        if metric in data.memory_df.columns:
            values = data.memory_df[metric]
            if normalize:
                values = (values - values.min()) / (values.max() - values.min())
            
            ax.plot(range(len(values)), values, label=name, linewidth=2)
    
    ax.set_xlabel("Time (relative)")
    ax.set_ylabel(f"{metric} {'(normalized)' if normalize else ''}")
    ax.set_title(f"Scenario Comparison: {metric}")
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    # Save comparison plot
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    comparison_file = output_path / f"comparison_{metric}_{'_'.join(scenario_names)}.png"
    plt.savefig(comparison_file, dpi=300, bbox_inches='tight')
    
    typer.echo(f"✓ Comparison saved to {comparison_file}")


@app.command()
def export_data(
    scenario_name: str = typer.Argument(..., help="Name of the scenario"),
    format: str = typer.Option("json", "--format", "-f", help="Export format (json, csv, excel)"),
    output_file: str = typer.Option(None, "--output", "-o", help="Output file path"),
    include_phases: bool = typer.Option(True, "--include-phases", help="Include phase information")
):
    """Export scenario data in various formats for external analysis."""
    
    metrics_file = f"output/{scenario_name}_metrics.csv"
    phases_file = f"output/{scenario_name}_phases.json" if include_phases else None
    
    if not Path(metrics_file).exists():
        typer.echo(f"Error: Metrics file not found: {metrics_file}", err=True)
        raise typer.Exit(1)
    
    try:
        from visualization import MetricsData
        import pandas as pd
        import json
    except ImportError as e:
        typer.echo(f"Error: Required dependencies not available: {e}", err=True)
        raise typer.Exit(1)
    
    # Load data
    data = MetricsData(csv_file=metrics_file, json_file=phases_file)
    
    if not output_file:
        output_file = f"{scenario_name}_export.{format}"
    
    # Export based on format
    if format == "json":
        export_data = {
            "scenario": scenario_name,
            "memory_metrics": data.memory_df.to_dict("records") if not data.memory_df.empty else [],
            "performance_metrics": data.performance_df.to_dict("records") if not data.performance_df.empty else [],
            "phases": [
                {
                    "name": p.name,
                    "type": p.phase_type,
                    "start_time": p.start_time,
                    "end_time": p.end_time,
                    "duration": p.duration,
                    "description": p.description,
                    "metadata": p.metadata
                } for p in data.phases
            ] if include_phases else []
        }
        
        with open(output_file, 'w') as f:
            json.dump(export_data, f, indent=2)
            
    elif format == "csv":
        # Combine all data into single CSV
        combined_df = pd.concat([data.memory_df, data.performance_df], ignore_index=True, sort=False)
        combined_df.to_csv(output_file, index=False)
        
    elif format == "excel":
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            if not data.memory_df.empty:
                data.memory_df.to_excel(writer, sheet_name='Memory_Metrics', index=False)
            if not data.performance_df.empty:
                data.performance_df.to_excel(writer, sheet_name='Performance_Metrics', index=False)
            
            # Add phases sheet if available
            if include_phases and data.phases:
                phases_df = pd.DataFrame([
                    {
                        "name": p.name,
                        "type": p.phase_type,
                        "start_time": p.start_time,
                        "end_time": p.end_time,
                        "duration": p.duration,
                        "description": p.description
                    } for p in data.phases
                ])
                phases_df.to_excel(writer, sheet_name='Phases', index=False)
    
    typer.echo(f"✓ Data exported to {output_file}")


if __name__ == "__main__":
    app()
