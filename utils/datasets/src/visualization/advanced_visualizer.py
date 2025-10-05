#!/usr/bin/env python3
"""
Advanced scenario visualization tools for Valkey Search Delete Stress Test.

Generates interactive graphs with embedded phase information showing:
- Memory usage patterns over time
- Performance metrics correlation
- Fragmentation analysis
- Phase-based annotations

Dependencies: matplotlib, plotly, pandas, numpy
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Rectangle
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import json
import argparse
import os
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScenarioPhase:
    """Represents a phase in a scenario execution."""
    
    def __init__(self, name: str, start_time: float, end_time: float, 
                 phase_type: str, description: str = "", metadata: Dict = None):
        self.name = name
        self.start_time = start_time
        self.end_time = end_time
        self.phase_type = phase_type
        self.description = description
        self.metadata = metadata or {}
        
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time
        
    def contains_time(self, timestamp: float) -> bool:
        return self.start_time <= timestamp <= self.end_time


class MetricsData:
    """Container for scenario metrics data."""
    
    def __init__(self, csv_file: Optional[str] = None, json_file: Optional[str] = None):
        self.memory_df = pd.DataFrame()
        self.performance_df = pd.DataFrame()
        self.phases = []
        
        if csv_file:
            self.load_csv_data(csv_file)
        if json_file:
            self.load_phase_data(json_file)
    
    def load_csv_data(self, csv_file: str):
        """Load metrics from CSV file."""
        try:
            df = pd.read_csv(csv_file)
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
            
            # Separate memory and performance metrics
            memory_cols = ['timestamp', 'datetime', 'rss_mb', 'active_mb', 
                          'resident_mb', 'allocated_mb', 'fragmentation_ratio']
            perf_cols = ['timestamp', 'datetime', 'phase', 'qps', 'ips', 'dps',
                        'p50_ms', 'p95_ms', 'p99_ms']
            
            # Check which columns exist and create subsets accordingly
            available_memory_cols = [col for col in memory_cols if col in df.columns]
            available_perf_cols = [col for col in perf_cols if col in df.columns]
            
            if len(available_memory_cols) >= 3:  # timestamp, datetime + at least one metric
                self.memory_df = df[available_memory_cols].copy()
                
            if len(available_perf_cols) >= 3:  # timestamp, datetime + at least one metric
                self.performance_df = df[available_perf_cols].copy()
                
            logger.info(f"Loaded {len(df)} metrics records from {csv_file}")
            
        except Exception as e:
            logger.error(f"Error loading CSV data: {e}")
    
    def load_phase_data(self, json_file: str):
        """Load phase information from JSON file."""
        try:
            with open(json_file, 'r') as f:
                phase_data = json.load(f)
                
            for phase_info in phase_data.get('phases', []):
                phase = ScenarioPhase(
                    name=phase_info['name'],
                    start_time=phase_info['start_time'],
                    end_time=phase_info['end_time'],
                    phase_type=phase_info.get('type', 'unknown'),
                    description=phase_info.get('description', ''),
                    metadata=phase_info.get('metadata', {})
                )
                self.phases.append(phase)
                
            logger.info(f"Loaded {len(self.phases)} phases from {json_file}")
            
        except Exception as e:
            logger.error(f"Error loading phase data: {e}")


class AdvancedVisualizer:
    """Advanced visualization generator for scenario metrics."""
    
    def __init__(self, output_dir: str = "_output/visualizations"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Color schemes for different phase types
        self.phase_colors = {
            'workload': '#FF6B6B',
            'checkpoint': '#4ECDC4', 
            'wait': '#45B7D1',
            'cleanup': '#96CEB4',
            'parallel': '#FECA57',
            'conditional': '#FF9FF3',
            'loop': '#54A0FF'
        }
        
        # Configure matplotlib style
        plt.style.use('seaborn-v0_8')
        plt.rcParams['figure.figsize'] = (15, 10)
        plt.rcParams['font.size'] = 10
    
    def create_comprehensive_dashboard(self, data: MetricsData, scenario_name: str):
        """Create a comprehensive dashboard with all metrics."""
        
        # Create interactive plotly dashboard
        fig = make_subplots(
            rows=4, cols=2,
            subplot_titles=(
                'Memory Usage Over Time', 'Memory Fragmentation',
                'Performance Metrics', 'Operation Rates',
                'Phase Timeline', 'Memory vs Performance Correlation',
                'Memory Growth Analysis', 'System Resource Usage'
            ),
            specs=[
                [{"secondary_y": True}, {"secondary_y": False}],
                [{"secondary_y": True}, {"secondary_y": False}],
                [{"colspan": 2}, None],
                [{"secondary_y": False}, {"secondary_y": False}]
            ],
            vertical_spacing=0.08
        )
        
        # Memory usage over time
        self._add_memory_timeline(fig, data, row=1, col=1)
        
        # Memory fragmentation
        self._add_fragmentation_analysis(fig, data, row=1, col=2)
        
        # Performance metrics
        self._add_performance_metrics(fig, data, row=2, col=1)
        
        # Operation rates
        self._add_operation_rates(fig, data, row=2, col=2)
        
        # Phase timeline
        self._add_phase_timeline(fig, data, row=3, col=1)
        
        # Memory growth analysis
        self._add_memory_growth_analysis(fig, data, row=4, col=1)
        
        # System resource usage
        self._add_resource_usage(fig, data, row=4, col=2)
        
        # Update layout
        fig.update_layout(
            title=f"Valkey Search Delete Stress Test Dashboard - {scenario_name}",
            height=1200,
            showlegend=True,
            template="plotly_white"
        )
        
        # Save interactive dashboard
        output_file = self.output_dir / f"{scenario_name}_dashboard.html"
        fig.write_html(str(output_file))
        logger.info(f"Saved interactive dashboard to {output_file}")
        
        return fig
    
    def create_memory_phase_analysis(self, data: MetricsData, scenario_name: str):
        """Create detailed memory analysis with phase annotations."""
        
        fig, axes = plt.subplots(3, 1, figsize=(16, 12))
        
        if data.memory_df.empty:
            logger.warning("No memory data available for visualization")
            return fig
            
        times = data.memory_df['datetime']
        
        # Memory usage plot
        ax1 = axes[0]
        ax1.plot(times, data.memory_df['rss_mb'], label='RSS Memory', linewidth=2, color='#e74c3c')
        ax1.plot(times, data.memory_df['active_mb'], label='Active Memory', linewidth=2, color='#3498db')
        ax1.plot(times, data.memory_df['allocated_mb'], label='Allocated Memory', linewidth=2, color='#f39c12')
        
        self._add_phase_annotations(ax1, data.phases)
        
        ax1.set_ylabel('Memory (MB)')
        ax1.set_title(f'Memory Usage Analysis - {scenario_name}', fontsize=14, fontweight='bold')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        
        # Memory fragmentation plot
        ax2 = axes[1]
        ax2.plot(times, data.memory_df['fragmentation_ratio'], 
                label='Fragmentation Ratio', linewidth=2, color='#9b59b6')
        ax2.axhline(y=1.0, color='red', linestyle='--', alpha=0.7, label='Ideal (1.0)')
        ax2.axhline(y=1.5, color='orange', linestyle='--', alpha=0.7, label='Warning (1.5)')
        
        self._add_phase_annotations(ax2, data.phases)
        
        ax2.set_ylabel('Fragmentation Ratio')
        ax2.set_title('Memory Fragmentation Analysis', fontsize=14, fontweight='bold')
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)
        
        # Memory efficiency plot
        ax3 = axes[2]
        if 'resident_mb' in data.memory_df.columns:
            efficiency = data.memory_df['allocated_mb'] / data.memory_df['rss_mb'] * 100
            ax3.plot(times, efficiency, label='Memory Efficiency (%)', 
                    linewidth=2, color='#27ae60')
            ax3.axhline(y=80, color='green', linestyle='--', alpha=0.7, label='Good (80%)')
            ax3.axhline(y=60, color='orange', linestyle='--', alpha=0.7, label='Warning (60%)')
        
        self._add_phase_annotations(ax3, data.phases)
        
        ax3.set_ylabel('Efficiency (%)')
        ax3.set_xlabel('Time')
        ax3.set_title('Memory Efficiency Analysis', fontsize=14, fontweight='bold')
        ax3.legend(loc='upper left')
        ax3.grid(True, alpha=0.3)
        
        # Format x-axis
        for ax in axes:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        
        # Save the plot
        output_file = self.output_dir / f"{scenario_name}_memory_analysis.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        logger.info(f"Saved memory analysis to {output_file}")
        
        return fig
    
    def create_performance_correlation_analysis(self, data: MetricsData, scenario_name: str):
        """Create performance correlation analysis with phase information."""
        
        if data.performance_df.empty or data.memory_df.empty:
            logger.warning("Insufficient data for performance correlation analysis")
            return None
            
        # Merge memory and performance data by timestamp
        merged_df = pd.merge_asof(
            data.performance_df.sort_values('timestamp'),
            data.memory_df.sort_values('timestamp'),
            on='timestamp',
            direction='nearest',
            suffixes=('', '_mem')
        )
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        
        # QPS vs Memory usage
        ax1 = axes[0, 0]
        scatter = ax1.scatter(merged_df['rss_mb'], merged_df['qps'], 
                            c=merged_df['timestamp'], cmap='viridis', alpha=0.6)
        ax1.set_xlabel('RSS Memory (MB)')
        ax1.set_ylabel('Queries per Second')
        ax1.set_title('QPS vs Memory Usage')
        plt.colorbar(scatter, ax=ax1, label='Time')
        
        # Latency vs Memory usage
        ax2 = axes[0, 1]
        ax2.scatter(merged_df['rss_mb'], merged_df['p95_ms'], 
                   alpha=0.6, label='P95 Latency', color='#e74c3c')
        ax2.scatter(merged_df['rss_mb'], merged_df['p99_ms'], 
                   alpha=0.6, label='P99 Latency', color='#c0392b')
        ax2.set_xlabel('RSS Memory (MB)')
        ax2.set_ylabel('Latency (ms)')
        ax2.set_title('Latency vs Memory Usage')
        ax2.legend()
        
        # Fragmentation vs Performance
        ax3 = axes[1, 0]
        ax3.scatter(merged_df['fragmentation_ratio'], merged_df['qps'], 
                   alpha=0.6, color='#3498db')
        ax3.set_xlabel('Fragmentation Ratio')
        ax3.set_ylabel('Queries per Second')
        ax3.set_title('Performance vs Fragmentation')
        
        # Operations timeline
        ax4 = axes[1, 1]
        times = merged_df['datetime']
        ax4.plot(times, merged_df['qps'], label='QPS', linewidth=2)
        ax4.plot(times, merged_df['ips'], label='IPS', linewidth=2)
        if 'dps' in merged_df.columns:
            ax4.plot(times, merged_df['dps'], label='DPS', linewidth=2)
        
        self._add_phase_annotations(ax4, data.phases)
        
        ax4.set_xlabel('Time')
        ax4.set_ylabel('Operations per Second')
        ax4.set_title('Operations Timeline')
        ax4.legend()
        ax4.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        
        # Save the plot
        output_file = self.output_dir / f"{scenario_name}_performance_correlation.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        logger.info(f"Saved performance correlation to {output_file}")
        
        return fig
    
    def create_phase_comparison_heatmap(self, data: MetricsData, scenario_name: str):
        """Create a heatmap comparing metrics across phases."""
        
        if not data.phases or data.memory_df.empty:
            logger.warning("Insufficient data for phase comparison")
            return None
            
        # Calculate metrics for each phase
        phase_metrics = []
        
        for phase in data.phases:
            phase_data = data.memory_df[
                (data.memory_df['timestamp'] >= phase.start_time) &
                (data.memory_df['timestamp'] <= phase.end_time)
            ]
            
            if phase_data.empty:
                continue
                
            metrics = {
                'Phase': phase.name,
                'Type': phase.phase_type,
                'Duration (min)': phase.duration / 60,
                'Avg RSS (MB)': phase_data['rss_mb'].mean(),
                'Max RSS (MB)': phase_data['rss_mb'].max(),
                'Avg Fragmentation': phase_data['fragmentation_ratio'].mean(),
                'Max Fragmentation': phase_data['fragmentation_ratio'].max(),
                'Memory Growth (MB)': phase_data['rss_mb'].max() - phase_data['rss_mb'].min(),
            }
            
            # Add performance metrics if available
            if not data.performance_df.empty:
                perf_data = data.performance_df[
                    (data.performance_df['timestamp'] >= phase.start_time) &
                    (data.performance_df['timestamp'] <= phase.end_time)
                ]
                if not perf_data.empty:
                    metrics.update({
                        'Avg QPS': perf_data['qps'].mean(),
                        'Avg P95 (ms)': perf_data['p95_ms'].mean(),
                    })
            
            phase_metrics.append(metrics)
        
        if not phase_metrics:
            logger.warning("No phase metrics calculated")
            return None
            
        # Create DataFrame and heatmap
        df = pd.DataFrame(phase_metrics)
        
        # Select numeric columns for heatmap
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        heatmap_data = df[numeric_cols].T
        
        fig, ax = plt.subplots(figsize=(16, 10))
        
        im = ax.imshow(heatmap_data.values, cmap='RdYlBu_r', aspect='auto')
        
        # Set ticks and labels
        ax.set_xticks(range(len(df)))
        ax.set_xticklabels([f"{row['Phase']}\n({row['Type']})" for _, row in df.iterrows()], 
                          rotation=45, ha='right')
        ax.set_yticks(range(len(numeric_cols)))
        ax.set_yticklabels(numeric_cols)
        
        # Add colorbar
        plt.colorbar(im, ax=ax, label='Normalized Values')
        
        # Add text annotations
        for i, col in enumerate(numeric_cols):
            for j, (_, row) in enumerate(df.iterrows()):
                value = row[col]
                ax.text(j, i, f'{value:.1f}', ha='center', va='center',
                       color='white' if abs(value) > heatmap_data.values.std() else 'black')
        
        ax.set_title(f'Phase Metrics Comparison - {scenario_name}', 
                    fontsize=14, fontweight='bold')
        
        plt.tight_layout()
        
        # Save the plot
        output_file = self.output_dir / f"{scenario_name}_phase_heatmap.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        logger.info(f"Saved phase heatmap to {output_file}")
        
        return fig
    
    def _add_memory_timeline(self, fig, data: MetricsData, row: int, col: int):
        """Add memory timeline to subplot."""
        if data.memory_df.empty:
            return
            
        fig.add_trace(
            go.Scatter(
                x=data.memory_df['datetime'],
                y=data.memory_df['rss_mb'],
                name='RSS Memory',
                line=dict(color='#e74c3c', width=2)
            ),
            row=row, col=col
        )
        
        fig.add_trace(
            go.Scatter(
                x=data.memory_df['datetime'],
                y=data.memory_df['active_mb'],
                name='Active Memory',
                line=dict(color='#3498db', width=2)
            ),
            row=row, col=col
        )
        
        # Add phase annotations
        self._add_plotly_phase_annotations(fig, data.phases, row, col)
    
    def _add_fragmentation_analysis(self, fig, data: MetricsData, row: int, col: int):
        """Add fragmentation analysis to subplot."""
        if data.memory_df.empty:
            return
            
        fig.add_trace(
            go.Scatter(
                x=data.memory_df['datetime'],
                y=data.memory_df['fragmentation_ratio'],
                name='Fragmentation Ratio',
                line=dict(color='#9b59b6', width=2)
            ),
            row=row, col=col
        )
        
        # Add warning lines
        fig.add_hline(y=1.0, line_dash="dash", line_color="green", 
                     annotation_text="Ideal", row=row, col=col)
        fig.add_hline(y=1.5, line_dash="dash", line_color="orange", 
                     annotation_text="Warning", row=row, col=col)
    
    def _add_performance_metrics(self, fig, data: MetricsData, row: int, col: int):
        """Add performance metrics to subplot."""
        if data.performance_df.empty:
            return
            
        fig.add_trace(
            go.Scatter(
                x=data.performance_df['datetime'],
                y=data.performance_df['p95_ms'],
                name='P95 Latency',
                yaxis='y2',
                line=dict(color='#e74c3c', width=2)
            ),
            row=row, col=col, secondary_y=True
        )
        
        fig.add_trace(
            go.Scatter(
                x=data.performance_df['datetime'],
                y=data.performance_df['qps'],
                name='QPS',
                line=dict(color='#27ae60', width=2)
            ),
            row=row, col=col, secondary_y=False
        )
    
    def _add_operation_rates(self, fig, data: MetricsData, row: int, col: int):
        """Add operation rates to subplot."""
        if data.performance_df.empty:
            return
            
        fig.add_trace(
            go.Scatter(
                x=data.performance_df['datetime'],
                y=data.performance_df['qps'],
                name='Queries/sec',
                line=dict(color='#3498db', width=2)
            ),
            row=row, col=col
        )
        
        fig.add_trace(
            go.Scatter(
                x=data.performance_df['datetime'],
                y=data.performance_df['ips'],
                name='Inserts/sec',
                line=dict(color='#e74c3c', width=2)
            ),
            row=row, col=col
        )
    
    def _add_phase_timeline(self, fig, data: MetricsData, row: int, col: int):
        """Add phase timeline visualization."""
        if not data.phases:
            return
            
        y_pos = 0
        for i, phase in enumerate(data.phases):
            start_dt = datetime.fromtimestamp(phase.start_time)
            end_dt = datetime.fromtimestamp(phase.end_time)
            
            fig.add_trace(
                go.Scatter(
                    x=[start_dt, end_dt],
                    y=[y_pos, y_pos],
                    mode='lines+markers',
                    name=f"{phase.name} ({phase.phase_type})",
                    line=dict(width=8, color=self.phase_colors.get(phase.phase_type, '#95a5a6')),
                    marker=dict(size=8),
                    hovertemplate=f"Phase: {phase.name}<br>Type: {phase.phase_type}<br>Duration: {phase.duration:.1f}s"
                ),
                row=row, col=col
            )
            y_pos += 1
    
    def _add_memory_growth_analysis(self, fig, data: MetricsData, row: int, col: int):
        """Add memory growth analysis."""
        if data.memory_df.empty:
            return
            
        # Calculate memory growth rate
        memory_growth = data.memory_df['rss_mb'].diff()
        
        fig.add_trace(
            go.Scatter(
                x=data.memory_df['datetime'],
                y=memory_growth,
                name='Memory Growth Rate',
                line=dict(color='#f39c12', width=2)
            ),
            row=row, col=col
        )
        
        fig.add_hline(y=0, line_dash="dash", line_color="black", 
                     annotation_text="No Growth", row=row, col=col)
    
    def _add_resource_usage(self, fig, data: MetricsData, row: int, col: int):
        """Add system resource usage."""
        if data.memory_df.empty:
            return
            
        # Calculate memory utilization percentage
        if 'allocated_mb' in data.memory_df.columns:
            utilization = (data.memory_df['allocated_mb'] / data.memory_df['rss_mb']) * 100
            
            fig.add_trace(
                go.Scatter(
                    x=data.memory_df['datetime'],
                    y=utilization,
                    name='Memory Utilization %',
                    line=dict(color='#27ae60', width=2)
                ),
                row=row, col=col
            )
    
    def _add_phase_annotations(self, ax, phases: List[ScenarioPhase]):
        """Add phase annotations to matplotlib axis."""
        if not phases:
            return
            
        y_min, y_max = ax.get_ylim()
        
        for phase in phases:
            start_dt = datetime.fromtimestamp(phase.start_time)
            end_dt = datetime.fromtimestamp(phase.end_time)
            
            color = self.phase_colors.get(phase.phase_type, '#95a5a6')
            
            # Add colored background rectangle
            rect = Rectangle(
                (start_dt, y_min), 
                end_dt - start_dt, 
                y_max - y_min,
                facecolor=color, 
                alpha=0.2, 
                edgecolor='none'
            )
            ax.add_patch(rect)
            
            # Add phase label
            mid_time = start_dt + (end_dt - start_dt) / 2
            ax.annotate(
                phase.name,
                xy=(mid_time, y_max * 0.95),
                ha='center',
                va='top',
                fontsize=8,
                rotation=90,
                alpha=0.7
            )
    
    def _add_plotly_phase_annotations(self, fig, phases: List[ScenarioPhase], row: int, col: int):
        """Add phase annotations to plotly subplot."""
        for phase in phases:
            start_dt = datetime.fromtimestamp(phase.start_time)
            end_dt = datetime.fromtimestamp(phase.end_time)
            
            color = self.phase_colors.get(phase.phase_type, '#95a5a6')
            
            fig.add_vrect(
                x0=start_dt, x1=end_dt,
                fillcolor=color, opacity=0.2,
                line_width=0,
                row=row, col=col
            )


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(description="Generate advanced visualizations for Valkey-search delete stress test scenarios")
    parser.add_argument("--csv", required=True, help="Path to CSV metrics file")
    parser.add_argument("--phases", help="Path to JSON phases file")
    parser.add_argument("--scenario", required=True, help="Scenario name")
    parser.add_argument("--output-dir", default="visualizations", help="Output directory")
    parser.add_argument("--type", choices=['dashboard', 'memory', 'performance', 'heatmap', 'all'], 
                       default='all', help="Type of visualization to generate")
    
    args = parser.parse_args()
    
    # Load data
    data = MetricsData(csv_file=args.csv, json_file=args.phases)
    
    # Create visualizer
    visualizer = AdvancedVisualizer(output_dir=args.output_dir)
    
    # Generate visualizations based on type
    if args.type in ['dashboard', 'all']:
        visualizer.create_comprehensive_dashboard(data, args.scenario)
    
    if args.type in ['memory', 'all']:
        visualizer.create_memory_phase_analysis(data, args.scenario)
    
    if args.type in ['performance', 'all']:
        visualizer.create_performance_correlation_analysis(data, args.scenario)
    
    if args.type in ['heatmap', 'all']:
        visualizer.create_phase_comparison_heatmap(data, args.scenario)
    
    logger.info(f"Visualization generation complete. Check {args.output_dir} directory.")


if __name__ == "__main__":
    main()
