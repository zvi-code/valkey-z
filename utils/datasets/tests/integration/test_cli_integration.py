"""Integration tests for CLI commands."""

import pytest
import tempfile
import yaml
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch, Mock

from cli.main import app
from typer.testing import CliRunner


@pytest.fixture
def cli_runner():
    """Provide CLI test runner."""
    return CliRunner()


@pytest.fixture
def sample_config():
    """Create a sample configuration file."""
    config_data = {
        "valkey": {
            "host": "localhost",
            "port": 6379,
            "db": 0,
            "max_connections": 100
        },
        "index": {
            "algorithm": "HNSW",
            "dimensions": 1536,
            "m": 16,
            "ef_construction": 200,
            "ef_runtime": 100,
            "distance_metric": "L2"
        },
        "workload": {
            "n_threads": 4,
            "n_clients": 50,
            "batch_size": 100,
            "operation_timeout": 30.0,
            "query_k": 10
        },
        "monitoring": {
            "sampling_interval": 5.0,
            "memory_metrics": ["rss_mb", "active_mb"],
            "export_format": "csv"
        },
        "output": {
            "csv_path": "output/metrics.csv",
            "summary_path": "output/summary.csv",
            "log_level": "INFO"
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        yield Path(f.name)
    
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def sample_scenario():
    """Create a sample scenario file."""
    scenario_data = {
        "name": "integration_test_scenario",
        "description": "Integration test scenario with multiple steps",
        "dataset": "openai-5m",
        "global_config": {
            "n_threads": 2,
            "n_clients": 10,
            "batch_size": 50
        },
        "steps": [
            {
                "name": "setup_index",
                "type": "checkpoint",
                "parameters": {
                    "index_name": "test_index",
                    "clear_existing": True,
                    "actions": ["collect_memory_info"]
                }
            },
            {
                "name": "initial_ingest",
                "type": "workload",
                "workload": "ingest",
                "parameters": {
                    "target_vectors": 1000,
                    "batch_size": 100
                }
            },
            {
                "name": "query_workload",
                "type": "workload",
                "workload": "query",
                "duration_seconds": 30,
                "parameters": {
                    "queries_per_second": 10,
                    "query_k": 5
                }
            },
            {
                "name": "shrink_index",
                "type": "workload",
                "workload": "shrink",
                "parameters": {
                    "target_deletion_ratio": 0.2
                }
            },
            {
                "name": "final_query",
                "type": "workload",
                "workload": "query",
                "duration_seconds": 15,
                "parameters": {
                    "queries_per_second": 5
                }
            }
        ]
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(scenario_data, f)
        yield Path(f.name)
    
    Path(f.name).unlink(missing_ok=True)


@pytest.mark.integration
class TestCLIIntegration:
    """Integration tests for CLI functionality."""
    
    def test_full_cli_workflow(self, cli_runner, sample_config, sample_scenario):
        """Test a complete CLI workflow."""
        with tempfile.TemporaryDirectory() as output_dir:
            # Step 1: Validate configuration
            result = cli_runner.invoke(app, ["validate", "config", str(sample_config)])
            assert result.exit_code == 0
            assert "Configuration validation successful" in result.stdout
            
            # Step 2: Validate scenario  
            result = cli_runner.invoke(app, ["validate", "scenario", str(sample_scenario)])
            assert result.exit_code == 0
            assert "Scenario validation successful" in result.stdout
            
            # Step 3: Show system info
            result = cli_runner.invoke(app, ["info", "system"])
            assert result.exit_code == 0
            assert "System Information" in result.stdout
            
            # Step 4: List workloads
            result = cli_runner.invoke(app, ["info", "workloads"])
            assert result.exit_code == 0
            assert "Available Workloads" in result.stdout
            
            # Step 5: Dry run scenario
            result = cli_runner.invoke(app, [
                "run", "scenario", str(sample_scenario),
                "--config", str(sample_config),
                "--output", output_dir,
                "--dry-run"
            ])
            assert result.exit_code == 0
            assert "Dry run - scenario validation completed successfully" in result.stdout
    
    def test_cli_error_handling(self, cli_runner):
        """Test CLI error handling with invalid inputs."""
        # Test with non-existent config file
        result = cli_runner.invoke(app, ["validate", "config", "nonexistent.yaml"])
        assert result.exit_code == 1
        assert "not found" in result.stdout
        
        # Test with non-existent scenario file
        result = cli_runner.invoke(app, ["validate", "scenario", "nonexistent.yaml"])
        assert result.exit_code == 1
        assert "not found" in result.stdout
        
        # Test with invalid dataset name
        result = cli_runner.invoke(app, ["dataset", "download", "invalid-dataset"])
        assert result.exit_code == 1
        assert "Unknown dataset" in result.stdout
    
    @patch('search_delete_test.cli.commands.info._get_valkey_info')
    def test_valkey_connection_handling(self, mock_get_valkey_info, cli_runner):
        """Test Valkey connection scenarios."""
        # Test successful connection
        mock_get_valkey_info.return_value = {
            "valkey_version": "7.0.0",
            "used_memory_human": "2MB",
            "role": "master"
        }
        
        result = cli_runner.invoke(app, ["info", "valkey"])
        assert result.exit_code == 0
        assert "Valkey Server Information" in result.stdout
        
        # Test connection failure
        mock_get_valkey_info.return_value = {"error": "Connection refused"}
        result = cli_runner.invoke(app, ["info", "valkey"])
        assert result.exit_code == 1
        assert "Connection failed" in result.stdout
    
    def test_logging_configuration(self, cli_runner, sample_config):
        """Test logging configuration with different verbosity levels."""
        # Test quiet mode
        result = cli_runner.invoke(app, ["--quiet", "validate", "config", str(sample_config)])
        assert result.exit_code == 0
        # In quiet mode, should only see essential output
        
        # Test verbose mode
        result = cli_runner.invoke(app, ["--verbose", "validate", "config", str(sample_config)])
        assert result.exit_code == 0
        # Should work without errors (verbose output goes to stderr)
        
        # Test conflicting flags
        result = cli_runner.invoke(app, ["--verbose", "--quiet", "version"])
        assert result.exit_code == 1
        assert "Cannot use both --verbose and --quiet" in result.stdout


@pytest.mark.integration
class TestCommandChaining:
    """Test chaining multiple CLI commands."""
    
    def test_dataset_workflow(self, cli_runner):
        """Test dataset management workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # List available datasets
            result = cli_runner.invoke(app, ["dataset", "list"])
            assert result.exit_code == 0
            assert "Available Datasets" in result.stdout
            
            # Mock download process (don't actually download)
            with patch('urllib.request.urlretrieve') as mock_download:
                def mock_download_func(url, filename, reporthook=None):
                    Path(filename).touch()  # Create empty file
                    if reporthook:
                        reporthook(1, 1024, 1024)  # Simulate progress
                
                mock_download.side_effect = mock_download_func
                
                # Download dataset
                result = cli_runner.invoke(app, [
                    "dataset", "download", "openai-5m", 
                    "--output", temp_dir
                ])
                assert result.exit_code == 0
                assert "Download completed" in result.stdout
    
    def test_validation_workflow(self, cli_runner, sample_config, sample_scenario):
        """Test validation workflow."""
        # Validate config first
        result = cli_runner.invoke(app, ["validate", "config", str(sample_config)])
        assert result.exit_code == 0
        
        # Then validate scenario
        result = cli_runner.invoke(app, ["validate", "scenario", str(sample_scenario)])
        assert result.exit_code == 0
        
        # Both should pass
        assert "validation successful" in result.stdout


@pytest.mark.integration 
class TestRealExecutionScenarios:
    """Test scenarios that would run in real environments."""
    @patch('search_delete_test.cli.commands.run.AsyncMemoryCollector')
    @patch('search_delete_test.cli.commands.run.ScenarioRunner')
    @patch('search_delete_test.cli.commands.run.ConnectionManager')
    def test_scenario_execution_mocked(self, mock_conn_class, mock_runner_class, mock_collector_class,
                                     cli_runner, sample_scenario, sample_config):
        """Test scenario execution with mocked dependencies."""
        with tempfile.TemporaryDirectory() as output_dir:
            # Mock the connection manager
            mock_conn_manager = Mock()
            
            # Mock async methods of connection manager
            async def mock_initialize():
                pass
            
            async def mock_get_client():
                mock_client = Mock()
                mock_client.ping = Mock()
                return mock_client
            
            mock_pool = Mock()
            mock_pool.get_client = mock_get_client
            mock_conn_manager.initialize = mock_initialize
            mock_conn_manager.get_pool.return_value = mock_pool
            mock_conn_class.return_value = mock_conn_manager
            
            # Mock the async memory collector
            mock_collector = Mock()
            mock_collector_class.return_value = mock_collector
            
            # Mock the scenario runner
            mock_runner = Mock()
            mock_results = {
                "scenario_name": "integration_test_scenario",
                "total_duration": 75.0,
                "steps_completed": 5,
                "metrics": {
                    "vectors_inserted": 1000,
                    "queries_executed": 450,
                    "vectors_deleted": 200
                }
            }
            
            async def mock_run_scenario(scenario):
                return mock_results
            
            mock_runner.run_scenario = mock_run_scenario
            mock_runner.generate_report.return_value = "Test execution completed successfully"
            mock_runner_class.return_value = mock_runner
            
            # Run the scenario
            result = cli_runner.invoke(app, [
                "run", "scenario", str(sample_scenario),
                "--config", str(sample_config),
                "--output", output_dir
            ])
            
            assert result.exit_code == 0
            assert "Starting scenario execution" in result.stdout
            assert "Test execution completed successfully" in result.stdout
    
    @patch('search_delete_test.cli.commands.run.AsyncMemoryCollector')
    @patch('search_delete_test.cli.commands.run.ScenarioRunner')
    @patch('search_delete_test.cli.commands.run.ConnectionManager')
    def test_quick_run_command(self, mock_conn_class, mock_runner_class, mock_collector_class, cli_runner):
        """Test quick run command."""
        with tempfile.TemporaryDirectory() as output_dir:
            # Mock the connection manager
            mock_conn_manager = Mock()
            
            # Mock async methods of connection manager
            async def mock_initialize():
                pass
            
            async def mock_get_client():
                mock_client = Mock()
                mock_client.ping = Mock()
                return mock_client
            
            mock_pool = Mock()
            mock_pool.get_client = mock_get_client
            mock_conn_manager.initialize = mock_initialize
            mock_conn_manager.get_pool.return_value = mock_pool
            mock_conn_class.return_value = mock_conn_manager
            
            # Mock the async memory collector
            mock_collector = Mock()
            mock_collector_class.return_value = mock_collector
            
            # Mock the scenario runner
            mock_runner = Mock()
            mock_results = {"status": "completed"}
            
            async def mock_run_scenario(scenario):
                return mock_results
            
            mock_runner.run_scenario = mock_run_scenario
            mock_runner.generate_report.return_value = "Quick test completed"
            mock_runner_class.return_value = mock_runner
            
            result = cli_runner.invoke(app, [
                "run", "quick",
                "--dataset", "openai-5m",
                "--workload", "mixed",
                "--duration", "60",
                "--output", output_dir
            ])
            
            assert result.exit_code == 0
            assert "Running quick test" in result.stdout


@pytest.mark.integration
class TestCLIPerformance:
    """Test CLI performance and resource usage."""
    
    def test_cli_startup_time(self, cli_runner):
        """Test that CLI starts up quickly."""
        import time
        
        start_time = time.time()
        result = cli_runner.invoke(app, ["--help"])
        end_time = time.time()
        
        assert result.exit_code == 0
        # CLI should start in under 2 seconds
        assert (end_time - start_time) < 2.0
    
    def test_memory_usage(self, cli_runner):
        """Test CLI memory usage is reasonable."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss
        
        # Run several commands
        commands = [
            ["--help"],
            ["version"],
            ["info", "system"],
            ["info", "workloads"],
            ["dataset", "list"]
        ]
        
        for cmd in commands:
            result = cli_runner.invoke(app, cmd)
            assert result.exit_code == 0
        
        final_memory = process.memory_info().rss
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (less than 50MB)
        assert memory_increase < 50 * 1024 * 1024


@pytest.mark.integration 
class TestCLIConfiguration:
    """Test CLI configuration handling."""
    
    def test_environment_variable_override(self, cli_runner, sample_config):
        """Test environment variable configuration override."""
        import os
        
        # Set environment variables
        test_env = {
            "VALKEY_HOST": "test-valkey-host",
            "VALKEY_PORT": "7000", 
            "VST_LOG_LEVEL": "DEBUG"
        }
        
        with patch.dict(os.environ, test_env):
            result = cli_runner.invoke(app, ["validate", "config", str(sample_config)])
            assert result.exit_code == 0
            # Configuration should still be valid with env overrides
    
    def test_config_file_precedence(self, cli_runner):
        """Test configuration file precedence."""
        # Create configs with different values
        config1_data = {"valkey": {"host": "host1", "port": 6379}}
        config2_data = {"valkey": {"host": "host2", "port": 6380}}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f1:
            yaml.dump(config1_data, f1)
            config1_path = Path(f1.name)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f2:
            yaml.dump(config2_data, f2)
            config2_path = Path(f2.name)
        
        try:
            # Validate both configs
            result1 = cli_runner.invoke(app, ["validate", "config", str(config1_path)])
            assert result1.exit_code == 0
            assert "host1" in result1.stdout
            
            result2 = cli_runner.invoke(app, ["validate", "config", str(config2_path)])
            assert result2.exit_code == 0
            assert "host2" in result2.stdout
            
        finally:
            config1_path.unlink(missing_ok=True)
            config2_path.unlink(missing_ok=True)
