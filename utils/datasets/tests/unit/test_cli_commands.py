"""Unit tests for CLI commands."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import yaml
from typer.testing import CliRunner

from cli.commands import info, validate, dataset, run
from cli.main import app


@pytest.fixture
def cli_runner():
    """Provide CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_config_file():
    """Provide a mock configuration file."""
    config_data = {
        "valkey": {
            "host": "localhost",
            "port": 6379,
            "max_connections": 100
        },
        "index": {
            "algorithm": "HNSW",
            "dimensions": 1536,
            "m": 16
        },
        "workload": {
            "n_threads": 4,
            "n_clients": 50,
            "batch_size": 100
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config_data, f)
        yield Path(f.name)
    
    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.fixture
def mock_scenario_file():
    """Provide a mock scenario file."""
    scenario_data = {
        "name": "test_scenario",
        "description": "Test scenario for CLI testing",
        "dataset": "openai-5m",
        "global_config": {
            "n_threads": 2,
            "n_clients": 10
        },
        "steps": [
            {
                "name": "ingest_step",
                "type": "workload",
                "workload": "ingest",
                "parameters": {"target_vectors": 1000}
            },
            {
                "name": "query_step", 
                "type": "workload",
                "workload": "query",
                "duration_seconds": 60,
                "parameters": {"queries_per_second": 10}
            }
        ]
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(scenario_data, f)
        yield Path(f.name)
    
    # Cleanup
    Path(f.name).unlink(missing_ok=True)


@pytest.mark.unit
class TestInfoCommands:
    """Test info commands."""
    
    def test_system_info_command(self, cli_runner):
        """Test system info command."""
        result = cli_runner.invoke(info.app, ["system"])
        
        assert result.exit_code == 0
        assert "System Information:" in result.stdout
        assert "Python Version:" in result.stdout
        assert "Platform:" in result.stdout
        assert "CPU Count:" in result.stdout
        assert "Total Memory:" in result.stdout
    
    @patch('cli.commands.info._get_valkey_info')
    def test_valkey_info_command_success(self, mock_get_valkey_info, cli_runner):
        """Test valkey info command with successful connection."""
        # Mock successful valkey info
        mock_get_valkey_info.return_value = {
            "valkey_version": "7.0.0",
            "used_memory_human": "1.5MB",
            "total_connections_received": "100",
            "role": "master"
        }
        
        result = cli_runner.invoke(info.app, ["valkey"])
        
        assert result.exit_code == 0
        assert "Valkey Server Information" in result.stdout
        assert "valkey_version: 7.0.0" in result.stdout
        
    @patch('cli.commands.info._get_valkey_info')
    def test_valkey_info_command_connection_failure(self, mock_get_valkey_info, cli_runner):
        """Test valkey info command with connection failure."""
        # Mock Valkey connection failure
        mock_get_valkey_info.return_value = {"error": "Connection failed"}
        
        result = cli_runner.invoke(info.app, ["valkey"])
        
        assert result.exit_code == 1
        assert "Connection failed" in result.stdout
    
    def test_workloads_command(self, cli_runner):
        """Test workloads listing command."""
        result = cli_runner.invoke(info.app, ["workloads"])
        
        assert result.exit_code == 0
        assert "Available Workloads:" in result.stdout
        # Should show built-in workloads
        assert "ingest" in result.stdout.lower()
        assert "query" in result.stdout.lower()
        assert "shrink" in result.stdout.lower()


@pytest.mark.unit
class TestValidateCommands:
    """Test validate commands."""
    
    def test_validate_scenario_success(self, cli_runner, mock_scenario_file):
        """Test successful scenario validation."""
        result = cli_runner.invoke(validate.app, ["scenario", str(mock_scenario_file)])
        
        assert result.exit_code == 0
        assert "Scenario validation successful!" in result.stdout
        assert "test_scenario" in result.stdout
        
    def test_validate_scenario_file_not_found(self, cli_runner):
        """Test scenario validation with missing file."""
        result = cli_runner.invoke(validate.app, ["scenario", "nonexistent.yaml"])
        
        assert result.exit_code == 1
        assert "Scenario file not found" in result.stdout
        
    def test_validate_scenario_invalid_yaml(self, cli_runner):
        """Test scenario validation with invalid YAML."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")
            invalid_file = Path(f.name)
        
        try:
            result = cli_runner.invoke(validate.app, ["scenario", str(invalid_file)])
            assert result.exit_code == 1
            assert "Invalid YAML in scenario file" in result.stdout
        finally:
            invalid_file.unlink(missing_ok=True)
    
    def test_validate_config_success(self, cli_runner, mock_config_file):
        """Test successful configuration validation."""
        result = cli_runner.invoke(validate.app, ["config", str(mock_config_file)])
        
        assert result.exit_code == 0
        assert "Configuration validation successful!" in result.stdout
        
    def test_validate_config_file_not_found(self, cli_runner):
        """Test config validation with missing file."""
        result = cli_runner.invoke(validate.app, ["config", "nonexistent.yaml"])
        
        assert result.exit_code == 1
        assert "Configuration file not found" in result.stdout


@pytest.mark.unit
class TestDatasetCommands:
    """Test dataset commands."""
    
    def test_dataset_list_command(self, cli_runner):
        """Test dataset listing command."""
        result = cli_runner.invoke(dataset.app, ["list"])
        
        assert result.exit_code == 0
        assert "External Datasets" in result.stdout
        assert "openai-5m" in result.stdout
        assert "sift-128d" in result.stdout
    
    @patch('urllib.request.urlretrieve')
    def test_dataset_download_success(self, mock_urlretrieve, cli_runner):
        """Test successful dataset download."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the download
            output_path = Path(temp_dir) / "test_download.zip"
            
            def mock_download(url, filename, reporthook=None):
                # Simulate download with progress
                if reporthook:
                    reporthook(0, 1024, 10240)  # 0% 
                    reporthook(5, 1024, 10240)  # 50%
                    reporthook(10, 1024, 10240) # 100%
                # Create dummy file
                Path(filename).touch()
                
            mock_urlretrieve.side_effect = mock_download
            
            result = cli_runner.invoke(dataset.app, [
                "download", "openai-5m", "--output", temp_dir
            ])
            
            assert result.exit_code == 0
            assert "Download completed" in result.stdout
    
    def test_dataset_download_unknown_dataset(self, cli_runner):
        """Test download with unknown dataset name."""
        result = cli_runner.invoke(dataset.app, ["download", "unknown-dataset"])
        
        assert result.exit_code == 1
        assert "Unknown dataset" in result.stdout
        assert "Available datasets:" in result.stdout
    
    @patch('h5py.File')
    def test_dataset_info_hdf5(self, mock_h5py_file, cli_runner):
        """Test dataset info command for HDF5 files."""
        # Create temporary HDF5-like file
        with tempfile.NamedTemporaryFile(suffix='.h5', delete=False) as f:
            temp_file = Path(f.name)
        
        try:
            # Mock HDF5 structure
            mock_file = Mock()
            mock_dataset = Mock()
            mock_dataset.shape = (10000, 1536)
            mock_dataset.dtype = 'float32'
            mock_dataset.size = 15360000
            mock_dataset.__getitem__ = Mock(return_value=[[0.1, 0.2, 0.3]])
            
            mock_file.keys.return_value = ['train', 'test']
            mock_file.__getitem__ = Mock(return_value=mock_dataset)
            mock_file.__enter__ = Mock(return_value=mock_file)
            mock_file.__exit__ = Mock(return_value=None)
            
            mock_h5py_file.return_value = mock_file
            
            result = cli_runner.invoke(dataset.app, ["info", str(temp_file)])
            
            assert result.exit_code == 0
            assert "Dataset Information:" in result.stdout
            assert "Format: HDF5" in result.stdout
            
        finally:
            temp_file.unlink(missing_ok=True)
    
    def test_dataset_info_file_not_found(self, cli_runner):
        """Test dataset info with missing file."""
        result = cli_runner.invoke(dataset.app, ["info", "nonexistent.h5"])
        
        assert result.exit_code == 1
        assert "Dataset file not found" in result.stdout


@pytest.mark.unit
class TestRunCommands:
    """Test run commands."""
    
    @patch('scenarios.ScenarioLoader')
    @patch('scenarios.ScenarioRunner')
    def test_validate_command(self, mock_runner_class, mock_loader_class, cli_runner, mock_scenario_file):
        """Test scenario validation via run command."""
        # Mock scenario loader
        mock_scenario = Mock()
        mock_scenario.name = "test_scenario"
        mock_scenario.description = "Test description"
        mock_scenario.steps = [{"name": "step1"}]
        mock_scenario.get_total_duration.return_value = 120.0
        
        mock_loader = Mock()
        mock_loader.load_scenario.return_value = mock_scenario
        mock_loader_class.return_value = mock_loader
        
        result = cli_runner.invoke(run.app, ["validate", str(mock_scenario_file)])
        
        assert result.exit_code == 0
        assert "test_scenario" in result.stdout
        assert "is valid" in result.stdout
    
    def test_list_scenarios_command(self, cli_runner):
        """Test list scenarios command."""
        with patch('scenarios.ScenarioLoader') as mock_loader_class:
            mock_loader = Mock()
            mock_loader.list_builtin_scenarios.return_value = [
                "continuous_growth", "grow_shrink_grow"
            ]
            mock_loader_class.return_value = mock_loader
            
            result = cli_runner.invoke(run.app, ["list-scenarios"])
            
            assert result.exit_code == 0
            assert "Available built-in scenarios:" in result.stdout
            assert "continuous_growth" in result.stdout
            assert "grow_shrink_grow" in result.stdout


@pytest.mark.unit 
class TestMainCLI:
    """Test main CLI functionality."""
    
    def test_version_command(self, cli_runner):
        """Test version command."""
        result = cli_runner.invoke(app, ["version"])
        
        assert result.exit_code == 0
        assert "search_delete_test version" in result.stdout
    
    def test_help_command(self, cli_runner):
        """Test help command."""
        result = cli_runner.invoke(app, ["--help"])
        
        assert result.exit_code == 0
        assert "Search Module Delete Test Testing Tool" in result.stdout
        assert "run" in result.stdout
        assert "dataset" in result.stdout
        assert "validate" in result.stdout
        assert "info" in result.stdout
    
    def test_verbose_and_quiet_conflict(self, cli_runner):
        """Test that verbose and quiet flags conflict."""
        result = cli_runner.invoke(app, ["--verbose", "--quiet", "version"])
        
        assert result.exit_code == 1
        assert "Cannot use both --verbose and --quiet" in result.stdout


@pytest.mark.unit
class TestCLIErrorHandling:
    """Test CLI error handling."""
    
    def test_nonexistent_command(self, cli_runner):
        """Test calling non-existent command."""
        result = cli_runner.invoke(app, ["nonexistent"])
        
        assert result.exit_code != 0
    
    def test_missing_required_argument(self, cli_runner):
        """Test missing required argument."""
        result = cli_runner.invoke(validate.app, ["scenario"])
        
        assert result.exit_code != 0
        assert "Missing argument" in result.stdout or "Usage:" in result.stdout
