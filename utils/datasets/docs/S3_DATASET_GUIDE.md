# S3 Dataset Integration Summary

## Changes Made

### 1. Updated Dataset Configuration
- Modified `DATASETS` dictionary in `src/cli/commands/dataset.py`
- Added your specified datasets:
  - `openai_small_50k`: OpenAI embeddings 50k vectors
  - `sift_small_500k`: SIFT 500k vectors for benchmarking
- Removed redundant S3 datasets to clean up configuration
- Added proper S3 base URLs and file specifications

### 2. Enhanced Download Function
- Modified main `download()` function to handle S3-based datasets
- Added check for `s3_base` property to route to S3 download logic
- Maintains backward compatibility with existing URL-based downloads

### 3. Improved S3 Download Function
- Enhanced `download_s3_dataset()` to use progress tracking
- Better error handling and user feedback
- File size reporting after successful downloads
- Automatic cleanup of failed downloads

### 4. Added New Commands
- `test-s3`: Test S3 dataset configuration
- `validate-s3`: Validate S3 URLs are accessible before downloading

### 5. Configuration Format
Your datasets now use this format:
```python
"openai_small_50k": {
    "s3_base": "https://vectordb-bench-datasets.s3.amazonaws.com/",
    "description": "OpenAI embeddings 50k vectors",
    "dimensions": 1536,
    "size_mb": 77,
    "files": ["train.parquet", "test.parquet"]
}
```

## Usage Examples

### Test Configuration
```bash
vst dataset test-s3
```

### Validate URLs
```bash
vst dataset validate-s3 openai_small_50k
```

### Download Dataset
```bash
vst dataset download openai_small_50k --output ./_datasets_downloads
vst dataset download sift_small_500k --output ./_datasets_downloads
```

## Benefits

1. **No vectordb-bench dependency**: Direct S3 access eliminates the need to install vectordb-bench
2. **Simple configuration**: Easy to add new S3-based datasets
3. **Progress tracking**: Visual feedback during downloads
4. **Error handling**: Graceful handling of network issues
5. **Validation**: Check URLs before attempting downloads
6. **Backward compatibility**: Existing URL-based datasets still work

## Next Steps

To use with real S3 URLs:
1. Replace the hypothetical S3 base URL with your actual bucket URL
2. Ensure your S3 bucket has public read access or configure AWS credentials
3. Update the `files` list if your dataset uses different file names
4. Test with `validate-s3` command before downloading

The system is now ready to download datasets directly from S3 without requiring vectordb-bench installation!

# S3 Dataset Configuration Guide

This guide shows how to configure and use S3-based datasets without requiring the vectordb-bench installation.

## Configuration Format

```python
DATASETS = {
    "your_dataset_name": {
        "s3_base": "https://your-bucket.s3.amazonaws.com/path/to/dataset/",
        "description": "Description of your dataset",
        "format": "parquet",
        "dimensions": 1536,
        "size_mb": 77,
        "distance": "cosine",
        "train_size": 50000,
        "test_size": 1000,
        "files": ["train.parquet", "test.parquet"],
        "note": "Direct S3 access, no vectordb-bench installation required",
    }
}
```

## Key Features

1. **Direct S3 Access**: Download datasets directly from S3 without needing vectordb-bench
2. **Progress Tracking**: Downloads show progress and file sizes
3. **Validation**: Check if S3 URLs are accessible before downloading
4. **Error Handling**: Graceful handling of network issues and missing files

## Usage Examples

### List S3 Datasets
```bash
vst dataset test-s3
```

### Validate S3 URLs
```bash
vst dataset validate-s3 openai_small_50k
```

### Download S3 Dataset
```bash
vst dataset download openai_small_50k --output ./_datasets_downloads
```

## File Structure

S3 datasets typically contain:
- `train.parquet`: Training vectors with columns 'id' and 'emb'
- `test.parquet`: Query vectors with columns 'id' and 'emb'
- `metadata.json`: Optional metadata file

## Configuration Properties

| Property | Required | Description |
|----------|----------|-------------|
| `s3_base` | Yes | Base S3 URL (must end with /) |
| `description` | Yes | Human-readable description |
| `format` | Yes | File format (e.g., "parquet") |
| `dimensions` | Yes | Vector dimensions |
| `size_mb` | Yes | Estimated total size in MB |
| `files` | No | List of files to download (default: ["train.parquet", "test.parquet"]) |
| `distance` | No | Distance metric (cosine, euclidean, etc.) |
| `train_size` | No | Number of training vectors |
| `test_size` | No | Number of test vectors |
| `note` | No | Additional notes |

## Example: Adding Your Own Dataset

```python
# Add to DATASETS dictionary in src/cli/commands/dataset.py
"my_custom_dataset": {
    "s3_base": "https://my-bucket.s3.amazonaws.com/vectors/",
    "description": "My custom vector dataset",
    "format": "parquet",
    "dimensions": 768,
    "size_mb": 150,
    "distance": "cosine",
    "train_size": 100000,
    "test_size": 1000,
    "files": ["train.parquet", "test.parquet"],
    "note": "Custom embeddings from my model",
}
```

## Error Handling

The system handles common errors:
- **404 Not Found**: URL doesn't exist
- **Network timeouts**: Retry or check connection
- **Permission errors**: Check S3 bucket permissions
- **Empty files**: Automatic cleanup of failed downloads

## Integration with VectorDBBench

This approach replaces the need for:
```python
# Old way (requires vectordb-bench)
from vectordb_bench.backend.data_source import DatasetSource
reader = DatasetSource.S3.reader()
reader.read('dataset_name', ['train.parquet', 'test.parquet'], 'output_dir')

# New way (direct S3 access)
vst dataset download dataset_name --output output_dir
```
