"""
VKV (Valkey Key-Value) Binary Format Implementation

Streaming-optimized binary format for vector datasets with the following structure:

Header (64 bytes):
- Magic bytes: "VKV\x01" (4 bytes)
- Vector count: uint64 (8 bytes) 
- Dimension: uint32 (4 bytes)
- Data type: uint8 (1 byte) [0=float32, 1=float64, 2=int8, 3=uint8]
- Compression: uint8 (1 byte) [0=none, 1=zstd, 2=lz4]
- Block size: uint32 (4 bytes) - vectors per block for streaming
- Reserved: 42 bytes

Data blocks (repeated):
- Block header (16 bytes):
  - Block index: uint32
  - Vector count in block: uint32  
  - Compressed size: uint32
  - Uncompressed size: uint32
- Key-Value pairs (repeated):
  - Key length: uint16
  - Key: variable length UTF-8
  - Vector data: fixed size (dimension * data_type_size)
"""

import struct
from typing import Iterator, Tuple, Optional, Union, AsyncIterator, List
from pathlib import Path
from enum import IntEnum
import numpy as np
from io import BytesIO
import logging

# Optional compression libraries
try:
    import zstandard as zstd
    HAS_ZSTD = True
except ImportError:
    HAS_ZSTD = False

try:
    import lz4.block
    HAS_LZ4 = True
except ImportError:
    HAS_LZ4 = False

logger = logging.getLogger(__name__)


class DataType(IntEnum):
    """Supported vector data types."""
    FLOAT32 = 0
    FLOAT64 = 1
    INT8 = 2
    UINT8 = 3


class CompressionType(IntEnum):
    """Supported compression algorithms."""
    NONE = 0
    ZSTD = 1
    LZ4 = 2


class VKVFormat:
    """VKV binary format reader and writer for streaming vector data."""
    
    MAGIC_BYTES = b"VKV\x01"
    HEADER_SIZE = 64
    BLOCK_HEADER_SIZE = 16
    
    # Data type size mappings
    DTYPE_SIZES = {
        DataType.FLOAT32: 4,
        DataType.FLOAT64: 8,
        DataType.INT8: 1,
        DataType.UINT8: 1,
    }
    
    # NumPy dtype mappings
    NUMPY_DTYPES = {
        DataType.FLOAT32: np.float32,
        DataType.FLOAT64: np.float64,
        DataType.INT8: np.int8,
        DataType.UINT8: np.uint8,
    }
    
    def __init__(self):
        """Initialize VKV format handler."""
        self._zstd_compressor = None
        self._zstd_decompressor = None
        
        if HAS_ZSTD:
            self._zstd_compressor = zstd.ZstdCompressor(level=3)
            self._zstd_decompressor = zstd.ZstdDecompressor()
    
    def create_header(self, vector_count: int, dimension: int, 
                     data_type: DataType, compression: CompressionType,
                     block_size: int) -> bytes:
        """Create VKV format header."""
        if dimension <= 0:
            raise ValueError("Dimension must be positive")
        if vector_count < 0:
            raise ValueError("Vector count cannot be negative")  
        if block_size <= 0:
            raise ValueError("Block size must be positive")
        if data_type not in self.DTYPE_SIZES:
            raise ValueError(f"Unsupported data type: {data_type}")
        if compression not in CompressionType:
            raise ValueError(f"Unsupported compression type: {compression}")
            
        # Pack header: magic(4) + count(8) + dim(4) + dtype(1) + comp(1) + block_size(4) + reserved(42)
        header = struct.pack(
            '<4sQIBBI42s',  # Little-endian format
            self.MAGIC_BYTES,
            vector_count,
            dimension,
            data_type.value,
            compression.value,
            block_size,
            b'\x00' * 42  # Reserved bytes
        )
        
        assert len(header) == self.HEADER_SIZE
        return header
    
    def parse_header(self, header_bytes: bytes) -> dict:
        """Parse VKV format header and return metadata."""
        if len(header_bytes) != self.HEADER_SIZE:
            raise ValueError(f"Invalid header size: {len(header_bytes)}, expected {self.HEADER_SIZE}")
            
        magic, vector_count, dimension, dtype_val, comp_val, block_size, _ = struct.unpack(
            '<4sQIBBI42s', header_bytes
        )
        
        if magic != self.MAGIC_BYTES:
            raise ValueError(f"Invalid magic bytes: {magic}, expected {self.MAGIC_BYTES}")
            
        try:
            data_type = DataType(dtype_val)
        except ValueError:
            raise ValueError(f"Invalid data type value: {dtype_val}")
            
        try:
            compression = CompressionType(comp_val)
        except ValueError:
            raise ValueError(f"Invalid compression type value: {comp_val}")
            
        return {
            'vector_count': vector_count,
            'dimension': dimension,
            'data_type': data_type,
            'compression': compression,
            'block_size': block_size,
            'dtype_size': self.DTYPE_SIZES[data_type],
            'numpy_dtype': self.NUMPY_DTYPES[data_type],
        }
    
    def create_block_header(self, block_index: int, vector_count: int,
                           compressed_size: int, uncompressed_size: int) -> bytes:
        """Create block header for data block."""
        if block_index < 0:
            raise ValueError("Block index cannot be negative")
        if vector_count < 0:
            raise ValueError("Vector count cannot be negative")  
        if compressed_size < 0:
            raise ValueError("Compressed size cannot be negative")
        if uncompressed_size < 0:
            raise ValueError("Uncompressed size cannot be negative")
            
        header = struct.pack('<IIII', block_index, vector_count, compressed_size, uncompressed_size)
        assert len(header) == self.BLOCK_HEADER_SIZE
        return header
    
    def parse_block_header(self, block_bytes: bytes) -> dict:
        """Parse block header and return block metadata."""
        if len(block_bytes) != self.BLOCK_HEADER_SIZE:
            raise ValueError(f"Invalid block header size: {len(block_bytes)}, expected {self.BLOCK_HEADER_SIZE}")
            
        block_index, vector_count, compressed_size, uncompressed_size = struct.unpack('<IIII', block_bytes)
        
        return {
            'block_index': block_index,
            'vector_count': vector_count,
            'compressed_size': compressed_size,
            'uncompressed_size': uncompressed_size,
        }
    
    def compress_block(self, data: bytes, compression_type: CompressionType) -> bytes:
        """Compress block data using specified algorithm."""
        if compression_type == CompressionType.NONE:
            return data
        elif compression_type == CompressionType.ZSTD:
            if not HAS_ZSTD:
                logger.warning("ZSTD compression requested but not available, falling back to no compression")
                return data
            return self._zstd_compressor.compress(data)
        elif compression_type == CompressionType.LZ4:
            if not HAS_LZ4:
                logger.warning("LZ4 compression requested but not available, falling back to no compression")
                return data
            return lz4.block.compress(data)
        else:
            raise ValueError(f"Unsupported compression type: {compression_type}")
    
    def decompress_block(self, data: bytes, compression_type: CompressionType,
                        uncompressed_size: int) -> bytes:
        """Decompress block data."""
        if compression_type == CompressionType.NONE:
            if len(data) != uncompressed_size:
                raise ValueError(f"Uncompressed data size mismatch: {len(data)} != {uncompressed_size}")
            return data
        elif compression_type == CompressionType.ZSTD:
            if not HAS_ZSTD:
                raise RuntimeError("ZSTD decompression required but not available")
            decompressed = self._zstd_decompressor.decompress(data)
            if len(decompressed) != uncompressed_size:
                raise ValueError(f"Decompressed size mismatch: {len(decompressed)} != {uncompressed_size}")
            return decompressed
        elif compression_type == CompressionType.LZ4:
            if not HAS_LZ4:
                raise RuntimeError("LZ4 decompression required but not available")
            decompressed = lz4.block.decompress(data, uncompressed_size=uncompressed_size)
            return decompressed
        else:
            raise ValueError(f"Unsupported compression type: {compression_type}")
    
    def encode_key_value_pair(self, key: str, vector: np.ndarray) -> bytes:
        """Encode single key-value pair for storage."""
        if not isinstance(key, str):
            raise TypeError("Key must be a string")
        if not isinstance(vector, np.ndarray):
            raise TypeError("Vector must be a numpy array")
        if vector.ndim != 1:
            raise ValueError("Vector must be 1-dimensional")
            
        key_bytes = key.encode('utf-8')
        key_length = len(key_bytes)
        
        if key_length > 65535:  # uint16 max
            raise ValueError(f"Key too long: {key_length} bytes, max 65535")
            
        # Pack: key_length(2) + key + vector_data
        return struct.pack('<H', key_length) + key_bytes + vector.tobytes()
    
    def decode_key_value_pair(self, data: bytes, offset: int, 
                             dimension: int, dtype: np.dtype) -> Tuple[str, np.ndarray, int]:
        """Decode single key-value pair from data."""
        if offset + 2 > len(data):
            raise ValueError("Not enough data to read key length")
            
        key_length = struct.unpack('<H', data[offset:offset+2])[0]
        offset += 2
        
        if offset + key_length > len(data):
            raise ValueError("Not enough data to read key")
            
        key = data[offset:offset+key_length].decode('utf-8')
        offset += key_length
        
        vector_size = dimension * dtype().itemsize
        if offset + vector_size > len(data):
            raise ValueError("Not enough data to read vector")
            
        vector_bytes = data[offset:offset+vector_size]
        vector = np.frombuffer(vector_bytes, dtype=dtype)
        offset += vector_size
        
        if len(vector) != dimension:
            raise ValueError(f"Vector dimension mismatch: {len(vector)} != {dimension}")
            
        return key, vector, offset


class VKVWriter:
    """Streaming writer for VKV format files."""
    
    def __init__(self, file_path: Path, dimension: int, 
                 data_type: DataType = DataType.FLOAT32,
                 compression: CompressionType = CompressionType.ZSTD,
                 block_size: int = 1000):
        """Initialize VKV writer."""
        self.file_path = Path(file_path)
        self.dimension = dimension
        self.data_type = data_type
        self.compression = compression
        self.block_size = block_size
        
        self.format = VKVFormat()
        self.file = None
        self.vector_count = 0
        self.current_block = []
        self.block_index = 0
        
        # Validate parameters
        if dimension <= 0:
            raise ValueError("Dimension must be positive")
        if block_size <= 0:
            raise ValueError("Block size must be positive")
    
    def __enter__(self):
        """Context manager entry."""
        self.file = self.file_path.open('wb')
        # Reserve space for header (will be written in finalize)
        self.file.write(b'\x00' * VKVFormat.HEADER_SIZE)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if exc_type is None:
            self.finalize()
        if self.file and not self.file.closed:
            self.file.close()
    
    def write_vectors(self, vectors: Iterator[Tuple[str, np.ndarray]]):
        """Write vectors in streaming fashion."""
        for key, vector in vectors:
            self._validate_vector(vector)
            self.current_block.append((key, vector))
            
            if len(self.current_block) >= self.block_size:
                self.write_block(self.current_block)
                self.current_block = []
                self.block_index += 1
        
        # Write remaining vectors in final block
        if self.current_block:
            self.write_block(self.current_block)
            self.current_block = []
    
    def write_block(self, vectors: List[Tuple[str, np.ndarray]]):
        """Write a single block of vectors."""
        if not vectors:
            return
            
        # Serialize all key-value pairs in block
        block_data = BytesIO()
        for key, vector in vectors:
            kv_bytes = self.format.encode_key_value_pair(key, vector)
            block_data.write(kv_bytes)
        
        block_bytes = block_data.getvalue()
        uncompressed_size = len(block_bytes)
        
        # Compress if needed
        compressed_bytes = self.format.compress_block(block_bytes, self.compression)
        compressed_size = len(compressed_bytes)
        
        # Write block header
        block_header = self.format.create_block_header(
            self.block_index, len(vectors), compressed_size, uncompressed_size
        )
        self.file.write(block_header)
        
        # Write compressed block data
        self.file.write(compressed_bytes)
        
        self.vector_count += len(vectors)
        
        logger.debug(f"Wrote block {self.block_index} with {len(vectors)} vectors "
                    f"({uncompressed_size} -> {compressed_size} bytes)")
    
    def finalize(self):
        """Finalize file by updating header with final counts."""
        if self.file and not self.file.closed:
            # Create final header
            header = self.format.create_header(
                self.vector_count, self.dimension, self.data_type, 
                self.compression, self.block_size
            )
            
            # Write header at beginning of file
            self.file.seek(0)
            self.file.write(header)
            self.file.flush()
            
            logger.info(f"Finalized VKV file with {self.vector_count} vectors")
    
    def _validate_vector(self, vector: np.ndarray):
        """Validate vector meets format requirements."""
        if not isinstance(vector, np.ndarray):
            raise TypeError("Vector must be a numpy array")
        if vector.ndim != 1:
            raise ValueError("Vector must be 1-dimensional")
        if len(vector) != self.dimension:
            raise ValueError(f"Vector dimension mismatch: {len(vector)} != {self.dimension}")
        
        # Convert to target data type if needed
        target_dtype = VKVFormat.NUMPY_DTYPES[self.data_type]
        if vector.dtype != target_dtype:
            logger.warning(f"Converting vector from {vector.dtype} to {target_dtype}")
            vector = vector.astype(target_dtype)


class VKVReader:
    """Streaming reader for VKV format files."""
    
    def __init__(self, file_path: Path):
        """Initialize VKV reader."""
        self.file_path = Path(file_path)
        self.file = None
        self.metadata = {}
        self.format = VKVFormat()
        self.block_positions = []  # Cache of block positions for random access
        
    def __enter__(self):
        """Context manager entry."""
        self.file = self.file_path.open('rb')
        self._parse_header()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        if self.file and not self.file.closed:
            self.file.close()
    
    def get_metadata(self) -> dict:
        """Get dataset metadata from header."""
        if self.metadata is None:
            raise RuntimeError("Reader not initialized - use as context manager")
        return self.metadata.copy()
    
    def stream_vectors(self, start_offset: int = 0, 
                      max_vectors: Optional[int] = None,
                      batch_size: int = 1000) -> Iterator[List[Tuple[str, np.ndarray]]]:
        """Stream vectors in batches from file."""
        vectors_read = 0
        vectors_skipped = 0
        
        # Position file after header
        self.file.seek(VKVFormat.HEADER_SIZE)
        
        while vectors_read < (max_vectors or self.metadata['vector_count']):
            try:
                # Read block header
                block_header_bytes = self.file.read(VKVFormat.BLOCK_HEADER_SIZE)
                if len(block_header_bytes) != VKVFormat.BLOCK_HEADER_SIZE:
                    break  # End of file
                    
                block_info = self.format.parse_block_header(block_header_bytes)
                block_vector_count = block_info['vector_count']
                
                # Skip blocks until we reach start_offset
                if vectors_skipped + block_vector_count <= start_offset:
                    # Skip entire block
                    self.file.seek(block_info['compressed_size'], 1)  # Seek relative
                    vectors_skipped += block_vector_count
                    continue
                
                # Read and decompress block data
                compressed_data = self.file.read(block_info['compressed_size'])
                if len(compressed_data) != block_info['compressed_size']:
                    raise ValueError("Incomplete block data read")
                    
                block_data = self.format.decompress_block(
                    compressed_data, 
                    self.metadata['compression'],
                    block_info['uncompressed_size']
                )
                
                # Decode vectors from block
                vectors = self._decode_block_vectors(block_data, block_vector_count)
                
                # Apply start_offset and max_vectors filtering
                if vectors_skipped < start_offset:
                    skip_count = start_offset - vectors_skipped
                    vectors = vectors[skip_count:]
                    vectors_skipped += skip_count
                
                if max_vectors and vectors_read + len(vectors) > max_vectors:
                    remaining = max_vectors - vectors_read
                    vectors = vectors[:remaining]
                
                if vectors:
                    vectors_read += len(vectors)
                    yield vectors
                    
            except Exception as e:
                logger.error(f"Error reading block: {e}")
                break
                
        logger.debug(f"Streamed {vectors_read} vectors")
    
    def read_block(self, block_index: int) -> List[Tuple[str, np.ndarray]]:
        """Read a specific block by index."""
        if not self.block_positions:
            self._index_blocks()
            
        if block_index >= len(self.block_positions):
            raise IndexError(f"Block index {block_index} out of range")
            
        position = self.block_positions[block_index]
        self.file.seek(position)
        
        # Read block header
        block_header_bytes = self.file.read(VKVFormat.BLOCK_HEADER_SIZE)
        block_info = self.format.parse_block_header(block_header_bytes)
        
        # Read and decompress block data
        compressed_data = self.file.read(block_info['compressed_size'])
        block_data = self.format.decompress_block(
            compressed_data, 
            self.metadata['compression'],
            block_info['uncompressed_size']
        )
        
        return self._decode_block_vectors(block_data, block_info['vector_count'])
    
    def get_vector_by_key(self, key: str) -> Optional[np.ndarray]:
        """Find and return vector by key (slower, requires scanning)."""
        for batch in self.stream_vectors():
            for k, vector in batch:
                if k == key:
                    return vector
        return None
    
    def _parse_header(self):
        """Parse file header and store metadata."""
        self.file.seek(0)
        header_bytes = self.file.read(VKVFormat.HEADER_SIZE)
        if len(header_bytes) != VKVFormat.HEADER_SIZE:
            raise ValueError("Incomplete header read")
        self.metadata = self.format.parse_header(header_bytes)
        
    def _decode_block_vectors(self, block_data: bytes, vector_count: int) -> List[Tuple[str, np.ndarray]]:
        """Decode all vectors from a block."""
        vectors = []
        offset = 0
        
        for _ in range(vector_count):
            key, vector, offset = self.format.decode_key_value_pair(
                block_data, offset, 
                self.metadata['dimension'],
                self.metadata['numpy_dtype']
            )
            vectors.append((key, vector))
            
        return vectors
    
    def _index_blocks(self):
        """Build index of block positions for random access."""
        self.block_positions = []
        self.file.seek(VKVFormat.HEADER_SIZE)
        
        while True:
            pos = self.file.tell()
            block_header_bytes = self.file.read(VKVFormat.BLOCK_HEADER_SIZE)
            if len(block_header_bytes) != VKVFormat.BLOCK_HEADER_SIZE:
                break
                
            self.block_positions.append(pos)
            block_info = self.format.parse_block_header(block_header_bytes)
            
            # Skip block data
            self.file.seek(block_info['compressed_size'], 1)
            
        logger.debug(f"Indexed {len(self.block_positions)} blocks")
