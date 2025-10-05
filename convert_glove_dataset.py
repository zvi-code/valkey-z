#!/usr/bin/env python3
"""Convert GloVe HDF5 dataset to our binary dataset format."""

import h5py
import struct
import numpy as np

def convert_glove_to_binary(hdf5_path, output_path, max_vectors=None, max_queries=None):
    """Convert GloVe HDF5 to binary dataset format."""

    with h5py.File(hdf5_path, 'r') as f:
        train_vectors = f['train'][:]
        test_queries = f['test'][:]
        ground_truth = f['neighbors'][:]

        # Limit dataset size for testing
        if max_vectors:
            train_vectors = train_vectors[:max_vectors]
        if max_queries:
            test_queries = test_queries[:max_queries]
            ground_truth = ground_truth[:max_queries]

        # Filter ground truth to only include valid vector IDs
        num_vectors = len(train_vectors)
        valid_mask = ground_truth < num_vectors

        # For each query, keep only neighbors that exist in our vector set
        filtered_neighbors = []
        for i in range(len(ground_truth)):
            valid_neighbors = ground_truth[i][valid_mask[i]]
            # Pad or truncate to exactly 100 neighbors
            if len(valid_neighbors) >= 100:
                filtered_neighbors.append(valid_neighbors[:100])
            else:
                # Pad with -1 (invalid) if not enough valid neighbors
                padded = np.full(100, -1, dtype=np.int64)
                padded[:len(valid_neighbors)] = valid_neighbors
                filtered_neighbors.append(padded)

        ground_truth = np.array(filtered_neighbors, dtype=np.int64)

        print(f"Dataset statistics:")
        print(f"  Vectors: {len(train_vectors):,} x {train_vectors.shape[1]}D")
        print(f"  Queries: {len(test_queries):,} x {test_queries.shape[1]}D")
        print(f"  Ground truth: {ground_truth.shape}")
        print(f"  Valid neighbors per query: {np.sum(ground_truth >= 0, axis=1).mean():.1f}")

        # Our dataset format constants
        DATASET_MAGIC = 0xDECDB001
        header_size = 4096

        # Calculate offsets
        vectors_offset = header_size
        vectors_size = len(train_vectors) * train_vectors.shape[1] * 4  # float32
        queries_offset = vectors_offset + vectors_size
        queries_size = len(test_queries) * test_queries.shape[1] * 4    # float32
        gt_offset = queries_offset + queries_size

        # Create binary file
        with open(output_path, 'wb') as f:
            # Write header (4KB)
            header = struct.pack(
                '<I I 256s B B 2x I Q Q I 4x Q Q Q 3808x',
                DATASET_MAGIC,                    # magic
                1,                               # version
                b'glove-25-angular',             # dataset_name
                0,                               # distance_metric (L2)
                0,                               # dtype (float32)
                train_vectors.shape[1],          # dim
                len(train_vectors),              # num_vectors
                len(test_queries),               # num_queries
                100,                             # num_neighbors
                vectors_offset,                  # vectors_offset
                queries_offset,                  # queries_offset
                gt_offset                        # ground_truth_offset
            )
            f.write(header)

            # Write vectors
            f.seek(vectors_offset)
            train_vectors.astype(np.float32).tofile(f)

            # Write queries
            f.seek(queries_offset)
            test_queries.astype(np.float32).tofile(f)

            # Write ground truth
            f.seek(gt_offset)
            ground_truth.astype(np.int64).tofile(f)

        print(f"Created binary dataset: {output_path}")
        print(f"  File size: {gt_offset + len(ground_truth) * 100 * 8:,} bytes")

if __name__ == "__main__":
    # Create a manageable subset for testing
    convert_glove_to_binary(
        'glove-25.hdf5',
        'glove_subset.bin',
        max_vectors=10000,  # 10K vectors for fast testing
        max_queries=100     # 100 queries
    )