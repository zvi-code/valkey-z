#!/usr/bin/env python3
"""Create a dataset where ground truth matches inserted vectors."""

import h5py
import struct
import numpy as np

def create_matching_dataset():
    """Create a small dataset where ground truth IDs match actual vector IDs."""

    with h5py.File('glove-25.hdf5', 'r') as f:
        # Take first 1000 vectors for our database
        vectors = f['train'][:1000]  # IDs 0-999

        # Take first 10 queries
        queries = f['test'][:10]

        # Create ground truth that references vectors 0-999
        # For each query, find the 20 closest vectors from our 1000 vectors
        print("Computing ground truth for 1000 vectors...")

        ground_truth = []
        for i, query in enumerate(queries):
            # Compute distances to all 1000 vectors
            distances = np.linalg.norm(vectors - query, axis=1)
            # Get indices of 20 closest vectors (these will be 0-999)
            closest_indices = np.argsort(distances)[:20]

            # Pad to 100 neighbors with -1 (invalid)
            gt_row = np.full(100, -1, dtype=np.int64)
            gt_row[:20] = closest_indices
            ground_truth.append(gt_row)

            print(f"Query {i}: closest vectors = {closest_indices[:5]}...")

        ground_truth = np.array(ground_truth, dtype=np.int64)

        print(f"\nDataset created:")
        print(f"  Vectors: {len(vectors):,} x {vectors.shape[1]}D (IDs 0-{len(vectors)-1})")
        print(f"  Queries: {len(queries):,} x {queries.shape[1]}D")
        print(f"  Ground truth: {ground_truth.shape}")
        print(f"  GT range: {ground_truth[ground_truth >= 0].min()} to {ground_truth[ground_truth >= 0].max()}")

        # Our dataset format constants
        DATASET_MAGIC = 0xDECDB001
        header_size = 4096

        # Calculate offsets
        vectors_offset = header_size
        vectors_size = len(vectors) * vectors.shape[1] * 4  # float32
        queries_offset = vectors_offset + vectors_size
        queries_size = len(queries) * queries.shape[1] * 4    # float32
        gt_offset = queries_offset + queries_size

        # Create binary file
        with open('matching_dataset.bin', 'wb') as f:
            # Write header (4KB)
            header = struct.pack(
                '<I I 256s B B 2x I Q Q I 4x Q Q Q 3808x',
                DATASET_MAGIC,                    # magic
                1,                               # version
                b'glove-matching',               # dataset_name
                0,                               # distance_metric (L2)
                0,                               # dtype (float32)
                vectors.shape[1],                # dim
                len(vectors),                    # num_vectors
                len(queries),                    # num_queries
                100,                             # num_neighbors
                vectors_offset,                  # vectors_offset
                queries_offset,                  # queries_offset
                gt_offset                        # ground_truth_offset
            )
            f.write(header)

            # Write vectors
            f.seek(vectors_offset)
            vectors.astype(np.float32).tofile(f)

            # Write queries
            f.seek(queries_offset)
            queries.astype(np.float32).tofile(f)

            # Write ground truth
            f.seek(gt_offset)
            ground_truth.astype(np.int64).tofile(f)

        print(f"\nCreated: matching_dataset.bin")
        print(f"  File size: {gt_offset + len(ground_truth) * 100 * 8:,} bytes")

        # Show sample ground truth
        print(f"\nSample ground truth for query 0: {ground_truth[0][ground_truth[0] >= 0]}")

if __name__ == "__main__":
    create_matching_dataset()