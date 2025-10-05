#!/usr/bin/env python3
"""Create a large-scale dataset for testing at 1M+ scale."""

import h5py
import struct
import numpy as np

def create_large_dataset():
    """Create a 1M+ vector dataset with proper ground truth."""

    with h5py.File('glove-25.hdf5', 'r') as f:
        print("Loading GloVe dataset...")
        # Use ALL training vectors (1.18M vectors)
        all_vectors = f['train'][:]
        print(f"Loaded {len(all_vectors):,} vectors")

        # Use 10,000 test queries
        all_queries = f['test'][:]
        print(f"Loaded {len(all_queries):,} queries")

        # Use precomputed ground truth from the dataset!
        ground_truth = f['neighbors'][:]
        print(f"Loaded precomputed ground truth: {ground_truth.shape}")

        # Verify the ground truth references valid vector IDs
        max_gt_id = ground_truth.max()
        print(f"Ground truth ID range: 0 to {max_gt_id:,}")
        print(f"Available vectors: 0 to {len(all_vectors)-1:,}")

        if max_gt_id >= len(all_vectors):
            print(f"WARNING: Some ground truth IDs exceed available vectors!")

        k_neighbors = ground_truth.shape[1]

        print(f"\nLarge dataset created:")
        print(f"  Vectors: {len(all_vectors):,} x {all_vectors.shape[1]}D (IDs 0-{len(all_vectors)-1:,})")
        print(f"  Queries: {len(all_queries):,} x {all_queries.shape[1]}D")
        print(f"  Ground truth: {ground_truth.shape}")
        print(f"  GT range: {ground_truth.min()} to {ground_truth.max()}")

        # Calculate file size
        vectors_size = len(all_vectors) * all_vectors.shape[1] * 4  # float32
        queries_size = len(all_queries) * all_queries.shape[1] * 4
        gt_size = len(ground_truth) * k_neighbors * 8  # int64
        total_size = 4096 + vectors_size + queries_size + gt_size
        print(f"  Estimated file size: {total_size / (1024**3):.2f} GB")

        # Our dataset format constants
        DATASET_MAGIC = 0xDECDB001
        header_size = 4096

        # Calculate offsets
        vectors_offset = header_size
        queries_offset = vectors_offset + vectors_size
        gt_offset = queries_offset + queries_size

        # Create binary file
        output_file = 'large_dataset.bin'
        print(f"\nWriting to {output_file}...")

        with open(output_file, 'wb') as f:
            # Write header (4KB)
            header = struct.pack(
                '<I I 256s B B 2x I Q Q I 4x Q Q Q 3808x',
                DATASET_MAGIC,                    # magic
                1,                               # version
                b'glove-large-scale',            # dataset_name
                0,                               # distance_metric (L2)
                0,                               # dtype (float32)
                all_vectors.shape[1],            # dim
                len(all_vectors),                # num_vectors
                len(all_queries),                # num_queries
                k_neighbors,                     # num_neighbors
                vectors_offset,                  # vectors_offset
                queries_offset,                  # queries_offset
                gt_offset                        # ground_truth_offset
            )
            f.write(header)

            # Write vectors
            print("Writing vectors...")
            f.seek(vectors_offset)
            all_vectors.astype(np.float32).tofile(f)

            # Write queries
            print("Writing queries...")
            f.seek(queries_offset)
            all_queries.astype(np.float32).tofile(f)

            # Write ground truth
            print("Writing ground truth...")
            f.seek(gt_offset)
            ground_truth.astype(np.int64).tofile(f)

        actual_size = f.tell()
        print(f"\nCreated: {output_file}")
        print(f"  Actual file size: {actual_size / (1024**3):.2f} GB")
        print(f"  Vectors: {len(all_vectors):,}")
        print(f"  Queries: {len(all_queries):,}")
        print(f"  Ground truth computed for all queries")

        # Show sample ground truth
        print(f"\nSample ground truth for query 0: {ground_truth[0][:10]}")
        print(f"Sample ground truth for query 1: {ground_truth[1][:10]}")

if __name__ == "__main__":
    create_large_dataset()