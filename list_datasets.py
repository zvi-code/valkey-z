from vectordb_bench.backend.dataset import Dataset

print("Available datasets in vectordb-bench:\n")
print("=" * 80)

for dataset in Dataset:
    print(f"\nDataset: {dataset.name}")
    print(f"  Value: {dataset.value}")
    try:
        # Try to get manager info
        manager = dataset.manager
        print(f"  Manager: {manager}")
    except Exception as e:
        print(f"  Manager error: {e}")
