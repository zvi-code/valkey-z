from vectordb_bench.backend.dataset import Dataset

print("Available datasets in vectordb-bench with sizes:\n")
print("=" * 100)

datasets_info = []

for dataset in Dataset:
    info = {
        'name': dataset.name,
        'sizes': [],
        'dim': None,
        'metric': None
    }
    
    # Common sizes to try
    test_sizes = [100_000, 500_000, 1_000_000, 5_000_000, 10_000_000, 25_000_000, 100_000_000]
    
    for size in test_sizes:
        try:
            manager = dataset.manager(size=size)
            if hasattr(manager, 'data'):
                if info['dim'] is None:
                    info['dim'] = getattr(manager.data, 'dim', 'Unknown')
                if info['metric'] is None:
                    info['metric'] = getattr(manager.data, 'metric_type', 'Unknown')
            info['sizes'].append(size)
        except Exception:
            pass
    
    datasets_info.append(info)

# Print summary
for info in datasets_info:
    print(f"\n{info['name']}")
    print(f"  Dimensions: {info['dim']}")
    print(f"  Metric: {info['metric']}")
    print(f"  Available sizes: {', '.join([f'{s:,}' for s in info['sizes']])}")

print("\n" + "=" * 100)
print("\nUsage example:")
print("  from vectordb_bench.backend.dataset import Dataset")
print("  data = Dataset.COHERE.manager(size=1_000_000)  # 1M vectors")
print("  data.prepare()")
