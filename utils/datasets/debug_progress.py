#!/usr/bin/env python3
"""Debug the progress bar functionality."""

import urllib.request
from pathlib import Path
import sys

def debug_progress_hook(block_num, block_size, total_size):
    """Debug version of progress hook with explicit output."""
    downloaded = block_num * block_size
    
    # Force immediate output
    print(f"HOOK CALLED: block={block_num}, size={block_size}, total={total_size}", flush=True)
    
    if total_size > 0:
        percent = min(100, (downloaded / total_size) * 100)
        mb_downloaded = downloaded / (1024 * 1024)
        mb_total = total_size / (1024 * 1024)
        
        # Create a progress bar
        bar_length = 20  # Shorter for debugging
        filled_length = int(bar_length * percent / 100)
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        # Use print with carriage return for proper updating
        print(f"\r    Progress: {percent:6.1f}% |{bar}| {mb_downloaded:7.1f}/{mb_total:7.1f} MB", end='', flush=True)
    else:
        # Unknown size, just show downloaded amount
        mb_downloaded = downloaded / (1024 * 1024)
        print(f"\r    Downloaded: {mb_downloaded:7.1f} MB", end='', flush=True)

if __name__ == "__main__":
    # Try a smaller file for testing
    url = "https://httpbin.org/drip?numbytes=1048576&duration=10"  # 1MB over 10 seconds
    output_path = Path("_test_download/debug_test.bin")
    output_path.parent.mkdir(exist_ok=True)
    
    print(f"Testing download from: {url}")
    print(f"Output: {output_path}")
    print("Starting download with debug hooks...")
    
    # Set up proper headers
    opener = urllib.request.build_opener()
    opener.addheaders = [
        ('User-Agent', 'Mozilla/5.0 (Compatible VST Dataset Downloader)')
    ]
    urllib.request.install_opener(opener)
    
    try:
        urllib.request.urlretrieve(url, output_path, reporthook=debug_progress_hook)
        print()  # New line after progress
        print(f"✅ Download completed! File size: {output_path.stat().st_size} bytes")
    except Exception as e:
        print(f"\n❌ Download failed: {e}")
    
    # Clean up
    if output_path.exists():
        output_path.unlink()