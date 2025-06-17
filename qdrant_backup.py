#!/usr/bin/env python3
# example CMD line: python qdrant_backup.py -c n8n_qdrant -o C:/qdrant_backups
import argparse
import datetime
import os
import subprocess
import time
import requests
import json

def create_qdrant_snapshot(container_name, output_dir):
    try:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Step 1: Get container port
        port_result = subprocess.run(
            ["docker", "port", container_name, "6333"],
            capture_output=True, text=True, check=True
        )
        host_port = port_result.stdout.strip().split(":")[-1]
        base_url = f"http://localhost:{host_port}"

        # Step 2: List all collections
        collections_url = f"{base_url}/collections"
        collections = requests.get(collections_url).json()["result"]["collections"]
        
        if not collections:
            raise Exception("No collections found - nothing to back up")

        # Step 3: Create and download snapshots for each collection
        for collection in collections:
            col_name = collection["name"]
            print(f"Processing collection: {col_name}")
            
            # Trigger snapshot
            snapshot_url = f"{base_url}/collections/{col_name}/snapshots"
            snap_response = requests.post(snapshot_url)
            snap_response.raise_for_status()
            
            # Get snapshot name
            snapshot_name = snap_response.json()["result"]["name"]
            print(f"Created snapshot: {snapshot_name}")
            
            # Download snapshot
            download_url = f"{base_url}/collections/{col_name}/snapshots/{snapshot_name}"
            output_path = os.path.join(output_dir, f"{col_name}_{timestamp}.snapshot")
            
            with requests.get(download_url, stream=True) as r:
                r.raise_for_status()
                with open(output_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            print(f"Saved {os.path.getsize(output_path)/1024:.2f} KB to {output_path}")

    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--container", required=True)
    parser.add_argument("-o", "--output", required=True)
    args = parser.parse_args()
    create_qdrant_snapshot(args.container, args.output)