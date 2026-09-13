"""
Standalone smoke test: runs the real pipeline (extract + transform)
against sample CSV data, substituting the MockLoader for the real
warehouse loader so it can be verified without live credentials.
"""
import etl.loaders as loaders_module
from etl.mock_loader import MockLoader
from etl.pipeline import ETLPipeline

# Patch in the mock loader for this test run only
loaders_module.LOADER_REGISTRY["mock"] = MockLoader

pipeline = ETLPipeline("./config/config.test.yaml")
results = pipeline.run()

print("\n--- RESULTS ---")
for source, status in results.items():
    print(f"{source}: {status}")

import pandas as pd
output = pd.read_csv("./data/staging/raw_orders_csv.csv")
print("\n--- OUTPUT DATA ---")
print(output)

assert len(output) == 2, f"Expected 2 rows after dedup+filter, got {len(output)}"
assert "source_system" in output.columns
assert (output["amount"] > 0).all()
print("\n✅ Smoke test passed: dedup, type casting, filtering, and enrichment all worked correctly.")
