# Global Sky Model - Examples

This directory contains example scripts demonstrating how to use the Global Sky Model service.

## Available Examples

### request_lsm_demo.py

Interactive script for requesting Local Sky Models (LSMs) from the Global Sky Model service.

**Features:**
- Create LSM requests with custom coordinates
- Monitor request status
- Built-in examples using test catalog coordinates

**Prerequisites:**
```bash
pip install ska-sdp-config
```

**Basic Usage:**

```bash
# Request LSM with default test coordinates (RA=45°, Dec=2°, FOV=1.5°)
python examples/request_lsm_demo.py

# Request LSM with custom coordinates
python examples/request_lsm_demo.py --ra 45.0 --dec 2.0 --fov 1.5 --field-id my_field

# Request LSM with specific catalog version
python examples/request_lsm_demo.py --ra 45.0 --dec 2.0 --fov 1.5 --version 1.0.0

# Check status of a request
python examples/request_lsm_demo.py --status --pb-id pb-demo-20260216-123456 --flow-name local-sky-model-my_field
```

**Test Catalog Coordinates:**

The test data in `tests/data/test_catalog_1.csv` contains components in the range:
- RA: 42.8° to 46.5° (0.747 to 0.811 radians)
- Dec: 0.2° to 4.3° (0.003 to 0.075 radians)

Use these coordinates for testing with the provided test data.

## Programmatic Usage

For integration into your own scripts:

```python
import math
import pathlib
import ska_sdp_config
from ska_sdp_config.entity import Flow
from ska_sdp_config.entity.flow import DataProduct, FlowSource, PVCPath

def deg_to_rad(degrees):
    return degrees * math.pi / 180.0

# Define field parameters
ra = deg_to_rad(45.0)    # Right Ascension in radians
dec = deg_to_rad(2.0)    # Declination in radians
fov = deg_to_rad(1.5)    # Field of view radius in radians
version = \"latest\"        # Catalog version (or \"1.0.0\", \"0.1.0\", etc.)

# Create Configuration DB connection
config = ska_sdp_config.Config()

# Create LSM request Flow
for txn in config.txn():
    flow = Flow(
        key=Flow.Key(pb_id="pb-demo-12345", name="local-sky-model-field1"),
        sink=DataProduct(
            data_dir=PVCPath(
                k8s_namespaces=[],
                k8s_pvc_name="",
                pvc_mount_path="/mnt/data",
                pvc_subpath=pathlib.Path("product/eb-demo/ska-sdp/pb-demo-12345/ska-sdm/sky/field1"),
            ),
            paths=[],
        ),
        sources=[
            FlowSource(
                uri="gsm://request/lsm",
                function="GlobalSkyModel.RequestLocalSkyModel",
                parameters={"ra": ra, "dec": dec, "fov": fov, "version": version},
            )
        ],
        data_model="CsvNamedColumns",
        expiry_time=-1,
    )
    txn.flow.add(flow)
    txn.flow.state(flow).create({"status": "INITIALISED"})

config.close()
```

## Output Files

Once processing completes, you'll find:

1. **CSV File**: Contains sky model data
   ```
   /mnt/data/product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/{field_id}/local_sky_model.csv
   ```

2. **Metadata YAML**: Contains metadata
   ```
   /mnt/data/product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/ska-data-product.yaml
   ```

## Request States

- `INITIALISED` - Request created, waiting for processing
- `FLOWING` - Currently being processed
- `COMPLETED` - Successfully completed, files are ready
- `FAILED` - Processing failed, check the reason field

## More Information

See the [user guide documentation](../docs/src/userguide/requesting_a_lsm.rst) for detailed information about:
- Flow entry structure
- Query parameters
- Database query details
- Output format specifications
