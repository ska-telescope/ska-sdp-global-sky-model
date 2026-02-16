#!/usr/bin/env python3
"""
Demo script for requesting Local Sky Models from the Global Sky Model service.

This script demonstrates how to create LSM requests using the Configuration DB
Flow interface. The examples use coordinates from the test catalog data.

Prerequisites:
- GSM service running with test data ingested
- ska-sdp-config library installed
- Access to the Configuration DB (etcd)

Test data coordinates (from tests/data/test_catalog_1.csv):
- RA range: ~42.8° to 46.5° (0.747 to 0.811 radians)
- Dec range: ~0.2° to 4.3° (0.003 to 0.075 radians)
"""

import argparse
import math
import pathlib
import sys
import time

try:
    import ska_sdp_config
    from ska_sdp_config.entity import Flow
    from ska_sdp_config.entity.flow import DataProduct, FlowSource, PVCPath
except ImportError:
    print("Error: ska-sdp-config not installed")
    print("Install with: pip install ska-sdp-config")
    sys.exit(1)


def deg_to_rad(degrees):
    """Convert degrees to radians"""
    return degrees * math.pi / 180.0


def rad_to_deg(radians):
    """Convert radians to degrees"""
    return radians * 180.0 / math.pi


def create_lsm_request(
    ra_deg, dec_deg, fov_deg, eb_id=None, pb_id=None, field_id="demo_field"
):
    """
    Create an LSM request Flow entry in the Configuration DB.

    Args:
        ra_deg: Right Ascension in degrees
        dec_deg: Declination in degrees
        fov_deg: Field of view radius in degrees
        eb_id: Execution block ID (auto-generated if None)
        pb_id: Processing block ID (auto-generated if None)
        field_id: Field identifier
    """
    # Convert to radians
    ra = deg_to_rad(ra_deg)
    dec = deg_to_rad(dec_deg)
    fov = deg_to_rad(fov_deg)

    # Generate IDs if not provided
    if eb_id is None:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        eb_id = f"eb-demo-{timestamp}"
    if pb_id is None:
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        pb_id = f"pb-demo-{timestamp}"

    print(f"\nCreating LSM request:")
    print(f"  Field ID: {field_id}")
    print(f"  Center: RA={ra_deg:.4f}° ({ra:.6f} rad), Dec={dec_deg:.4f}° ({dec:.6f} rad)")
    print(f"  FOV radius: {fov_deg:.4f}° ({fov:.6f} rad)")
    print(f"  Execution Block: {eb_id}")
    print(f"  Processing Block: {pb_id}")

    # Connect to Configuration DB
    try:
        config = ska_sdp_config.Config()
    except Exception as e:
        print(f"\nError: Could not connect to Configuration DB: {e}")
        print("Make sure etcd is running and accessible")
        sys.exit(1)

    # Create the Flow entry
    try:
        for txn in config.txn():
            flow = Flow(
                key=Flow.Key(pb_id=pb_id, name=f"local-sky-model-{field_id}"),
                sink=DataProduct(
                    data_dir=PVCPath(
                        k8s_namespaces=[],
                        k8s_pvc_name="",
                        pvc_mount_path="/mnt/data",
                        pvc_subpath=pathlib.Path(
                            f"product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/{field_id}"
                        ),
                    ),
                    paths=[],
                ),
                sources=[
                    FlowSource(
                        uri="gsm://request/lsm",
                        function="GlobalSkyModel.RequestLocalSkyModel",
                        parameters={
                            "ra": ra,
                            "dec": dec,
                            "fov": fov,
                            "version": "latest",
                        },
                    )
                ],
                data_model="CsvNamedColumns",
                expiry_time=-1,
            )
            txn.flow.add(flow)
            txn.flow.state(flow).create({"status": "INITIALISED"})

        print(f"\n✓ LSM request created successfully!")
        print(f"  Flow key: {pb_id}/{flow.key.name}")
        print(f"\nOutput will be written to:")
        print(f"  CSV: /mnt/data/product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/sky/{field_id}/local_sky_model.csv")
        print(f"  Metadata: /mnt/data/product/{eb_id}/ska-sdp/{pb_id}/ska-sdm/ska-data-product.yaml")
        print(f"\nMonitor status with:")
        print(f"  python {__file__} --status --pb-id {pb_id} --flow-name local-sky-model-{field_id}")

        config.close()
        return pb_id, flow.key.name

    except Exception as e:
        print(f"\nError creating Flow: {e}")
        config.close()
        sys.exit(1)


def check_status(pb_id, flow_name):
    """
    Check the status of an LSM request.

    Args:
        pb_id: Processing block ID
        flow_name: Flow name
    """
    try:
        config = ska_sdp_config.Config()
    except Exception as e:
        print(f"\nError: Could not connect to Configuration DB: {e}")
        sys.exit(1)

    try:
        for txn in config.txn():
            flow_key = Flow.Key(pb_id=pb_id, name=flow_name)
            state = txn.flow.state(flow_key).get()

            if state:
                status = state.get("status", "UNKNOWN")
                print(f"\nFlow: {pb_id}/{flow_name}")
                print(f"Status: {status}")

                if "reason" in state:
                    print(f"Reason: {state['reason']}")

                if "last_updated" in state:
                    import datetime

                    timestamp = datetime.datetime.fromtimestamp(state["last_updated"])
                    print(f"Last updated: {timestamp}")

                # Print status meaning
                print("\nStatus meanings:")
                print("  INITIALISED - Request created, waiting for processing")
                print("  FLOWING - Currently being processed")
                print("  COMPLETED - Successfully completed, LSM files are ready")
                print("  FAILED - Processing failed, check the reason field")
            else:
                print(f"\nFlow not found: {pb_id}/{flow_name}")
                print("Check that the Flow was created successfully")

        config.close()

    except Exception as e:
        print(f"\nError checking status: {e}")
        config.close()
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Request Local Sky Models from the Global Sky Model service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples based on test catalog data:

  # Request LSM for a field centered at RA=45°, Dec=2° with 1.5° radius
  %(prog)s --ra 45 --dec 2 --fov 1.5 --field-id test_field_01

  # Request LSM using the default test coordinates
  %(prog)s

  # Check status of a request
  %(prog)s --status --pb-id pb-demo-20260216-123456 --flow-name local-sky-model-demo_field

Note: The test catalog (tests/data/test_catalog_1.csv) contains components in the range:
  RA: 42.8° to 46.5° (0.747 to 0.811 radians)
  Dec: 0.2° to 4.3° (0.003 to 0.075 radians)
        """,
    )

    parser.add_argument(
        "--ra", type=float, default=45.0, help="Right Ascension in degrees (default: 45.0)"
    )
    parser.add_argument("--dec", type=float, default=2.0, help="Declination in degrees (default: 2.0)")
    parser.add_argument(
        "--fov", type=float, default=1.5, help="Field of view radius in degrees (default: 1.5)"
    )
    parser.add_argument(
        "--field-id", type=str, default="demo_field", help="Field identifier (default: demo_field)"
    )
    parser.add_argument("--eb-id", type=str, help="Execution block ID (auto-generated if not provided)")
    parser.add_argument(
        "--pb-id", type=str, help="Processing block ID (auto-generated if not provided)"
    )
    parser.add_argument("--status", action="store_true", help="Check status instead of creating request")
    parser.add_argument("--flow-name", type=str, help="Flow name (required with --status)")

    args = parser.parse_args()

    if args.status:
        if not args.pb_id or not args.flow_name:
            parser.error("--status requires --pb-id and --flow-name")
        check_status(args.pb_id, args.flow_name)
    else:
        create_lsm_request(
            args.ra, args.dec, args.fov, args.eb_id, args.pb_id, args.field_id
        )


if __name__ == "__main__":
    main()
