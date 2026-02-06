# sky_model_client.py
import asyncio

import httpx


async def main():
    """
    Main entry point for the sky model client.

    Makes an HTTP GET request to a FastAPI endpoint and displays the received model.

    Usage:
        python sky_model_client.py

    Example input parameters:
        ra: Right ascension in degrees
        dec: Declination in degrees
        flux_wide: Wide-field flux in Jy
        telescope: Name of the telescope
        fov: Field of view in arcminutes
    """
    # Replace with your actual FastAPI endpoint URL
    api_url = "http://localhost:37313/local_sky_model"

    # Example input parameters (you can customize these)
    params = {
        "ra": 195.0,
        "dec": -43.0,
        "flux_wide": 2,
        "telescope": "MWA",
        "fov": 200.0,
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(api_url, params=params, timeout=300)
            response_data = response.json()

            # Display the received model (you can format this as needed)
            print("Received model:")
            print(response_data)

    except httpx.RequestError as e:
        print(f"Error making request: {e}")


if __name__ == "__main__":
    # initialise

    asyncio.run(main())
