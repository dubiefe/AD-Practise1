import ODM

import time
from geopy.exc import (
    GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
)
from geojson import Point

# Simple in-memory cache for address geocoding
_address_cache = {}

def getLocationPoint(address: str) -> Point:
    """
    Geocode a street address and return coordinates as geojson.Point.

    Uses geopy to obtain coordinates for an address string.
    Caches results for repeated use. Retries up to 3 times, sleeping between
    calls due to geopy rate limits.

    Parameters
    ----------
    address : str
        Full street address to geocode.

    Returns
    -------
    geojson.Point or None
        geojson.Point coordinates if successful, else None.

    Notes
    -----
    - If address is empty/None, returns None.
    - Caches successful geocode results.
    - Retries on geocoding errors (up to 3 times).
    """
    if not address or address.strip() == "":
        print(f"Skipping geocoding for empty/invalid address: {address}")
        return None
    if address in _address_cache:
        return _address_cache[address]

    max_attempts = 3
    attempts = 0
    location = None

    while location is None and attempts < max_attempts:
        try:
            time.sleep(1)
            geolocator = ODM.Nominatim(user_agent="Emilie_Itziar_AdvDB")
            location = geolocator.geocode(address)
            attempts += 1
        except (
            GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
        ):
            attempts += 1
            print(
                f"Geocoding timed out for address '{address}', "
                f"attempt {attempts}"
            )
            continue
        except Exception as e:
            print(f"Unexpected error during geocoding: {e}")
            attempts += 1
            continue

    if location is not None:
        point = Point([location.longitude, location.latitude])
    else:
        print(f"Could not geocode address '{address}'. Returning None.")
        point = None

    _address_cache[address] = point
    return point
