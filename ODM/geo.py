# Import from __init__.py
from ODM import Nominatim

# GeoJSON and geo locator
import time
from geopy.exc import GeocoderTimedOut
#from typing import Generator, Any, Self
from geojson import Point

@patch("ODM.Nominatim")
def getLocationPoint(address: str) -> Point:
    """
    Gets the coordinates of an address in geojson.Point format.
    Uses the geopy API to obtain the coordinates of the address.
    Be careful, the API is public and has a request limit, use sleeps.

    Parameters
    ----------
    address : str
        Full address from which to obtain coordinates

    Returns
    -------
    geojson.Point
        Coordinates of the address point
    """

    max_attempts = 5
    attempts = 0
    location = None

    # While the location hasn't been obtained and max number of attempts hasn't
    # been reached, keep trying to obtain location
    while location is None and attempts < max_attempts:
        try:
            time.sleep(1)

            # A user_agent is required to use the API
            # Use a random name for the user_agent
            geolocator = Nominatim(user_agent="Emilie_Itziar_AdvDB")
            location = geolocator.geocode(address)
        except GeocoderTimedOut:
            # Throw an exception if timeout is exceeded
            attempts += 1
            continue

    if location is None:
        raise ValueError("No se pudieron obtener coordenadas")

    # Return coordinate points of location
    #lon = round(location.longitude, 4)
    #lat = round(location.latitude, 4)
    #return Point((lon, lat))
    # return Point((location.longitude, location.latitude))
    return Point([location.longitude, location.latitude])
