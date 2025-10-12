import pytest
import json
import geojson

SCHEMA_PATH = "models.yml"
USER_JSON = "users.json"
EDU_JSON = "educational_centers.json"
COMPANY_JSON = "companies.json"

@pytest.fixture(autouse=True)
def patch_geocode(monkeypatch):
    # Patch ODM.getLocationPoint to always return dummy geojson.Point
    import ODM
    monkeypatch.setattr(ODM, "getLocationPoint", lambda address: geojson.Point([0, 0]))

    # Patch Nominatim.geocode to avoid any network call
    from geopy.geocoders import Nominatim
    class DummyLocation:
        longitude = 0
        latitude = 0
    def dummy_geocode(self, address):
        return DummyLocation()
    monkeypatch.setattr(Nominatim, "geocode", dummy_geocode)

@pytest.fixture(scope="module")
def setup_models():
    from ODM import initApp
    scope = {}
    initApp(definitions_path=SCHEMA_PATH, scope=scope)
    return scope

def load_json(path):
    with open(path, "r") as f:
        return json.load(f)

def test_user_model_modification_tracking(setup_models):
    User = setup_models["User"]
    users = load_json(USER_JSON)
    user_inst = User(**users[0])
    assert user_inst._modified_vars == set()
    orig_name = user_inst.name
    user_inst.name = orig_name + " Modificado"
    assert "name" in user_inst._modified_vars
    user_inst.save()
    assert user_inst._modified_vars == set()

def test_educational_center_model_modification_tracking(setup_models):
    Educational_Center = setup_models["Educational_Center"]
    centers = load_json(EDU_JSON)
    center_inst = Educational_Center(**centers[0])
    assert center_inst._modified_vars == set()
    orig_address = center_inst.address
    center_inst.address = orig_address + " Campus Norte"
    assert "address" in center_inst._modified_vars
    center_inst.save()
    assert center_inst._modified_vars == set()

def test_company_model_modification_tracking(setup_models):
    Company = setup_models["Company"]
    companies = load_json(COMPANY_JSON)
    comp_inst = Company(**companies[0])
    assert comp_inst._modified_vars == set()
    orig_address = comp_inst.address
    comp_inst.address = orig_address + " Edificio B"
    assert "address" in comp_inst._modified_vars
    comp_inst.save()
    assert comp_inst._modified_vars == set()
