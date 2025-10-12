import json
from ODM import initApp

# If locations aren't appearing, its due to using the actual API and not the
# mock one.

def main():
    scope = {}
    # Use the real models.yml path!
    initApp(definitions_path="models.yml", db_name="abd", scope=scope)
    # Get model classes
    User = scope.get("User")
    Company = scope.get("Company")
    Educational_Center = scope.get("Educational_Center")

    # Load JSON data
    with open("users.json", "r", encoding="utf-8") as f:
        users = json.load(f)
    with open("companies.json", "r", encoding="utf-8") as f:
        companies = json.load(f)
    with open("educational_centers.json", "r", encoding="utf-8") as f:
        ed_centers = json.load(f)

    # Helper to filter admissible/required vars
    def valid_args(model, d):
        valid = model._required_vars.union(model._admissible_vars)
        return {k: v for k, v in d.items() if k in valid}

    # Test User
    print("\n=== Creating and saving Users from JSON ===")
    _users = []
    for user in users:
        args = valid_args(User, user)
        _user = User(**args)
        _user.save()
        _users.append(_user)
        print("User saved:", _user._data)
        if hasattr(_user, "address_loc"):
            print("  address_loc:", _user.address_loc)

    # Test Company
    print("\n=== Creating and saving Companies from JSON ===")
    comps = []
    for company in companies:
        args = valid_args(Company, company)
        comp = Company(**args)
        comp.save()
        comps.append(comp)
        print("Company saved:", comp._data)
        if hasattr(comp, "address_loc"):
            print("  address_loc:", comp.address_loc)

    # Test Educational_Center
    print("\n=== Creating and saving Educational Centers from JSON ===")
    centers = []
    for center in ed_centers:
        args = valid_args(Educational_Center, center)
        edu = Educational_Center(**args)
        edu.save()
        centers.append(edu)
        print("Educational Center saved:", edu._data)
        if hasattr(edu, "address_loc"):
            print("  address_loc:", edu.address_loc)

    # Query and print back all
    print("\n=== Querying all Users ===")
    for u in User.find({}):
        print(u._data)
        print("  address_loc:", getattr(u, "address_loc", None))
    print("\n=== Querying all Companies ===")
    for c in Company.find({}):
        print(c._data)
        print("  address_loc:", getattr(c, "address_loc", None))
    print("\n=== Querying all Educational Centers ===")
    for e in Educational_Center.find({}):
        print(e._data)
        print("  address_loc:", getattr(e, "address_loc", None))

    print("\nODM schema test completed successfully.")

if __name__ == "__main__":
    main()
