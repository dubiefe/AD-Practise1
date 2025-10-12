import json
import ODM

from ODM import initApp 

def main():
    scope = {}
    # Use the real models.yml path!
    initApp(definitions_path="data/models.yml", db_name="abd", scope=scope)
    # Get model classes
    User = scope.get("User")
    Company = scope.get("Company")
    Educational_Center = scope.get("Educational_Center")

    # Load JSON data
    with open("data/users.json", "r", encoding="utf-8") as f:
        users_data = json.load(f)
    with open("data/companies.json", "r", encoding="utf-8") as f:
        companies_data = json.load(f)
    with open("data/educational_centers.json", "r", encoding="utf-8") as f:
        ed_centers_data = json.load(f)

    # Helper to filter admissible/required vars
    def valid_args(model, d):
        valid = model._required_vars.union(model._admissible_vars)
        return {k: v for k, v in d.items() if k in valid}

    # Test User
    print("\n=== ✍️ Creating and saving Users from JSON ===")
    users = []
    for u in users_data[0:3]:
        args = valid_args(User, u)
        user = User(**args)
        user.save()
        users.append(user)
        print("User saved:", user._data)
        if hasattr(user, "address_loc"):
            print("  address_loc:", user.address_loc)

    # Test Company
    print("\n=== ✍️ Creating and saving Companies from JSON ===")
    companies = []
    for c in companies_data[0:3]:
        args = valid_args(Company, c)
        company = Company(**args)
        company.save()
        companies.append(company)
        print("Company saved:", company._data)
        if hasattr(company, "address_loc"):
            print("  address_loc:", company.address_loc)

    # Test Educational_Center
    print("\n=== ✍️ Creating and saving Educational Centers from JSON ===")
    ed_centers = []
    for ec in ed_centers_data[0:3]:
        args = valid_args(Educational_Center, ec)
        ed_center = Educational_Center(**args)
        ed_center.save()
        ed_centers.append(ed_center)
        print("Educational Center saved:", ed_center._data)
        if hasattr(ed_center, "address_loc"):
            print("  address_loc:", ed_center.address_loc)

    # Query and print back all
    print("\n=== 📟 Querying all Users ===")
    for u in User.find({}):
        print(u._data)
        print("  address_loc:", getattr(u, "address_loc", None))
    print("\n=== 📟 Querying all Companies ===")
    for c in Company.find({}):
        print(c._data)
        print("  address_loc:", getattr(c, "address_loc", None))
    print("\n=== 📟 Querying all Educational Centers ===")
    for ec in Educational_Center.find({}):
        print(ec._data)
        print("  address_loc:", getattr(ec, "address_loc", None))

    print("\nODM schema test completed successfully.")

if __name__ == "__main__":
    main()
