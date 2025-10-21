# Advanced Databases - Practice 2

College project for Advanced Databases at U-TAD

Authors:

- Emilie Dubief
- Itziar Morales Rodríguez

Toolset:

- Python (3.12)
- MongoDB
- Nix (dependencies)
- LaTeX (documentation)

## TODO

### Files to hand in

- [ ] Jupyter notebook with queries
- [ ] JSON files
- [ ] YML dependencies
- [ ] Initialization of database
- [ ] ODM library

### Project TODO list

- [x] Modify YML to include extra data:
  - user description
  - user companies
  - user educational centers
- [x] Modify JSON files to include extra data:
  - user descriptions
  - user completion dates
  - companies: Google, Microsoft
  - educational centers: UPM, UAM
- [ ] Remove limits
- [ ] Get all GeoJSON data for users
- [ ] Copy database initialization to Jupyter Notebook
- [ ] Put all queries in Jupyter Notebook

### Queries

- [ ] 1st query
- [ ] 2nd query
- [ ] 3rd query
- [ ] 4th query
- [ ] 5th query
- [ ] 6th query
- [ ] 7th query

## Project structure

- The necessary files for the ODM are in the `ODM/` folder.
- `data/` contains mock data for users, companies and educational centers plus
  the model schemas.
- `nix.shell` only needed to install necessary python dependencies
- `doc.pdf` contains a brief explanation of the project

```
project_root/
|-- ODM/
|   |-- __init__.py
|   |-- app.py
|   |-- geo.py
|   |-- model.py
|-- data/
|   |-- users.json
|   |-- companies.json
|   |-- educational_centers.json
|   |-- models.yml
|-- README.md
|-- ODM.py
|-- doc.pdf
|-- nix.shell
```

## Dependencies

If you don't have the Python dependencies installed locally, you can get them
through the shell.nix file with the following commands:

```bash
# Install nix
sh <(curl --proto '=https' --tlsv1.2 -L https://nixos.org/nix/install) --daemon

# Initialize nix-shell environemnt using shell.nix
nix-shell
```

## Exceptions

`ODM.getGeoLocation` can return a timed out error due to API but cannot be
changed due to the test.
