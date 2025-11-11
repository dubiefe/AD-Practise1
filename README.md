# Advanced Databases - Assignment 3

College project for Advanced Databases at U-TAD

Authors:

- Emilie Dubief
- Itziar Morales Rodríguez

Toolset:

- Python (3.12)
- MongoDB
- Redis
- Nix (dependencies)
- LaTeX (documentation)

## TODO

### Files to hand in

- [ ] ODM file including cache
- [ ] main.py for the ODM cache
- [ ] Session class
- [ ] HelpDesk class
- [ ] main.py for the session and Helpdesk

### Project TODO list

- [ ] Do the updates in ODM and add the cache
  - find_one_by_id
  - save()
  - delete()
  - **setattr**
- [ ] Test the ODM cache
- [ ] Create Session class
  - CRUD methods
  - login()
  - login_token()
- [ ] Create HelpDesk
- [ ] Test the session and helpdesk in the same file
- [ ] Document the code (!! Exceptions !!)

## Project structure

- The necessary files for the ODM are in the `ODM/` folder.
- `data/` contains mock data for users, companies and educational centers plus
  the model schemas.
- `queries.ipynb` is the jupyter notebook that runs a series of queries
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
|-- queries.ipynb
|-- environment.yml
|-- nix.shell
```

## Dependencies

If you don't have the Python dependencies installed locally, you can get them
through the shell.nix file with the following commands:

### Conda

If you have conda, you can install the dependencies using:

```bash
# Create conda environment from YML file
conda env create -f environment.yml
conda activate ODMEmilieItziar
```

In case the wrong kernel is used, run the following command to get the right
kernel to be used (change Kernel in Jupyter to the proper one)

```bash
python -m ipykernel install --user --name ODMEmilieItziar --display-name "Python (ODMEmilieItziar)"
```

### Nix

Using the nix package manager if you don't have conda:

```bash
# Install nix (authenticate with sudo)
sh <(curl --proto '=https' --tlsv1.2 -L https://nixos.org/nix/install) --daemon

# Initialize nix-shell environment using shell.nix
nix-shell

# Run jupyter notebook
jupyter lab
```

## How to Run

- Make sure you have `mongodb` running as a service in your system.
- Make sure you have the dependencies installed.
- Make sure you have `jupyter lab` or `jupyter notebook` running.

```bash
jupyter lab
```

## Issues

### Jupyter

Sometimes the jupyter notebook doesn't render stuff properly, re run all cells
whenever this happens.

### ODM

`ODM.getGeoLocation` can return a timed out error due to API but cannot be
changed due to the test.

### Session

### HelpDesk
