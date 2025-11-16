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

## Project definition

### ODM

### Session

For the session part, we needed to store the session data and the token.

The session data is composed of:
  - username
  - fullname
  - password
  - privileges
All this attributes will be stored as string in redis to avoid using too many types of data.
The key used for the session will be user:username, this will create unique keys because two users won't be able to have the same username.
The attributes od the session will be stored in a hash in redis with the following template:
  "user:username" {"username":"username", "fullname":"fullname", "password":"password", "privileges":"privileges"}

The token will be stored independantly because it needs to have an expiration date of one month.
The token value will appear in the name of the key and the value will be the username of the session linked to the token.
It will be stored in redis with the following template:
  "token:token_value" "username"
  
### HelpDesk

For the helpdesk, we needed to store the help requests (username, priority and message) and the list of help requests

The list of help requests is called "HelpRequests", its values are the ids of help requests, they are composed like this:
  priority:token

The help requests are stored individually as hashes composed of:
  priority:token {"username":"username", "message":"message"}

When a help request has been read and deleted in the list, the hash related is deleted too.

To test this functionment, you need to launch the main.py in a terminal and the send_request.py in another terminal. 
This allows you to test the functionment of the waiting of help requests.

## Dependencies

If you don't have the Python dependencies installed locally, you can get them
through the shell.nix file with the following commands:

### Conda

If you have conda, you can install the dependencies using:

```bash
# Create conda environment from YML file
conda env create -f environment.yml
conda activate ODMEmilieItziarRedis
```

In case the wrong kernel is used, run the following command to get the right
kernel to be used (change Kernel in Jupyter to the proper one)

```bash
python -m ipykernel install --user --name ODMEmilieItziarRedis --display-name "Python (ODMEmilieItziarRedis)"
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

## Exceptions

### ODM

`ODM.getGeoLocation` can return a timed out error due to API but cannot be
changed due to the test.

### Session

`Session.create` can return two exceptions:
  - an Exception if a key is missing between username, fullname, password and privileges
  - an Exception if the username for the new session is already used by another session
