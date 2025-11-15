{
  pkgs ? import <nixpkgs> { },
}:

let
  python = pkgs.python312;
  # Create a single Python environment that bundles the python interpreter
  # with the Python packages you listed plus Jupyter pieces.
  pyEnv = python.withPackages (
    ps: with ps; [
      pymongo
      geopy
      geojson
      pyyaml
      pytest
      tqdm
      ipywidgets
      redis

      # Jupyter pieces to run notebooks
      notebook
      ipykernel
      jupyterlab
    ]
  );
in

pkgs.mkShell {
  buildInputs = [
    pyEnv
    pkgs.git
  ];

  shellHook = ''
    echo "Python dependencies for ODM.py are available!"

    echo
    echo "Common commands:"
    echo "  - Start JupyterLab : jupyter lab --no-browser --ip=0.0.0.0"
    echo "  - Start classic NB : jupyter notebook --no-browser --ip=0.0.0.0"
    echo
    if python -c "import ipykernel" >/dev/null 2>&1; then
      echo "Optional: register this environment as a kernel:"
      echo "  python -m ipykernel install --user --name=odm312 --display-name='ODM (py312)'"
    fi
  '';
}
