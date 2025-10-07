{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    python311
    python311Packages.pymongo
    python311Packages.geopy
    python311Packages.geojson
    python311Packages.pyyaml
    # You may also want ipython for testing
    # python311Packages.ipython
  ];

  shellHook = ''
    echo "Python dependencies for ODM_template.py are available!"
  '';
}
