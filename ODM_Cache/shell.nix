{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    python312
    python312Packages.pymongo
    python312Packages.geopy
    python312Packages.geojson
    python312Packages.pyyaml
    python312Packages.pytest
  ];

  shellHook = ''
    echo "Python dependencies for ODM.py are available!"
  '';
}
