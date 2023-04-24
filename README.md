# station-climate-etl-ipfs
Station etl tools is...

# How to generate new package
pip install pip==23.1.1
pip install -r requirements.txt
python -m build

# Install package in a new project
pip install dist/station_etl_tools-0.0.1.tar.gz

# Import
import etls.station_set

# Development: How to run
# From station-climate-etl-ipfs/src folder:
python -m etls.generate

Virtual env
------------

```
python -m venv {name-your-env}
source {name-your-env}/bin/activate
deactivate
```