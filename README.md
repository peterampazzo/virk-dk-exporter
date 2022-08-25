# virk-dk-exporter

This project uses `poetry`, run `poetry install` to set up your enviroment.

To query all the CVR in a municipality:

```
ELASTIC_USERNAME=<your_username> ELASTIC_KEY=<your_apikey> poetry run exporter municipality <kommune_id>
```

All the returned data will be saved as CSV file in the `data/` folder.
Muncipality codes can be found [here](https://www.dst.dk/da/Statistik/dokumentation/nomenklaturer/nuts)