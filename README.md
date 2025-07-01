# Install gtfs via postgres
``` sh
npm install -g gtfs-via-postgres
```

# Import data
```sh
export PGUSER=postgres
export PGHOST=localhost
export PGPORT=25432
export PGDATABASE=prague

npm exec -- gtfs-to-sql --require-dependencies -- *.txt | psql -b
```

# Ignored files
fare_attributes.txt  route_sub_agencies.txt   vehicle_categories.txt
fare_rules.txt       vehicle_allocations.txt  vehicle_couplings.txt
route_stops.txt      vehicle_boardings.txt
