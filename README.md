# Proyecto: Integración de GTFS, PostgreSQL y Valhalla

## Integrantes

- Vilamowski, Abril (62495)
- Wischñevsky, David (62494)

## Profesor

- Vaisman, Alejandro Ariel

---

## 1. Instalación y carga de datos GTFS

### Instalar `gtfs-via-postgres`

```sh
npm install -g gtfs-via-postgres
```

### Importar datos a PostgreSQL

Configurar variables de entorno para conectarse a la base de datos:

```sh
export PGUSER=postgres
export PGHOST=localhost
export PGPORT=25432
export PGDATABASE=prague
```

Importar archivos `.txt` de GTFS a la base (ignora los archivos listados debajo):

```sh
npm exec -- gtfs-to-sql --require-dependencies -- *.txt | psql -b
```

**Archivos ignorados:**\
`fare_attributes.txt`, `fare_rules.txt`, `route_sub_agencies.txt`, `vehicle_allocations.txt`,\
`vehicle_boardings.txt`, `vehicle_categories.txt`, `vehicle_couplings.txt`, `route_stops.txt`

---

## 2. Correr el servidor Valhalla

Levantar Valhalla con Docker y los datos de OSM para República Checa:

```sh
docker run -dt --name valhalla_gis-ops -p 8002:8002 \
  -v $PWD/custom_files:/custom_files \
  -e tile_urls=https://download.geofabrik.de/europe/czech-republic-latest.osm.pbf \
  ghcr.io/nilsnolde/docker-valhalla/valhalla:latest
```

---

## 3. Estructura del proyecto

```plaintext
.
|-- README.md
|-- gtfs_realtime
|   |-- definitions
|   |   |-- gtfs_realtime_OVapi_pb2.py
|   |   `-- gtfs_realtime_pb2.py
|   |-- gtfs_rt_inspector.py
|   |-- map_matching.py
|   |-- mdb_importer_realtime_new.sql
|   |-- queries.sql
|   |-- requirements.txt
|   |-- rest_gtfs_rt_inspector.py
|   `-- visualize.py
`-- gtfs_schedule
    |-- agg_routes_per_segment.py
    |-- mdb_importer_scheduled.sql
    |-- queries.sql
    |-- requirements.txt
    `-- trips_near_shopping.py
```

---

## 4. Cómo ejecutar los scripts de Python

Las dependencias deben instalarse por separado en cada una de las carpetas `gtfs_realtime` y `gtfs_schedule`, ejecutando:

```sh
pip install -r requirements.txt
```

---

## 5. Ejecución de scripts Python

A continuación se detallan los scripts principales del proyecto, su funcionalidad y cómo ejecutarlos desde la raíz (`prague_GTFS/`):

### En `gtfs_realtime/`:

- **gtfs\_rt\_inspector.py**\
  Versión alternativa del extractor de datos en realtime, pero utilizando la API basada en protobuf de GTFS-RT. Permite inspeccionar y procesar el feed en tiempo real, generando un archivo Parquet con las posiciones de los vehículos.
  Nota: Es necesario preprocesar el archivo resultante con la función convert_parquet de visualize.py.
  No se recomienda su uso, ya que la calidad de los datos obtenidos por este método es significativamente inferior.
  **Ejecutar:**

  ```sh
  cd gtfs_realtime
  python3 gtfs_rt_inspector.py
  ```

- **map\_matching.py**\
  Realiza el map matching de los vehículos, ajustando sus posiciones GPS a la red obtenida de OpenStreetMap mediante Valhalla. Usa los datos de posición registrados y rutas estimadas para cada viaje, generando archivos de salida en formato GeoJSON y CSV con las trayectorias ajustadas y los puntos coincidentes.

  **Ejecutar:**

  ```sh
  cd gtfs_realtime
  python3 map_matching.py <archivo_de_entrada.parquet>
  ```

- **rest\_gtfs\_rt\_inspector.py**\
  Obtiene en tiempo real las posiciones de los vehículos desde la API REST de Golemio, realizando consultas cada 20 segundos. Permite definir un tiempo máximo de captura o interrumpir el proceso manualmente con Ctrl+C.

  **Ejecutar:**

  ```sh
  cd gtfs_realtime
  python3 rest_gtfs_rt_inspector.py
  ```

- **errors.py**\
  Script para analizar los tipos de rutas fallidos en el proceso de map matching. 

  **Ejecutar:**

  ```sh
  cd gtfs_realtime
  python3 errors.py map_matching_errors.geojson
  ```

- **visualize.py**\
  Script deprecado. Se utilizaba cuando se trabajaba con la API basada en protobuf para la extracción de datos. Convierte el archivo Parquet original a uno nuevo, extrayendo únicamente los atributos necesarios y renombrando ciertos campos.
  **Nota:** Este script ya no se utiliza, ya que se ha migrado a un enfoque diferente para el procesamiento de datos.\
  **Ejecutar:**

  ```sh
  cd gtfs_realtime
  python3 visualize.py 
  ```

---

### En `gtfs_schedule/`:

- **agg\_routes\_per\_segment.py**\
  Genera un histograma que muestra la cantidad de rutas que circulan por cada segmento de la red.
  **Ejecutar:**

  ```sh
  cd gtfs_schedule
  python3 agg_routes_per_segment.py
  ```

- **trips\_near\_shopping.py**\
  Genera un gráfico que muestra la cantidad de trips cercanos a cada shopping en intervalos de 2 horas.
  **Ejecutar:**

  ```sh
  cd gtfs_schedule
  python3 trips_near_shopping.py
  ```
---
## 6. Ejecución de scripts SQL

A continuación se detallan los scripts SQL principales del proyecto, su funcionalidad y cómo ejecutarlos desde la raíz (`prague_GTFS/`).  
Recuerda posicionarte en la carpeta correspondiente antes de ejecutar cada archivo.

### En `gtfs_realtime/`:

- **mdb_importer_realtime_new.sql**  
  Importa los datos realtime a la base de datos.

  **Ejecutar:**
  ```sh
  cd gtfs_realtime
  psql -h localhost -U postgres -p 25432 -d prague -f mdb_importer_realtime_new.sql
  ```

- **queries.sql**  
  Contiene consultas auxiliares y de análisis sobre los datos de tiempo real ya importados.

  **Ejecutar:**
  ```sh
  cd gtfs_realtime
  psql -h localhost -U postgres -p 25432 -d prague -f queries.sql
  ```

---

### En `gtfs_schedule/`:

- **mdb_importer_scheduled.sql**  
  Importa los datos scheduled a la base de datos PostgreSQL.

  **Ejecutar:**
  ```sh
  cd gtfs_schedule
  psql -h localhost -U postgres -p 25432 -d prague -f mdb_importer_scheduled.sql
  ```

- **queries.sql**  
  Contiene consultas auxiliares y de análisis sobre los datos programados ya importados.

  **Ejecutar:**
  ```sh
  cd gtfs_schedule
  psql -h localhost -U postgres -p 25432 -d prague -f queries.sql
  ```
