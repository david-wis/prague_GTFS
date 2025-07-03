# PID Toolkit - GTFS Realtime Inspector

> A tool for inspecting GTFS Realtime feeds from Golemio API and dumping them to a file as JSON

List of available feeds can be found in the [API docs](https://api.golemio.cz/v2/pid/docs/openapi/#/%F0%9F%97%BA%20GTFS%20Realtime)

## Requirements

-   [Python 3](https://www.python.org/downloads/)
-   [pip](https://pip.pypa.io/en/stable/installation/)

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```bash
# gtfs_rt_inspector.py <server_url_prefix> <feed_name> <n_messages_to_print>
python3 gtfs_rt_inspector.py api.golemio.cz vehicle_positions 1
```
