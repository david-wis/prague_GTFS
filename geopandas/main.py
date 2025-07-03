def map_individual_lines(cur):
    """
    Creates an interactive map to visualize individual transit lines from the 'shapes_aggregated' view
    using vector tiles as the map background.
    
    Parameters:
    - cur: PostgreSQL cursor object to execute SQL queries.
    """
    # SQL query to get the transit line shapes
    sql = 'SELECT * FROM shapes_aggregated;'
    shapes_gdf = gpd.GeoDataFrame.from_postgis(sql, cur.connection, geom_col='shape', crs='EPSG:4326')

    # Initialize the map centered on Riga with vector tiles
    map_indiv_lines = fl.Map(location=[56.937, 24.109], tiles=None, zoom_start=13, control_scale=True)

    # Vector Tile Service URL and Attribution (Example: OpenStreetMap or MapTiler)
    vector_tile_url = "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
    vector_tile_attr = "OpenStreetMap"

    # Add vector tiles as the base layer
    fl.TileLayer(
        tiles=vector_tile_url,
        attr=vector_tile_attr,
        name="Vector Tiles",
        overlay=False,
        control=True
    ).add_to(map_indiv_lines)

    # Add each route as a FeatureGroup
    for shape_id, shape_group in shapes_gdf.groupby('shape_id'):
        # Initialize a feature group for each individual line (shape_id)
        feature_group = fl.FeatureGroup(name=f"Route {shape_id}", show=True)  # Initially show all routes by default
        
        # Add each LineString geometry as a PolyLine
        for geometry in shape_group.geometry:
            if geometry.geom_type == 'LineString':
                coords = [(lat, lon) for lon, lat in geometry.coords]  # Reverse coords from (lon, lat) to (lat, lon)
                fl.PolyLine(locations=coords, color="blue", weight=2, opacity=0.8).add_to(feature_group)

        # Add the feature group to the map
        feature_group.add_to(map_indiv_lines)

    # Add a layer control to toggle between different routes
    fl.LayerControl(collapsed=False).add_to(map_indiv_lines)

    return map_indiv_lines

# Generate the map
individual_lines_map = map_individual_lines(cur)

# Save the map
individual_lines_map.save("RigaNetworkIndividualLines.html")

# Display the map
display(individual_lines_map)