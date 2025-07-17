import psycopg2
import pandas as pd
import matplotlib.pyplot as plt

DB_HOST = "localhost"
DB_PORT = 25432
DB_NAME = "prague"
DB_USER = "postgres"
DB_PASS = ""

# 1. Query que une trips y distancia de cada shopping
query = """
SELECT sti.shopping_name, sti.intervalo, sti.trips_nearby, tcs.distance_km
FROM shopping_trip_intervals sti
JOIN trajectories_center_shopping tcs
  ON sti.shopping_name = tcs.name
ORDER BY sti.intervalo, sti.shopping_name;
"""


def fetch_shopping_trips():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    df = pd.DataFrame(
        data, columns=["shopping_name", "intervalo", "trips_nearby", "distance_km"]
    )
    return df


def plot_shopping_trips(df):
    # Ordenar shoppings por distancia promedio
    avg_distance = (
        df.groupby("shopping_name")
        .agg({"distance_km": "mean"})
        .sort_values("distance_km", ascending=True)  # Cerca = más oscuro
        .reset_index()
    )

    n = len(avg_distance)
    cmap = plt.cm.magma
    # Para que el MÁS LEJANO sea el más claro: el primero de la lista es el más cercano (más oscuro)
    colors = [cmap(i / (n - 1)) for i in range(n)]
    color_dict = dict(zip(avg_distance["shopping_name"], colors))

    pivot = df.pivot(
        index="intervalo", columns="shopping_name", values="trips_nearby"
    ).fillna(0)

    plt.figure(figsize=(12, 7))

    for shopping in avg_distance["shopping_name"]:
        if shopping in pivot.columns:
            label = f"{shopping} ({avg_distance.set_index('shopping_name').loc[shopping, 'distance_km']:.2f} km)"
            plt.plot(
                pivot.index,
                pivot[shopping],
                marker="o",
                label=label,
                color=color_dict[shopping],
            )

    plt.title("Cantidad de trips por intervalo para cada shopping")
    plt.xlabel("Intervalo horario")
    plt.ylabel("Cantidad de trips cercanos")
    plt.legend(
        title="Shopping (Distancia al centro)",
        bbox_to_anchor=(1.05, 1),
        loc="upper left",
        ncol=1,
    )
    plt.grid(True, which="both", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    df = fetch_shopping_trips()
    plot_shopping_trips(df)
