import psycopg2
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as mcolors

conn_params = {
    "host": "localhost",
    "dbname": "prague",
    "user": "postgres",
    "password": "",  # si aplica
    "port": 25432,
}

sql = """
SELECT
  CONCAT(
    FLOOR(diff / 5) * 5, ' to ', FLOOR(diff / 5) * 5 + 5
  ) AS speed_diff_range,
  COUNT(*) AS segment_count
FROM trip_speeds_diffs
GROUP BY speed_diff_range
ORDER BY MIN(FLOOR(diff / 5) * 5);
"""


def main():
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    conn.close()

    labels = [row[0] for row in results]
    counts = np.array([row[1] for row in results])

    # Normalizar counts para mapear a intensidad de color (0 a 1)
    norm = mcolors.Normalize(vmin=counts.min(), vmax=counts.max())

    # Usar un colormap rojo con variación de intensidad (más oscuro = más segmentos)
    cmap = plt.cm.Reds

    # Mapear counts a colores
    colors = cmap(norm(counts))

    plt.figure(figsize=(12, 6))
    bars = plt.bar(labels, counts, color=colors)
    plt.xticks(rotation=90)
    plt.xlabel("Speed Difference Range (km/h)")
    plt.ylabel("Number of Segments")
    plt.title("Number of Segments by Speed Difference Range")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
