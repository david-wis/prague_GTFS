import psycopg2
import matplotlib.pyplot as plt

DB_HOST = "localhost"
DB_PORT = 25432
DB_NAME = "prague"
DB_USER = "postgres"
DB_PASS = ""  

# Query the SegmentsDisplay table
query = "SELECT num_routes FROM SegmentsDisplay;"

def fetch_num_routes():
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASS
    )
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    # Flatten to 1D list
    return [row[0] for row in data]

def plot_histogram(num_routes_list):
    plt.figure(figsize=(10,6))
    plt.hist(num_routes_list, bins=30, color="#3794eb", edgecolor="black")
    plt.xlabel('Number of Routes per Segment')
    plt.ylabel('Count of Segments')
    plt.title('Histogram of Number of Routes per Segment')
    plt.grid(axis='y', alpha=0.5)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    num_routes_list = fetch_num_routes()
    plot_histogram(num_routes_list)
