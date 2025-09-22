# query.py

from trino.dbapi import connect

def main():
    conn = connect(
        host='localhost',
        port=8080,
        user='admin',
        catalog='iceberg',
        schema='suicide_data'
    )
    cursor = conn.cursor()

    # top 3 by suicide rate
    sql = """
    SELECT 
        country,
        ROUND(AVG(suicides_100k_pop), 2) AS avg_suicide_rate
    FROM suicide_rates
    GROUP BY country
    ORDER BY avg_suicide_rate DESC
    LIMIT 3
    """

    cursor.execute(sql)
    results = cursor.fetchall()

    print("TOP 3 COUNTRIES BY AVERAGE SUICIDE RATE:")
    for i, row in enumerate(results, 1):
        print(f"{i}. {row[0]}: {row[1]} per 100k")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
