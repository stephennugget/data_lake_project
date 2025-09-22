import pandas as pd
import trino
import os

def get_connection():
    return trino.dbapi.connect(
        host='localhost',
        port=8080,
        user='admin',
        catalog='iceberg'
    )

def create_schema_and_table():
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("CREATE SCHEMA IF NOT EXISTS iceberg.suicide_data")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS iceberg.suicide_data.suicide_rates (
                country VARCHAR,
                year INTEGER,
                sex VARCHAR,
                age VARCHAR,
                suicides_no INTEGER,
                population INTEGER,
                suicides_100k_pop DOUBLE,
                country_year VARCHAR,
                hdi_for_year DOUBLE,
                gdp_for_year BIGINT,
                gdp_per_capita DOUBLE,
                generation VARCHAR
            ) WITH (
                location = 's3://warehouse/suicide_data/suicide_rates',
                format = 'PARQUET'
            )
        """)
        print("Schema and table created successfully")
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()
        conn.close()

def load_csv(csv_file="./data/master.csv"):
    if not os.path.exists(csv_file):
        print(f"Error: CSV file not found: {csv_file}")
        return
    
    # read and clean data
    df = pd.read_csv(csv_file)
    df = df.fillna({
        'country': 'Unknown',
        'year': 0,
        'sex': 'Unknown', 
        'age': 'Unknown',
        'suicides_no': 0,
        'population': 0,
        'suicides/100k pop': 0.0,
        'country-year': 'Unknown',
        'HDI for year': 0.0,
        ' gdp_for_year ($) ': 0,
        'gdp_per_capita ($)': 0.0,
        'generation': 'Unknown'
    })
    
    # clean GDP column (remove commas and dollar signs)
    gdp_col = ' gdp_for_year ($) '
    df[gdp_col] = df[gdp_col].astype(str).str.replace(r'[$,]', '', regex=True)
    df[gdp_col] = pd.to_numeric(df[gdp_col], errors='coerce').fillna(0).astype(int)
    
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("USE iceberg.suicide_data")
    
    try:
        cursor.execute("DELETE FROM suicide_rates")
        

        chunk_size = 4000
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            
            values = []
            for _, row in chunk.iterrows():
                # escape single quotes in string values
                country = str(row['country']).replace("'", "''")
                sex = str(row['sex']).replace("'", "''")
                age = str(row['age']).replace("'", "''") 
                country_year = str(row['country-year']).replace("'", "''")
                generation = str(row['generation']).replace("'", "''")
                
                values.append(f"""(
                    '{country}', {int(row['year'])}, '{sex}', '{age}',
                    {int(row['suicides_no'])}, {int(row['population'])}, 
                    {float(row['suicides/100k pop'])}, '{country_year}',
                    {float(row['HDI for year'])}, {int(row[gdp_col])},
                    {float(row['gdp_per_capita ($)'])}, '{generation}'
                )""")
            
            insert_sql = f"""
                INSERT INTO suicide_rates VALUES {','.join(values)}
            """
            
            cursor.execute(insert_sql)
            print(f"Inserted chunk {i//chunk_size + 1}: {min(i+chunk_size, len(df)):,} rows")
        
        cursor.execute("SELECT COUNT(*) FROM suicide_rates")
        print(f"Total rows loaded: {cursor.fetchone()[0]:,}")
        
    except Exception as e:
        print(f"Error loading data: {e}")
    finally:
        cursor.close()
        conn.close()

def main():
    create_schema_and_table()
    load_csv()

if __name__ == "__main__":
    main()