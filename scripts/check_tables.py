import duckdb

conn = duckdb.connect('data/warehouse.duckdb')
qs = [
    'SELECT count(*) FROM raw.covid_surveillance',
    'SELECT count(*) FROM analytics_staging.stg_covid_surveillance',
    'SELECT count(*) FROM raw.places_county',
]
for q in qs:
    try:
        print(q, '->', conn.execute(q).fetchall())
    except Exception as e:
        print(q, '-> ERROR:', e)
