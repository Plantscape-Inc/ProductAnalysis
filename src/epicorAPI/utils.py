import polars as pl
from decimal import Decimal
from datetime import datetime, date


def sqlexec(cursor, sql_string: str) -> pl.DataFrame:
    cursor.execute(sql_string)
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]

    clean_rows = []
    for row in rows:
        clean_row = []
        for v in row:
            if isinstance(v, Decimal):
                clean_row.append(float(v))
            elif isinstance(v, (datetime, date)):
                clean_row.append(v.isoformat())
            else:
                clean_row.append(v)
        clean_rows.append(clean_row)

    return pl.DataFrame(clean_rows, columns, orient="row", infer_schema_length=len(clean_rows))


def sqlexec_local(table_name, sql_string: str) -> pl.DataFrame:
    df = pl.read_csv(f"../data/tables/{table_name}.csv", infer_schema_length=10000)
    ctx = pl.SQLContext()
    ctx.register(table_name, df)

    return pl.DataFrame(ctx.execute(sql_string).collect())
