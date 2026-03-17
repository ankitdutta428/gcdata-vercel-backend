import psycopg2
from contextlib import contextmanager
from dotenv import load_dotenv
import os
import re
from typing import Literal

# Import column map from schemas
from schemas import COLUMN_MAP, COMMON_COLUMNS



# Load environment variables
load_dotenv()


# Helper function for connection generation
@contextmanager
def get_connection():
    conn = psycopg2.connect(
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host= os.environ["DB_HOST"],
        port= os.environ["DB_PORT"]
    )
    try:
        yield conn
    finally:
        conn.close()





def analytics_query(
    dimensions: list[str] | str | None = None,
    aggregations: list[tuple[str, str]] | tuple[str,str] | None= None,
    filters: list[tuple[str, str, list[str]]] | tuple[str, str, list[str]] | list[tuple[str, str, str]] | tuple[str, str, str] | None = None,
    sort: str | None = None,
    order: Literal["ASC","DESC"] = "ASC",
    limit: int | None = None,
    offset: int | None = None,
    custom_select: list[str] | None = None,
    group_by: list[str] | None = None,
    table: str | None = None
) -> list[tuple]:
    """
    Perform an SQL query using the given parameters and return the output.

    Args:
        dimensions : A list(string) of columns to be outputed.
        aggregations : A list(tuple) of 2-tuples of aggregations needed in the form ({agg_function}, {agg_column}).
        filters : A list(tuple) of 3-tuples of filters to be implemented in the form ({filter_column}, {filter_op}, {filter_value}).
        sort : Column to be sorted in some order. In case of an aggregated column, use the format {agg_function}_{agg_column}.
        order : Either ASC or DESC, defaults to ASC.
        limit : Limit the rows to this amount.
        offset : Offset rows by this amount.

    Returns:
        List[tuples]: Result of the query

    Example:
        >>>analytics_query(dimensions= "channel", aggregations= ("sum","uploaded_count"), sort= "sum_uploaded_count", order= "DESC", limit= 1) 
        [(A, 2567)]   
    """

    # Convert everything to list so single element queries can be handled
    if dimensions and not isinstance(dimensions, list):
        dimensions= [dimensions]

    if aggregations and not isinstance(aggregations, list):
        aggregations= [aggregations]

    if filters and not isinstance(filters, list):
        filters= [filters]

    if custom_select and not isinstance(custom_select, list):
        custom_select = [custom_select]

    if group_by and not isinstance(group_by, list):
        group_by = [group_by]


    query = "SELECT "
    params = []

    select_parts = []

    # Dimensions
    if dimensions:
        select_parts.extend(dimensions)

    # Aggregations
    if aggregations:
        for agg, col in aggregations:
            alias = f"{agg.lower()}_{col}"
            select_parts.append(f"{agg.upper()}({col}) AS {alias}")

    if custom_select:
        select_parts.extend(custom_select)

    query += ", ".join(select_parts)


    all_colums = []
    if dimensions: all_colums.extend(dimensions)
    if aggregations: all_colums.extend([agg[1] for agg in aggregations])
    if filters: all_colums.extend([f[0] for f in filters])
    if custom_select: all_colums.extend(custom_select)
    if group_by: all_colums.extend(group_by)
    if sort: all_colums.append(sort)


    if table:
        resolved_table = [table]
    else:
        # Search up unique table names robustly ignoring sql functions
        table_names = []
        for col_str in all_colums:
            words = set(re.findall(r'\b[a-zA-Z_]\w*\b', str(col_str)))
            for word in words:
                if word in COLUMN_MAP and word not in COMMON_COLUMNS:
                    table_names.append(COLUMN_MAP[word])

        resolved_table = list(set(table_names))

        #If none, it is the table channel_and_user
        if not resolved_table:
            resolved_table = ["channel_and_user"]

    query += f" FROM {resolved_table[0]}"

    # Filters
    if filters:
        conditions = []
        for f in filters:
            if len(f) == 3:
                col, op, val = f
                if op.upper() in ("IS NULL", "IS NOT NULL"):
                    conditions.append(f"{col} {op}")
                elif op.upper() == "IN":
                    placeholders = ", ".join(["%s"] * len(val))
                    conditions.append(f"{col} IN ({placeholders})")
                    params.extend(val)
                else:
                    conditions.append(f"{col} {op} %s")
                    params.append(val)
            else:
                conditions.append(f[0])

        query += " WHERE " + " AND ".join(conditions)

    # Group by
    if group_by:
        query += " GROUP BY " + ", ".join(group_by)
    elif dimensions and aggregations:
        query += " GROUP BY " + ", ".join(dimensions)

    # Sorting
    if sort:
        query += f" ORDER BY {sort} {order}"

    # Pagination
    if limit:
        query += " LIMIT %s"
        params.append(limit)

    if offset:
        query += " OFFSET %s"
        params.append(offset)

    # Query execution
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        data = cursor.fetchall()

    return data



if __name__ == "__main__":
    filters = [
        (
            "channel",
            "IN",
            ["A","B","C"]
        )
    ]
    print(analytics_query(dimensions= "channel",filters= filters, aggregations= ("sum","uploaded_count"), sort= "sum_uploaded_count", order= "DESC", limit= 5))

