import os
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import RedirectResponse
from datetime import datetime, timedelta
import uvicorn
from decimal import Decimal
from typing import Optional

# Import the existing query builder from analytics
from analytics import analytics_query

app = FastAPI(title="Executive Tab API")

@app.get("/", include_in_schema=False)
def docs_redirect():
    return RedirectResponse(url='/docs')

def safe_float(val):
    if val is None:
        return 0.0
    if isinstance(val, Decimal):
        return float(val)
    return float(val)

def safe_int(val):
    if val is None:
        return 0
    return int(val)


@app.get("/api/executive_tab")
def get_executive_tab_data(
    start_date: Optional[str] = Query(None, description="Start date in YYYY-MM-DD format (defaults to 30 days ago)"),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format (defaults to today)"),
    interval: str = Query("month", description="Interval for trends: day, week, month, quarter")
):
    """
    Fetches all Executive Tab data for a given date range and interval using analytics_query.
    If no dates are provided, defaults to the last 30 days.
    """
    try:
        if end_date:
            end_dt = datetime.strptime(end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S")
        else:
            end_dt = datetime.now()

        if start_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start_dt = end_dt - timedelta(days=30)
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD.")

    # Base date filters
    date_filters = [
        ("created_ts", ">=", start_dt),
        ("created_ts", "<=", end_dt)
    ]

    table_name = "video_list_data_synthesized"

    # -------------------------------------------------------------------------
    # 1. Top Level Metrics
    #    - Counts for uploads / created / published
    #    - Duration totals split by uploaded / created / published in ONE query
    # -------------------------------------------------------------------------
    top_metrics_select = [
        # Counts
        "COUNT(DISTINCT source)",                               # 0 - uploads
        "COUNT(video_id)",                                      # 1 - created
        "COUNT(video_id) FILTER (WHERE published = 'Yes')",     # 2 - published
        "COUNT(DISTINCT channel)",                              # 3 - active channels
        "COUNT(DISTINCT uploaded_by)",                          # 4 - active users
        "COUNT(DISTINCT type)",                                 # 5 - output types count
        # Duration totals
        "SUM(duration_s)",                                      # 6 - total uploaded duration (all rows)
        "SUM(duration_s)",                                      # 7 - total created duration (same — all rows are created)
        "SUM(duration_s) FILTER (WHERE published = 'Yes')",     # 8 - total published duration
    ]

    top_metrics_res = analytics_query(
        custom_select=top_metrics_select,
        table=table_name,
        filters=date_filters
    )

    top = top_metrics_res[0] if top_metrics_res else (0, 0, 0, 0, 0, 0, 0.0, 0.0, 0.0)

    uploads             = safe_int(top[0])
    created             = safe_int(top[1])
    published           = safe_int(top[2])
    active_channels     = safe_int(top[3])
    active_users        = safe_int(top[4])
    count_output_types  = safe_int(top[5])
    uploaded_duration_s = safe_float(top[6])
    created_duration_s  = safe_float(top[7])
    published_duration_s = safe_float(top[8])

    activity_score = created / uploads if uploads > 0 else 0
    quality_score  = published/uploads if uploads > 0 else 0

    # -------------------------------------------------------------------------
    # 2. Breakdowns
    # -------------------------------------------------------------------------

    # Output / Input Types
    output_types_res = analytics_query(
        dimensions=["type"],
        custom_select=["COUNT(video_id)", "SUM(duration_s)"],
        table=table_name,
        filters=date_filters,
        group_by=["type"],
        sort="COUNT(video_id)",
        order="DESC"
    )
    output_types = [
        {
            "type": r[0] if r[0] else "Unknown",
            "count": safe_int(r[1]),
            "total_duration_s": safe_float(r[2])
        }
        for r in output_types_res
    ]

    # Languages
    languages_res = analytics_query(
        dimensions=["language"],
        custom_select=["COUNT(video_id)", "SUM(duration_s)"],
        table=table_name,
        filters=date_filters,
        group_by=["language"],
        sort="COUNT(video_id)",
        order="DESC"
    )
    languages = [
        {
            "language": r[0] if r[0] else "Unknown",
            "count": safe_int(r[1]),
            "total_duration_s": safe_float(r[2])
        }
        for r in languages_res
    ]

    # Social Platforms
    platform_filters = date_filters.copy()
    platform_filters.extend([
        ("published", "=", "Yes"),
        ("published_platform", "IS NOT NULL", "")
    ])
    platforms_res = analytics_query(
        dimensions=["published_platform"],
        custom_select=["COUNT(video_id)", "SUM(duration_s)"],
        table=table_name,
        filters=platform_filters,
        group_by=["published_platform"],
        sort="COUNT(video_id)",
        order="DESC"
    )
    platforms = [
        {
            "platform": r[0],
            "count": safe_int(r[1]),
            "total_duration_s": safe_float(r[2])
        }
        for r in platforms_res
    ]

    # -------------------------------------------------------------------------
    # 3. Trends
    # -------------------------------------------------------------------------
    if interval.lower() == "quarter":
        trunc_interval = "month"
    else:
        trunc_interval = "day"

    trends_res = analytics_query(
        custom_select=[
            f"DATE_TRUNC('{trunc_interval}', created_ts) as period",
            "COUNT(DISTINCT source)",                               # uploads
            "COUNT(video_id)",                                      # created
            "COUNT(video_id) FILTER (WHERE published = 'Yes')",     # published
            "SUM(duration_s)",                                      # uploaded duration
            "SUM(duration_s)",                                      # created duration
            "SUM(duration_s) FILTER (WHERE published = 'Yes')",     # published duration
        ],
        table=table_name,
        filters=date_filters,
        group_by=[f"DATE_TRUNC('{trunc_interval}', created_ts)"],
        sort="period",
        order="ASC"
    )

    trends = []
    for r in trends_res:
        if r[0]:
            period_str = r[0].strftime("%Y-%m-%d") if isinstance(r[0], datetime) else str(r[0])
            trends.append({
                "period": period_str,
                "uploads": safe_int(r[1]),
                "created": safe_int(r[2]),
                "published": safe_int(r[3]),
                "uploaded_duration_s": safe_float(r[4]),
                "created_duration_s": safe_float(r[5]),
                "published_duration_s": safe_float(r[6]),
            })

    # -------------------------------------------------------------------------
    # 4. Hourly Distribution
    # -------------------------------------------------------------------------
    hourly_res = analytics_query(
        custom_select=[
            "EXTRACT(HOUR FROM created_ts) as hour",
            "COUNT(video_id)",
            "SUM(duration_s)"
        ],
        table=table_name,
        filters=date_filters,
        group_by=["EXTRACT(HOUR FROM created_ts)"],
        sort="EXTRACT(HOUR FROM created_ts)",
        order="ASC"
    )
    hourly = [
        {
            "hour": safe_int(r[0]),
            "count": safe_int(r[1]),
            "total_duration_s": safe_float(r[2])
        }
        for r in hourly_res if r[0] is not None
    ]

    # -------------------------------------------------------------------------
    # Response
    # -------------------------------------------------------------------------
    return {
        "overview": {
            "total_uploads": uploads,
            "total_created": created,
            "total_published": published,
            "active_channels": active_channels,
            "active_users": active_users,
            "activity_score": activity_score,
            "quality_score": quality_score,
            "no_of_output_types": {
                "count": count_output_types,
            },
            # ✅ NEW: duration totals split by status
            "duration_summary": {
                "uploaded_duration_s": uploaded_duration_s,
                "created_duration_s": created_duration_s,
                "published_duration_s": published_duration_s,
            }
        },
        "breakdowns": {
            "output_types": output_types,
            "input_types": output_types,
            "languages": languages,
            "social_platforms": platforms
        },
        "trends": {
            "interval": interval,
            "truncation_used": trunc_interval,
            "usage_trends": trends,       # ✅ each period now has uploaded/created/published_duration_s
            "hourly_distribution": hourly
        }
    }
    

@app.get("/api/debug")
def debug():
    import os
    try:
        from analytics import get_connection
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return {"status": "connected", "result": cursor.fetchone()}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


if __name__ == "__main__":
    uvicorn.run("new_executive_tab_api:app", host="0.0.0.0", port=8000, reload=True)