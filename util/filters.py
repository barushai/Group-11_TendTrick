# util/filters.py
import streamlit as st
from datetime import date, datetime

def _coerce_date_obj(x):
    """Đưa x về datetime.date hoặc None."""
    if x is None:
        return None
    try:
        import pandas as pd
    except Exception:
        pd = None

    # Đã là date (không kèm thời gian)
    if isinstance(x, date) and not isinstance(x, datetime):
        return x
    # Là datetime -> lấy phần ngày
    if isinstance(x, datetime):
        return x.date()
    # Pandas Timestamp
    if pd is not None and isinstance(x, pd.Timestamp):
        return x.date()
    # Chuỗi ISO "YYYY-MM-DD" hoặc có thời gian "YYYY-MM-DD hh:mm:ss"
    if isinstance(x, str) and x.strip():
        try:
            return date.fromisoformat(x[:10])
        except Exception:
            return None
    return None

def sidebar_filters(min_d, max_d, countries: list[str], industries: list[str]):
    st.sidebar.header("Filters")
    c1, c2 = st.sidebar.columns(2)

    start_d = c1.date_input("Start date", value=_coerce_date_obj(min_d))
    end_d   = c2.date_input("End date",   value=_coerce_date_obj(max_d))

    country   = st.sidebar.multiselect("Countries",  options=["ALL"] + countries,  default=["ALL"])
    industry  = st.sidebar.multiselect("Industries", options=["ALL"] + industries, default=["ALL"])
    keyword   = st.sidebar.text_input("Keyword in hashtag", value="")
    topn      = st.sidebar.selectbox("Top N", options=[10,20,30,50,100], index=1)

    return (
        start_d.isoformat() if isinstance(start_d, date) else "",
        end_d.isoformat()   if isinstance(end_d,   date) else "",
        country, industry, keyword.strip().lower(), int(topn)
    )
