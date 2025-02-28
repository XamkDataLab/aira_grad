import os
import tempfile
import streamlit as st
import pandas as pd
import plotly.express as px
import numba

# Set environment variables before importing any numba-using libraries
os.environ["NUMBA_DISABLE_CACHE"] = "1"
cache_dir = os.path.join(os.getcwd(), ".numba_cache")
if not os.path.exists(cache_dir):
    os.makedirs(cache_dir)
os.environ["NUMBA_CACHE_DIR"] = cache_dir
numba.config.DISABLE_CACHE = True

# Optionally set the working directory to the directory of the script
if '__file__' in globals():
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Reduce top padding above the heading with a small CSS snippet.
st.markdown(
    """
    <style>
    .css-18e3th9 { padding-top: 10px; }
    </style>
    """,
    unsafe_allow_html=True
)

st.title("Palotapahtumien analyysi")

@st.cache_data(show_spinner=False)
def load_and_merge_data():
    # Load Events Data
    try:
        events = pd.read_csv('data/events.csv')
    except Exception as e:
        st.error(f"Error loading events data: {e}")
        return pd.DataFrame()
    
    if not events.empty:
        events['event_type'] = events['event_type'].str.lower()
        events['timestamp'] = pd.to_datetime(events['timestamp'])
        events['month'] = events['timestamp'].dt.strftime('%Y-%m')
        events['is_palo'] = events['event_type'].str.contains('palo', na=False).astype(int)
        palo_counts = events.groupby('month')['is_palo'].sum().reset_index(name='palo_count')
        palo_category_counts = (
            events[events['event_type'].str.contains('palo', na=False)]
            .groupby(['month', 'event_type'])
            .size()
            .reset_index(name='count')
        )
        def format_category(row):
            return f"{row['event_type']} ({row['count']})"
        palo_categories = (
            palo_category_counts.groupby('month')[['event_type', 'count']]
            .apply(lambda df: "<br>".join(df.sort_values('count', ascending=False)
                                           .apply(format_category, axis=1)))
            .reset_index(name='palo_events')
        )
    else:
        palo_counts = pd.DataFrame()
        palo_categories = pd.DataFrame()

    # Load Weather Data
    try:
        weather = pd.read_csv('data/weather.csv')
    except Exception as e:
        st.error(f"Error loading weather data: {e}")
        return pd.DataFrame()
    
    if not weather.empty:
        # Mapping Finnish column names to shorter English names
        column_mapping = {
            'asema': 'station',
            'keskilämpötila °c': 'avg_temp',
            'maksimilämpötila °c': 'max_temp',
            'minimilämpötila °c': 'min_temp',
            'sademäärä mm': 'precip_mm',
            'lumensyvyys cm': 'snow_cm',
            'date': 'date'
        }
        weather.rename(columns=lambda x: column_mapping.get(x.strip().lower(), x.strip().lower()), inplace=True)
        weather['date'] = pd.to_datetime(weather['date'])
        weather['month'] = weather['date'].dt.strftime('%Y-%m')
        avg_temp_month = weather.groupby('month')['avg_temp'].mean().reset_index(name='avg_temp')
        avg_precip_month = weather.groupby('month')['precip_mm'].mean().reset_index(name='avg_precip_mm')
        avg_max_temp = weather.groupby('month')['max_temp'].mean().reset_index(name='avg_max_temp')
        avg_min_temp = weather.groupby('month')['min_temp'].mean().reset_index(name='avg_min_temp')
        avg_snow_cm = weather.groupby('month')['snow_cm'].mean().reset_index(name='avg_snow_cm')
    else:
        avg_temp_month = pd.DataFrame()
        avg_precip_month = pd.DataFrame()
        avg_max_temp = pd.DataFrame()
        avg_min_temp = pd.DataFrame()
        avg_snow_cm = pd.DataFrame()

    # Merge into a Summary DataFrame
    if not palo_counts.empty and not avg_temp_month.empty and not avg_precip_month.empty:
        summary = pd.merge(palo_counts, avg_temp_month, on='month', how='left')
        summary = pd.merge(summary, palo_categories, on='month', how='left')
        summary = pd.merge(summary, avg_precip_month, on='month', how='left')
        summary = pd.merge(summary, avg_max_temp, on='month', how='left')
        summary = pd.merge(summary, avg_min_temp, on='month', how='left')
        summary = pd.merge(summary, avg_snow_cm, on='month', how='left')
    else:
        summary = pd.DataFrame()
    
    return summary

# Load and cache the summary data
summary = load_and_merge_data()

# Sidebar UI for x-axis selection
axis_choice = st.sidebar.selectbox(
    "Valitse X-akseli",
    ["Keskisademäärä", "Keskilämpötila", "Maksimilämpötila", "Minimilämpötila", "Lumensyvyys"],
    index=0
)

# Map the selection to the corresponding column name and label
axis_map = {
    "Keskisademäärä": ("avg_precip_mm", "Keskisademäärä (mm)"),
    "Keskilämpötila": ("avg_temp", "Keskilämpötila (°C)"),
    "Maksimilämpötila": ("avg_max_temp", "Maksimilämpötila (°C)"),
    "Minimilämpötila": ("avg_min_temp", "Minimilämpötila (°C)"),
    "Lumensyvyys": ("avg_snow_cm", "Lumensyvyys (cm)")
}
selected_x_col, selected_x_label = axis_map[axis_choice]

# Define a mapping from our x column name to its index in the customdata array.
# customdata order: [month, avg_temp, avg_precip_mm, avg_max_temp, avg_min_temp, avg_snow_cm, palo_count, palo_events]
customdata_index_map = {
    "avg_temp": 1,
    "avg_precip_mm": 2,
    "avg_max_temp": 3,
    "avg_min_temp": 4,
    "avg_snow_cm": 5
}
x_index = customdata_index_map[selected_x_col]

# Create the Plotly chart using the cached data
if not summary.empty:
    fig = px.scatter(
        summary,
        x=selected_x_col,
        y="palo_count",
        title="Palotapahtumien määrä vs. lämpötila/sademäärä",
        labels={
            selected_x_col: selected_x_label,
            "palo_count": "Palotapahtumien määrä"
        }
    )
    # Include additional weather metrics in customdata for the hover tooltip.
    customdata = summary[['month', 'avg_temp', 'avg_precip_mm', 'avg_max_temp', 'avg_min_temp', 'avg_snow_cm', 'palo_count', 'palo_events']].values

    # Dynamically build the hover template to show the selected X-axis value.
    hovertemplate = (
        "<b>Kuukausi</b>: %{customdata[0]}<br>" +
        f"<b>{selected_x_label}</b>: %{{customdata[{x_index}]:.2f}}<br>" +
        "<b>Palotapahtumien määrä</b>: %{customdata[6]}<br>" +
        "<b>Palotapahtumat</b>: %{customdata[7]}<extra></extra>"
    )
    
    fig.update_traces(
        marker=dict(color="navy", size=10, opacity=0.6),
        customdata=customdata,
        hovertemplate=hovertemplate
    )
    fig.update_layout(
        plot_bgcolor="white",
        paper_bgcolor="white",
        font=dict(color="black")
    )
    fig.update_xaxes(
        showline=True,
        linecolor="black",
        linewidth=1,
        gridcolor="lightgray",
        zeroline=False,
        tickfont=dict(color="black"),
        title_font=dict(color="black")
    )
    fig.update_yaxes(
        showline=True,
        linecolor="black",
        linewidth=1,
        gridcolor="lightgray",
        zeroline=False,
        tickfont=dict(color="black"),
        title_font=dict(color="black")
    )
    st.plotly_chart(fig)
else:
    st.write("Summary data is empty. Please check your CSV files.")

# Display data summary table at the end
st.write("Data summary (first few rows):", summary.head())
