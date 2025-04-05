from db import *
import plotly.express as px

def palotapahtumat_etl(event_type_filter=None):
    """
    Load and merge fire event data with weather data.
    
    Parameters:
        event_type_filter (list): Optional list of event types to filter by
    
    Returns:
        pandas.DataFrame: Summary data for fire analysis
    """
    # Base WHERE clause for filtering event types
    where_clause = "WHERE lower(event_type) LIKE '%palo%'"
    
    # Add additional filtering if event types are specified
    if event_type_filter and len(event_type_filter) > 0:
        # Create a parameterized filter for specific event types
        event_filter_parts = []
        for event_type in event_type_filter:
            # Extract the actual event type without the count in parentheses
            clean_event_type = event_type.split(' (')[0] if ' (' in event_type else event_type
            event_filter_parts.append(f"lower(event_type) = '{clean_event_type.lower()}'")
        
        # Combine the event type filters with OR
        if event_filter_parts:
            where_clause = f"WHERE ({' OR '.join(event_filter_parts)})"
    
    query = f"""
    WITH event_summary AS (
        SELECT 
            to_char(timestamp, 'YYYY-MM') AS month,
            SUM(1) AS palo_count
        FROM tilanteet
        {where_clause}
        GROUP BY month
    ),
    event_categories AS (
        SELECT 
            month,
            STRING_AGG(event_desc, '<br>' ORDER BY cnt DESC) AS palo_events
        FROM (
            SELECT 
                to_char(timestamp, 'YYYY-MM') AS month,
                lower(event_type) || ' (' || COUNT(*) || ')' AS event_desc,
                COUNT(*) AS cnt
            FROM tilanteet
            {where_clause}
            GROUP BY month, lower(event_type)
        ) ec
        GROUP BY month
    ),
    weather_summary AS (
        SELECT 
            to_char(paivamaara, 'YYYY-MM') AS month,
            AVG(keskilampotila::numeric) AS avg_temp,
            AVG(sademaara::numeric) AS avg_precip_mm,
            AVG(maksimilampotila::numeric) AS avg_max_temp,
            AVG(minimilampotila::numeric) AS avg_min_temp,
            AVG(lumensyvyys::numeric) AS avg_snow_cm
        FROM saatilat
        GROUP BY month
    )
    SELECT 
        e.month,
        e.palo_count,
        ec.palo_events,
        w.avg_temp,
        w.avg_precip_mm,
        w.avg_max_temp,
        w.avg_min_temp,
        w.avg_snow_cm
    FROM event_summary e
    LEFT JOIN event_categories ec ON e.month = ec.month
    LEFT JOIN weather_summary w ON e.month = w.month
    ORDER BY e.month;
    """
    
    summary = execute_query(query)
    return summary

def create_fire_analysis_chart(summary=None, event_type_filter=None):
    """
    Create and display a fire analysis chart based on the provided summary data.
    If summary is None, it will load the data using palotapahtumat_etl().
    
    Parameters:
        summary (pandas.DataFrame): Pre-loaded summary data (optional)
        event_type_filter (list): List of event types to filter by (optional)
    
    Returns:
        pandas.DataFrame: The summary data used for the chart
    """
    # Get list of available event types first to populate the dropdown
    # This query retrieves all unique event types
    event_types_query = """
    SELECT DISTINCT lower(event_type) AS event_type, COUNT(*) AS count
    FROM tilanteet
    WHERE lower(event_type) LIKE '%palo%'
    GROUP BY lower(event_type)
    HAVING COUNT(*) > 100
    ORDER BY count DESC;
    """
    all_event_types = execute_query(event_types_query)
    
    # Format event types with counts for display
    event_type_options = [f"{row['event_type']} ({row['count']})" for _, row in all_event_types.iterrows()]
    
    # Create a row with two columns for filters
    col1, col2 = st.columns(2)
    
    # X-axis selection in the first column
    with col1:
        axis_choice = st.selectbox(
            "Valitse X-akseli",
            ["Keskisademäärä", "Keskilämpötila", "Maksimilämpötila", "Minimilämpötila", "Lumensyvyys"],
            index=0
        )
    
    # Add multiselect filter for event type in the second column
    with col2:
        selected_event_types = st.multiselect(
            "Valitse palotapahtuman tyyppi",
            options=event_type_options,
            default=[]  # Default to no selection, which will show all types
        )
    
    # If event types are selected, apply the filter at the database query level
    if selected_event_types and summary is None:
        summary = palotapahtumat_etl(event_type_filter=selected_event_types)
    elif summary is None:
        summary = palotapahtumat_etl()

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

    # Create the Plotly chart using the filtered data
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
        
        # Display event type filter information if any are selected
        if selected_event_types:
            st.info(f"Suodatin palotyyppit: {', '.join(selected_event_types)}")
        
        # Display data summary table at the end
        st.write("Data summary (first few rows):", summary.head())
    else:
        st.warning("No data available for the selected event types. Please adjust your filter.")
    
    return summary