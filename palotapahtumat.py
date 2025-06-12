from db import *
import plotly.express as px
import pandas as pd

def palotapahtumat_etl(event_type_filter=None):
    """
    Load and merge fire event data with weather data.
    """
    where_clause = "WHERE lower(event_type) LIKE '%palo%'"
    
    if event_type_filter and len(event_type_filter) > 0:
        event_filter_parts = []
        for event_type in event_type_filter:
            clean_event_type = event_type.split(' (')[0] if ' (' in event_type else event_type
            event_filter_parts.append(f"lower(event_type) = '{clean_event_type.lower()}'")
        
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
    
    try:
        summary = execute_query(query)
        return summary
    except Exception as e:
        print(f"Error in palotapahtumat_etl: {e}")
        return pd.DataFrame()  

def create_fire_analysis_chart(axis_choice, selected_event_types):
    """
    Create fire analysis chart for Gradio
    """
    try:
        event_types_query = """
        SELECT DISTINCT lower(event_type) AS event_type, COUNT(*) AS count
        FROM tilanteet
        WHERE lower(event_type) LIKE '%palo%'
        GROUP BY lower(event_type)
        HAVING COUNT(*) > 100
        ORDER BY count DESC;
        """
        
        all_event_types_df = execute_query(event_types_query)
        
        if not all_event_types_df.empty:
            event_type_options = [f"{row['event_type']} ({row['count']})" for _, row in all_event_types_df.iterrows()]
        else:
            event_type_options = []
        
        if selected_event_types:
            summary = palotapahtumat_etl(event_type_filter=selected_event_types)
        else:
            summary = palotapahtumat_etl()
        
        if not isinstance(summary, pd.DataFrame) or summary.empty:
            return None, "No data available for the selected event types.", event_type_options
        
        axis_map = {
            "Keskisademäärä": ("avg_precip_mm", "Keskisademäärä (mm)"),
            "Keskilämpötila": ("avg_temp", "Keskilämpötila (°C)"),
            "Maksimilämpötila": ("avg_max_temp", "Maksimilämpötila (°C)"),
            "Minimilämpötila": ("avg_min_temp", "Minimilämpötila (°C)"),
            "Lumensyvyys": ("avg_snow_cm", "Lumensyvyys (cm)")
        }
        
        if axis_choice not in axis_map:
            return None, f"Invalid axis choice: {axis_choice}", event_type_options
            
        selected_x_col, selected_x_label = axis_map[axis_choice]
        
        if selected_x_col not in summary.columns or "palo_count" not in summary.columns:
            return None, f"Required columns not found in data. Available columns: {list(summary.columns)}", event_type_options
        
        clean_summary = summary.dropna(subset=[selected_x_col, "palo_count"])
        
        if clean_summary.empty:
            return None, f"No valid data available for {selected_x_label}", event_type_options
        
        fig = px.scatter(
            clean_summary,
            x=selected_x_col,
            y="palo_count",
            title="Palotapahtumien määrä vs. lämpötila/sademäärä",
            labels={
                selected_x_col: selected_x_label,
                "palo_count": "Palotapahtumien määrä"
            }
        )
        
        fig.update_traces(marker=dict(color="navy", size=10, opacity=0.6))
        fig.update_layout(
            plot_bgcolor="white",
            paper_bgcolor="white",
            font=dict(color="black"),
            height=600
        )
        
        info_text = ""
        if selected_event_types:
            info_text = f"Suodatin palotyyppit: {', '.join(selected_event_types)}"
        else:
            info_text = "Näytetään kaikki palotapahtumat"
        
        return fig, info_text, event_type_options
        
    except Exception as e:
        print(f"Error in create_fire_analysis_chart: {e}")
        return None, f"Error creating chart: {str(e)}", []