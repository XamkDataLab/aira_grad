from db import *
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
from sqlalchemy.sql import text
import time


# Cache the database query function
@st.cache_data(ttl=3600)  # Cache for 1 hour
def cached_execute_query(query, params=None):
    return execute_query(query, params)


def rescue_events_dashboard():
    st.title("Vasteiden aikasarja-analyysi")

    # Start timer
    start_time = time.time()

    # Get date range - This doesn't change often, so cache it
    @st.cache_data(ttl=86400)  # Cache for 24 hours
    def get_date_range():
        date_range_query = """
        SELECT 
            MIN(timestamp) as min_date,
            MAX(timestamp) as max_date
        FROM tilanteet
        """
        return cached_execute_query(date_range_query)

    date_range_df = get_date_range()

    if date_range_df.empty or pd.isna(date_range_df['min_date'].iloc[0]):
        st.error("Could not retrieve date range from database.")
        return

    min_date = pd.to_datetime(date_range_df['min_date'].iloc[0]).date()
    max_date = pd.to_datetime(date_range_df['max_date'].iloc[0]).date()

    # Get event types - Cache this as it rarely changes
    @st.cache_data(ttl=86400)  # Cache for 24 hours
    def get_frequent_event_types():
        frequent_events_query = """
        SELECT event_type 
        FROM tilanteet 
        GROUP BY event_type 
        HAVING COUNT(*) > 100 
        ORDER BY event_type
        """
        return cached_execute_query(frequent_events_query)

    event_types_df = get_frequent_event_types()
    all_event_types = event_types_df['event_type'].tolist()

    # Get all municipalities - Cache this as it rarely changes
    @st.cache_data(ttl=86400)  # Cache for 24 hours
    def get_municipalities():
        municipalities_query = "SELECT DISTINCT municipality FROM tilanteet ORDER BY municipality"
        return cached_execute_query(municipalities_query)

    municipalities_df = get_municipalities()
    all_municipalities = municipalities_df['municipality'].tolist()
    
    # Get all hake values - Cache this as it rarely changes
    @st.cache_data(ttl=86400)  # Cache for 24 hours
    def get_hake_values():
        hake_query = "SELECT DISTINCT hake FROM tilanteet WHERE hake IS NOT NULL ORDER BY hake"
        return cached_execute_query(hake_query)

    hake_df = get_hake_values()
    all_hake_values = hake_df['hake'].tolist()

    # === UI Controls (Main area) ===
    st.subheader("Filter Options")

    # Date range
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Aloituspäivä", min_date, min_value=min_date, max_value=max_date)
    with col2:
        end_date = st.date_input("Lopetuspäivä", max_date, min_value=min_date, max_value=max_date)

    # Event types
    selected_event_types = st.multiselect(
        "Valitse vasteen tyyppi (vain ne joita yli 100 näytetään)",
        options=all_event_types,
        default=[]
    )

    # Municipalities
    selected_municipalities = st.multiselect(
        "Valitse kunta (valinnainen)",
        options=all_municipalities,
        default=[]
    )
    
    # Selected hake values
    selected_hake_values = st.multiselect(
        "Valitse hätäkeskus (valinnainen)",
        options=all_hake_values,
        default=[]
    )

    # Time aggregation
    time_aggregation = st.selectbox(
        "Aikajakson ryhmittely",
        options=["Päivä", "Viikko", "Kuukausi", "Vuosi"],
        index=0
    )

    # Convert to datetime
    start_datetime = datetime.combine(start_date, datetime.min.time())
    end_datetime = datetime.combine(end_date, datetime.max.time())

    if not selected_event_types:
        st.warning("Please select at least one event type.")
        return

    # Cache the filtered data queries
    @st.cache_data(ttl=3600, show_spinner=False)  # Cache for 1 hour
    def get_filtered_data(start_dt, end_dt, event_types, municipalities, hake_values, time_agg):
        # === Build Queries ===
        base_params = {"start_date": start_dt, "end_date": end_dt}

        selected_query = """
        SELECT 
            timestamp,
            event_type,
            hake
        FROM 
            tilanteet
        WHERE 
            timestamp BETWEEN :start_date AND :end_date
        """

        # Add filters
        if event_types:
            event_placeholders = ', '.join([f':event_type_{i}' for i in range(len(event_types))])
            selected_query += f" AND event_type IN ({event_placeholders})"
            for i, etype in enumerate(event_types):
                base_params[f"event_type_{i}"] = etype

        if municipalities:
            muni_placeholders = ', '.join([f':municipality_{i}' for i in range(len(municipalities))])
            selected_query += f" AND municipality IN ({muni_placeholders})"
            for i, muni in enumerate(municipalities):
                base_params[f"municipality_{i}"] = muni
                
        if hake_values:
            hake_placeholders = ', '.join([f':hake_{i}' for i in range(len(hake_values))])
            selected_query += f" AND hake IN ({hake_placeholders})"
            for i, hake in enumerate(hake_values):
                base_params[f"hake_{i}"] = hake

        total_query = """
        SELECT 
            timestamp,
            hake
        FROM 
            tilanteet
        WHERE 
            timestamp BETWEEN :start_date AND :end_date
        """
        total_params = base_params.copy()

        if municipalities:
            muni_placeholders = ', '.join([f':municipality_{i}' for i in range(len(municipalities))])
            total_query += f" AND municipality IN ({muni_placeholders})"
            
        if hake_values:
            hake_placeholders = ', '.join([f':hake_{i}' for i in range(len(hake_values))])
            total_query += f" AND hake IN ({hake_placeholders})"

        # Query Execution
        with st.spinner("Fetching data..."):
            total_df = cached_execute_query(total_query, total_params)
            selected_df = cached_execute_query(selected_query, base_params)

        return total_df, selected_df, time_agg

    # Process the data
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def process_data(total_df, selected_df, time_aggregation):
        if total_df.empty or selected_df.empty:
            return None, None, None
        
        total_df = total_df.copy()
        selected_df = selected_df.copy()
        
        total_df['timestamp'] = pd.to_datetime(total_df['timestamp'])
        selected_df['timestamp'] = pd.to_datetime(selected_df['timestamp'])

        # Time grouping
        if time_aggregation == "Päivä":
            total_df['time_group'] = total_df['timestamp'].dt.floor('D')
            selected_df['time_group'] = selected_df['timestamp'].dt.floor('D')
        elif time_aggregation == "Viikko":
            total_df['time_group'] = total_df['timestamp'].dt.to_period('W').dt.start_time
            selected_df['time_group'] = selected_df['timestamp'].dt.to_period('W').dt.start_time
        elif time_aggregation == "Kuukausi":
            total_df['time_group'] = total_df['timestamp'].dt.to_period('M').dt.start_time
            selected_df['time_group'] = selected_df['timestamp'].dt.to_period('M').dt.start_time
        elif time_aggregation == "Vuosi":
            total_df['time_group'] = total_df['timestamp'].dt.to_period('Y').dt.start_time
            selected_df['time_group'] = selected_df['timestamp'].dt.to_period('Y').dt.start_time

        # Group and calculate
        total_counts = total_df.groupby('time_group').size().reset_index(name='total_count')
        selected_counts = selected_df.groupby(['time_group', 'event_type']).size().reset_index(name='selected_count')

        merged_df = pd.merge(selected_counts, total_counts, on='time_group', how='left')
        merged_df['percentage'] = (merged_df['selected_count'] / merged_df['total_count'] * 100).round(2)

        return total_df, selected_df, merged_df
        
    # Process data by hake
    @st.cache_data(ttl=3600)  # Cache for 1 hour
    def process_data_by_hake(total_df, selected_df, time_aggregation):
        if total_df.empty or selected_df.empty:
            return None
            
        total_df = total_df.copy()
        selected_df = selected_df.copy()
        
        # Time grouping (Same as in process_data)
        if time_aggregation == "Päivä":
            total_df['time_group'] = total_df['timestamp'].dt.floor('D')
            selected_df['time_group'] = selected_df['timestamp'].dt.floor('D')
        elif time_aggregation == "Viikko":
            total_df['time_group'] = total_df['timestamp'].dt.to_period('W').dt.start_time
            selected_df['time_group'] = selected_df['timestamp'].dt.to_period('W').dt.start_time
        elif time_aggregation == "Kuukausi":
            total_df['time_group'] = total_df['timestamp'].dt.to_period('M').dt.start_time
            selected_df['time_group'] = selected_df['timestamp'].dt.to_period('M').dt.start_time
        elif time_aggregation == "Vuosi":
            total_df['time_group'] = total_df['timestamp'].dt.to_period('Y').dt.start_time
            selected_df['time_group'] = selected_df['timestamp'].dt.to_period('Y').dt.start_time
            
        # Group by hake and time
        total_counts_by_hake = total_df.groupby(['time_group', 'hake']).size().reset_index(name='total_count')
        selected_counts_by_hake = selected_df.groupby(['time_group', 'hake', 'event_type']).size().reset_index(name='selected_count')
        
        # Merge to calculate percentages within each hake
        merged_df_by_hake = pd.merge(
            selected_counts_by_hake, 
            total_counts_by_hake, 
            on=['time_group', 'hake'], 
            how='left'
        )
        
        # Calculate percentage within each hake group
        merged_df_by_hake['percentage'] = (merged_df_by_hake['selected_count'] / merged_df_by_hake['total_count'] * 100).round(2)
        
        return merged_df_by_hake

    # Get the data with caching
    total_df, selected_df, time_agg = get_filtered_data(
        start_datetime, 
        end_datetime, 
        selected_event_types, 
        selected_municipalities,
        selected_hake_values,
        time_aggregation
    )

    # Process the data with caching
    total_df, selected_df, merged_df = process_data(total_df, selected_df, time_aggregation)

    if merged_df is None:
        st.warning("No data available with the current filter settings.")
        return

    # Summary stats
    st.subheader("Yhteenveto")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Vasteita yhteensä", total_df.shape[0])
    with col2:
        st.metric("Valitut vasteet", selected_df.shape[0])
    with col3:
        percentage = (selected_df.shape[0] / total_df.shape[0] * 100) if total_df.shape[0] > 0 else 0
        st.metric("Prosenttia kaikista vasteista", f"{percentage:.2f}%")

    # Line chart
    st.subheader(f"Valitut vasteet prosentteina kaikista ({time_aggregation})")
    
    @st.cache_data  # Cache the chart generation
    def create_line_chart(merged_df, time_aggregation):
        fig = px.line(
            merged_df,
            x='time_group',
            y='percentage',
            color='event_type',
            labels={
                'time_group': 'Time',
                'percentage': 'Percentage of All Events (%)',
                'event_type': 'Event Type'
            }
        )
        fig.update_layout(
            xaxis_title='Time',
            yaxis_title='Percentage of All Events (%)',
            legend_title='Event Type',
            height=500
        )
        return fig
    
    fig = create_line_chart(merged_df, time_aggregation)
    st.plotly_chart(fig, use_container_width=True)

    # Button to break down by hake
    st.subheader("Hätäkeskus analyysi")
    show_hake_breakdown = st.button("Näytä vasteet hätäkeskuksittain")
    
    if show_hake_breakdown:
        # Process data by hake
        merged_df_by_hake = process_data_by_hake(total_df, selected_df, time_aggregation)
        
        if merged_df_by_hake is None or merged_df_by_hake.empty:
            st.warning("No data available for breakdown by Hake values. Please ensure you've selected Hake values in the filter.")
        else:
            # Create breakdown chart - combined visualization
            st.subheader(f"Valittujen vasteiden osuus hätäkeskusten kaikista vasteista ({time_aggregation})")
            
            @st.cache_data  # Cache the chart generation
            def create_hake_breakdown_chart(merged_df_by_hake, time_aggregation):
                # Create a figure for each event type
                event_types = merged_df_by_hake['event_type'].unique()
                
                # Initialize a dictionary to store figures
                event_figures = {}
                
                for event_type in event_types:
                    # Filter data for the current event type
                    event_data = merged_df_by_hake[merged_df_by_hake['event_type'] == event_type]
                    
                    # Create simplified hake labels for the legend
                    event_data['hake_label'] = 'Hake: ' + event_data['hake'].astype(str)
                    
                    # Create figure for this event type
                    fig = px.line(
                        event_data,
                        x='time_group',
                        y='percentage',
                        color='hake_label',
                        labels={
                            'time_group': 'Aika',
                            'percentage': 'Prosenttiosuus kaikista vasteista (%)',
                            'hake_label': 'Hätäkeskus'
                        }
                    )
                    
                    fig.update_layout(
                        title=f"Vasteen tyyppi: {event_type}",
                        xaxis_title='Aika',
                        yaxis_title='Prosenttiosuus kaikista vasteista (%)',
                        legend_title='Hätäkeskus',
                        height=400,  # Reduced height for each chart
                        legend=dict(
                            orientation="v",
                            yanchor="top",
                            y=0.99,
                            xanchor="right",
                            x=0.99
                        )
                    )
                    
                    event_figures[event_type] = fig
                
                return event_figures
            
            event_figures = create_hake_breakdown_chart(merged_df_by_hake, time_aggregation)
            
            # Display a chart for each event type
            for event_type, fig in event_figures.items():
                st.plotly_chart(fig, use_container_width=True)
                
            # Add explanation about the visualization
            st.info("Kaavio näyttää valitun vasteen prosenttiosuuden hätäkeskusten kaikista vasteista .")
            
            # Show data table for the hake breakdown
            st.subheader("Hätäkeskus data")
            st.dataframe(merged_df_by_hake.sort_values(['hake', 'time_group']))
            
            # Download hake breakdown data
            csv_hake = merged_df_by_hake.to_csv(index=False).encode('utf-8')
            st.download_button(
                "Lataa Data",
                csv_hake,
                "hake_breakdown_data.csv",
                "text/csv",
                key='download-hake-csv'
            )

    # Show raw data sample
    st.subheader("Data Sample")
    sample_df = selected_df.sort_values('timestamp', ascending=False).head(100)
    st.dataframe(sample_df)

    # Download
    csv = selected_df.to_csv(index=False).encode('utf-8')
    st.download_button(
        "Download Filtered Data",
        csv,
        "filtered_rescue_events.csv",
        "text/csv",
        key='download-csv'
    )

    # Add cache management widget
    with st.expander("Cache Management"):
        if st.button("Clear All Caches"):
            st.cache_data.clear()
            st.success("All caches cleared! Refresh the page to fetch new data.")

    # Show elapsed time
    elapsed = time.time() - start_time
    st.caption(f"⏱️ Visualization generated in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    rescue_events_dashboard()