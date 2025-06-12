from db import *
import pandas as pd
import gradio as gr
import plotly.express as px
from datetime import datetime, timedelta
from functools import lru_cache

@lru_cache(maxsize=128, typed=True)
def cached_execute_query(query, params_tuple=None):
    """Cache database queries. Params must be hashable (tuple instead of dict)"""
    if params_tuple:
        params = dict(params_tuple)
    else:
        params = None
    return execute_query(query, params)

def get_data_ranges():
    """Get data ranges for date pickers and dropdown options"""
    try:
        date_range_query = """
        SELECT 
            MIN(timestamp) as min_date,
            MAX(timestamp) as max_date
        FROM tilanteet
        """
        date_range_df = cached_execute_query(date_range_query, ())
        
        if date_range_df.empty or pd.isna(date_range_df['min_date'].iloc[0]):
            return None, None, [], [], []
        
        min_date = pd.to_datetime(date_range_df['min_date'].iloc[0]).date()
        max_date = pd.to_datetime(date_range_df['max_date'].iloc[0]).date()
        
        frequent_events_query = """
        SELECT event_type 
        FROM tilanteet 
        GROUP BY event_type 
        HAVING COUNT(*) > 100 
        ORDER BY event_type
        """
        event_types_df = cached_execute_query(frequent_events_query, ())
        all_event_types = event_types_df['event_type'].tolist() if not event_types_df.empty else []
        
        municipalities_query = "SELECT DISTINCT municipality FROM tilanteet ORDER BY municipality"
        municipalities_df = cached_execute_query(municipalities_query, ())
        all_municipalities = municipalities_df['municipality'].tolist() if not municipalities_df.empty else []
        
        hake_query = "SELECT DISTINCT hake FROM tilanteet WHERE hake IS NOT NULL ORDER BY hake"
        hake_df = cached_execute_query(hake_query, ())
        all_hake_values = hake_df['hake'].tolist() if not hake_df.empty else []
        
        return min_date, max_date, all_event_types, all_municipalities, all_hake_values
        
    except Exception as e:
        print(f"Error getting data ranges: {e}")
        return None, None, [], [], []
    
def update_rescue_events(start, end, events, munis, hakes, time_agg, show_hake):
    try:
        result = process_rescue_events(start, end, events, munis, hakes, time_agg, show_hake)
        
        if result is None or len(result) != 4:
            return None, "No data available", pd.DataFrame(), gr.Column(visible=False), *[None]*5
        
        fig, hake_figs, summary, sample = result
    
        if hake_figs is None or not isinstance(hake_figs, list):
            hake_figs = []
        
        if sample is None:
            sample = pd.DataFrame()
        
        container_visibility = show_hake and len(hake_figs) > 0
        outputs = [fig, summary, sample]
        
        for i, hake_chart in enumerate(hake_chart_outputs):
            if i < len(hake_figs) and hake_figs[i] is not None:
                outputs.append(gr.Plot(value=hake_figs[i], visible=True))
            else:
                outputs.append(gr.Plot(visible=False))
        
        return outputs
        
    except Exception as e:
        print(f"Error in update_rescue_events: {e}")
        return None, f"Error: {str(e)}", pd.DataFrame(), *[gr.Plot(visible=False)]*5
    


def process_rescue_events(start_date, end_date, selected_event_types, selected_municipalities, 
                         selected_hake_values, time_aggregation, show_hake_breakdown):
    """Process rescue events data and create visualizations"""
    
    try:
        if not selected_event_types:
            return None, [], "Please select at least one event type.", pd.DataFrame()
        
        if isinstance(start_date, str):
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start_datetime = datetime.combine(start_date, datetime.min.time())
        
        if isinstance(end_date, str):
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)
        else:
            end_datetime = datetime.combine(end_date, datetime.max.time())
        
        base_params = [("start_date", start_datetime), ("end_date", end_datetime)]
        
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
        
        if selected_event_types:
            event_placeholders = ', '.join([f':event_type_{i}' for i in range(len(selected_event_types))])
            selected_query += f" AND event_type IN ({event_placeholders})"
            for i, etype in enumerate(selected_event_types):
                base_params.append((f"event_type_{i}", etype))
        
        if selected_municipalities:
            muni_placeholders = ', '.join([f':municipality_{i}' for i in range(len(selected_municipalities))])
            selected_query += f" AND municipality IN ({muni_placeholders})"
            for i, muni in enumerate(selected_municipalities):
                base_params.append((f"municipality_{i}", muni))
        
        if selected_hake_values:
            hake_placeholders = ', '.join([f':hake_{i}' for i in range(len(selected_hake_values))])
            selected_query += f" AND hake IN ({hake_placeholders})"
            for i, hake in enumerate(selected_hake_values):
                base_params.append((f"hake_{i}", hake))
        
        total_query = """
        SELECT 
            timestamp,
            hake
        FROM 
            tilanteet
        WHERE 
            timestamp BETWEEN :start_date AND :end_date
        """
        
        total_params = [("start_date", start_datetime), ("end_date", end_datetime)]
        
        if selected_municipalities:
            muni_placeholders = ', '.join([f':municipality_{i}' for i in range(len(selected_municipalities))])
            total_query += f" AND municipality IN ({muni_placeholders})"
            for i, muni in enumerate(selected_municipalities):
                total_params.append((f"municipality_{i}", muni))
        
        if selected_hake_values:
            hake_placeholders = ', '.join([f':hake_{i}' for i in range(len(selected_hake_values))])
            total_query += f" AND hake IN ({hake_placeholders})"
            for i, hake in enumerate(selected_hake_values):
                total_params.append((f"hake_{i}", hake))
        
       
        total_df = cached_execute_query(total_query, tuple(total_params))
        selected_df = cached_execute_query(selected_query, tuple(base_params))
        
        if total_df is None or selected_df is None or total_df.empty or selected_df.empty:
            return None, [], "No data available with the current filter settings.", pd.DataFrame()
        
        total_df = total_df.copy()
        selected_df = selected_df.copy()
        total_df['timestamp'] = pd.to_datetime(total_df['timestamp'])
        selected_df['timestamp'] = pd.to_datetime(selected_df['timestamp'])
        
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
        
        total_counts = total_df.groupby('time_group').size().reset_index(name='total_count')
        selected_counts = selected_df.groupby(['time_group', 'event_type']).size().reset_index(name='selected_count')
        
        merged_df = pd.merge(selected_counts, total_counts, on='time_group', how='left')
        merged_df['percentage'] = (merged_df['selected_count'] / merged_df['total_count'] * 100).round(2)
        
        fig = px.line(
            merged_df,
            x='time_group',
            y='percentage',
            color='event_type',
            labels={
                'time_group': 'Aika',
                'percentage': 'Prosenttiosuus kaikista vasteista (%)',
                'event_type': 'Vasteen tyyppi'
            },
            title=f"Valitut vasteet prosentteina kaikista ({time_aggregation})"
        )
        fig.update_layout(
            height=500,
            xaxis_title='Aika',
            yaxis_title='Prosenttiosuus kaikista vasteista (%)',
            legend_title='Vasteen tyyppi'
        )
        
       
        total_events = total_df.shape[0]
        selected_events = selected_df.shape[0]
        percentage = (selected_events / total_events * 100) if total_events > 0 else 0
        
        summary_text = f"""
**Yhteenveto:**
- Vasteita yhteensä: {total_events:,}
- Valitut vasteet: {selected_events:,}
- Prosenttia kaikista vasteista: {percentage:.2f}%
        """
        
        hake_figs = []
        if show_hake_breakdown:
            print(f"Creating hake breakdown charts...")
            print(f"Selected hake values: {selected_hake_values}")
            print(f"Available hake values in data: {selected_df['hake'].unique()}")
            
            if not selected_hake_values:
                available_hakes = selected_df['hake'].dropna().unique()
                print(f"Using all available hakes: {available_hakes}")
            else:
                available_hakes = selected_hake_values
            
            total_counts_by_hake = total_df.groupby(['time_group', 'hake']).size().reset_index(name='total_count')
            selected_counts_by_hake = selected_df.groupby(['time_group', 'hake', 'event_type']).size().reset_index(name='selected_count')
            
            merged_df_by_hake = pd.merge(
                selected_counts_by_hake, 
                total_counts_by_hake, 
                on=['time_group', 'hake'], 
                how='left'
            )
            
            merged_df_by_hake['percentage'] = (merged_df_by_hake['selected_count'] / merged_df_by_hake['total_count'] * 100).round(2)
            
            print(f"Merged hake data shape: {merged_df_by_hake.shape}")
            print(f"Event types in hake data: {merged_df_by_hake['event_type'].unique()}")
            
            event_types = merged_df_by_hake['event_type'].unique()
            print(f"Creating charts for event types: {event_types}")
            
            for i, event_type in enumerate(event_types[:5]): 
                print(f"Creating chart {i+1} for event type: {event_type}")
                
                event_data = merged_df_by_hake[merged_df_by_hake['event_type'] == event_type].copy()
                
                if event_data.empty:
                    print(f"No data for event type: {event_type}")
                    continue
                
                event_data['hake_label'] = 'Hätäkeskus ' + event_data['hake'].astype(str)
                
                print(f"Event data shape for {event_type}: {event_data.shape}")
                print(f"Hake values in event data: {event_data['hake'].unique()}")
                
                colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', 
                         '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
                
                hake_fig = px.line(
                    event_data,
                    x='time_group',
                    y='percentage',
                    color='hake_label',
                    title=f"Vasteen tyyppi: {event_type} (hätäkeskuksittain)",
                    labels={
                        'time_group': 'Aika',
                        'percentage': 'Prosenttiosuus hätäkeskuksen kaikista vasteista (%)',
                        'hake_label': 'Hätäkeskus'
                    },
                    color_discrete_sequence=colors
                )
                hake_fig.update_layout(
                    height=400,
                    xaxis_title='Aika',
                    yaxis_title='Prosenttiosuus hätäkeskuksen kaikista vasteista (%)',
                    legend_title='Hätäkeskus',
                    showlegend=True
                )
                
                hake_fig.update_traces(
                    line=dict(width=2),
                    marker=dict(size=4)
                )
                hake_figs.append(hake_fig)
                print(f"Created chart {i+1} successfully")
            
            print(f"Total hake charts created: {len(hake_figs)}")
        
        sample_data = selected_df.head(100)
        
        return fig, hake_figs, summary_text, sample_data
        
    except Exception as e:
        print(f"Error in process_rescue_events: {e}")
        import traceback
        traceback.print_exc()
        return None, [], f"Error processing data: {str(e)}", pd.DataFrame()