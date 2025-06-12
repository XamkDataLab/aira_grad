import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from db import *

def load_municipality_data():
    """Load and preprocess data from PostgreSQL for municipality analysis"""
    
    population_query = """
    SELECT alue, vakiluku 
    FROM vakiluvut 
    WHERE vuosi = 2024
    """
    
    incidents_query = """
    SELECT municipality, event_type, COUNT(*) as incident_count
    FROM tilanteet 
    GROUP BY municipality, event_type
    HAVING COUNT(*) > 100
    """
    
    population_df = execute_query(population_query)
    incidents_df = execute_query(incidents_query)
    
    population_df = population_df.rename(columns={'alue': 'municipality'})
    
    merged_df = incidents_df.merge(
        population_df, 
        on='municipality', 
        how='inner'
    )
    
    event_types = sorted(merged_df['event_type'].unique().tolist())
    
    return merged_df, event_types

def calculate_municipality_rates(df, selected_event_types):
    """Calculate normalized incident rates for selected event types (per 1000 inhabitants)"""
    
    filtered_df = df[df['event_type'].isin(selected_event_types)].copy()
    
    filtered_df['normalized_rate'] = (
        filtered_df['incident_count'] / filtered_df['vakiluku'] * 1000
    )
    
    filtered_df['normalized_rate'] = filtered_df['normalized_rate'].round(2)
    
    return filtered_df

def get_top_municipalities_analysis(df, selected_event_types):
    """Get top 10 municipalities for each selected event type"""
    
    normalized_df = calculate_municipality_rates(df, selected_event_types)
    
    results = []
    
    for event_type in selected_event_types:
        event_data = normalized_df[normalized_df['event_type'] == event_type].copy()
        
        
        top_10 = event_data.nlargest(10, 'normalized_rate')
        
        top_10['rank'] = range(1, len(top_10) + 1)
        
        top_10['selected_event_type'] = event_type
        
        results.append(top_10)
    
    if results:
        final_df = pd.concat(results, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()

def create_municipality_visualization(df):
    """Create bar chart visualization for municipality analysis"""
    
    if df.empty:
        return go.Figure().add_annotation(text="Ei dataa näytettäväksi", 
                                        xref="paper", yref="paper", 
                                        x=0.5, y=0.5, showarrow=False)
    
    event_types = df['event_type'].unique()
    
    if len(event_types) == 1:
        event_data = df[df['event_type'] == event_types[0]].head(10)
        
        fig = px.bar(
            event_data,
            x='normalized_rate',
            y='municipality',
            orientation='h',
            title=f'Top 10 kuntaa: {event_types[0]}',
            labels={
                'normalized_rate': 'Tapauksia per 1,000 asukasta',
                'municipality': 'Kunta'
            },
            text='normalized_rate'
        )
        
        fig.update_traces(texttemplate='%{text}', textposition='outside')
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
        
    else:
        # Multiple event types - grouped bar chart
        fig = px.bar(
            df,
            x='normalized_rate',
            y='municipality',
            color='event_type',
            orientation='h',
            title=f'Top 10 kuntaa tapahtumatyypeittäin',
            labels={
                'normalized_rate': 'Tapauksia per 1,000 asukasta',
                'municipality': 'Kunta',
                'event_type': 'Tapahtumatyyppi'
            },
            barmode='group'
        )
        
        fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    
    fig.update_layout(
        height=max(400, len(df) * 25),
        margin=dict(l=20, r=20, t=40, b=20)
    )
    
    return fig

def format_municipality_table(df):
    """Format results for display table"""
    
    if df.empty:
        return pd.DataFrame({"Viesti": ["Ei dataa saatavilla valituilla kriteereillä"]})
    
    display_df = df[[
        'rank', 'municipality', 'event_type', 'incident_count', 
        'vakiluku', 'normalized_rate'
    ]].copy()
    
    display_df.columns = [
        'Sija', 'Kunta', 'Tapahtumatyyppi', 'Tapauksia yhteensä', 
        'Väkiluku', 'Tapauksia per 1,000'
    ]
    
    display_df['Väkiluku'] = display_df['Väkiluku'].apply(lambda x: f"{x:,}")
    
    return display_df

def analyze_municipality_incidents(selected_event_types, municipality_data_df):
    """Main analysis function for municipality incidents (per 1000 inhabitants)"""
    
    if not selected_event_types:
        empty_df = pd.DataFrame({"Viesti": ["Valitse vähintään yksi tapahtumatyyppi"]})
        empty_fig = go.Figure().add_annotation(text="Valitse tapahtumatyyppejä", 
                                             xref="paper", yref="paper", 
                                             x=0.5, y=0.5, showarrow=False)
        return empty_df, empty_fig, "Valitse tapahtumatyyppejä analysoitavaksi."
    
    
    top_municipalities = get_top_municipalities_analysis(municipality_data_df, selected_event_types)
    
    results_table = format_municipality_table(top_municipalities)
    
    chart = create_municipality_visualization(top_municipalities)
    
    if not top_municipalities.empty:
        total_incidents = top_municipalities['incident_count'].sum()
        total_municipalities = len(top_municipalities['municipality'].unique())
        summary = f"""
        **Analyysin yhteenveto:**
        - Analysoidut tapahtumatyypit: {', '.join(selected_event_types)}
        - Tapauksia yhteensä top-kunnissa: {total_incidents:,}
        - Näytettäviä kuntia: {total_municipalities}
        - Normalisointi: per 1,000 asukasta
        """
    else:
        summary = "Ei dataa saatavilla valituilla kriteereillä."
    
    return results_table, chart, summary