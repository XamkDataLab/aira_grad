"""
Institution Analysis Tab for Gradio App
Provides institution mention analysis with time-series visualization
"""

import gradio as gr
import plotly.graph_objects as go
import json
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
from db import execute_query
# Import your execute_query function
# from your_db_module import execute_query

# ============================================================================
# CONFIGURATION: Manually excluded actors
# Add actor names here that you want to exclude from Top 10 Actors list
# ============================================================================

def debug_key_events():
    """Debug function to see what key_events contains"""
    query = """
    SELECT analyze_institutions(
        p_institution_names := ARRAY['Ministry of Emergency Situations'],
        p_date_from := CURRENT_DATE - 180,
        p_date_to := CURRENT_DATE,
        p_time_bucket := 'week'
    )
    """
    
    df, error = execute_query(query)
    
    if error or df.empty:
        print(f"Error: {error}")
        return
    
    result_json = df.iloc[0]['analyze_institutions']
    data = json.loads(result_json) if isinstance(result_json, str) else result_json
    
    if data.get('key_events') and len(data['key_events']) > 0:
        print("First key event structure:")
        print(json.dumps(data['key_events'][0], indent=2))
    else:
        print("No key events")

# Call this once when the app loads
debug_key_events()

EXCLUDED_ACTORS = [
    "Example Excluded Actor",  # Replace with actual names to exclude
]


def load_institutions_list():
    """Load all institutions for the dropdown"""
    query = """
        SELECT * FROM get_institutions_list(NULL, 500)
        ORDER BY article_count DESC
    """
    
    df, error = execute_query(query)
    
    if error:
        print(f"Error loading institutions: {error}")
        return []
    
    if df.empty:
        return []
    
    # Create choices as "Institution Name (count)" for better UX
    institutions = []
    
    for _, row in df.iterrows():
        name = row['institution_name']
        count = row['article_count']
        display_name = f"{name} ({count:,} articles)"
        institutions.append(display_name)
    
    return institutions


def load_government_functions():
    """Load all government functions for filter dropdown"""
    query = "SELECT * FROM get_government_functions_list();"
    
    df, error = execute_query(query)
    
    if error:
        print(f"Error loading government functions: {error}")
        return []
    
    if df.empty:
        return []
    
    return df['function_name'].tolist()


def load_interaction_types():
    """Load all interaction types for filter dropdown"""
    query = "SELECT * FROM get_interaction_types_list();"
    
    df, error = execute_query(query)
    
    if error:
        print(f"Error loading interaction types: {error}")
        return []
    
    if df.empty:
        return []
    
    return df['interaction_type'].tolist()


def format_actors_list(actors_data):
    """Format top actors as HTML, excluding manually specified actors"""
    if not actors_data:
        return "<p>No actors found</p>"
    
    # Filter out excluded actors
    filtered_actors = [
        actor for actor in actors_data 
        if actor.get('actor_name', '') not in EXCLUDED_ACTORS
    ]
    
    if not filtered_actors:
        return "<p>No actors found (all filtered)</p>"
    
    html = "<div style='max-height: 500px; overflow-y: auto;'>"
    html += "<h3>Top 10 Actors</h3>"
    
    for i, actor in enumerate(filtered_actors[:10], 1):
        name = actor.get('actor_name', 'Unknown')
        position = actor.get('actor_position', 'N/A')
        role = actor.get('actor_role', 'N/A')
        mentions = actor.get('mention_count', 0)
        level = actor.get('government_level', 'N/A')
        
        html += f"""
        <div style='margin-bottom: 15px; padding: 10px; background-color: #f5f5f5; border-radius: 5px;'>
            <div style='font-weight: bold; font-size: 16px;'>{i}. {name}</div>
            <div style='margin-top: 5px; color: #555;'>
                <strong>Position:</strong> {position}<br>
                <strong>Role:</strong> {role}<br>
                <strong>Level:</strong> {level}<br>
                <strong>Mentions:</strong> {mentions:,}
            </div>
        </div>
        """
    
    html += "</div>"
    return html


def format_key_events(events_data):
    """Format key events grouped by URL with source information"""
    if not events_data:
        return "<p>No key events found</p>"
    
    # Group events by URL (already sorted by newest first in SQL)
    events_by_url = defaultdict(list)
    for event in events_data:
        url = event.get('url', 'Unknown URL')
        events_by_url[url].append(event)
    
    html = "<div style='max-height: 500px; overflow-y: auto;'>"
    html += "<h3>Key Events</h3>"
    
    for url, url_events in events_by_url.items():
        # Get publish date from first event (they all have same date for same URL)
        publish_date = url_events[0].get('publish_date', 'Unknown date')
        
        # Format date nicely if it's a valid date
        try:
            if publish_date and publish_date != 'Unknown date':
                date_obj = pd.to_datetime(publish_date)
                formatted_date = date_obj.strftime('%Y-%m-%d')
            else:
                formatted_date = 'Unknown date'
        except:
            formatted_date = str(publish_date)
        
        # First event is the main headline
        main_event = url_events[0]
        main_description = main_event.get('event_description', 'Unknown event')
        significance = main_event.get('significance', '')
        
        html += f"""
        <div style='margin-bottom: 20px; padding: 12px; background-color: #f0f8ff; border-left: 4px solid #2E86AB; border-radius: 5px;'>
            <div style='font-weight: bold; font-size: 15px; margin-bottom: 8px;'>{main_description}</div>
        """
        
        # Show significance if available
        if significance:
            html += f"<div style='margin-bottom: 8px; font-style: italic; color: #666; font-size: 13px;'>{significance}</div>"
        
        # Show source (URL and date)
        # Truncate URL if too long
        display_url = url if len(url) < 80 else url[:77] + '...'
        html += f"""
            <div style='margin-top: 8px; padding: 6px; background-color: #fff; border-radius: 3px; font-size: 12px; color: #555;'>
                <strong>Source:</strong> <a href="{url}" target="_blank" style="color: #2E86AB;">{display_url}</a><br>
                <strong>Date:</strong> {formatted_date}
            </div>
        """
        
        # If there are additional events from the same URL, show them underneath
        if len(url_events) > 1:
            html += "<div style='margin-top: 10px; margin-left: 15px; border-left: 2px solid #ccc; padding-left: 10px;'>"
            html += "<div style='font-size: 12px; color: #888; margin-bottom: 5px;'><em>Additional events from this source:</em></div>"
            
            for sub_event in url_events[1:]:
                sub_description = sub_event.get('event_description', 'Unknown event')
                sub_significance = sub_event.get('significance', '')
                
                html += f"""
                <div style='margin-bottom: 8px; padding: 6px; background-color: #fafafa; border-radius: 3px; font-size: 13px;'>
                    <div style='font-weight: 500;'>{sub_description}</div>
                """
                
                if sub_significance:
                    html += f"<div style='margin-top: 3px; font-style: italic; color: #777; font-size: 12px;'>{sub_significance}</div>"
                
                html += "</div>"
            
            html += "</div>"
        
        html += "</div>"
    
    html += "</div>"
    return html


def analyze_institution(selected_display_name, date_range_months, time_bucket, gov_functions, interactions):
    """
    Analyze selected institution and create visualization
    
    Args:
        selected_display_name: Display name from dropdown
        date_range_months: Number of months to analyze
        time_bucket: Aggregation level ('day', 'week', 'month')
        gov_functions: List of selected government functions
        interactions: List of selected interaction types
    """
    if not selected_display_name:
        return None, "<p>Please select an institution</p>", "<p>Please select an institution</p>"
    
    # Extract actual institution name from display name
    institution_name = selected_display_name.split(" (")[0]
    
    # Escape single quotes in institution name
    institution_name_escaped = institution_name.replace("'", "''")
    
    # Build date filter
    if date_range_months == "all":
        date_from = "NULL"
        date_to = "NULL"
    else:
        date_from = f"(CURRENT_DATE - INTERVAL '{date_range_months} months')::DATE"
        date_to = "CURRENT_DATE"
    
    # Build gov functions filter
    if gov_functions and len(gov_functions) > 0:
        # Escape and format for PostgreSQL array
        escaped_functions = [f.replace("'", "''") for f in gov_functions]
        gov_functions_param = "ARRAY['" + "','".join(escaped_functions) + "']"
    else:
        gov_functions_param = "NULL"
    
    # Build interactions filter
    if interactions and len(interactions) > 0:
        # Escape and format for PostgreSQL array
        escaped_interactions = [i.replace("'", "''") for i in interactions]
        interactions_param = "ARRAY['" + "','".join(escaped_interactions) + "']"
    else:
        interactions_param = "NULL"
    
    # Query the database
    query = f"""
        SELECT analyze_institutions(
            p_institution_names := ARRAY['{institution_name_escaped}'],
            p_date_from := {date_from},
            p_date_to := {date_to},
            p_time_bucket := '{time_bucket}',
            p_gov_functions := {gov_functions_param},
            p_interactions := {interactions_param}
        )
    """
    
    df, error = execute_query(query)
    
    if error:
        return None, f"<p>Error: {error}</p>", ""
    
    if df.empty:
        return None, "<p>No data found</p>", ""
    
    try:
        # Get the JSON result from the first row
        result_json = df.iloc[0]['analyze_institutions']
        
        # Parse if it's a string
        if isinstance(result_json, str):
            data = json.loads(result_json)
        else:
            data = result_json
        
        # Extract data
        time_series = data.get('time_series', [])
        top_actors = data.get('top_actors', [])
        key_events = data.get('key_events', [])
        
        if not time_series:
            return None, "<p>No time series data found</p>", ""
        
        # Prepare data for plotting
        dates = []
        percentages = []
        institution_counts = []
        total_articles = []
        
        for point in time_series:
            dates.append(point['period'])
            percentages.append(point['percentage'] or 0)
            institution_counts.append(point['institution_count'])
            total_articles.append(point['total_articles'])
        
        # Create Plotly figure
        fig = go.Figure()
        
        # Add percentage line
        fig.add_trace(go.Scatter(
            x=dates,
            y=percentages,
            mode='lines+markers',
            name='Percentage of Articles',
            line=dict(color='#2E86AB', width=3),
            marker=dict(size=6),
            hovertemplate=(
                '<b>%{x}</b><br>' +
                'Percentage: %{y:.2f}%<br>' +
                'Institution: %{customdata[0]:,}<br>' +
                'Total Articles: %{customdata[1]:,}<br>' +
                '<extra></extra>'
            ),
            customdata=list(zip(institution_counts, total_articles))
        ))
        
        # Update layout
        fig.update_layout(
            title=dict(
                text=f"{institution_name}<br><sub>Mentions as Percentage of All Articles</sub>",
                x=0.5,
                xanchor='center',
                font=dict(size=18)
            ),
            xaxis=dict(
                title=f"Date ({time_bucket.capitalize()})",
                showgrid=True,
                gridcolor='rgba(128,128,128,0.2)'
            ),
            yaxis=dict(
                title="Percentage (%)",
                showgrid=True,
                gridcolor='rgba(128,128,128,0.2)',
                rangemode='tozero'
            ),
            hovermode='x unified',
            plot_bgcolor='white',
            height=450,
            margin=dict(t=100, b=60, l=60, r=20)
        )
        
        # Format actors and events
        actors_html = format_actors_list(top_actors)
        events_html = format_key_events(key_events)
        
        return fig, actors_html, events_html
        
    except Exception as e:
        return None, f"<p>Error processing results: {str(e)}</p>", ""


def build_institution_analysis_tab(app):
    """
    Build the institution analysis tab
    
    Args:
        app: Gradio Blocks app instance
    """
    
    # Load dropdown options on startup
    institutions_display = load_institutions_list()
    gov_functions_list = load_government_functions()
    interactions_list = load_interaction_types()
    
    with gr.Tab("Institution Analysis"):
        gr.Markdown("## Institution Mention Analysis")
        gr.Markdown("Select an institution to see how frequently it appears in articles over time.")
        
        with gr.Row():
            # Left column: Institution selector and basic controls
            with gr.Column(scale=1):
                institution_dropdown = gr.Dropdown(
                    choices=institutions_display,
                    label="Select Institution",
                    info="Choose an institution to analyze",
                    filterable=True,
                    interactive=True
                )
                
                date_range = gr.Radio(
                    choices=[
                        ("Last Month", "1"),
                        ("Last 3 Months", "3"),
                        ("Last 6 Months", "6"),
                        ("Last Year", "12"),
                    ],
                    value="6",
                    label="Time Range"
                )
                
                time_bucket = gr.Radio(
                    choices=[
                        ("Daily", "day"),
                        ("Weekly", "week"),
                        ("Monthly", "month")
                    ],
                    value="week",
                    label="Time Bucket"
                )
                
                analyze_btn = gr.Button("Analyze", variant="primary", size="lg")
            
            # Middle column: Time series visualization and filters
            with gr.Column(scale=2):
                plot_output = gr.Plot(label="Institution Mentions Over Time")
                
                with gr.Row():
                    gov_functions_filter = gr.Dropdown(
                        choices=gov_functions_list,
                        label="Government Functions Filter",
                        info="Filter by government functions (optional)",
                        multiselect=True,
                        interactive=True
                    )
                    
                    interactions_filter = gr.Dropdown(
                        choices=interactions_list,
                        label="Interactions Filter",
                        info="Filter by interaction types (optional)",
                        multiselect=True,
                        interactive=True
                    )
        
        # Bottom row: Actors and Events side by side
        with gr.Row():
            with gr.Column(scale=1):
                actors_output = gr.HTML(label="Top 10 Actors")
            
            with gr.Column(scale=1):
                events_output = gr.HTML(label="Key Events")
        
        # Event handlers
        analyze_btn.click(
            fn=analyze_institution,
            inputs=[institution_dropdown, date_range, time_bucket, gov_functions_filter, interactions_filter],
            outputs=[plot_output, actors_output, events_output]
        )
        
        # Also trigger on dropdown change for quick analysis
        institution_dropdown.change(
            fn=analyze_institution,
            inputs=[institution_dropdown, date_range, time_bucket, gov_functions_filter, interactions_filter],
            outputs=[plot_output, actors_output, events_output]
        )