"""
Putin Proximity Analysis Tab for Gradio App
Analyzes which actors appear alongside Vladimir Putin in articles
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
# CONFIGURATION: Manually excluded actors (filtered in SQL, not Python)
# ============================================================================
EXCLUDED_PUTIN_ACTORS = [
    "Dmitry Peskov",
    "Donald Trump",
    "Volodymyr Zelensky",
    "Xi Jinping",
    "Alexander Lukashenko",
    "Emmanuel Macron",
    "Steve Whitcomb",
    "Marco Rubio",
    "Kim Jong Un",
    " Maria Zakharova"
]


def load_primary_topics():
    """Load all primary topics for filter dropdown"""
    query = "SELECT * FROM get_primary_topics_list();"
    
    df, error = execute_query(query)
    
    if error:
        print(f"Error loading primary topics: {error}")
        return []
    
    if df.empty:
        return []
    
    return df['topic'].tolist()


def load_government_levels():
    """Load all government levels for filter dropdown"""
    query = "SELECT * FROM get_government_levels_list();"
    
    df, error = execute_query(query)
    
    if error:
        print(f"Error loading government levels: {error}")
        return []
    
    if df.empty:
        return []
    
    return df['level'].tolist()


def load_article_stances():
    """Load all article stances for filter dropdown"""
    query = "SELECT * FROM get_article_stances_list();"
    
    df, error = execute_query(query)
    
    if error:
        print(f"Error loading article stances: {error}")
        return []
    
    if df.empty:
        return []
    
    return df['stance'].tolist()


def load_government_functions():
    """Load government functions (reuse from institution analysis)"""
    query = "SELECT * FROM get_government_functions_list();"
    
    df, error = execute_query(query)
    
    if error:
        print(f"Error loading government functions: {error}")
        return []
    
    if df.empty:
        return []
    
    return df['function_name'].tolist()


def format_putin_actors_list(actors_data):
    """Format top actors appearing with Putin as HTML - NO filtering (done in SQL)"""
    if not actors_data:
        return "<p>No actors found</p>"
    
    html = "<div style='max-height: 600px; overflow-y: auto;'>"
    html += "<h3>Top 40 Actors Appearing with Putin</h3>"
    
    for i, actor in enumerate(actors_data[:40], 1):
        name = actor.get('actor_name', 'Unknown')
        position = actor.get('actor_position', 'N/A')
        role = actor.get('actor_role', 'N/A')
        coappearances = actor.get('coappearance_count', 0)
        level = actor.get('actor_government_level', 'N/A')
        institutions = actor.get('institutions', [])
        topics = actor.get('topics_appeared_in', [])
        
        # Format institutions and topics
        inst_str = ', '.join(institutions[:3]) if institutions else 'N/A'
        if institutions and len(institutions) > 3:
            inst_str += f' (+{len(institutions)-3} more)'
        
        topics_str = ', '.join(topics[:3]) if topics else 'N/A'
        if topics and len(topics) > 3:
            topics_str += f' (+{len(topics)-3} more)'
        
        html += f"""
        <div style='margin-bottom: 15px; padding: 12px; background-color: #f5f5f5; border-radius: 5px; border-left: 4px solid #d4af37;'>
            <div style='font-weight: bold; font-size: 16px; color: #333;'>{i}. {name}</div>
            <div style='margin-top: 6px; color: #555; font-size: 14px;'>
                <strong>Position:</strong> {position}<br>
                <strong>Role:</strong> {role}<br>
                <strong>Level:</strong> {level}<br>
                <strong>Co-appearances with Putin:</strong> {coappearances:,} articles<br>
                <strong>Institutions:</strong> {inst_str}<br>
                <strong>Topics:</strong> {topics_str}
            </div>
        </div>
        """
    
    html += "</div>"
    return html


def create_putin_timeseries_chart(time_series_data, overall_putin_data, time_bucket):
    """Create multi-line time series chart - NO filtering (done in SQL)"""
    if not time_series_data:
        fig = go.Figure()
        fig.add_annotation(
            text="No time series data available",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16)
        )
        return fig
    
    # Group data by actor (filtering already done in SQL)
    actor_timeseries = defaultdict(lambda: {'dates': [], 'counts': []})
    
    for point in time_series_data:
        actor = point.get('actor_name', 'Unknown')
        period = point.get('period')
        count = point.get('appearance_count', 0)
        
        actor_timeseries[actor]['dates'].append(period)
        actor_timeseries[actor]['counts'].append(count)
    
    # Create figure
    fig = go.Figure()
    
    # Color palette for actors
    colors = ['#d4af37', '#C0C0C0', '#CD7F32', '#4169E1', '#DC143C']
    
    # Add line for each actor
    for i, (actor, data) in enumerate(actor_timeseries.items()):
        color = colors[i % len(colors)]
        fig.add_trace(go.Scatter(
            x=data['dates'],
            y=data['counts'],
            mode='lines+markers',
            name=actor,
            line=dict(color=color, width=2.5),
            marker=dict(size=6),
            hovertemplate=(
                f'<b>{actor}</b><br>' +
                'Date: %{x}<br>' +
                'Appearances: %{y}<br>' +
                '<extra></extra>'
            )
        ))
    
    # Update layout
    fig.update_layout(
        title=dict(
            text=f"Top Actors Co-appearing with Putin Over Time<br><sub>By {time_bucket}</sub>",
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
            title="Number of Co-appearances",
            showgrid=True,
            gridcolor='rgba(128,128,128,0.2)',
            rangemode='tozero'
        ),
        hovermode='x unified',
        plot_bgcolor='white',
        height=500,
        margin=dict(t=100, b=80, l=80, r=40),
        legend=dict(
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=0.01,
            bgcolor="rgba(255,255,255,0.8)"
        )
    )
    
    return fig


def analyze_putin_proximity(date_range_months, time_bucket, gov_functions, 
                            primary_topics, govt_levels, article_stances):
    """
    Analyze Putin proximity with filters
    Exclusions passed to SQL for filtering
    """
    # Build date filter
    if date_range_months == "all":
        date_from = "NULL"
        date_to = "NULL"
    else:
        date_from = f"(CURRENT_DATE - INTERVAL '{date_range_months} months')::DATE"
        date_to = "CURRENT_DATE"
    
    # Build excluded actors parameter
    excluded_param = "NULL"
    if EXCLUDED_PUTIN_ACTORS:
        escaped_excluded = [name.replace("'", "''") for name in EXCLUDED_PUTIN_ACTORS]
        excluded_param = "ARRAY['" + "','".join(escaped_excluded) + "']"
    
    # Build filter parameters
    gov_functions_param = "NULL"
    if gov_functions and len(gov_functions) > 0:
        escaped_functions = [f.replace("'", "''") for f in gov_functions]
        gov_functions_param = "ARRAY['" + "','".join(escaped_functions) + "']"
    
    topics_param = "NULL"
    if primary_topics and len(primary_topics) > 0:
        escaped_topics = [t.replace("'", "''") for t in primary_topics]
        topics_param = "ARRAY['" + "','".join(escaped_topics) + "']"
    
    levels_param = "NULL"
    if govt_levels and len(govt_levels) > 0:
        escaped_levels = [l.replace("'", "''") for l in govt_levels]
        levels_param = "ARRAY['" + "','".join(escaped_levels) + "']"
    
    stances_param = "NULL"
    if article_stances and len(article_stances) > 0:
        escaped_stances = [s.replace("'", "''") for s in article_stances]
        stances_param = "ARRAY['" + "','".join(escaped_stances) + "']"
    
    # Query the database
    query = f"""
        SELECT analyze_putin_proximity(
            p_excluded_actors := {excluded_param},
            p_gov_functions := {gov_functions_param},
            p_primary_topics := {topics_param},
            p_government_levels := {levels_param},
            p_article_stance := {stances_param},
            p_date_from := {date_from},
            p_date_to := {date_to},
            p_time_bucket := '{time_bucket}'
        )
    """
    
    df, error = execute_query(query)
    
    if error:
        return None, f"<p>Error: {error}</p>"
    
    if df.empty:
        return None, "<p>No data found</p>"
    
    try:
        # Get the JSON result
        result_json = df.iloc[0]['analyze_putin_proximity']
        
        # Parse if string
        if isinstance(result_json, str):
            data = json.loads(result_json)
        else:
            data = result_json
        
        # Extract data
        top_actors = data.get('top_actors', [])
        time_series = data.get('time_series_by_actor', [])
        putin_overall = data.get('putin_overall_timeline', [])
        metadata = data.get('metadata', {})
        
        # Format actors list (no filtering needed, SQL already did it)
        actors_html = format_putin_actors_list(top_actors)
        
        # Create time series chart (no filtering needed, SQL already did it)
        chart = create_putin_timeseries_chart(time_series, putin_overall, time_bucket)
        
        return chart, actors_html
        
    except Exception as e:
        return None, f"<p>Error processing results: {str(e)}</p>"


def build_putin_proximity_tab(app):
    """
    Build the Putin proximity analysis tab
    
    Args:
        app: Gradio Blocks app instance
    """
    
    # Load dropdown options on startup
    topics_list = load_primary_topics()
    levels_list = load_government_levels()
    stances_list = load_article_stances()
    functions_list = load_government_functions()
    
    with gr.Tab("Putin Proximity Analysis"):
        gr.Markdown("## Putin Proximity Analysis")
        gr.Markdown("Analyze which actors appear alongside **Vladimir Putin** in articles, with time-series tracking and advanced filtering.")
        
        with gr.Row():
            # Left column: Controls
            with gr.Column(scale=1):
                date_range = gr.Radio(
                    choices=[
                        ("Last Month", "1"),
                        ("Last 3 Months", "3"),
                        ("Last 6 Months", "6"),
                        ("Last Year", "12"),
                        ("All Time", "all")
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
                
                analyze_btn = gr.Button("Analyze Putin Proximity", variant="primary", size="lg")
            
            # Middle column: Time series chart
            with gr.Column(scale=2):
                plot_output = gr.Plot(label="Actor Co-appearances with Putin Over Time")
                
                # Filters below the chart
                gr.Markdown("### Filters")
                with gr.Row():
                    with gr.Column():
                        primary_topics_filter = gr.Dropdown(
                            choices=topics_list,
                            label="Primary Topics",
                            info="Filter by article topics",
                            multiselect=True,
                            interactive=True
                        )
                        
                        gov_levels_filter = gr.Dropdown(
                            choices=levels_list,
                            label="Government Levels",
                            info="Filter by actor government level",
                            multiselect=True,
                            interactive=True
                        )
                    
                    with gr.Column():
                        gov_functions_filter = gr.Dropdown(
                            choices=functions_list,
                            label="Government Functions",
                            info="Filter by government functions",
                            multiselect=True,
                            interactive=True
                        )
                        
                        article_stance_filter = gr.Dropdown(
                            choices=stances_list,
                            label="Article Stance",
                            info="Filter by article stance",
                            multiselect=True,
                            interactive=True
                        )
        
        # Bottom row: Top actors list
        with gr.Row():
            with gr.Column():
                actors_output = gr.HTML(label="Top 40 Actors Appearing with Putin")
        
        # Event handlers
        analyze_btn.click(
            fn=analyze_putin_proximity,
            inputs=[
                date_range, 
                time_bucket, 
                gov_functions_filter,
                primary_topics_filter,
                gov_levels_filter,
                article_stance_filter
            ],
            outputs=[plot_output, actors_output]
        )