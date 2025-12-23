"""
PESTEL Data Visualization with Gradio
Part 2: Interactive Dashboard

This app provides interactive visualizations of PESTEL-categorized articles
including temporal analysis using article publish dates.
"""

from db import execute_query
import gradio as gr
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# =====================================================
# Data fetching functions
# =====================================================

def get_aggregated_details_case_insensitive(category, detail_type, limit=None):
    """Get aggregated details with case-insensitive grouping"""
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"""
    SELECT 
        LOWER(detail_value) as item_normalized,
        STRING_AGG(DISTINCT detail_value, ', ') as original_variations,
        SUM(frequency) as total_frequency,
        COUNT(DISTINCT detail_value) as variation_count,
        ROUND(100.0 * SUM(frequency) / SUM(SUM(frequency)) OVER(), 2) as percentage
    FROM mv_pestel_details_freq
    WHERE primary_category = '{category}' AND detail_type = '{detail_type}'
    GROUP BY LOWER(detail_value)
    ORDER BY total_frequency DESC
    {limit_clause}
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_sectors_for_detail_items(category, detail_type, detail_values):
    """Get top sectors for specific detail values - optimized version"""
    if not detail_values:
        return {}
    
    # Limit the number of items to prevent query overflow
    # Take only top 50 items if there are too many
    if len(detail_values) > 50:
        print(f"Limiting sector analysis to top 50 items (from {len(detail_values)})")
        detail_values = detail_values[:50]
    
    # Build the conditions for each detail value
    conditions = []
    for val in detail_values:
        safe_val = val.replace("'", "''")
        conditions.append(f"""
            EXISTS (
                SELECT 1 
                FROM jsonb_array_elements_text(t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}') as elem 
                WHERE LOWER(elem) = LOWER('{safe_val}')
            )
        """)
    
    where_clause = " OR ".join(conditions)
    
    query = f"""
    WITH article_sectors AS (
        SELECT 
            LOWER(jsonb_array_elements_text(t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}')) as detail_item,
            jsonb_array_elements_text(t.pestel_validated -> 'entities' -> 'sectors') as sector
        FROM texts t
        WHERE 
            t.pestel_validated -> 'pestel' ->> 'primary' = '{category}'
            AND ({where_clause})
    )
    SELECT 
        detail_item,
        sector,
        COUNT(*) as frequency
    FROM article_sectors
    GROUP BY detail_item, sector
    ORDER BY detail_item, frequency DESC
    """
    
    df, error = execute_query(query)
    if error:
        print(f"Database error in get_sectors: {error}")
        return {}
    
    if df.empty:
        return {}
    
    # Group by detail_item and get top 5 sectors for each
    result = {}
    for item in df['detail_item'].unique():
        item_sectors = df[df['detail_item'] == item].head(5)
        result[item] = [
            f"{row['sector']} ({row['frequency']})" 
            for _, row in item_sectors.iterrows()
        ]
    
    return result

def get_locations_and_institutions_for_detail_items(category, detail_type, detail_values):
    """Get locations and institutions for specific detail values - optimized version"""
    if not detail_values:
        return {}, {}
    
    # Limit the number of items to prevent query overflow
    # Take only top 50 items if there are too many
    if len(detail_values) > 50:
        print(f"Limiting location/institution analysis to top 50 items (from {len(detail_values)})")
        detail_values = detail_values[:50]
    
    # Build the conditions for each detail value
    conditions = []
    for val in detail_values:
        safe_val = val.replace("'", "''")
        conditions.append(f"""
            EXISTS (
                SELECT 1 
                FROM jsonb_array_elements_text(t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}') as elem 
                WHERE LOWER(elem) = LOWER('{safe_val}')
            )
        """)
    
    where_clause = " OR ".join(conditions)
    
    # Query for locations
    locations_query = f"""
    WITH article_locations AS (
        SELECT 
            LOWER(jsonb_array_elements_text(t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}')) as detail_item,
            jsonb_array_elements_text(t.pestel_validated -> 'entities' -> 'locations') as location
        FROM texts t
        WHERE 
            t.pestel_validated -> 'pestel' ->> 'primary' = '{category}'
            AND ({where_clause})
    )
    SELECT 
        detail_item,
        location,
        COUNT(*) as frequency
    FROM article_locations
    GROUP BY detail_item, location
    ORDER BY detail_item, frequency DESC
    """
    
    # Query for institutions  
    institutions_query = f"""
    WITH article_institutions AS (
        SELECT 
            LOWER(jsonb_array_elements_text(t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}')) as detail_item,
            inst->>'name' as institution_name,
            inst->>'type' as institution_type
        FROM texts t,
             jsonb_array_elements(t.pestel_validated -> 'entities' -> 'institutions') as inst
        WHERE 
            t.pestel_validated -> 'pestel' ->> 'primary' = '{category}'
            AND ({where_clause})
    )
    SELECT 
        detail_item,
        institution_name,
        institution_type,
        COUNT(*) as frequency
    FROM article_institutions
    WHERE institution_name IS NOT NULL
    GROUP BY detail_item, institution_name, institution_type
    ORDER BY detail_item, frequency DESC
    """
    
    locations_df, loc_error = execute_query(locations_query)
    institutions_df, inst_error = execute_query(institutions_query)
    
    if loc_error:
        print(f"Locations query error: {loc_error}")
        locations_df = pd.DataFrame()
        
    if inst_error:
        print(f"Institutions query error: {inst_error}")
        institutions_df = pd.DataFrame()
    
    # Process locations
    locations_result = {}
    if not locations_df.empty:
        for item in locations_df['detail_item'].unique():
            item_locs = locations_df[locations_df['detail_item'] == item].head(5)
            locations_result[item] = [
                f"{row['location']} ({row['frequency']})" 
                for _, row in item_locs.iterrows()
            ]
    
    # Process institutions
    institutions_result = {}
    if not institutions_df.empty:
        for item in institutions_df['detail_item'].unique():
            item_insts = institutions_df[institutions_df['detail_item'] == item].head(5)
            institutions_result[item] = [
                f"{row['institution_name']} - {row['institution_type']} ({row['frequency']})" 
                for _, row in item_insts.iterrows()
            ]
    
    return locations_result, institutions_result

def get_articles_with_details(start_date, end_date, category, detail_type, detail_values=None):
    """Get articles with specific detail values in a date range"""
    detail_clause = ""
    if detail_values:
        # Escape single quotes and create case-insensitive match
        safe_values = [v.replace("'", "''") for v in detail_values]
        detail_conditions = " OR ".join([
            f"EXISTS (SELECT 1 FROM jsonb_array_elements_text(t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}') as elem WHERE LOWER(elem) = LOWER('{val}'))"
            for val in safe_values
        ])
        detail_clause = f" AND ({detail_conditions})"
    
    query = f"""
    SELECT DISTINCT
        t.url,
        a.headline,
        a.timestamp,
        t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}' as detail_items,
        t.pestel_validated -> 'entities' -> 'sectors' as sectors,
        t.pestel_validated -> 'entities' -> 'institutions' as institutions,
        t.pestel_validated -> 'entities' -> 'actors' as actors,
        t.pestel_validated -> 'entities' -> 'locations' as locations
    FROM texts t
    JOIN articles a ON t.url = a.url
    WHERE
        a.timestamp::date BETWEEN '{start_date}' AND '{end_date}'
        AND t.pestel_validated -> 'pestel' ->> 'primary' = '{category}'
        {detail_clause}
    ORDER BY a.timestamp DESC
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_total_article_count(start_date, end_date, category, detail_type):
    """Get total count of articles matching the criteria"""
    query = f"""
    SELECT COUNT(DISTINCT t.url) as total_count
    FROM texts t
    JOIN articles a ON t.url = a.url
    WHERE
        a.timestamp::date BETWEEN '{start_date}' AND '{end_date}'
        AND t.pestel_validated -> 'pestel' ->> 'primary' = '{category}'
        AND jsonb_array_length(t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}') > 0
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return 0
    return df['total_count'].iloc[0] if not df.empty else 0

def generate_ai_report(category, detail_type, aggregated_data, articles_data, start_date, end_date):
    """Generate an AI report using the language model API"""
    
    # Get API endpoint from environment
    api_url = os.getenv('OSS_API', 'http://192.168.10.30:8019/v1/completions')
    
    # Check if we should use a simpler approach for large datasets
    if len(aggregated_data) > 100:
        print(f"Large dataset detected ({len(aggregated_data)} items), using simplified analysis")
    
    # Prepare the aggregated data summary
    if not aggregated_data.empty:
        # Get ALL detail values for comprehensive analysis
        all_detail_values = []
        for _, row in aggregated_data.iterrows():
            # Take first variation from the comma-separated list
            first_variation = row['original_variations'].split(',')[0].strip()
            all_detail_values.append(first_variation)
        
        # Get sectors for all items (limits applied inside function)
        sectors_data = get_sectors_for_detail_items(category, detail_type, all_detail_values)
        
        # Include ALL items in categories - no limits
        high_freq = aggregated_data[aggregated_data['total_frequency'] >= 10]
        medium_freq = aggregated_data[(aggregated_data['total_frequency'] >= 4) & (aggregated_data['total_frequency'] < 10)]
        low_freq = aggregated_data[aggregated_data['total_frequency'] <= 3]
        
        # Track if dataset is large but include everything
        truncated = False
        
        # Format high frequency items (show ALL with sectors)
        high_freq_text = f"HIGH FREQUENCY ITEMS (10+ occurrences, {len(high_freq)} items):\n" if not high_freq.empty else ""
        for idx, row in high_freq.iterrows():
            item_key = row['item_normalized']
            high_freq_text += f"\n- {row['original_variations']} (frequency: {row['total_frequency']}, {row['percentage']}%)\n"
            if item_key in sectors_data and sectors_data[item_key]:
                high_freq_text += f"  Sectors: {', '.join(sectors_data[item_key][:5])}\n"
        
        # Format medium frequency items (show ALL with sectors)
        medium_freq_text = f"\n\nMEDIUM FREQUENCY ITEMS (4-9 occurrences, {len(medium_freq)} items):\n" if not medium_freq.empty else ""
        for idx, row in medium_freq.iterrows():
            item_key = row['item_normalized']
            medium_freq_text += f"\n- {row['original_variations']} (frequency: {row['total_frequency']}, {row['percentage']}%)\n"
            if item_key in sectors_data and sectors_data[item_key]:
                medium_freq_text += f"  Sectors: {', '.join(sectors_data[item_key][:3])}\n"
        
        # Format weak signals/low frequency (show ALL)
        weak_signals_text = f"\n\nWEAK SIGNALS / LOW FREQUENCY (1-3 occurrences, {len(low_freq)} items):\n" if not low_freq.empty else ""
        for idx, row in low_freq.iterrows():
            item_key = row['item_normalized']
            weak_signals_text += f"\n- {row['original_variations']} (frequency: {row['total_frequency']})\n"
            if item_key in sectors_data and sectors_data[item_key]:
                weak_signals_text += f"  Primary sector: {sectors_data[item_key][0]}\n"
        
        all_items_text = high_freq_text + medium_freq_text + weak_signals_text
    else:
        all_items_text = "No aggregated data available."
    
    # Skip sample articles to reduce context size
    # sample_urls = ""
    # if not articles_data.empty:
    #     sample_articles = articles_data.head(5)
    #     sample_urls = "\nSample articles:\n"
    #     for _, row in sample_articles.iterrows():
    #         sample_urls += f"- {row['headline']}: {row['url']}\n"
    
    # Build the prompt (without sample articles)
    # Calculate how many items are actually included in the analysis
    items_in_prompt = len(high_freq) + len(medium_freq) + len(low_freq)
    limitation_note = ""
    if truncated:
        limitation_note = f"\nNote: Full dataset contains {len(aggregated_data)} items. All items are included in the analysis with contextual data."
    
    # Build the prompt (without sample articles)
    prompt = f"""Analyze the following PESTEL data for the {category} category, focusing on {detail_type}.
    
Period: {start_date} to {end_date}

DATA ANALYSIS (Complete dataset: {len(aggregated_data)} unique items - ALL items included below):{limitation_note}

{all_items_text}

Your task is to identify thematic patterns and clusters from the items listed above. Look for:
- Semantically similar items that represent the same underlying theme
- Related concepts that form coherent topic clusters
- Common patterns across different frequency levels

## REPORT STRUCTURE:

### 1. THEMATIC CLUSTERING TABLE (MANDATORY CENTERPIECE)

Create a markdown table summarizing the key themes you identify:

| Theme | Approximate Frequency | Key Sectors |
|-------|----------------------|-------------|

Guidelines:
- **Theme**: Descriptive name that captures a cluster of related items
- **Approximate Frequency**: Indicate the general magnitude (e.g., "High (50+)", "Medium (10-20)", "Low (5-10)") based on the combined frequencies of items in this theme
- **Key Sectors**: List the 2-4 most relevant sectors associated with this theme, with rough frequency indicators if clear from the data (e.g., "Energy, Manufacturing, Technology")

Focus on identifying meaningful thematic patterns rather than exact counting. Group similar items creatively.

### 2. THEMATIC ANALYSIS
For each major theme identified in your table:
- Explain what items were grouped together and why
- Describe the significance and implications
- Discuss sector involvement and cross-sector patterns
- Note whether this is a dominant theme or emerging signal

### 3. CROSS-CUTTING PATTERNS
Identify patterns that span multiple themes:
- Sector interconnections
- Frequency distribution insights (what's prominent vs. emerging)
- Temporal or contextual patterns if evident

### 4. WEAK SIGNALS AND EMERGING TRENDS
Highlight noteworthy low-frequency items that may indicate:
- Novel developments
- Early warning indicators
- Potential future trends

### 5. STRATEGIC IMPLICATIONS
Based on your thematic analysis:
- Priority areas requiring attention
- Opportunities and risks identified
- Recommendations for monitoring and action

Use clear markdown formatting with headers and bullet points. The table in section 1 is the centerpiece - make it comprehensive and insightful."""

    # Make API request with increased timeout
    try:
        response = requests.post(
            api_url,
            json={
                "prompt": prompt,
                "max_tokens": 6000,
                "temperature": 0.7,
                "top_p": 0.9
            },
            headers={"Content-Type": "application/json"},
            timeout=180  # Increased timeout to 3 minutes
        )
        
        if response.status_code == 200:
            result = response.json()
            if 'choices' in result and len(result['choices']) > 0:
                return result['choices'][0].get('text', 'No response generated.')
            else:
                return "Error: Invalid response format from API."
        else:
            return f"Error: API returned status code {response.status_code}"
    except requests.exceptions.Timeout:
        return "Error: API request timed out after 3 minutes. The data might be too complex. Try selecting a smaller date range or fewer categories."
    except Exception as e:
        return f"Error generating report: {str(e)}"

def get_category_distribution():
    """Get PESTEL category distribution for pie chart"""
    query = """
    SELECT primary_category, article_count, percentage
    FROM mv_pestel_category_stats
    ORDER BY article_count DESC
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_category_timeline_data(start_date=None, end_date=None):
    """Get category timeline data for visualization"""
    where_clauses = []
    if start_date:
        where_clauses.append(f"month >= '{start_date}'")
    if end_date:
        where_clauses.append(f"month <= '{end_date}'")
    
    where_str = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    
    query = f"""
    SELECT 
        month::date as date,
        primary_category,
        article_count
    FROM mv_pestel_timeline
    {where_str}
    ORDER BY month, primary_category
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_trending_entities(days=30):
    """Get recently trending entities"""
    query = f"""
    WITH recent AS (
        SELECT entity_name, entity_type, COUNT(*) as recent_count
        FROM pestel_entities
        WHERE article_timestamp >= CURRENT_DATE - INTERVAL '{days} days'
            AND entity_name IS NOT NULL
        GROUP BY entity_name, entity_type
    ),
    historical AS (
        SELECT entity_name, entity_type, 
               COUNT(*) * 1.0 / GREATEST(1, DATE_PART('day', MAX(article_timestamp) - MIN(article_timestamp))) as avg_daily_historical
        FROM pestel_entities
        WHERE article_timestamp < CURRENT_DATE - INTERVAL '{days} days'
            AND entity_name IS NOT NULL
        GROUP BY entity_name, entity_type
    )
    SELECT 
        r.entity_name,
        r.entity_type,
        r.recent_count,
        ROUND((r.recent_count * 1.0 / {days})::numeric, 2) as avg_daily_recent,
        ROUND(COALESCE(h.avg_daily_historical, 0)::numeric, 2) as avg_daily_historical,
        CASE 
            WHEN COALESCE(h.avg_daily_historical, 0) > 0 
            THEN ROUND((((r.recent_count * 1.0 / {days}) - h.avg_daily_historical) / h.avg_daily_historical * 100)::numeric, 1)
            ELSE NULL 
        END as growth_percentage
    FROM recent r
    LEFT JOIN historical h ON r.entity_name = h.entity_name AND r.entity_type = h.entity_type
    WHERE r.recent_count >= 3
    ORDER BY r.recent_count DESC
    LIMIT 20
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def create_timeline_chart(start_date=None, end_date=None):
    """Create timeline chart of PESTEL categories"""
    df = get_category_timeline_data(start_date, end_date)
    if df.empty:
        return None
    
    fig = px.line(df, 
                  x='date', 
                  y='article_count',
                  color='primary_category',
                  title='PESTEL Categories Over Time',
                  labels={'article_count': 'Number of Articles', 'date': 'Month'},
                  color_discrete_map={
                      'Political': '#FF6B6B',
                      'Economic': '#4ECDC4',
                      'Social': '#45B7D1',
                      'Technological': '#96CEB4',
                      'Environmental': '#DDA77B',
                      'Legal': '#9B59B6'
                  })
    fig.update_layout(height=500, hovermode='x unified')
    return fig

def create_entity_trend_chart(entity_name, entity_type=None):
    """Create trend chart for specific entity over time"""
    type_clause = f"AND entity_type = '{entity_type}'" if entity_type else ""
    query = f"""
    SELECT 
        month::date as date,
        monthly_appearances as appearances,
        unique_articles as articles
    FROM mv_pestel_entity_timeline
    WHERE entity_name = '{entity_name}' {type_clause}
    ORDER BY month
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    
    if df.empty:
        return None
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=df['date'], y=df['appearances'], name='Mentions', yaxis='y'))
    fig.add_trace(go.Scatter(x=df['date'], y=df['articles'], name='Unique Articles', yaxis='y2', mode='lines+markers'))
    
    fig.update_layout(
        title=f'Timeline: {entity_name}',
        xaxis=dict(title='Month'),
        yaxis=dict(title='Total Mentions', side='left'),
        yaxis2=dict(title='Unique Articles', overlaying='y', side='right'),
        hovermode='x unified',
        height=400
    )
    return fig

def get_entities_for_category(category, entity_type='all'):
    """Get entities for a specific category"""
    type_filter = "" if entity_type == 'all' else f"AND entity_type = '{entity_type}'"
    query = f"""
    SELECT entity_type, entity_name, frequency, article_appearances
    FROM mv_pestel_entities_by_category
    WHERE primary_category = '{category}' {type_filter}
    ORDER BY frequency DESC
    LIMIT 20
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_top_actors():
    """Get top actors across all categories"""
    query = """
    SELECT entity_name as actor, total_frequency as appearances, 
           array_to_string(categories, ', ') as categories
    FROM mv_pestel_top_entities
    WHERE entity_type = 'actor'
    ORDER BY total_frequency DESC
    LIMIT 15
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_top_institutions():
    """Get top institutions across all categories"""
    query = """
    SELECT entity_name as institution, total_frequency as appearances,
           array_to_string(categories, ', ') as categories
    FROM mv_pestel_top_entities
    WHERE entity_type = 'institution'
    ORDER BY total_frequency DESC
    LIMIT 15
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_trends_for_category(category):
    """Get trends for a specific category"""
    query = f"""
    SELECT detail_value as trend, frequency
    FROM mv_pestel_details_freq
    WHERE primary_category = '{category}' AND detail_type = 'trends'
    ORDER BY frequency DESC
    LIMIT 10
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_category_details(category):
    """Get all details (trends, impacts, etc.) for a category"""
    query = f"""
    SELECT detail_type, detail_value, frequency
    FROM mv_pestel_details_freq
    WHERE primary_category = '{category}'
    ORDER BY detail_type, frequency DESC
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return {}
    
    # Group by detail_type for better presentation
    if not df.empty:
        grouped = df.groupby('detail_type').apply(
            lambda x: x.nlargest(5, 'frequency')[['detail_value', 'frequency']].to_dict('records')
        ).to_dict()
        return grouped
    return {}

def get_location_heatmap():
    """Get location frequency for heatmap"""
    query = """
    SELECT entity_name as location, 
           COUNT(DISTINCT url) as article_count,
           array_agg(DISTINCT primary_category) as categories
    FROM pestel_entities
    WHERE entity_type = 'location'
    GROUP BY entity_name
    HAVING COUNT(DISTINCT url) > 2
    ORDER BY article_count DESC
    LIMIT 30
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_sector_distribution():
    """Get sector distribution across categories"""
    query = """
    SELECT primary_category, entity_name as sector, 
           COUNT(*) as frequency
    FROM pestel_entities
    WHERE entity_type = 'sector'
    GROUP BY primary_category, entity_name
    HAVING COUNT(*) > 2
    ORDER BY frequency DESC
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_article_view_data():
    """Get a sample of articles with their validation status and reasoning."""
    query = """
    SELECT url, pestel_validated, pestel_reasoning 
    FROM texts 
    WHERE pestel_validated IS NOT NULL 
    ORDER BY RANDOM() 
    LIMIT 50
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_articles_for_detail(category, detail_type, detail_value):
    """Fetch articles that contain a specific detail value."""
    if not all([category, detail_type, detail_value]):
        return pd.DataFrame()

    # Sanitize detail_value to escape single quotes for the SQL query
    safe_detail_value = detail_value.replace("'", "''")

    query = f"""
    SELECT
        t.url,
        a.headline,
        t.pestel_validated ->> 'summary' as summary
    FROM texts t
    JOIN articles a ON t.url = a.url
    WHERE
        t.pestel_validated -> 'pestel' ->> 'primary' = '{category}'
        AND EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}') as elem
            WHERE elem = '{safe_detail_value}'
        )
    ORDER BY a.timestamp DESC
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

def get_filtered_articles(start_date, end_date, category, analysis_type):
    """Fetch articles based on date, category, and analysis type."""
    if not all([start_date, end_date, category, analysis_type]):
        return pd.DataFrame()

    query = f"""
    SELECT
        t.url,
        a.headline,
        t.pestel_validated ->> 'summary' as summary,
        t.pestel_validated -> 'pestel' -> 'details' -> '{analysis_type}' as analysis_details,
        t.pestel_validated -> 'entities' -> 'sectors' as sectors,
        t.pestel_validated -> 'entities' -> 'institutions' as institutions,
        t.pestel_validated -> 'entities' -> 'actors' as actors
    FROM texts t
    JOIN articles a ON t.url = a.url
    WHERE
        a.timestamp::date BETWEEN '{start_date}' AND '{end_date}'
        AND t.pestel_validated -> 'pestel' ->> 'primary' = '{category}'
        AND jsonb_typeof(t.pestel_validated -> 'pestel' -> 'details' -> '{analysis_type}') = 'array'
        AND jsonb_array_length(t.pestel_validated -> 'pestel' -> 'details' -> '{analysis_type}') > 0
    ORDER BY a.timestamp DESC
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return pd.DataFrame()
    return df

# =====================================================
# Visualization functions
# =====================================================

def create_category_pie_chart():
    """Create pie chart of PESTEL categories"""
    df = get_category_distribution()
    if df.empty:
        return None
    
    fig = px.pie(df, 
                 values='article_count', 
                 names='primary_category',
                 title='Distribution of PESTEL Categories',
                 color_discrete_map={
                     'Political': '#FF6B6B',
                     'Economic': '#4ECDC4',
                     'Social': '#45B7D1',
                     'Technological': '#96CEB4',
                     'Environmental': '#DDA77B',
                     'Legal': '#9B59B6'
                 })
    fig.update_layout(height=400)
    return fig

def create_entity_bar_chart(category, entity_type):
    """Create bar chart for entities in a category"""
    df = get_entities_for_category(category, entity_type)
    if df.empty:
        return None
    
    # Take top 15 for readability
    df = df.head(15)
    
    fig = px.bar(df, 
                 x='frequency', 
                 y='entity_name',
                 orientation='h',
                 title=f"Top {entity_type.title()}s in {category} Category",
                 labels={'frequency': 'Frequency', 'entity_name': entity_type.title()},
                 color='frequency',
                 color_continuous_scale='Blues')
    fig.update_layout(height=500, yaxis={'categoryorder':'total ascending'})
    return fig

def create_trends_table(category):
    """Create a formatted table of trends for a category"""
    df = get_trends_for_category(category)
    if df.empty:
        return pd.DataFrame({"Message": ["No trends found for this category"]})
    return df

def create_sector_sunburst():
    """Create sunburst chart for sectors by category"""
    df = get_sector_distribution()
    if df.empty:
        return None
    
    # Prepare data for sunburst
    df = df.head(50)  # Limit for performance
    
    fig = px.sunburst(df, 
                      path=['primary_category', 'sector'], 
                      values='frequency',
                      title='Sectors Distribution Across PESTEL Categories')
    fig.update_layout(height=500)
    return fig

def create_details_accordion(category):
    """Create formatted text for category details"""
    details = get_category_details(category)
    if not details:
        return "No details available for this category"
    
    output = []
    for detail_type, items in details.items():
        output.append(f"### {detail_type.replace('_', ' ').title()}\n")
        for item in items[:5]:  # Top 5 for each type
            output.append(f"- **{item['detail_value']}** (frequency: {item['frequency']})")
        output.append("\n")
    
    return "\n".join(output)

def create_comparison_matrix():
    """Create a comparison matrix of categories vs entity types"""
    query = """
    SELECT primary_category, entity_type, COUNT(*) as count
    FROM pestel_entities
    GROUP BY primary_category, entity_type
    ORDER BY primary_category, entity_type
    """
    df, error = execute_query(query)
    if error:
        print(f"Database error: {error}")
        return None
    
    if df.empty:
        return None
    
    # Pivot for heatmap
    pivot_df = df.pivot(index='primary_category', columns='entity_type', values='count').fillna(0)
    
    fig = px.imshow(pivot_df, 
                    title='Entity Types Distribution Across PESTEL Categories',
                    labels=dict(x="Entity Type", y="PESTEL Category", color="Count"),
                    aspect='auto',
                    color_continuous_scale='YlOrRd')
    fig.update_layout(height=400)
    return fig

# =====================================================
# Gradio Interface
# =====================================================

def build_pestel_tab(app: gr.Blocks):
        gr.Markdown("""
        # PESTEL-analyysin hallintapaneeli
        Interaktiivinen visualisointi PESTEL-luokitelluista artikkeleista (Poliittinen, Taloudellinen, Sosiaalinen, Teknologinen, Ympäristöllinen, Oikeudellinen).
        """)
        
        # Add custom CSS for report styling
        gr.HTML("""
        <style>
            .report-output {
                background-color: #f8f9fa;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                padding: 20px;
                font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            }
            .report-output h1 { color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; }
            .report-output h2 { color: #34495e; margin-top: 20px; }
            .report-output h3 { color: #546e7a; }
            .report-output ul { margin-left: 20px; }
            .report-output li { margin: 8px 0; }
            .report-output strong { color: #2c3e50; }
            .report-output hr { border-color: #dee2e6; margin: 20px 0; }
        </style>
        """)
        
        with gr.Tab("Pika-analyysi"):
            gr.Markdown("""
            ### Kyselytyökalu
            Käytä tätä osiota räätälöityjen kyselyiden tekemiseen syvempää analyysia varten. Valitse kohde tulostaulukosta nähdäksesi siihen liittyvät artikkelit.
            """)
            
            with gr.Row():
                with gr.Column(scale=1):
                    analysis_category = gr.Dropdown(
                        choices=['Political', 'Economic', 'Social', 'Technological', 'Environmental', 'Legal'],
                        label="Kategoria"
                    )
                    analysis_type = gr.Radio(
                        choices=['trends', 'impacts', 'opportunities', 'threats', 'issues_identified'],
                        label="Analyysityyppi",
                        value='trends'
                    )
                    analyze_btn = gr.Button("Analysoi", variant="primary")
                    analysis_output = gr.Dataframe(label="Analyysitulokset", interactive=True)

                with gr.Column(scale=2):
                    reading_view = gr.Markdown(label="Liittyvät artikkelit")
            
            def run_analysis(category, detail_type):
                if not category:
                    return pd.DataFrame({"Virhe": ["Valitse kategoria"]})
                
                # Use case-insensitive aggregation - no limit to show ALL items
                df = get_aggregated_details_case_insensitive(category, detail_type, limit=None)
                if df.empty:
                    return pd.DataFrame({"Viesti": ["Dataa ei löytynyt"]})
                
                # Format for display - take first variation from each group
                display_df = pd.DataFrame({
                    'Kohde': df['original_variations'].apply(lambda x: x.split(',')[0].strip()),
                    'Frekvenssi': df['total_frequency'],
                    'Prosenttiosuus (%)': df['percentage']
                })
                return display_df

            def format_articles_markdown(df):
                if df is None or df.empty:
                    return "Valitulle kohteelle ei löytynyt artikkeleita."
                
                output_parts = []
                for _, row in df.iterrows():
                    url = row['url']
                    headline = row.get('headline', 'Otsikko ei saatavilla')
                    summary = row['summary']
                    if summary:
                        output_parts.append(f"### {headline}\n**URL:** [{url}]({url})\n\n**Summary:**\n{summary}\n\n---\n")
                
                return "\n".join(output_parts)

            def show_articles_for_item(category, detail_type, evt: gr.SelectData):
                if not category or not detail_type:
                    return "Please select a category and analysis type first."
                
                # Re-run the analysis to get the dataframe and find the selected item
                df = run_analysis(category, detail_type)
                if df.empty or evt.index[0] >= len(df):
                    return "Could not retrieve article details."
                
                # Get all variations of the selected item (it's a comma-separated string)
                selected_variations = df.iloc[evt.index[0]]['item']
                
                # For querying, we'll use all variations
                all_variations = [v.strip() for v in selected_variations.split(',')]
                
                # Modified query to handle multiple variations with case-insensitive matching
                query = f"""
                SELECT
                    t.url,
                    a.headline,
                    t.pestel_validated ->> 'summary' as summary
                FROM texts t
                JOIN articles a ON t.url = a.url
                WHERE
                    t.pestel_validated -> 'pestel' ->> 'primary' = '{category}'
                    AND EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements_text(t.pestel_validated -> 'pestel' -> 'details' -> '{detail_type}') as elem
                        WHERE LOWER(elem) IN ({','.join([f"LOWER('{v.replace(chr(39), chr(39)+chr(39))}')" for v in all_variations])})
                    )
                ORDER BY a.timestamp DESC
                LIMIT 10
                """
                
                articles_df, error = execute_query(query)
                if error:
                    return f"Error fetching articles: {error}"
                    
                return format_articles_markdown(articles_df)

            def format_articles_markdown(df):
                if df is None or df.empty:
                    return "Valitulle kohteelle ei löytynyt artikkeleita."
                
                output_parts = []
                for _, row in df.iterrows():
                    url = row['url']
                    headline = row.get('headline', 'Otsikko ei saatavilla')
                    summary = row['summary']
                    if summary:
                        output_parts.append(f"### {headline}\n**URL:** [{url}]({url})\n\n**Yhteenveto:**\n{summary}\n\n---\n")
                
                return "\n".join(output_parts)

            def show_articles_for_item(category, detail_type, evt: gr.SelectData):
                if not category or not detail_type:
                    return "Valitse ensin kategoria ja analyysityyppi."
                
                # Re-run the analysis to get the dataframe and find the selected item
                df = run_analysis(category, detail_type)
                if df.empty or evt.index[0] >= len(df):
                    return "Artikkelin tietojen haku epäonnistui."
                
                selected_item = df.iloc[evt.index[0]]['item']
                
                articles_df = get_articles_for_detail(category, detail_type, selected_item)
                return format_articles_markdown(articles_df)

            analyze_btn.click(
                run_analysis,
                inputs=[analysis_category, analysis_type],
                outputs=[analysis_output]
            )

            analysis_output.select(
                show_articles_for_item,
                inputs=[analysis_category, analysis_type],
                outputs=[reading_view]
            )

            analysis_output.select(
                show_articles_for_item,
                inputs=[analysis_category, analysis_type],
                outputs=[reading_view]
            )
        
        with gr.Tab("Artikkelinäkymä"):
            gr.Markdown("### Artikkelien validointinäkymä")
            
            # State to hold the dataframe and the current index
            article_data = gr.State()
            current_index = gr.State(0)

            # UI components
            article_url = gr.Markdown(label="Artikkelin URL")
            with gr.Row():
                with gr.Column(scale=1):
                    gr.Markdown("### PESTEL-validointi (JSON)")
                    pestel_json = gr.Code(label="", language="json", lines=30)
                with gr.Column(scale=1):
                    gr.Markdown("### PESTEL-perustelut")
                    pestel_reasoning = gr.Markdown(elem_classes=["reasoning-text"])

            with gr.Row():
                prev_btn = gr.Button("Edellinen")
                next_btn = gr.Button("Seuraava")
                refresh_btn = gr.Button("Päivitä uusi näyte")

            # Function to update the UI based on the current index
            def update_view(data, index):
                if data is None or data.empty or not (0 <= index < len(data)):
                    return "No articles loaded.", "{}", "", gr.update(interactive=False), gr.update(interactive=False)

                article = data.iloc[index]
                url_markdown = f"**URL:** [{article['url']}]({article['url']})"
                
                validated_data = article['pestel_validated']
                # Format for gr.Code component
                if isinstance(validated_data, (dict, list)):
                    json_display = json.dumps(validated_data, indent=2, ensure_ascii=False)
                elif isinstance(validated_data, str):
                    try:
                        parsed_json = json.loads(validated_data)
                        json_display = json.dumps(parsed_json, indent=2, ensure_ascii=False)
                    except (json.JSONDecodeError, TypeError):
                        json_display = '{"error": "Could not parse JSON from database."}'
                else:
                    json_display = "{}"

                reasoning_text = article['pestel_reasoning']
                
                # Update button interactivity
                prev_interactive = index > 0
                next_interactive = index < len(data) - 1
                
                return url_markdown, json_display, reasoning_text, gr.update(interactive=prev_interactive), gr.update(interactive=next_interactive)

            # Navigation function
            def navigate(data, index, direction):
                new_index = index + direction
                if data is not None and 0 <= new_index < len(data):
                    return new_index, *update_view(data, new_index)
                # If out of bounds, just return the old view
                return index, *update_view(data, index)

            # Refresh function
            def refresh_data():
                new_data = get_article_view_data()
                new_index = 0
                url, json_val, reasoning, prev_inter, next_inter = update_view(new_data, new_index)
                return new_data, new_index, url, json_val, reasoning, prev_inter, next_inter

            # Button click handlers
            prev_btn.click(
                lambda data, index: navigate(data, index, -1),
                inputs=[article_data, current_index],
                outputs=[current_index, article_url, pestel_json, pestel_reasoning, prev_btn, next_btn]
            )
            next_btn.click(
                lambda data, index: navigate(data, index, 1),
                inputs=[article_data, current_index],
                outputs=[current_index, article_url, pestel_json, pestel_reasoning, prev_btn, next_btn]
            )
            refresh_btn.click(
                refresh_data,
                inputs=[],
                outputs=[article_data, current_index, article_url, pestel_json, pestel_reasoning, prev_btn, next_btn]
            )

            # Initial data load
            app.load(refresh_data, outputs=[article_data, current_index, article_url, pestel_json, pestel_reasoning, prev_btn, next_btn])
        
        with gr.Tab("Räätälöity analyysi"):
            gr.Markdown("### Artikkelien haku kriteerien perusteella")
            gr.Markdown("Valitse aikaväli, PESTEL-kategoria ja analyysityyppi hakeaksesi relevantteja artikkeliyhteenvetoja.")

            with gr.Row():
                start_date_input = gr.Textbox(label="Aloituspäivä (VVVV-KK-PP)", value="2025-01-01")
                end_date_input = gr.Textbox(label="Lopetuspäivä (VVVV-KK-PP)", value="2025-12-31")

            with gr.Row():
                custom_analysis_category = gr.Dropdown(
                    choices=['Political', 'Economic', 'Social', 'Technological', 'Environmental', 'Legal'],
                    label="Kategoria"
                )
                custom_analysis_type = gr.Radio(
                    choices=['trends', 'impacts', 'opportunities', 'threats', 'issues_identified'],
                    label="Analyysityyppi",
                    value='trends'
                )

            run_custom_analysis_btn = gr.Button("Hae artikkeliyhteenvetoja", variant="primary")

            custom_analysis_output = gr.Markdown(label="Tulokset")

            def format_analysis_output(df, analysis_type):
                if df is None or df.empty:
                    return "Valituille kriteereille ei löytynyt artikkeleita."
                
                output_parts = []
                for _, row in df.iterrows():
                    url = row['url']
                    headline = row.get('headline', 'Otsikko ei saatavilla')
                    summary = row['summary']
                    
                    output_parts.append(f"### {headline}\n**URL:** [{url}]({url})\n\n**Yhteenveto:**\n{summary}\n")

                    # Analysis details
                    analysis_details = row.get('analysis_details')
                    if analysis_details and isinstance(analysis_details, list):
                        title = analysis_type.replace('_', ' ').title()
                        items = ", ".join([f"`{item}`" for item in analysis_details])
                        output_parts.append(f"**{title}:** {items}\n")

                    # Sectors
                    sectors = row.get('sectors')
                    if sectors and isinstance(sectors, list):
                        items = ", ".join([f"`{item}`" for item in sectors])
                        output_parts.append(f"**Sektorit:** {items}\n")

                    # Institutions
                    institutions = row.get('institutions')
                    if institutions and isinstance(institutions, list):
                        names = [inst.get('name') for inst in institutions if inst.get('name')]
                        if names:
                            items = ", ".join([f"`{name}`" for name in names])
                            output_parts.append(f"**Instituutiot:** {items}\n")

                    # Actors
                    actors = row.get('actors')
                    if actors and isinstance(actors, list):
                        names = [actor.get('name') for actor in actors if actor.get('name')]
                        if names:
                            items = ", ".join([f"`{name}`" for name in names])
                            output_parts.append(f"**Toimijat:** {items}\n")
                    
                    output_parts.append("\n---\n")
                
                return "\n".join(output_parts)

            def run_custom_analysis(start_date, end_date, category, analysis_type):
                if not category:
                    return "Valitse kategoria."
                df = get_filtered_articles(start_date, end_date, category, analysis_type)
                return format_analysis_output(df, analysis_type)

            run_custom_analysis_btn.click(
                run_custom_analysis,
                inputs=[start_date_input, end_date_input, custom_analysis_category, custom_analysis_type],
                outputs=[custom_analysis_output]
            )
        
        with gr.Tab("AI-raporttien generointi"):
            gr.Markdown("""
            ### AI-pohjainen analyysiraportin generointi
            Luo kattavia raportteja käyttäen tekoälyä analysoimaan koottua PESTEL-dataa kirjainkoosta riippumattomalla ryhmittelyllä.
            Raportti tunnistaa kuviot, samankaltaiset merkinnät ja heikot signaalit datasta.
            """)
            
            with gr.Row():
                with gr.Column(scale=1):
                    # Input controls
                    report_start_date = gr.Textbox(
                        label="Aloituspäivä (VVVV-KK-PP)", 
                        value="2025-01-01"
                    )
                    report_end_date = gr.Textbox(
                        label="Lopetuspäivä (VVVV-KK-PP)", 
                        value="2025-12-31"
                    )
                    report_category = gr.Dropdown(
                        choices=['Political', 'Economic', 'Social', 'Technological', 'Environmental', 'Legal'],
                        label="PESTEL-kategoria",
                        value='Political'
                    )
                    report_detail_type = gr.Radio(
                        choices=['trends', 'impacts', 'opportunities', 'threats', 'issues_identified'],
                        label="Analyysityyppi",
                        value='trends'
                    )
                    
                    with gr.Row():
                        preview_data_btn = gr.Button("Esikatsele koottua dataa", variant="secondary")
                        generate_report_btn = gr.Button("Luo AI-raportti", variant="primary")
                
                with gr.Column(scale=2):
                    # Preview section
                    aggregated_preview = gr.Dataframe(
                        label="Kootun datan esikatselu (kirjainkoosta riippumaton)",
                        interactive=False,
                        wrap=True
                    )
            
            # Report output section
            with gr.Row():
                report_output = gr.Markdown(
                    label="Generated Report",
                    elem_classes=["report-output"]
                )
            
            # Status message
            status_message = gr.Markdown("")
            
            def preview_aggregated_data(category, detail_type, start_date=None, end_date=None):
                """Preview the aggregated data with case-insensitive grouping"""
                if not category or not detail_type:
                    return pd.DataFrame({"Virhe": ["Valitse kategoria ja analyysityyppi"]}), ""
                
                # Use provided dates or defaults
                if not start_date:
                    start_date = "2025-01-01"
                if not end_date:
                    end_date = "2025-12-31"
                
                # No limit - show ALL items
                df = get_aggregated_details_case_insensitive(category, detail_type, limit=None)
                if df.empty:
                    return pd.DataFrame({"Viesti": ["Dataa ei löytynyt"]}), "Valituille kriteereille ei löytynyt dataa."
                
                # Get total article count
                total_articles = get_total_article_count(start_date, end_date, category, detail_type)
                
                # Get all detail values for sectors lookup
                all_detail_values = []
                for _, row in df.iterrows():
                    first_variation = row['original_variations'].split(',')[0].strip()
                    all_detail_values.append(first_variation)
                
                # Get sectors for all items (with limits applied inside the function)
                sectors_data = get_sectors_for_detail_items(category, detail_type, all_detail_values)
                
                # Format the dataframe for display with sectors only
                display_rows = []
                for _, row in df.iterrows():
                    item_key = row['item_normalized']
                    first_variation = row['original_variations'].split(',')[0].strip()
                    
                    # Get top sectors with counts
                    sectors_str = ""
                    if item_key in sectors_data and sectors_data[item_key]:
                        sectors_str = ", ".join(sectors_data[item_key][:3])  # Top 3 sectors
                    
                    display_rows.append({
                        'Kohde': first_variation,
                        'Frekvenssi': row['total_frequency'],
                        '% Osuus': f"{row['percentage']:.1f}%",
                        'Top Sektorit': sectors_str
                    })
                
                display_df = pd.DataFrame(display_rows)
                
                summary = f"**Löytyi {len(df)} uniikkia kohdetta yhteensä {int(df['total_frequency'].sum())} esiintymällä {total_articles} artikkelissa**"
                summary += f"\n\nAikaväli: {start_date} - {end_date}"
                summary += f"\nKategoria: {category}, Analyysityyppi: {detail_type}"
                return display_df, summary
            
            def generate_full_report(start_date, end_date, category, detail_type):
                """Generate the full AI report"""
                if not all([start_date, end_date, category, detail_type]):
                    return "Täytä kaikki vaaditut kentät.", ""
                
                status = "Haetaan koottua dataa..."
                yield "", status
                
                # Get aggregated data
                aggregated_df = get_aggregated_details_case_insensitive(category, detail_type)
                
                if aggregated_df.empty:
                    return "Valituille kriteereille ei löytynyt dataa.", "Dataa ei löytynyt"
                
                # Skip fetching sample articles to reduce processing time
                # status = "Haetaan artikkelinäytteitä..."
                # yield "", status
                
                # # Get sample articles
                # # Focus on top items for article sampling
                # top_items = aggregated_df.head(5)
                # sample_values = []
                # for _, row in top_items.iterrows():
                #     # Take first variation from the comma-separated list
                #     first_variation = row['original_variations'].split(',')[0].strip()
                #     sample_values.append(first_variation)
                
                # articles_df = get_articles_with_details(
                #     start_date, end_date, category, detail_type, sample_values
                # )
                
                status = "Luodaan AI-raporttia..."
                yield "", status
                
                # Generate the report (pass empty DataFrame for articles_df since we're not using it)
                articles_df = pd.DataFrame()  # Empty DataFrame since we're not using sample articles
                report = generate_ai_report(
                    category, detail_type, aggregated_df, articles_df, start_date, end_date
                )
                
                # If AI report fails, create a structured summary
                if "Error" in report or "timed out" in report:
                    status = "AI-raportin luonti epäonnistui, luodaan strukturoitu yhteenveto..."
                    yield "", status
                    
                    # Get sectors data for top items
                    all_detail_values = []
                    for _, row in aggregated_df.iterrows():
                        first_variation = row['original_variations'].split(',')[0].strip()
                        all_detail_values.append(first_variation)
                    
                    sectors_data = get_sectors_for_detail_items(category, detail_type, all_detail_values[:50])
                    
                    # Create a structured table format summary
                    report = f"""## PESTEL-analyysin yhteenveto

### Analysoidut tiedot:
- **Kategoria:** {category}
- **Analyysityyppi:** {detail_type}  
- **Aikaväli:** {start_date} - {end_date}
- **Löydetyt kohteet:** {len(aggregated_df)} uniikkia
- **Kokonaisesiintymiä:** {int(aggregated_df['total_frequency'].sum())}

### Top 30 kohdetta taulukkomuodossa:

| Kohde | Frekvenssi | % Osuus | Top Sektorit (määrät) |
|-------|------------|---------|-----------------------|
"""
                    top_30 = aggregated_df.head(30)
                    for _, row in top_30.iterrows():
                        item_key = row['item_normalized']
                        item_name = row['original_variations'].split(',')[0].strip()
                        freq = row['total_frequency']
                        pct = row['percentage']
                        
                        # Get sectors
                        sectors_str = "-"
                        if item_key in sectors_data and sectors_data[item_key]:
                            sectors_str = ", ".join(sectors_data[item_key][:3])
                        
                        report += f"| {item_name} | {freq} | {pct:.1f}% | {sectors_str} |\n"
                    
                    # Add frequency distribution
                    high_freq = len(aggregated_df[aggregated_df['total_frequency'] >= 10])
                    med_freq = len(aggregated_df[(aggregated_df['total_frequency'] >= 4) & (aggregated_df['total_frequency'] < 10)])
                    low_freq = len(aggregated_df[aggregated_df['total_frequency'] <= 3])
                    
                    report += f"""\n### Frekvenssijakauma:
- **Korkea frekvenssi (≥10):** {high_freq} kohdetta
- **Keskitaso (4-9):** {med_freq} kohdetta  
- **Heikot signaalit (≤3):** {low_freq} kohdetta

### Tilastot:
- **Keskimääräinen frekvenssi:** {aggregated_df['total_frequency'].mean():.1f}
- **Mediaani frekvenssi:** {aggregated_df['total_frequency'].median():.0f}
- **Maksimi frekvenssi:** {aggregated_df['total_frequency'].max()}

*Huom: AI-pohjainen teema-analyysi ei ollut saatavilla. Tämä on strukturoitu tilastollinen yhteenveto.*"""
                
                # Add metadata to the report
                metadata = f"""
---
**Raportin metatiedot:**
- **Luotu:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Aikaväli:** {start_date} - {end_date}
- **Kategoria:** {category}
- **Analyysityyppi:** {detail_type}
- **Kohteiden määrä:** {len(aggregated_df)} uniikkia (kirjainkoosta riippumaton)
- **Esiintymien määrä:** {aggregated_df['total_frequency'].sum()}
- **Analysoituja artikkeleita:** (artikkelinäytteitä ei sisällytetty)
---

"""
                
                full_report = metadata + report
                status = "Raportti luotu onnistuneesti!"
                
                yield full_report, status
            
            # Wire up the buttons
            preview_data_btn.click(
                preview_aggregated_data,
                inputs=[report_category, report_detail_type, report_start_date, report_end_date],
                outputs=[aggregated_preview, status_message]
            )
            
            generate_report_btn.click(
                generate_full_report,
                inputs=[report_start_date, report_end_date, report_category, report_detail_type],
                outputs=[report_output, status_message]
            )

