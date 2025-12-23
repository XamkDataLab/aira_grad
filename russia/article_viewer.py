import json
import pandas as pd
import gradio as gr
from collections import OrderedDict
from db import execute_query

# Import your execute_query function
# from your_db_module import execute_query


def reorder_json_keys(json_data):
    """Reorder JSON keys to match the original app's order instead of alphabetical"""
    if not json_data:
        return json_data

    key_order = [
        'relevant',
        'description',
        'actors',
        'institutions',
        'government_level',
        'geographic_scope',
        'policy_areas',
        'events',
        'issues',
        'analysis_metadata'
    ]

    ordered_data = OrderedDict()
    for key in key_order:
        if key in json_data:
            ordered_data[key] = json_data[key]

    for key, value in json_data.items():
        if key not in ordered_data:
            ordered_data[key] = value

    return dict(ordered_data)


class NewsArticleViewer:
    def __init__(self):
        self.current_data = []
        self.current_index = 0
        self.df = None
        self.use_all_articles = False

    def extract_full_reasoning(self, llm_text: str) -> str:
        """Extract the full reasoning from the LLM response"""
        if not llm_text:
            return "No reasoning available"
        json_start = llm_text.find('```json')
        reasoning = llm_text[:json_start].strip() if json_start > 0 else llm_text
        reasoning = reasoning.replace('\\n', '\n').replace('\\"', '"')
        return reasoning

    def load_articles(self, limit: int = 100, only_relevant: bool = True, use_all: bool = False):
        """Load articles from database with pre-parsed JSON"""
        self.use_all_articles = use_all
        
        query = """
        SELECT 
            r_texts.url,
            publish_date,
            rfeds1->>'url' as response_id,
            rfeds1->'usage'->>'total_tokens' as tokens_used,
            rfeds1->'choices'->0->>'text' as llm_text,
            rfeds1_validated as validated_json,
            rfeds1 as full_response
        FROM r_texts
        JOIN r_articles ON r_texts.url = r_articles.url
        WHERE rfeds1_validated IS NOT NULL
        """
        
        if only_relevant and not use_all:
            query += " AND (rfeds1_validated->>'relevant')::boolean = true"
        
        query += f" ORDER BY publish_date DESC LIMIT {limit}"

        try:
            df, error = execute_query(query)
            
            if error:
                print(f"Database error: {error}")
                return None, error
                
            if df is None or df.empty:
                return None, "No data found in the database"

            # Convert publish_date to datetime
            if 'publish_date' in df.columns:
                df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')

            # Process JSON data
            df['extracted_json'] = df['validated_json'].apply(
                lambda x: reorder_json_keys(x) if x else None
            )
            df['is_relevant'] = df['extracted_json'].apply(
                lambda x: x.get('relevant', False) if x else False
            )

            self.current_data = df.to_dict('records')
            self.df = df
            self.current_index = 0
            
            return df, None
            
        except Exception as e:
            print(f"Error loading articles: {e}")
            return None, f"Error loading articles: {str(e)}"

    def format_json_display(self, json_data: dict) -> str:
        """Pretty JSON"""
        if not json_data:
            return "No JSON data extracted"
        try:
            return json.dumps(json_data, indent=2, ensure_ascii=False)
        except Exception as e:
            return f"Error formatting JSON: {str(e)}"

    def get_current_article(self):
        """Return current article's displays"""
        if not self.current_data:
            return "No data loaded", "No data", "No metadata", 0, 0
            
        try:
            current = self.current_data[self.current_index]
            reasoning_text = self.extract_full_reasoning(current.get('llm_text', ''))
            json_data = current.get('extracted_json')
            json_display = self.format_json_display(json_data)

            metadata_parts = []
            metadata_parts.append(f"**Article URL:** {current.get('url', 'Unknown')}")
            metadata_parts.append(f"**Response ID:** {current.get('response_id', 'Unknown')}")
            
            publish_date = current.get('publish_date')
            if publish_date:
                if isinstance(publish_date, str):
                    metadata_parts.append(f"**Published:** {publish_date}")
                elif pd.notna(publish_date):
                    metadata_parts.append(f"**Published:** {publish_date.strftime('%Y-%m-%d')}")
            
            tokens = current.get('tokens_used')
            if tokens:
                metadata_parts.append(f"**Tokens Used:** {tokens}")
                
            metadata = "\n".join(metadata_parts)

            return (reasoning_text, json_display, metadata,
                    self.current_index + 1, len(self.current_data))
                    
        except Exception as e:
            print(f"Error getting current article: {e}")
            return f"Error: {str(e)}", "Error", "Error", 0, 0

    def set_current_by_article_id(self, article_id: str):
        """Jump to article by URL (article_id)"""
        if not self.current_data:
            return "No data loaded", "No data", "No metadata", 0, 0
            
        for i, rec in enumerate(self.current_data):
            if rec.get('url') == article_id:
                self.current_index = i
                break
                
        return self.get_current_article()

    def navigate_articles(self, direction: str):
        """Navigate through loaded articles"""
        if not self.current_data:
            return "No data loaded", "No data", "No metadata", 0, 0
            
        if direction == 'next':
            self.current_index = min(self.current_index + 1, len(self.current_data) - 1)
        elif direction == 'previous':
            self.current_index = max(self.current_index - 1, 0)
        elif direction == 'first':
            self.current_index = 0
        elif direction == 'last':
            self.current_index = len(self.current_data) - 1
            
        return self.get_current_article()

    def get_statistics(self) -> str:
        """Simple stats"""
        if self.df is None or self.df.empty:
            return "No data loaded"
            
        stats = []
        stats.append(f"**Total Articles:** {len(self.df)}")
        stats.append(f"**Relevant Articles:** {self.df['is_relevant'].sum()}")
        stats.append(f"**JSON Extraction Success:** {self.df['extracted_json'].notna().sum()}")
        return "\n".join(stats)


# Global viewer instance
viewer = NewsArticleViewer()


def build_article_viewer_tab(app: gr.Blocks):
    """Build the article viewer tab"""
    
    with gr.Tab("Article Viewer"):
        gr.Markdown("## Article Viewer - Model Reasoning & Extracted JSON")
        gr.Markdown("View the OSS-120B model's reasoning process and the structured JSON output")

        # TOP CONTROLS
        with gr.Row():
            load_btn = gr.Button("Load Articles", variant="primary", size="lg")
            first_btn = gr.Button("⏮ First")
            prev_btn = gr.Button("◀ Previous")
            next_btn = gr.Button("Next ▶")
            last_btn = gr.Button("Last ⏭")

        with gr.Row():
            selected_article = gr.Dropdown(
                label="Jump to Article URL", 
                choices=[], 
                value=None, 
                interactive=True,
                filterable=True
            )

        position_text = gr.Markdown("**Position:** 0 / 0")
        status_text = gr.Markdown("")
        metadata_text = gr.Markdown("No metadata")

        # MAIN CONTENT: Reasoning & JSON side-by-side
        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown("### Model Reasoning Process")
                reasoning_display = gr.Markdown(
                    "Click 'Load Articles' to view reasoning",
                    elem_classes=["reasoning-text"]
                )
                
            with gr.Column(scale=1):
                gr.Markdown("### Extracted Government Data (JSON)")
                json_display = gr.Code(
                    label="", 
                    language="json", 
                    lines=30, 
                    value="{}"
                )

        # Dataset Statistics
        gr.Markdown("### Dataset Statistics")
        stats_text = gr.Markdown("No statistics available")

        # Callbacks
        def load_articles_callback():
            """Load articles with limit=100, only_relevant=True"""
            global viewer
            
            try:
                df, error = viewer.load_articles(limit=100, only_relevant=True, use_all=False)

                if error:
                    return (
                        "Error occurred during loading",
                        "{}",
                        "No metadata",
                        "**Position:** 0 / 0",
                        f"❌ Error: {error}",
                        gr.Dropdown(choices=[], value=None),
                        "No data loaded"
                    )

                if df is None or df.empty:
                    return (
                        "No articles found",
                        "{}",
                        "No metadata",
                        "**Position:** 0 / 0",
                        "⚠️ Warning: No relevant articles found",
                        gr.Dropdown(choices=[], value=None),
                        "No data loaded"
                    )

                reasoning, json_str, metadata, current, total = viewer.get_current_article()
                ids = [rec.get('url') for rec in viewer.current_data if rec.get('url')]
                stats = viewer.get_statistics()
                
                return (
                    reasoning,
                    json_str,
                    metadata,
                    f"**Position:** {current} / {total}",
                    f"✅ Successfully loaded {total} articles",
                    gr.Dropdown(choices=ids, value=ids[0] if ids else None),
                    stats
                )
                
            except Exception as e:
                print(f"Error in load_articles_callback: {e}")
                return (
                    f"Error: {str(e)}",
                    "{}",
                    "No metadata",
                    "**Position:** 0 / 0",
                    f"❌ Error: {str(e)}",
                    gr.Dropdown(choices=[], value=None),
                    "No data loaded"
                )

        def nav_callback(direction):
            """Navigate articles"""
            global viewer
            
            try:
                reasoning, json_str, metadata, current, total = viewer.navigate_articles(direction)
                current_id = viewer.current_data[viewer.current_index].get('url') if viewer.current_data else None
                ids = [rec.get('url') for rec in viewer.current_data if rec.get('url')]
                stats = viewer.get_statistics()
                
                return (
                    reasoning, 
                    json_str, 
                    metadata,
                    f"**Position:** {current} / {total}",
                    "",
                    gr.Dropdown(choices=ids, value=current_id),
                    stats
                )
                
            except Exception as e:
                print(f"Error in nav_callback: {e}")
                return (
                    f"Error: {str(e)}", 
                    "{}", 
                    "Error",
                    "**Position:** 0 / 0",
                    f"❌ Error: {str(e)}",
                    gr.Dropdown(choices=[], value=None),
                    "Error"
                )

        def jump_to_callback(article_id):
            """Jump to specific article"""
            global viewer
            
            try:
                reasoning, json_str, metadata, current, total = viewer.set_current_by_article_id(article_id)
                stats = viewer.get_statistics()
                
                return (
                    reasoning, 
                    json_str, 
                    metadata,
                    f"**Position:** {current} / {total}",
                    "",
                    stats
                )
                
            except Exception as e:
                print(f"Error in jump_to_callback: {e}")
                return (
                    f"Error: {str(e)}", 
                    "{}", 
                    "Error",
                    "**Position:** 0 / 0",
                    f"❌ Error: {str(e)}",
                    "Error"
                )

        # Wire up all the controls
        load_btn.click(
            load_articles_callback,
            outputs=[reasoning_display, json_display, metadata_text, position_text, 
                    status_text, selected_article, stats_text]
        )

        first_btn.click(
            lambda: nav_callback('first'),
            outputs=[reasoning_display, json_display, metadata_text, position_text, 
                    status_text, selected_article, stats_text]
        )
        
        prev_btn.click(
            lambda: nav_callback('previous'),
            outputs=[reasoning_display, json_display, metadata_text, position_text, 
                    status_text, selected_article, stats_text]
        )
        
        next_btn.click(
            lambda: nav_callback('next'),
            outputs=[reasoning_display, json_display, metadata_text, position_text, 
                    status_text, selected_article, stats_text]
        )
        
        last_btn.click(
            lambda: nav_callback('last'),
            outputs=[reasoning_display, json_display, metadata_text, position_text, 
                    status_text, selected_article, stats_text]
        )

        selected_article.change(
            jump_to_callback,
            inputs=[selected_article],
            outputs=[reasoning_display, json_display, metadata_text, position_text, 
                    status_text, stats_text]
        )

        # Auto-load on startup
        app.load(
            load_articles_callback,
            outputs=[reasoning_display, json_display, metadata_text, position_text, 
                    status_text, selected_article, stats_text]
        )