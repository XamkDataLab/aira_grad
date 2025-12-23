import pandas as pd
import gradio as gr
from db import execute_query


class AppointmentsViewer:
    def __init__(self):
        self.df = None
        self.filtered_df = None

    def load_appointments(self):
        """Load appointments and dismissals from database"""
        query = """
        SELECT 
            rt.url,
            ra.publish_date,
            actor->>'name' as official,
            actor->>'position' as position,
            actor->>'institution' as institution,
            actor->>'government_level' as level,
            action::text as details,
            rt.rfeds1_validated->'key_events'->0->>'event' as first_key_event
        FROM r_texts rt
        JOIN r_articles ra ON rt.url = ra.url
        CROSS JOIN jsonb_array_elements(rt.rfeds1_validated->'actors') actor
        CROSS JOIN jsonb_array_elements(actor->'actions') action
        WHERE action::text ~* '(appointed|was dismissed|was fired|resigned|relieved|stepped down|took office)'
        ORDER BY publish_date DESC
        """

        try:
            df, error = execute_query(query)
            
            if error:
                print(f"Database error: {error}")
                return None, error
                
            if df is None or df.empty:
                return None, "No appointments/dismissals found in the database"

            # Convert publish_date to datetime
            if 'publish_date' in df.columns:
                df['publish_date'] = pd.to_datetime(df['publish_date'], errors='coerce')
                # Format for display
                df['publish_date'] = df['publish_date'].dt.strftime('%Y-%m-%d')

            # Filter out foreign government level
            df = df[df['level'].str.lower() != 'foreign'].copy()
            
            # Clean up details column (remove quotes and extra formatting)
            if 'details' in df.columns:
                df['details'] = df['details'].str.strip('"')
            
            self.df = df
            self.filtered_df = df.copy()
            
            return df, None
            
        except Exception as e:
            print(f"Error loading appointments: {e}")
            return None, f"Error loading appointments: {str(e)}"

    def get_filter_options(self):
        """Get unique values for filter dropdowns"""
        if self.df is None or self.df.empty:
            return [], []
        
        institutions = sorted(self.df['institution'].dropna().unique().tolist())
        levels = sorted(self.df['level'].dropna().unique().tolist())
        
        return institutions, levels

    def apply_filters(self, institution_filter, level_filter):
        """Apply selected filters to the dataframe"""
        if self.df is None or self.df.empty:
            return pd.DataFrame(), "No data loaded"
        
        filtered = self.df.copy()
        
        # Apply institution filter
        if institution_filter and institution_filter != "All":
            filtered = filtered[filtered['institution'] == institution_filter]
        
        # Apply government level filter
        if level_filter and level_filter != "All":
            filtered = filtered[filtered['level'] == level_filter]
        
        self.filtered_df = filtered
        
        stats = f"Showing {len(filtered)} of {len(self.df)} total appointments/dismissals"
        
        return filtered, stats


# Global viewer instance
appointments_viewer = AppointmentsViewer()


def build_appointments_tab(app: gr.Blocks):
    """Build the appointments and dismissals viewer tab"""
    
    with gr.Tab("Appointments & Dismissals"):
        gr.Markdown("## Government Appointments & Dismissals Tracker")
        gr.Markdown("View and filter government personnel changes including appointments, dismissals, and resignations")

        # CONTROLS
        with gr.Row():
            load_btn = gr.Button("Load Data", variant="primary", size="lg")
            refresh_btn = gr.Button("Reset Filters", size="lg")

        with gr.Row():
            institution_filter = gr.Dropdown(
                label="Filter by Institution",
                choices=["All"],
                value="All",
                interactive=True,
                filterable=True
            )
            level_filter = gr.Dropdown(
                label="Filter by Government Level",
                choices=["All"],
                value="All",
                interactive=True
            )

        status_text = gr.Markdown("")
        
        # DATA DISPLAY
        gr.Markdown("### Personnel Changes")
        data_table = gr.Dataframe(
            value=pd.DataFrame(),
            headers=["URL", "Publish Date", "Official", "Position", "Institution", "Level", "Details", "First Key Event"],
            interactive=False,
            wrap=True
        )

        # STATISTICS
        gr.Markdown("### Statistics")
        stats_text = gr.Markdown("No data loaded")

        # Callbacks
        def load_data_callback():
            """Load appointments data"""
            global appointments_viewer
            
            try:
                df, error = appointments_viewer.load_appointments()

                if error:
                    return (
                        pd.DataFrame(),
                        f"❌ Error: {error}",
                        "No data loaded",
                        gr.Dropdown(choices=["All"], value="All"),
                        gr.Dropdown(choices=["All"], value="All")
                    )

                if df is None or df.empty:
                    return (
                        pd.DataFrame(),
                        "⚠️ Warning: No appointments/dismissals found",
                        "No data loaded",
                        gr.Dropdown(choices=["All"], value="All"),
                        gr.Dropdown(choices=["All"], value="All")
                    )

                institutions, levels = appointments_viewer.get_filter_options()
                
                stats = f"""
**Total Records:** {len(df)}
**Unique Officials:** {df['official'].nunique()}
**Institutions:** {df['institution'].nunique()}
**Government Levels:** {df['level'].nunique()}
                """
                
                return (
                    df,
                    f"✅ Successfully loaded {len(df)} records (foreign government level excluded)",
                    stats,
                    gr.Dropdown(choices=["All"] + institutions, value="All"),
                    gr.Dropdown(choices=["All"] + levels, value="All")
                )
                
            except Exception as e:
                print(f"Error in load_data_callback: {e}")
                return (
                    pd.DataFrame(),
                    f"❌ Error: {str(e)}",
                    "No data loaded",
                    gr.Dropdown(choices=["All"], value="All"),
                    gr.Dropdown(choices=["All"], value="All")
                )

        def apply_filters_callback(institution, level):
            """Apply filters to the data"""
            global appointments_viewer
            
            try:
                filtered_df, filter_stats = appointments_viewer.apply_filters(institution, level)
                
                if filtered_df.empty:
                    stats = "No records match the selected filters"
                else:
                    stats = f"""
{filter_stats}

**Unique Officials:** {filtered_df['official'].nunique()}
**Institutions:** {filtered_df['institution'].nunique()}
**Government Levels:** {filtered_df['level'].nunique()}
                    """
                
                return (
                    filtered_df,
                    f"🔍 {filter_stats}",
                    stats
                )
                
            except Exception as e:
                print(f"Error in apply_filters_callback: {e}")
                return (
                    pd.DataFrame(),
                    f"❌ Error: {str(e)}",
                    "Error applying filters"
                )

        def reset_filters_callback():
            """Reset all filters"""
            global appointments_viewer
            
            if appointments_viewer.df is None or appointments_viewer.df.empty:
                return (
                    pd.DataFrame(),
                    "No data loaded",
                    "No data loaded",
                    "All",
                    "All"
                )
            
            df = appointments_viewer.df
            stats = f"""
**Total Records:** {len(df)}
**Unique Officials:** {df['official'].nunique()}
**Institutions:** {df['institution'].nunique()}
**Government Levels:** {df['level'].nunique()}
            """
            
            return (
                df,
                f"✅ Filters reset - showing all {len(df)} records",
                stats,
                "All",
                "All"
            )

        # Wire up controls
        load_btn.click(
            load_data_callback,
            outputs=[data_table, status_text, stats_text, institution_filter, level_filter]
        )

        refresh_btn.click(
            reset_filters_callback,
            outputs=[data_table, status_text, stats_text, institution_filter, level_filter]
        )

        institution_filter.change(
            apply_filters_callback,
            inputs=[institution_filter, level_filter],
            outputs=[data_table, status_text, stats_text]
        )

        level_filter.change(
            apply_filters_callback,
            inputs=[institution_filter, level_filter],
            outputs=[data_table, status_text, stats_text]
        )

        # Auto-load on startup
        app.load(
            load_data_callback,
            outputs=[data_table, status_text, stats_text, institution_filter, level_filter]
        )