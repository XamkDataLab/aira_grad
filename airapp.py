import gradio as gr
from db import *
from palotapahtumat import *
from vasteet import *
import pandas as pd
from vakiluvut import *

municipality_data_df = None
municipality_available_event_types = []

def load_municipality_data_on_startup():
    """Load municipality data when app starts"""
    try:
        global municipality_data_df, municipality_available_event_types
        municipality_data_df, municipality_available_event_types = load_municipality_data()
        print(f"Municipality data loaded. Available event types: {municipality_available_event_types}")
        return gr.Dropdown(choices=municipality_available_event_types, 
                         value=municipality_available_event_types[:2] if municipality_available_event_types else [],
                         multiselect=True)
    except Exception as e:
        print(f"Error loading municipality data: {e}")
        return gr.Dropdown(choices=[], value=[], multiselect=True)

def municipality_analysis_wrapper(selected_event_types):
    """Wrapper function for municipality analysis"""
    return analyze_municipality_incidents(selected_event_types, municipality_data_df)

def create_gradio_app():
    with gr.Blocks(title="Aira", theme=gr.themes.Base()) as app:
        gr.Markdown("# AIRA-projekti")
        
        with gr.Tabs():
            # Tab 1: Koti
            with gr.TabItem("Koti"):
               
                gr.Markdown("## Palotapahtumien analyysi")
                gr.Markdown("""
                **Datasetit:** Hätäkeskusten vasteet ja päivittäiset säähavainnot 10 vuoden ajalta
                
                **Analyysi:** Onko tulipalopakkasia vielä olemassa?
            
                """)
                
                gr.Markdown("## Vasteiden aikasarja-analyysi")
                gr.Markdown("""
                **Datasetti:** Hätäkeskusten vasteet 10 vuoden ajalta.
                
                **Analyysi:** Aikasarja-analyysi vasteiden tyypistä 
            
                """)
                
                gr.Markdown("## Vasteet/1000 asukasta")
                gr.Markdown("""
                **Datasetit:** Hätäkeskusten vasteet 10 vuoden ajalta ja Suomen väkiluvut
                
                **Analyysi:** Kuvaus tapahtumatiheyden analyysista kunnittain per 1000 asukasta (2024 väestömäärä)
                
                
                """)
                
                gr.Markdown("## Delfoi-asiantuntija")
                gr.Markdown("""
                **Datasetti:** Kaikki Suomen tekstimuotoiset uutiset
                
                **Analyysi:** AI-asiantuntijajärjestelmä
                
            
                """)
                
                gr.Markdown("## Vaste kartta")
                gr.Markdown("""
                **Datasetti:** 
                
                **Analyysi:**
                
                """)
                
                

            with gr.TabItem("Palotapahtumien analyysi"):
                gr.Markdown("## Palotapahtumien analyysi")
                
                with gr.Row():
                    axis_dropdown = gr.Dropdown(
                        choices=["Keskisademäärä", "Keskilämpötila", "Maksimilämpötila", 
                                "Minimilämpötila", "Lumensyvyys"],
                        value="Keskisademäärä",
                        label="Valitse X-akseli"
                    )
                    
                    event_type_select = gr.Dropdown(
                        choices=[],
                        value=[],
                        label="Valitse palotapahtuman tyyppi",
                        multiselect=True
                    )
                
                fire_chart = gr.Plot(label="Palotapahtumien määrä vs. valittu muuttuja")
                info_text = gr.Markdown("")
                
                def update_fire_analysis(axis, events):
                    try:
                        result = create_fire_analysis_chart(axis, events)
                        if result is None or len(result) != 3:
                            return None, "Error creating chart", gr.Dropdown(choices=[], value=[])
                        
                        fig, info, event_options = result
                        
                        # Ensure event_options is a list
                        if not isinstance(event_options, list):
                            event_options = []
                        
                        return fig, info, gr.Dropdown(choices=event_options, value=events)
                    except Exception as e:
                        print(f"Error in update_fire_analysis: {e}")
                        return None, f"Error: {str(e)}", gr.Dropdown(choices=[], value=[])
                
        
                def initial_fire_load():
                    try:
                        result = create_fire_analysis_chart("Keskisademäärä", [])
                        if result is None or len(result) != 3:
                            return None, "Error loading initial data", gr.Dropdown(choices=[], value=[])
                        
                        fig, info, event_options = result
                        if not isinstance(event_options, list):
                            event_options = []
                            
                        return fig, info, gr.Dropdown(choices=event_options, value=[])
                    except Exception as e:
                        print(f"Error in initial_fire_load: {e}")
                        return None, f"Error: {str(e)}", gr.Dropdown(choices=[], value=[])
                
                app.load(
                    fn=initial_fire_load,
                    outputs=[fire_chart, info_text, event_type_select]
                )
                
                # Update on change
                axis_dropdown.change(
                    fn=update_fire_analysis,
                    inputs=[axis_dropdown, event_type_select],
                    outputs=[fire_chart, info_text, event_type_select]
                )
                
                event_type_select.change(
                    fn=update_fire_analysis,
                    inputs=[axis_dropdown, event_type_select],
                    outputs=[fire_chart, info_text, event_type_select]
                )
            
            with gr.TabItem("Hätäkeskusten vasteet"):
                gr.Markdown("## Vasteiden aikasarja-analyysi")
                
                min_date, max_date, event_types, municipalities, hake_values = get_data_ranges()
                
                if min_date is None:
                    gr.Markdown("Could not retrieve data from database.")
                else:
                    with gr.Row():
                        start_date = gr.Textbox(
                            value=str(min_date) if min_date else "2020-01-01", 
                            label="Aloituspäivä (YYYY-MM-DD)",
                            placeholder="2020-01-01"
                        )
                        end_date = gr.Textbox(
                            value=str(max_date) if max_date else "2024-12-31", 
                            label="Lopetuspäivä (YYYY-MM-DD)",
                            placeholder="2024-12-31"
                        )
                    
                    with gr.Row():
                        event_select = gr.Dropdown(
                            choices=event_types,
                            value=[],
                            label="Valitse vasteen tyyppi (vain ne joita yli 100 näytetään)",
                            multiselect=True
                        )
                        time_agg = gr.Dropdown(
                            choices=["Päivä", "Viikko", "Kuukausi", "Vuosi"],
                            value="Päivä",
                            label="Aikajakson ryhmittely"
                        )
                    
                    with gr.Row():
                        municipality_select = gr.Dropdown(
                            choices=municipalities,
                            value=[],
                            label="Valitse kunta (valinnainen)",
                            multiselect=True
                        )
                        hake_select = gr.Dropdown(
                            choices=hake_values,
                            value=[],
                            label="Valitse hätäkeskus (valinnainen)",
                            multiselect=True
                        )
                    
                    show_hake = gr.Checkbox(label="Näytä vasteet hätäkeskuksittain", value=False)
                    
                    summary_markdown = gr.Markdown("")
                    main_chart = gr.Plot(label="Valitut vasteet prosentteina kaikista")
                    
                    with gr.Column() as hake_charts_container:
                        gr.Markdown("### Hätäkeskus analyysi")
                        hake_chart_outputs = [gr.Plot(label=f"Hake Chart {i+1}", visible=True) for i in range(5)]
                    
                    sample_data = gr.DataFrame(label="Data Sample (first 100 rows)")
                    
                    def update_rescue_events_wrapper(start, end, events, munis, hakes, time_agg, show_hake):
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
                    
                    inputs = [start_date, end_date, event_select, municipality_select, 
                             hake_select, time_agg, show_hake]
                    outputs = [main_chart, summary_markdown, sample_data] + hake_chart_outputs
                    
                    for input_component in inputs:
                        input_component.change(
                            fn=update_rescue_events_wrapper,
                            inputs=inputs,
                            outputs=outputs
                        )
            
            
            with gr.TabItem("Väkiluvut"):
                gr.Markdown("## Tapahtumatiheyden analyysi kunnittain")
                gr.Markdown("Analysoidaan vasteiden tapahtumatiheys per 1000 asukasta (2024 väkilukudata)")
                
                with gr.Row():
                    with gr.Column(scale=1):
                        municipality_event_selector = gr.Dropdown(
                            choices=[],
                            multiselect=True,
                            label="Valitse tapahtumatyypit",
                            info="Valitse yksi tai useampi tapahtumatyyppi analysoitavaksi",
                            value=[]
                        )
                        
                        municipality_analyze_btn = gr.Button("Analysoi", variant="primary")
                    
                    with gr.Column(scale=2):
                        municipality_summary_text = gr.Markdown("")
                
                with gr.Row():
                    municipality_results_table = gr.DataFrame(
                        label="Top 10 kuntaa tapahtumatyypeittäin",
                        interactive=False
                    )
                
                with gr.Row():
                    municipality_chart = gr.Plot(label="Visualisointi")

                app.load(
                    fn=load_municipality_data_on_startup,
                    outputs=[municipality_event_selector]
                    )
                
                municipality_analyze_btn.click(
                    fn=municipality_analysis_wrapper,
                    inputs=[municipality_event_selector],
                    outputs=[municipality_results_table, municipality_chart, municipality_summary_text]
                )
            
                 
            with gr.TabItem("Delfoi-asiantuntija"):
                gr.Markdown("## Kotimaisella datalla koulutettu Delfoi-asiantuntija koneäly jolla on holistinen käsitys riskeistä")
            
            with gr.TabItem("Vaste kartta"):
                gr.Markdown("## Vasteet kartalla")
                gr.Markdown("Karttanäkymä tulee tähän...") 
    
            with gr.TabItem("Muuta"):
                gr.Markdown("## Made in XAMK")
               
    
    
    return app

if __name__ == "__main__":
    app = create_gradio_app()
    app.launch(share=True)
