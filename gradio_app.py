import gradio as gr
from ulkomaiset_datasetit import *
from kotimaiset_datasetit.kotimaiset_datasetit_tabs import *
from kielimalliavustin.kielimalliavustin_tab import *

with gr.Blocks(title="AIRA", theme=gr.themes.Soft(), css="""
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', sans-serif !important;
    }
    .main-tabs button {
        font-weight: 700 !important;
        font-size: 18px !important;
        padding: 8px 12px !important;
    }

    .main-tabs .subtabs button {
        font-weight: 500 !important;
        font-size: 14px !important;
    }

    .icon-large {
        font-size: 150px !important;
        line-height: 1 !important;
    }
""") as demo:
    
    with gr.Row(equal_height=True, elem_id="header-row"):
        with gr.Column(scale=1, min_width=120):
            gr.Image("digitalia-logo-nelio-768x768.jpg", show_label=False, show_download_button=False, 
                    container=False, height=120, show_fullscreen_button=False)
        with gr.Column(scale=1, min_width=200):
            gr.Image("Logo_yhdistetty.png", show_label=False, show_download_button=False, 
                    container=False, height=150, show_fullscreen_button=False)
        with gr.Column(scale=8):
            gr.Markdown("")
    
    with gr.Tabs(elem_classes="main-tabs"):
        with gr.Tab("Koti"):
            
            with gr.Row():
                with gr.Column(scale=1):
                    gr.HTML("""
                    <div style="display: flex; justify-content: center; align-items: center; cursor: pointer;">
                        <img src="https://img.icons8.com/ios/100/bank-building.png" alt="RAG" style="width: 100px; height: 100px;">
                    </div>
                    """)
                    gr.Markdown("""
                    <div style="text-align: center;">
                    
                    ### RAG omasta datasta
                    - Lataa dokumentit digitalian turvalliselle palvelimelle
                    - Hermeettisesti eristetty turvallinen tekoäly-ympäristö
                    - "keskustele" datasi kanssa
                    - Esim: "miten vuoden 2018 arviot vertautuvat 2023 arvioon?"
                    - Toimii uusien dokumenttien luomisen apuna käyttäen vanhojen koko tietopohjaa
                    
                    </div>
                    """)
                
                with gr.Column(scale=1):
                    gr.HTML("""
                    <div style="display: flex; justify-content: center; align-items: center; cursor: pointer;">
                        <img src="https://img.icons8.com/ios/100/globe.png" alt="Julkinen data" style="width: 100px; height: 100px;">
                    </div>
                    """)
                    gr.Markdown("""
                    <div style="text-align: center;">
                    
                    ### Julkinen data
                    - Dokumenttikohtainen luokittelu annettujen ohjeiden mukaan
                    - Robotti lukee 10 000 uutista päivässä
                    - Esim: "mistä teknologisista uhkista on uutisoitu kuluneen kuukauden aikana"
                    - Ketkä venäjän federaation toimijat ovat vaihtuneet uusiin kuluneen kuukauden aikana
                
                    
                    </div>
                    """)
        
        with gr.Tab("Kielimalliavustin"):
            build_kielimalliavustin_tab(demo)

        with gr.Tab("Delfoi-paneeli"):
            build_delfoi_panel_tab(demo)

        with gr.Tab("Kotimaiset Aineistot"):
            build_kotimaiset_datasetit_tab(demo)
            
        with gr.Tab("Ulkomaiset aineistot"):
            with gr.Tabs(elem_classes="subtabs"):
                build_article_viewer_tab(demo)
                build_institution_analysis_tab(demo)
                build_putin_proximity_tab(demo)
                build_appointments_tab(demo)
    
# Launch the app
if __name__ == "__main__":
    demo.launch(share=False)