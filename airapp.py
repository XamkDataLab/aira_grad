import streamlit as st 
from db import *
from palotapahtumat import *
from vasteet import *
from uutiset import *
import folium
from folium.plugins import MarkerCluster
from geopy.geocoders import Nominatim
from streamlit_folium import folium_static
from LLM import *

st.set_page_config(page_title="Air App", layout="wide")

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Koti", "Palotapahtumien analyysi", "Semanttinen haku riskiarvioista", "Delfoi-asiantuntija", "Hätäkeskusten vasteet", "Data", "Vaste kartta"])

with tab1:
    st.header("Tervetuloa AIRA-applikaation kotisivulle!")
    #st.write("Data on Mikkeliläisellä virtuaalikoneella tietokannassa. Tämä äppi tulee olemaan myös siellä omalla koneellaan. Tähän on yhdistetty OpenAI:n API jolle voi syöttää tietokannasta tekstiä, tehtävänkuvauksen ja system-kehotteen ")
    st.image("data:image/png;base64,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")
with tab2:
    st.title("Palotapahtumien analyysi")
    
        # Reduce top padding above the heading with a small CSS snippet.
    st.markdown(
        """
        <style>
        .css-18e3th9 { padding-top: 10px; }
        </style>
        """,
    unsafe_allow_html=True
    )
    
    create_fire_analysis_chart()
    
with tab3:
    st.title("Semanttinen haku")
    
    st.markdown("[Google NotebookLM](https://notebooklm.google/)")
    
    # Alternatively, if you want the image itself to be a clickable link:
    st.markdown("""
        <a href="https://www.google.com" target="_blank">
            <img src="https://www.google.com/images/branding/googlelogo/1x/googlelogo_color_272x92dp.png" alt="Google Logo">
        </a>
        """, unsafe_allow_html=True)
    st.write("Järjestetäänkö sidosryhmille Google NotebookLM koulutusta?")

with tab4:
    st.title("Kotimaisella uutisdatalla koulutettu Delfoi-asiantuntija koneäly")
        # Path to your SQLite database
    db_path = db_file
    
    # Create the full_text table if it doesn't exist
    create_fulltext_table(db_path)
    
    # Read your DataFrame with URLs
    # Replace this with the actual code to load your DataFrame
    df = pd.read_sql("""
    SELECT 
        articles.url, articles.headline, articles.source, articles.category, 
        articles.click_count, articles.timestamp
    FROM 
        articles
    WHERE 
        articles.url NOT IN (SELECT url FROM full_text)
        AND (
            articles.url LIKE '%iltalehti.fi%' 
            OR articles.url LIKE '%is.fi%'
        )
    """, sqlite3.connect(db_path))
    st.dataframe(df)
    
    #df["api_response"] = df["headline"].apply(process_headline)
    #st.dataframe(df)
                                    
    
    if st.button("Hae artikkelien data"):
        # Show a progress message
        with st.spinner("Haetaan dataa..."):
            # Process the URLs from the DataFrame
            process_df_urls(df, 'url', db_path, max_concurrent=5, delay=1.0)
        
        # Show success message after completion
        st.success("Datan haku valmis!")
    #joo
    
    # Print statistics
    stats = get_fulltext_stats(db_path)
    st.write(f"Total articles: {stats['total_articles']}")
    st.write(f"Articles by source: {stats['by_source']}")
    st.write(f"Latest extraction: {stats['latest_extraction']}")
    

with tab5:
    rescue_events_dashboard()

with tab6:
    st.write("Datalähteet tänne")
    st.write("Tilannehuoneen tilanteet: 760 000 kpl")
    st.write("Riskiarviot: tekstidata luettu PDF-tiedostoista")
    st.write("Ylen ja Iltalehden uutiset: 1 000 000+ kpl")
    st.write("Ilmatieteenlaitoksen kaikkien sääasemien päivittäiset tiedot 10v ajalta")
    st.write("kaikki vauva.fi -foorumin postaukset")
    st.write("jne. jne.")

with tab7:
    #st.write("Made in Xamk. Euroopan Unionin osarahoittama")
    st.title("Vasteet kartalla")

    
