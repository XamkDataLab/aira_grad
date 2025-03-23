import streamlit as st 
from db import *
from palotapahtumat import *
from vasteet import *

st.set_page_config(page_title="Air App")#, layout="wide")

tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Koti", "Palotapahtumien analyysi", "Semanttinen haku riskiarvioista", "Delfoi-asiantuntija", "Hätäkeskusten vasteet", "Data", "Tietoja"])

with tab1:
    st.header("Tervetuloa AIRA-applikaation kotisivulle!")
    st.write("Data on Mikkeliläisellä virtuaalikoneella tietokannassa. Tämä äppi tulee olemaan myös siellä omalla koneellaan. Tähän on yhdistetty OpenAI:n API jolle voi syöttää tietokannasta tekstiä, tehtävänkuvauksen ja system-kehotteen ")

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
    st.write("'Semanttinen haku'")

with tab4:
    st.write("Kotimaisella uutisdatalla koulutettu Delfoi-asiantuntija koneäly")

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
    st.write("Made in Xamk. Euroopan Unionin osarahoittama")