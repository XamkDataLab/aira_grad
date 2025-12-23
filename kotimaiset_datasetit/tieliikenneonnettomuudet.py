import gradio as gr
import pandas as pd
import folium
from folium.plugins import MarkerCluster, HeatMap
import matplotlib.pyplot as plt
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# Use shared database connection from main app
from db import execute_query

# --------------------------------------

current_data = None

def load_data():
    """
    Load data from PostgreSQL table traffic_accidents with optimized query.
    """
    global current_data
    try:
        table_name = "traffic_accidents"
        # Select only needed columns to reduce data transfer
        query = f"""
        SELECT 
            wgs_lat, wgs_lon, etrs_x, etrs_y,
            ONNTYYPPI, VAKAV, VVONN, KKONN, KELLO,
            LKMHAPA, LKMLAKA, LKMJK, LKMPP, LKMMO, LKMMP, LKMMUUKULK
        FROM {table_name}
        WHERE wgs_lat IS NOT NULL 
        AND wgs_lon IS NOT NULL
        AND wgs_lat BETWEEN 59 AND 71
        AND wgs_lon BETWEEN 19 AND 32
        """
        
        df, db_err = execute_query(query)
        if db_err:
            return None, f"Virhe ladattaessa: {db_err}", None
        if df.empty:
            return None, f"Taulu '{table_name}' ei palauttanut rivejä.", None

        # Coordinates
        df['lat'] = df['wgs_lat']
        df['lon'] = df['wgs_lon']

        # Original coordinates if available
        df['original_x'] = df['etrs_x'] if 'etrs_x' in df.columns else 0
        df['original_y'] = df['etrs_y'] if 'etrs_y' in df.columns else 0

        # Normalize column names
        df.columns = [col.upper() if col.lower() in [
            'onntyyppi', 'vakav', 'vvonn', 'kkonn', 'kello',
            'lkmhapa', 'lkmlaka', 'lkmjk', 'lkmpp',
            'lkmmo', 'lkmmp', 'lkmmuukulk'
        ] else col for col in df.columns]

        # Numeric fields
        numeric_columns = ['VVONN', 'KKONN', 'VAKAV', 'ONNTYYPPI', 'KELLO']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                if col == 'ONNTYYPPI':
                    df[col] = df[col].fillna(99).astype(int)
                elif col == 'VAKAV':
                    df[col] = df[col].fillna(0).astype(int)
                else:
                    df[col] = df[col].fillna(0).astype(int)

        # Severity labels
        severity_labels = {
            0: "Tuntematon",
            1: "Kuolemaan johtava",
            2: "Vammautumiseen johtava",
            3: "Vakava loukkaantuminen",
            4: "Vakavuus 4"
        }
        df['severity_label'] = df['VAKAV'].map(severity_labels).fillna("Tuntematon")

        # Accident type labels
        accident_type_labels = {
            0: "Samat ajosuunnat (ajo suoraan)",
            1: "Samat ajosuunnat (ajo kääntyen)",
            2: "Vastakkaiset ajosuunnat (ajo suoraan)",
            3: "Vastakkaiset ajosuunnat (ajo kääntyen)",
            4: "Risteävät ajosuunnat (ajo suoraan)",
            5: "Risteävät ajosuunnat (ajo kääntyen)",
            6: "Jalankulkijaonnettomuus (suojatiellä)",
            7: "Jalankulkijaonnettomuus (muualla)",
            8: "Tieltä suistuminen",
            9: "Muu onnettomuus",
            99: "Tuntematon"
        }
        df['accident_type_label'] = df['ONNTYYPPI'].map(accident_type_labels).fillna("Tuntematon")

        # Vehicle counts
        vehicle_columns = ['LKMHAPA', 'LKMLAKA', 'LKMJK', 'LKMPP', 'LKMMO', 'LKMMP', 'LKMMUUKULK']
        for col in vehicle_columns:
            if col not in df.columns:
                df[col] = 0
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

        existing_vehicle_cols = [col for col in vehicle_columns if col in df.columns]
        df['total_vehicles'] = df[existing_vehicle_cols].sum(axis=1) if existing_vehicle_cols else 0

        # Datetime + month
        if 'VVONN' in df.columns and 'KKONN' in df.columns:
            df['datetime'] = pd.to_datetime(
                df['VVONN'].astype(str) + '-' + df['KKONN'].astype(str).str.zfill(2) + '-01',
                errors='coerce'
            )
            df['month_name'] = df['datetime'].dt.month_name().fillna("Tuntematon")

        current_data = df

        # Calculate cyclist statistics
        cyclist_accidents = (df['LKMPP'] > 0).sum() if 'LKMPP' in df.columns else 0
        total_cyclists = df['LKMPP'].sum() if 'LKMPP' in df.columns else 0

        # Get available years for filter
        available_years = sorted(df['VVONN'].unique().tolist())

        summary = f"""
        **Ladattu {len(df)} onnettomuustietuetta taulusta {table_name}!**

        **Yhteenveto:**
        - Onnettomuuksia yhteensä: {len(df):,}
        - Kuolemaan johtaneita: {(df['VAKAV'] == 1).sum():,}
        - Vammautumiseen johtaneita: {(df['VAKAV'] == 2).sum():,}
        - Vuosiväli: {df['VVONN'].min():.0f} - {df['VVONN'].max():.0f}
        
        **Pyöräilijät:**
        - Pyöräilijäonnettomuuksia: {cyclist_accidents:,}
        - Pyöräilijöitä yhteensä: {total_cyclists:,}
        """

        severity_options = df['severity_label'].unique().tolist()
        accident_type_options = df['accident_type_label'].unique().tolist()

        return df, summary, (severity_options, accident_type_options, available_years)

    except Exception as e:
        return None, f"Virhe ladattaessa: {str(e)}", None


# -------- Helper functions --------

def apply_filters(df, severity_filter, accident_filter, year_filter, vehicle_filter):
    """Apply all filters to the dataframe"""
    if df is None:
        return None
    
    filtered = df.copy()
    
    # Apply basic filters
    filtered = filtered[
        (filtered['severity_label'].isin(severity_filter)) &
        (filtered['accident_type_label'].isin(accident_filter)) &
        (filtered['VVONN'].isin(year_filter))
    ]
    
    # Apply vehicle filter
    if "Pyöräilijäonnettomuudet" in vehicle_filter:
        filtered = filtered[filtered['LKMPP'] > 0]
    if "Moottoripyöräonnettomuudet" in vehicle_filter:
        filtered = filtered[filtered['LKMMP'] > 0]
    
    return filtered


def create_summary_plots(df, severity_filter, accident_filter, year_filter, vehicle_filter):
    if df is None:
        return "Ei dataa ladattuna", None, None, None

    filtered = apply_filters(df, severity_filter, accident_filter, year_filter, vehicle_filter)

    cyclist_accidents = (filtered['LKMPP'] > 0).sum() if 'LKMPP' in filtered.columns else 0
    total_cyclists = filtered['LKMPP'].sum() if 'LKMPP' in filtered.columns else 0
    motorcycle_accidents = (filtered['LKMMP'] > 0).sum() if 'LKMMP' in filtered.columns else 0
    total_motorcycles = filtered['LKMMP'].sum() if 'LKMMP' in filtered.columns else 0

    stats = f"""
    **Suodatetut tilastot:**
    - Onnettomuuksia: {len(filtered):,}
    - Kuolemaan johtaneita: {len(filtered[filtered['VAKAV'] == 1]):,}
    - Vammautumiseen johtaneita: {len(filtered[filtered['VAKAV'] == 2]):,}
    - Ajoneuvoja keskimäärin: {filtered['total_vehicles'].mean():.1f}
    
    **Pyöräilijätilastot:**
    - Pyöräilijäonnettomuuksia: {cyclist_accidents:,}
    - Pyöräilijöitä yhteensä: {total_cyclists:,}
    
    **Moottoripyörätilastot:**
    - Moottoripyöräonnettomuuksia: {motorcycle_accidents:,}
    - Moottoripyöriä yhteensä: {total_motorcycles:,}
    """

    fig1, ax1 = plt.subplots(figsize=(10, 6))
    accident_dist = filtered['accident_type_label'].value_counts()
    ax1.barh(range(len(accident_dist)), accident_dist.values)
    ax1.set_yticks(range(len(accident_dist)))
    ax1.set_yticklabels(accident_dist.index, fontsize=9)
    ax1.set_xlabel('Määrä')
    ax1.set_title('Onnettomuustyypit')
    ax1.grid(True, alpha=0.3)
    plt.tight_layout()

    fig2, ax2 = plt.subplots(figsize=(8, 6))
    severity_dist = filtered['severity_label'].value_counts()
    ax2.pie(severity_dist.values, labels=severity_dist.index, autopct='%1.1f%%')
    ax2.set_title('Vakavuusaste')

    fig3, ax3 = plt.subplots(figsize=(10, 4))
    if 'KKONN' in filtered.columns:
        monthly_data = filtered.groupby('KKONN').size()
        month_names = ['Tammi', 'Helmi', 'Maalis', 'Huhti', 'Touko', 'Kesä',
                       'Heinä', 'Elo', 'Syys', 'Loka', 'Marras', 'Joulu']
        x_labels = [month_names[m-1] if m <= 12 else str(m) for m in monthly_data.index]
        ax3.plot(range(len(monthly_data)), monthly_data.values, marker='o', linewidth=2)
        ax3.set_xticks(range(len(monthly_data)))
        ax3.set_xticklabels(x_labels, rotation=45)
        ax3.set_ylabel('Onnettomuuksia')
        ax3.set_title('Määrät kuukaudessa')
        ax3.grid(True, alpha=0.3)
    plt.tight_layout()

    return stats, fig1, fig2, fig3


def create_map(df, severity_filter, accident_filter, year_filter, vehicle_filter, map_style, enable_cluster, show_heatmap):
    if df is None:
        return None

    filtered = apply_filters(df, severity_filter, accident_filter, year_filter, vehicle_filter)

    if len(filtered) == 0:
        return "<p>Ei näytettävää dataa kartalla</p>"

    # Sample data if too large for performance
    if len(filtered) > 5000 and not show_heatmap:
        filtered = filtered.sample(n=5000, random_state=42)

    center_lat = 64.0
    center_lon = 26.0

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=5,
        tiles=map_style,
        max_bounds=[[58, 18], [72, 33]]
    )

    if show_heatmap:
        heat_data = [[row['lat'], row['lon']] for _, row in filtered.iterrows()]
        HeatMap(heat_data, radius=15, blur=10, max_zoom=10).add_to(m)
    elif enable_cluster:
        marker_cluster = MarkerCluster().add_to(m)
        for _, row in filtered.iterrows():
            # Color coding based on severity and vehicle type
            if row.get('LKMPP', 0) > 0:
                color = 'blue'
                icon_type = 'bicycle'
            elif row['VAKAV'] == 1:
                color = 'red'
                icon_type = 'exclamation-sign'
            else:
                color = 'orange'
                icon_type = 'warning-sign'
            
            popup_text = f"""
            <b>Tyyppi:</b> {row['accident_type_label']}<br>
            <b>Vakavuus:</b> {row['severity_label']}<br>
            <b>Vuosi:</b> {int(row['VVONN'])}<br>
            """
            if row.get('LKMPP', 0) > 0:
                popup_text += f"<b>Pyöräilijöitä:</b> {int(row['LKMPP'])}<br>"
            if row.get('LKMMP', 0) > 0:
                popup_text += f"<b>Moottoripyöriä:</b> {int(row['LKMMP'])}<br>"
            
            folium.Marker(
                [row['lat'], row['lon']],
                popup=popup_text,
                icon=folium.Icon(color=color, icon=icon_type)
            ).add_to(marker_cluster)
    else:
        for _, row in filtered.head(1000).iterrows():
            if row.get('LKMPP', 0) > 0:
                color = 'blue'
            elif row['VAKAV'] == 1:
                color = 'red'
            else:
                color = 'orange'
            
            popup_text = f"""
            <b>Tyyppi:</b> {row['accident_type_label']}<br>
            <b>Vakavuus:</b> {row['severity_label']}<br>
            <b>Vuosi:</b> {int(row['VVONN'])}<br>
            """
            if row.get('LKMPP', 0) > 0:
                popup_text += f"<b>Pyöräilijöitä:</b> {int(row['LKMPP'])}<br>"
            if row.get('LKMMP', 0) > 0:
                popup_text += f"<b>Moottoripyöriä:</b> {int(row['LKMMP'])}<br>"
                
            folium.Marker(
                [row['lat'], row['lon']],
                popup=popup_text,
                icon=folium.Icon(color=color)
            ).add_to(m)

    return m._repr_html_()


def create_time_analysis(df, severity_filter, accident_filter, year_filter, vehicle_filter):
    if df is None:
        return None, None

    filtered = apply_filters(df, severity_filter, accident_filter, year_filter, vehicle_filter)

    fig1, ax1 = plt.subplots(figsize=(12, 5))
    if 'KELLO' in filtered.columns:
        hourly_data = filtered.groupby('KELLO').size()
        
        # Add cyclist data overlay
        if 'LKMPP' in filtered.columns:
            cyclist_hourly = filtered[filtered['LKMPP'] > 0].groupby('KELLO').size()
            ax1.bar(hourly_data.index, hourly_data.values, alpha=0.7, label='Kaikki')
            ax1.bar(cyclist_hourly.index, cyclist_hourly.values, alpha=0.7, 
                   color='blue', label='Pyöräilijäonnettomuudet')
            ax1.legend()
        else:
            ax1.bar(hourly_data.index, hourly_data.values, alpha=0.7)
            
        ax1.set_xlabel('Tunti (24h)')
        ax1.set_ylabel('Onnettomuuksia')
        ax1.set_title('Onnettomuudet tunneittain')
        ax1.set_xticks(range(0, 24))
        ax1.grid(True, alpha=0.3)
    plt.tight_layout()

    fig2, ax2 = plt.subplots(figsize=(14, 8))
    if 'KKONN' in filtered.columns and 'KELLO' in filtered.columns:
        pivot_data = filtered.groupby(['KELLO', 'KKONN']).size().unstack(fill_value=0)
        month_names = ['Tam', 'Hel', 'Maa', 'Huh', 'Tou', 'Kes',
                       'Hei', 'Elo', 'Syy', 'Lok', 'Mar', 'Jou']
        column_labels = [month_names[m-1] if m <= 12 else str(m) for m in pivot_data.columns]

        im = ax2.imshow(pivot_data.values, aspect='auto', cmap='YlOrRd')
        ax2.set_xticks(range(len(pivot_data.columns)))
        ax2.set_xticklabels(column_labels)
        ax2.set_yticks(range(len(pivot_data.index)))
        ax2.set_yticklabels(pivot_data.index)
        ax2.set_xlabel('Kuukausi')
        ax2.set_ylabel('Tunti')
        ax2.set_title('Onnettomuudet: Tunti vs Kuukausi')
        plt.colorbar(im, ax=ax2, label='Onnettomuuksia')
    plt.tight_layout()

    return fig1, fig2


def create_vehicle_analysis(df, severity_filter, accident_filter, year_filter, vehicle_filter):
    if df is None:
        return None, None, ""

    filtered = apply_filters(df, severity_filter, accident_filter, year_filter, vehicle_filter)

    vehicle_stats = {
        'Autot/pakut': filtered['LKMHAPA'].sum() if 'LKMHAPA' in filtered.columns else 0,
        'Bussit/kuorma': filtered['LKMLAKA'].sum() if 'LKMLAKA' in filtered.columns else 0,
        'Jalankulkijat': filtered['LKMJK'].sum() if 'LKMJK' in filtered.columns else 0,
        'Pyöräilijät': filtered['LKMPP'].sum() if 'LKMPP' in filtered.columns else 0,
        'Mopot': filtered['LKMMO'].sum() if 'LKMMO' in filtered.columns else 0,
        'Moottoripyörät': filtered['LKMMP'].sum() if 'LKMMP' in filtered.columns else 0,
        'Muut': filtered['LKMMUUKULK'].sum() if 'LKMMUUKULK' in filtered.columns else 0
    }

    fig1, ax1 = plt.subplots(figsize=(10, 6))
    # Remove the highlight for cyclists - all bars with same alpha
    bars = ax1.bar(range(len(vehicle_stats)), list(vehicle_stats.values()), alpha=0.7)
    
    ax1.set_xticks(range(len(vehicle_stats)))
    ax1.set_xticklabels(list(vehicle_stats.keys()), rotation=45, ha='right')
    ax1.set_ylabel('Määrä')
    ax1.set_title('Ajoneuvotyypit onnettomuuksissa')
    ax1.grid(True, alpha=0.3)
    plt.tight_layout()

    fig2, ax2 = plt.subplots(figsize=(12, 6))
    if 'KKONN' in filtered.columns:
        month_names = ['Tam', 'Hel', 'Maa', 'Huh', 'Tou', 'Kes',
                       'Hei', 'Elo', 'Syy', 'Lok', 'Mar', 'Jou']

        if 'LKMJK' in filtered.columns:
            pedestrian_data = filtered.groupby('KKONN')['LKMJK'].sum()
            ax2.plot(pedestrian_data.index, pedestrian_data.values, 
                    marker='o', label='Jalankulkijat', linewidth=2, color='green')

        if 'LKMPP' in filtered.columns:
            cyclist_data = filtered.groupby('KKONN')['LKMPP'].sum()
            ax2.plot(cyclist_data.index, cyclist_data.values, 
                    marker='s', label='Pyöräilijät', linewidth=2, color='blue')
            
            # Add shaded area for cyclist data
            ax2.fill_between(cyclist_data.index, 0, cyclist_data.values, 
                           alpha=0.2, color='blue')

        if 'LKMMP' in filtered.columns:
            motorcycle_data = filtered.groupby('KKONN')['LKMMP'].sum()
            ax2.plot(motorcycle_data.index, motorcycle_data.values, 
                    marker='^', label='Moottoripyörät', linewidth=2, color='red')

        ax2.set_xlabel('Kuukausi')
        ax2.set_ylabel('Määrä')
        ax2.set_title('Polkupyöräilijät, moottoripyörät ja jalankulkijat')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        if len(ax2.get_xticks()) > 0:
            ax2.set_xticks(range(1, 13))
            ax2.set_xticklabels(month_names)

    plt.tight_layout()

    # Statistics with fixed division by zero
    if 'total_vehicles' in filtered.columns:
        multi_vehicle = filtered[filtered['total_vehicles'] > 1]
        cyclist_involved = filtered[filtered['LKMPP'] > 0] if 'LKMPP' in filtered.columns else pd.DataFrame()
        motorcycle_involved = filtered[filtered['LKMMP'] > 0] if 'LKMMP' in filtered.columns else pd.DataFrame()

        if len(multi_vehicle) > 0:
            avg_vehicles = multi_vehicle['total_vehicles'].mean()
            max_vehicles = int(multi_vehicle['total_vehicles'].max())
        else:
            avg_vehicles = 0
            max_vehicles = 0

        # Fix division by zero
        cyclist_percentage = 0
        motorcycle_percentage = 0
        if len(filtered) > 0:
            cyclist_percentage = len(cyclist_involved)/len(filtered)*100
            motorcycle_percentage = len(motorcycle_involved)/len(filtered)*100

        multi_stats = f"""
        **Moniajoneuvo-onnettomuudet:**
        - Yhteensä: {len(multi_vehicle):,}
        - Ajoneuvoja keskimäärin: {avg_vehicles:.1f}
        - Enimmillään ajoneuvoja: {max_vehicles}
        
        **Pyöräilijäonnettomuudet:**
        - Yhteensä: {len(cyclist_involved):,}
        - Osuus kaikista: {cyclist_percentage:.1f}%
        - Pyöräilijöitä yhteensä: {cyclist_involved['LKMPP'].sum() if len(cyclist_involved) > 0 else 0:,}
        
        **Moottoripyöräonnettomuudet:**
        - Yhteensä: {len(motorcycle_involved):,}
        - Osuus kaikista: {motorcycle_percentage:.1f}%
        - Moottoripyöriä yhteensä: {motorcycle_involved['LKMMP'].sum() if len(motorcycle_involved) > 0 else 0:,}
        """
        
        # Add seasonal analysis for cyclists and motorcycles
        if len(cyclist_involved) > 0 and 'KKONN' in cyclist_involved.columns:
            summer_months = cyclist_involved[cyclist_involved['KKONN'].isin([5,6,7,8])]
            winter_months = cyclist_involved[cyclist_involved['KKONN'].isin([11,12,1,2])]
            multi_stats += f"""
        
        **Pyöräilykausi:**
        - Kesä (touko-elo): {len(summer_months):,} onnettomuutta
        - Talvi (marras-helmi): {len(winter_months):,} onnettomuutta
        """
        
        if len(motorcycle_involved) > 0 and 'KKONN' in motorcycle_involved.columns:
            mc_summer = motorcycle_involved[motorcycle_involved['KKONN'].isin([5,6,7,8])]
            mc_winter = motorcycle_involved[motorcycle_involved['KKONN'].isin([11,12,1,2])]
            multi_stats += f"""
        
        **Moottoripyöräilykausi:**
        - Kesä (touko-elo): {len(mc_summer):,} onnettomuutta
        - Talvi (marras-helmi): {len(mc_winter):,} onnettomuutta
        """
    else:
        multi_stats = "**Moniajoneuvo-onnettomuudet:** Ei ajoneuvotietoja"

    return fig1, fig2, multi_stats


# ---------------- UI Builder Function ----------------

def build_traffic_accidents_tab(demo):
    """Build the traffic accidents UI components within a parent Gradio app."""
    
    gr.Markdown("# Loukkaantumiseen ja kuolemaan johtaneet tieliikenneonnettomuudet")
    
    gr.Markdown("""
    <div style="background-color: #f0f8ff; padding: 15px; border-left: 4px solid #1976d2; margin: 15px 0; border-radius: 5px;">
        <strong>Datalähde:</strong><br>
        Tilastokeskus, & Ek, K. (2025). <em>Tieliikenneonnettomuudet 2023</em> (versio 1).<br>
        CSC - Tieteen tietotekniikan keskus Oy.<br>
        <a href="http://urn.fi/urn:nbn:fi:fd-0e08d8f5-3f42-30e3-91f1-a8d5a032db8f" target="_blank" style="color: #1976d2;">
        http://urn.fi/urn:nbn:fi:fd-0e08d8f5-3f42-30e3-91f1-a8d5a032db8f</a>
    </div>
    """)

    data_state = gr.State(None)
    filter_options = gr.State(None)

    with gr.Row():
        with gr.Column(scale=1):
            load_btn = gr.Button("Lataa tiedot", variant="primary")
            load_status = gr.Markdown()

            gr.Markdown("### Suodattimet")
            severity_filter = gr.CheckboxGroup(
                label="Vakavuus",
                choices=["Kuolemaan johtava", "Vammautumiseen johtava", "Tuntematon"],
                value=["Kuolemaan johtava", "Vammautumiseen johtava"]
            )
            accident_filter = gr.CheckboxGroup(
                label="Onnettomuustyyppi",
                choices=["Tieltä suistuminen", "Muu onnettomuus"],
                value=["Tieltä suistuminen", "Muu onnettomuus"]
            )
            year_filter = gr.CheckboxGroup(
                label="Vuosi",
                choices=[],
                value=[]
            )
            vehicle_filter = gr.CheckboxGroup(
                label="Ajoneuvosuodatin",
                choices=["Pyöräilijäonnettomuudet", "Moottoripyöräonnettomuudet"],
                value=[]
            )

        with gr.Column(scale=3):
            with gr.Tabs():
                with gr.Tab("Yhteenveto"):
                    summary_text = gr.Markdown()
                    with gr.Row():
                        plot_type = gr.Plot(label="Onnettomuustyypit")
                        plot_severity = gr.Plot(label="Vakavuusjakauma")
                    plot_monthly = gr.Plot(label="Määrät kuukaudessa")

                with gr.Tab("Kartta"):
                    with gr.Row():
                        map_style = gr.Dropdown(
                            choices=["OpenStreetMap", "CartoDB positron", "CartoDB dark_matter"],
                            value="OpenStreetMap",
                            label="Karttateema"
                        )
                        show_heatmap = gr.Checkbox(label="Lämpökartta", value=True)
                        enable_cluster = gr.Checkbox(label="Klusterointi", value=False)
                    map_display = gr.HTML(label="Onnettomuuskartta")

                with gr.Tab("Aika-analyysi"):
                    plot_hourly = gr.Plot(label="Tuntijakauma")
                    plot_heatmap = gr.Plot(label="Tunti vs Kuukausi")

                with gr.Tab("Ajoneuvot"):
                    plot_vehicles = gr.Plot(label="Ajoneuvotyypit")
                    plot_vulnerable = gr.Plot(label="Polkupyöräilijät, moottoripyörät ja jalankulkijat")
                    vehicle_stats = gr.Markdown()

    def update_filters(data, status, options):
        if options and data is not None:
            severity_opts, accident_opts, year_opts = options
            return {
                severity_filter: gr.update(choices=severity_opts, value=severity_opts),
                accident_filter: gr.update(choices=accident_opts, value=accident_opts),
                year_filter: gr.update(choices=[int(y) for y in year_opts], value=[int(y) for y in year_opts])
            }
        return {}

    # Auto-load data on app startup
    demo.load(
        fn=load_data,
        inputs=[],
        outputs=[data_state, load_status, filter_options]
    ).then(
        fn=update_filters,
        inputs=[data_state, load_status, filter_options],
        outputs=[severity_filter, accident_filter, year_filter]
    )

    # Also allow manual reload via button
    load_btn.click(
        fn=load_data,
        inputs=[],
        outputs=[data_state, load_status, filter_options]
    ).then(
        fn=update_filters,
        inputs=[data_state, load_status, filter_options],
        outputs=[severity_filter, accident_filter, year_filter]
    )

    for filter_input in [severity_filter, accident_filter, year_filter, vehicle_filter]:
        filter_input.change(
            fn=create_summary_plots,
            inputs=[data_state, severity_filter, accident_filter, year_filter, vehicle_filter],
            outputs=[summary_text, plot_type, plot_severity, plot_monthly]
        )

    for map_input in [severity_filter, accident_filter, year_filter, vehicle_filter, map_style, enable_cluster, show_heatmap]:
        map_input.change(
            fn=create_map,
            inputs=[data_state, severity_filter, accident_filter, year_filter, vehicle_filter,
                   map_style, enable_cluster, show_heatmap],
            outputs=[map_display]
        )

    for filter_input in [severity_filter, accident_filter, year_filter, vehicle_filter]:
        filter_input.change(
            fn=create_time_analysis,
            inputs=[data_state, severity_filter, accident_filter, year_filter, vehicle_filter],
            outputs=[plot_hourly, plot_heatmap]
        )

    for filter_input in [severity_filter, accident_filter, year_filter, vehicle_filter]:
        filter_input.change(
            fn=create_vehicle_analysis,
            inputs=[data_state, severity_filter, accident_filter, year_filter, vehicle_filter],
            outputs=[plot_vehicles, plot_vulnerable, vehicle_stats]
        )