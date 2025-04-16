import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from cassandra.cluster import Cluster
from io import BytesIO
import re
import sys

# Configuration Cassandra
CASSANDRA_CONFIG = {
    'host': "127.0.0.1",
    'port': 9042,
    'keyspace': "hiv_data",
    'table': "future_predictions",
    'historical_table': "hist"
}

def get_cassandra_session():
    """Crée une session Cassandra avec gestion des erreurs"""
    try:
        cluster = Cluster([CASSANDRA_CONFIG['host']], port=CASSANDRA_CONFIG['port'])
        return cluster.connect(CASSANDRA_CONFIG['keyspace'])
    except Exception as e:
        st.error(f"Échec de connexion à Cassandra: {str(e)}")
        return None

@st.cache_data(ttl=3600)
def load_data():
    """Charge toutes les données avec gestion robuste des erreurs"""
    try:
        session = get_cassandra_session()
        if not session:
            return pd.DataFrame(), pd.DataFrame()

        # Chargement des prédictions
        predictions_query = f"""
        SELECT entity, year, code, predicted_cases
        FROM {CASSANDRA_CONFIG['table']}
        WHERE year = 2026
        ALLOW FILTERING
        """
        predictions = pd.DataFrame(list(session.execute(predictions_query)))

        # Chargement des données historiques
        historical_query = f"""
        SELECT "Code" AS code, "Year" AS year, 
               "Entity" AS entity, 
               "Incidence - HIV/AIDS - Sex: Both - Age: All Ages (Number)" AS cases
        FROM {CASSANDRA_CONFIG['historical_table']}
        ALLOW FILTERING
        """
        historical = pd.DataFrame(list(session.execute(historical_query)))

        return predictions, historical

    except Exception as e:
        st.error(f"Erreur lors du chargement des données: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()
    finally:
        if 'session' in locals():
            session.cluster.shutdown()

def clean_data(df, is_historical=False):
    """Nettoie les données de manière robuste"""
    if df.empty:
        return df
    
    try:
        # Conversion des valeurs numériques
        num_col = 'cases' if is_historical else 'predicted_cases'
        if num_col in df.columns:
            df[num_col] = (
                df[num_col]
                .astype(str)
                .str.replace(',', '')
                .replace(r'[^\d.]', '', regex=True)
                .replace('', '0')
                .astype(float)
            )
        
        # Nettoyage des codes pays
        if 'code' in df.columns:
            df['code'] = (
                df['code']
                .astype(str)
                .str.replace('"', '')
                .replace(['NaN', 'nan', ''], None)
            )
        
        # Filtrage des régions si ce sont des prédictions
        if not is_historical and 'entity' in df.columns:
            df = df[~df['entity'].str.contains("Region|WB", case=False, na=False)]
        
        return df.dropna(subset=['entity'] if 'entity' in df.columns else [])
    
    except Exception as e:
        st.warning(f"Erreur lors du nettoyage des données: {str(e)}")
        return df

def setup_ui():
    """Configure l'interface utilisateur"""
    st.set_page_config(
        page_title="Prédictions VIH 2026",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    st.title("🌍 Analyse des prédictions VIH/SIDA pour 2026")

def display_metrics(df):
    """Affiche les métriques clés"""
    if df.empty:
        return
    
    cols = st.columns(3)
    metrics = {
        "Total mondial": df['predicted_cases'].sum(),
        "Moyenne par pays": df['predicted_cases'].mean(),
        "Pays le plus touché": (
            f"{df.loc[df['predicted_cases'].idxmax(), 'entity']} "
            f"({df['predicted_cases'].max():,.0f})"
        )
    }
    
    for (label, value), col in zip(metrics.items(), cols):
        col.metric(label, f"{value:,.0f}" if isinstance(value, float) else value)

def create_comparison_bar_chart(pred_df, hist_df, n_countries=20):
    """Crée un graphique à barres comparatif pour les N pays les plus touchés"""
    if pred_df.empty or hist_df.empty:
        st.warning("Données insuffisantes pour la comparaison")
        return None
    
    # Sélection des N pays les plus touchés en 2026
    top_countries = pred_df.nlargest(n_countries, 'predicted_cases')
    
    # Récupération des données historiques pour ces pays
    comparison_data = []
    for _, row in top_countries.iterrows():
        country_data = hist_df[hist_df['entity'] == row['entity']]
        if not country_data.empty:
            latest_year = country_data['year'].max()
            latest_value = country_data[country_data['year'] == latest_year]['cases'].values[0]
            evolution_pct = ((row['predicted_cases'] - latest_value) / latest_value * 100) if latest_value != 0 else 0
            comparison_data.append({
                'Pays': row['entity'],
                '2026 (Prédiction)': row['predicted_cases'],
                f"{latest_year} (Historique)": latest_value,
                'Évolution (%)': evolution_pct
            })
    
    if not comparison_data:
        st.warning("Aucune donnée historique correspondante trouvée")
        return None
    
    comparison_df = pd.DataFrame(comparison_data).sort_values('2026 (Prédiction)', ascending=False)
    
    # Création du graphique à barres groupées
    fig = go.Figure()
    
    # Ajout des barres pour les données historiques
    fig.add_trace(go.Bar(
        x=comparison_df['Pays'],
        y=comparison_df.iloc[:, 2],
        name=comparison_df.columns[2],
        marker_color='#636EFA'
    ))
    
    # Ajout des barres pour les prédictions 2026
    fig.add_trace(go.Bar(
        x=comparison_df['Pays'],
        y=comparison_df['2026 (Prédiction)'],
        name='2026 (Prédiction)',
        marker_color='#EF553B'
    ))
    
    # Ajout des annotations pour l'évolution
    for i, row in comparison_df.iterrows():
        fig.add_annotation(
            x=row['Pays'],
            y=max(row['2026 (Prédiction)'], row.iloc[2]),
            text=f"{row['Évolution (%)']:.1f}%",
            showarrow=False,
            yshift=10,
            font=dict(size=10)
        )
    
    fig.update_layout(
        title=f"Comparaison des {n_countries} pays les plus touchés (Historique vs Prédiction 2026)",
        xaxis_title="Pays",
        yaxis_title="Nombre de cas",
        barmode='group',
        height=600,
        xaxis_tickangle=-45,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    return comparison_df

def plot_choropleth_map(df):
    """Crée une carte choroplèthe robuste avec gestion des erreurs"""
    if df.empty or 'code' not in df.columns or 'predicted_cases' not in df.columns:
        st.warning("Données insuffisantes pour générer la carte")
        return
    
    try:
        # Nettoyage supplémentaire des codes pays
        plot_df = df.copy()
        plot_df['code'] = plot_df['code'].str.upper().str.strip()
        plot_df = plot_df[plot_df['code'].str.match(r'^[A-Z]{3}$') == True]
        
        if plot_df.empty:
            st.warning("Aucun code pays valide après nettoyage")
            return
        
        fig = px.choropleth(
            plot_df,
            locations="code",
            color="predicted_cases",
            hover_name="entity",
            hover_data=["year", "predicted_cases"],
            color_continuous_scale="OrRd",
            range_color=(0, plot_df['predicted_cases'].quantile(0.95)),
            projection="natural earth",
            title="Prédictions mondiales de VIH/SIDA (2026)",
            labels={'predicted_cases': 'Cas prédits'}
        )
        
        fig.update_layout(
            margin={"r":0, "t":40, "l":0, "b":0},
            coloraxis_colorbar=dict(
                title="Cas prédits",
                thickness=15
            )
        )
        
        fig.update_geos(
            showcountries=True,
            countrycolor="black",
            showocean=True,
            oceancolor="lightblue"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
    except Exception as e:
        st.error(f"Erreur lors de la génération de la carte : {str(e)}")

def create_visualizations(pred_df, hist_df):
    """Crée toutes les visualisations"""
    tab1, tab2, tab3, tab4 = st.tabs(["Carte", "Comparaisons", "Top 20 Pays", "Distributions"])
    
    with tab1:
        plot_choropleth_map(pred_df)
    
    with tab2:
        if not hist_df.empty and not pred_df.empty:
            selected = st.multiselect(
                "Pays à comparer (évolution temporelle)",
                options=pred_df['entity'].unique(),
                default=pred_df.nlargest(3, 'predicted_cases')['entity'].tolist()
            )
            
            if selected:
                combined = pd.concat([
                    hist_df[hist_df['entity'].isin(selected)],
                    pred_df[pred_df['entity'].isin(selected)].rename(
                        columns={'predicted_cases': 'cases'}
                    ).assign(year=2026)
                ])
                
                fig = px.line(
                    combined,
                    x="year",
                    y="cases",
                    color="entity",
                    markers=True,
                    title="Évolution historique et prédiction",
                    labels={'cases': 'Nombre de cas', 'year': 'Année'}
                )
                st.plotly_chart(fig)
        else:
            st.warning("Données insuffisantes pour la comparaison temporelle")
    
    with tab3:
        if not pred_df.empty and not hist_df.empty:
            comparison_df = create_comparison_bar_chart(pred_df, hist_df)
            
            if comparison_df is not None:
                st.subheader("Données détaillées")
                st.dataframe(
                    comparison_df.style.format({
                        '2026 (Prédiction)': '{:,.0f}',
                        comparison_df.columns[2]: '{:,.0f}',
                        'Évolution (%)': '{:.1f}%'
                    }),
                    height=400
                )
        else:
            st.warning("Données insuffisantes pour le classement")
    
    with tab4:
        if not pred_df.empty:
            st.plotly_chart(
                px.histogram(
                    pred_df,
                    x="predicted_cases",
                    nbins=20,
                    title="Distribution des cas prédits",
                    labels={'predicted_cases': 'Nombre de cas'}
                ),
                use_container_width=True
            )
        else:
            st.warning("Aucune donnée disponible pour l'histogramme")

def main():
    """Point d'entrée principal"""
    try:
        setup_ui()
        
        with st.spinner("Chargement des données..."):
            pred_df, hist_df = load_data()
            pred_df = clean_data(pred_df)
            hist_df = clean_data(hist_df, is_historical=True)
        
        if pred_df.empty:
            st.warning("Aucune donnée de prédiction disponible")
            return
        
        display_metrics(pred_df)
        create_visualizations(pred_df, hist_df)
        
    except Exception as e:
        st.error(f"Une erreur critique est survenue: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
