
import streamlit as st
import hashlib

# DEFINA A SENHA AQUI
SENHA = "tsevero555"  # ← TROQUE para cada projeto

def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        st.markdown("<div style='text-align: center; padding: 50px;'><h1>🔐 Acesso Restrito</h1></div>", unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            senha_input = st.text_input("Digite a senha:", type="password", key="pwd_input")
            if st.button("Entrar", use_container_width=True):
                if senha_input == SENHA:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("❌ Senha incorreta")
        st.stop()

check_password()


# =============================================================================
# 1. IMPORTS E CONFIGURAÇÕES INICIAIS
# =============================================================================

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import warnings
import ssl
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score
import pickle

# Configuração SSL
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

warnings.filterwarnings('ignore')

# Configuração da página
st.set_page_config(
    page_title="Sistema de DAFs",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================================================================
# 2. ESTILOS CSS CUSTOMIZADOS
# =============================================================================

st.markdown("""
<style>
    .main-header {
        font-size: 2.8rem;
        font-weight: bold;
        color: #0d47a1;
        text-align: center;
        margin-bottom: 2rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
        padding: 20px;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 15px;
        color: white;
    }
    
    .sub-header {
        font-size: 1.8rem;
        font-weight: bold;
        color: #1565c0;
        margin-top: 2rem;
        margin-bottom: 1rem;
        border-bottom: 3px solid #1565c0;
        padding-bottom: 10px;
    }

        /* ESTILO DOS GRÁFICOS (PLOTLY) */
    div[data-testid="stPlotlyChart"] {
        border: 2px solid #e0e0e0;       /* Borda: 2px, sólida, cor cinza-claro */
        border-radius: 10px;             /* Cantos arredondados (mesmo dos KPIs) */
        padding: 10px;                   /* Espaçamento interno (ajuste conforme gosto) */
        box-shadow: 0 2px 4px rgba(0,0,0,0.05); /* Sombra suave */
        background-color: #ffffff;       /* Fundo branco (opcional) */
    }
    
    /* ESTILO DOS KPIs - BORDA PRETA */
    div[data-testid="stMetric"] {
        background-color: #ffffff;        /* Fundo branco */
        border: 2px solid #2c3e50;        /* Borda: 2px de largura, sólida, cor cinza-escuro */
        border-radius: 10px;              /* Cantos arredondados (10 pixels de raio) */
        padding: 15px;                    /* Espaçamento interno (15px em todos os lados) */
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);  /* Sombra: horizontal=0, vertical=2px, blur=4px, cor preta 10% opacidade */
    }
    
    /* Título do métrica */
    div[data-testid="stMetric"] > label {
        font-weight: 600;                 /* Negrito médio */
        color: #2c3e50;                   /* Cor do texto */
    }
    
    /* Valor do métrica */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;                /* Tamanho da fonte do valor */
        font-weight: bold;                /* Negrito */
        color: #1f77b4;                   /* Cor azul */
    }
    
    /* Delta (variação) */
    div[data-testid="stMetricDelta"] {
        font-size: 0.9rem;                /* Tamanho menor para delta */
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        transition: transform 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.2);
    }
    
    .alert-critico {
        background-color: #ffebee;
        border-left: 5px solid #c62828;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .alert-alto {
        background-color: #fff3e0;
        border-left: 5px solid #ef6c00;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .alert-positivo {
        background-color: #e8f5e9;
        border-left: 5px solid #2e7d32;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .info-box {
        background-color: #e3f2fd;
        border-left: 4px solid #1976d2;
        padding: 15px;
        border-radius: 8px;
        margin: 10px 0;
    }
    
    .kpi-container {
        background: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        margin: 10px 0;
    }
    
    .stDataFrame {
        font-size: 0.9rem;
    }
    
    .sidebar .sidebar-content {
        background-color: #f5f5f5;
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# 3. CONFIGURAÇÃO DE CONEXÃO
# =============================================================================

IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'niat'

# Credenciais (usar st.secrets em produção)
IMPALA_USER = st.secrets.get("impala_credentials", {}).get("user", "tsevero")
IMPALA_PASSWORD = st.secrets.get("impala_credentials", {}).get("password", "")

@st.cache_resource
def get_impala_engine():
    """Cria engine de conexão Impala."""
    try:
        engine = create_engine(
            f'impala://{IMPALA_HOST}:{IMPALA_PORT}/{DATABASE}',
            connect_args={
                'user': IMPALA_USER,
                'password': IMPALA_PASSWORD,
                'auth_mechanism': 'LDAP',
                'use_ssl': True
            }
        )
        return engine
    except Exception as e:
        st.sidebar.error(f"❌ Erro na conexão: {str(e)[:100]}")
        return None

# =============================================================================
# 4. FUNÇÕES DE CARREGAMENTO DE DADOS
# =============================================================================

@st.cache_data(ttl=3600)
def carregar_dados_sistema(_engine):
    """Carrega tabelas do sistema - AGREGAÇÕES para dados grandes, completo para pequenos."""
    dados = {}
    
    if _engine is None:
        return {}
    
    # Testar conexão
    try:
        with _engine.connect() as conn:
            st.sidebar.success("✅ Conexão Impala OK!")
    except Exception as e:
        st.sidebar.error(f"❌ Falha na conexão: {str(e)[:100]}")
        return {}
    
    # Configuração: ESTRATÉGIA HÍBRIDA
    tabelas_config = {
        # ========== TABELAS PEQUENAS - CARREGA TUDO ==========
        'catalogo_tipos': {
            'query': f"SELECT * FROM {DATABASE}.mlh_catalogo_tipos_inconsistencia",
            'tipo': 'completo'
        },
        'benchmark_tipos': {
            'query': f"SELECT * FROM {DATABASE}.mlh_benchmark_tipo_inconsistencia",
            'tipo': 'completo'
        },
        'evolucao_mensal': {
            'query': f"SELECT * FROM {DATABASE}.mlh_evolucao_mensal",
            'tipo': 'completo'
        },
        'ranking_tipos': {
            'query': f"SELECT * FROM {DATABASE}.mlh_ranking_tipos_efetividade",
            'tipo': 'completo'
        },
        'exclusoes_auditores': {
            'query': f"SELECT * FROM {DATABASE}.mlh_analise_exclusoes_auditores",
            'tipo': 'completo'
        },
        'performance_contadores': {
            'query': f"SELECT * FROM {DATABASE}.mlh_performance_contadores",
            'tipo': 'completo'
        },
        'ranking_contadores': {
            'query': f"SELECT * FROM {DATABASE}.mlh_performance_contadores ORDER BY score_performance DESC",
            'tipo': 'completo'
        },
        'performance_dafs': {
            'query': f"""
                SELECT 
                    id_equipe,
                    qtd_empresas_acompanhadas,
                    qtd_contadores_acompanhados,
                    qtd_total_inconsistencias,
                    valor_total_inconsistencias,
                    
                    -- Taxas brutas
                    taxa_autonomia_pct,
                    taxa_exclusao_pct,
                    taxa_autuacao_pct,
                    taxa_ativas_pct,
                    
                    -- INDICADOR DE AUTONOMIA
                    ind_autonomia_pct,
                    ind_autonomia_nivel,
                    score_autonomia,
                    
                    -- INDICADOR DE ATIVAS
                    ind_ativas_pct,
                    ind_ativas_nivel,

                    -- INDICADOR DE FISCALIZAÇÃO
                    ind_fiscalizacao_pct,
                    ind_fiscalizacao_nivel,
                    score_fiscalizacao,
                    taxa_necessidade_fiscalizacao,
                    qtd_em_fiscalizacao,
                    qtd_fiscalizacao_total,
                    
                    -- INDICADOR DE EXCLUSÃO
                    ind_exclusao_pct,
                    ind_exclusao_nivel,
                    score_exclusao,
                    
                    -- INDICADOR DE AUTUAÇÃO
                    ind_autuacao_pct,
                    ind_autuacao_nivel,
                    score_autuacao,
                    
                    -- CLASSIFICAÇÃO GERAL
                    score_geral_ponderado,
                    classificacao_geral,
                    
                    -- FLAGS DE ALERTA
                    flag_alerta_autonomia_baixa,
                    flag_alerta_fiscalizacao_alta,
                    flag_alerta_autuacao_alta,
                    flag_alerta_exclusao_alta
                    
                FROM {DATABASE}.mlh_performance_dafs
            """,
            'tipo': 'completo'
        },
        'ranking_dafs': {
            'query': f"""
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY score_geral_ponderado DESC) as ranking_geral,
                    id_equipe,
                    qtd_empresas_acompanhadas,
                    qtd_contadores_acompanhados,
                    qtd_total_inconsistencias,
                    valor_total_inconsistencias,
                    
                    -- Taxas
                    taxa_autonomia_pct,
                    taxa_exclusao_pct,
                    taxa_autuacao_pct,
                    taxa_ativas_pct,
                    taxa_necessidade_fiscalizacao,
                    
                    -- Performance
                    score_geral_ponderado,
                    classificacao_geral,
                    
                    -- Tempo
                    media_dias_malha,
                    media_dias_resolucao_autonoma
                    
                FROM {DATABASE}.mlh_performance_dafs
                ORDER BY score_geral_ponderado DESC
            """,
            'tipo': 'completo'
        },
        
        # ========== EMPRESAS - CARREGAR TUDO (já é tabela otimizada) ==========
        'empresas_base_resumo': {
            'query': f"""
                SELECT 
                    nu_cnpj,
                    id_equipe,
                    nu_cpf_cnpj_contador,
                    flag_atualmente_em_malha
                FROM {DATABASE}.mlh_empresas_base
                WHERE flag_atualmente_em_malha = 1 
                  AND id_equipe IS NOT NULL
            """,
            'tipo': 'resumo'
        },
        
        # ========== INCONSISTÊNCIAS - APENAS AGREGAÇÕES! ==========
        'inconsistencias_agregadas': {
            'query': f"""
                SELECT 
                    i.cd_inconsistencia,
                    i.nm_inconsistencia,
                    i.natureza_inconsistencia,
                    i.gravidade_presumida,
                    i.canal_resolucao,
                    COUNT(*) as qtd_total,
                    COUNT(DISTINCT i.nu_cnpj) as qtd_empresas,
                    SUM(CASE WHEN i.flag_tem_valor_fiscal = 1 THEN i.vl_inconsistencia ELSE 0 END) as valor_total,
                    AVG(i.dias_na_malha) as media_dias_malha,
                    SUM(i.flag_resolucao_autonoma) as qtd_autonoma,
                    SUM(i.flag_exclusao_auditor) as qtd_exclusao,
                    SUM(i.flag_gerou_infracao) as qtd_infracao
                FROM {DATABASE}.mlh_inconsistencias_detalhadas i
                INNER JOIN {DATABASE}.mlh_empresas_base e 
                    ON i.nu_cnpj = e.nu_cnpj
                WHERE e.flag_atualmente_em_malha = 1 
                  AND e.id_equipe IS NOT NULL
                GROUP BY 
                    i.cd_inconsistencia, i.nm_inconsistencia, i.natureza_inconsistencia,
                    i.gravidade_presumida, i.canal_resolucao
            """,
            'tipo': 'agregacao'
        },
        
        # Agregação por empresa (para drill-down rápido)
        'inconsistencias_por_empresa': {
            'query': f"""
                SELECT 
                    i.nu_cnpj,
                    COUNT(*) as qtd_total,
                    COUNT(DISTINCT i.cd_inconsistencia) as qtd_tipos,
                    SUM(CASE WHEN i.flag_tem_valor_fiscal = 1 THEN i.vl_inconsistencia ELSE 0 END) as valor_total,
                    SUM(CASE WHEN i.canal_resolucao = 'ATIVA' THEN 1 ELSE 0 END) as qtd_ativas,
                    SUM(i.flag_resolucao_autonoma) as qtd_autonoma,
                    SUM(i.flag_exclusao_auditor) as qtd_exclusao,
                    AVG(i.dias_na_malha) as media_dias_malha
                FROM {DATABASE}.mlh_inconsistencias_detalhadas i
                INNER JOIN {DATABASE}.mlh_empresas_base e 
                    ON i.nu_cnpj = e.nu_cnpj
                WHERE e.flag_atualmente_em_malha = 1 
                  AND e.id_equipe IS NOT NULL
                GROUP BY i.nu_cnpj
            """,
            'tipo': 'agregacao'
        },
        
        # Agregação por DAF
        'inconsistencias_por_daf': {
            'query': f"""
                SELECT 
                    e.id_equipe,
                    COUNT(DISTINCT i.nu_cnpj) as qtd_empresas,
                    COUNT(*) as qtd_total,
                    COUNT(DISTINCT i.cd_inconsistencia) as qtd_tipos,
                    SUM(CASE WHEN i.flag_tem_valor_fiscal = 1 THEN i.vl_inconsistencia ELSE 0 END) as valor_total,
                    SUM(CASE WHEN i.canal_resolucao = 'ATIVA' THEN 1 ELSE 0 END) as qtd_ativas,
                    SUM(i.flag_resolucao_autonoma) as qtd_autonoma,
                    SUM(i.flag_exclusao_auditor) as qtd_exclusao,
                    AVG(i.dias_na_malha) as media_dias_malha
                FROM {DATABASE}.mlh_inconsistencias_detalhadas i
                INNER JOIN {DATABASE}.mlh_empresas_base e 
                    ON i.nu_cnpj = e.nu_cnpj
                WHERE e.flag_atualmente_em_malha = 1 
                  AND e.id_equipe IS NOT NULL
                GROUP BY e.id_equipe
            """,
            'tipo': 'agregacao'
        }
    }
    
    progress_bar = st.sidebar.progress(0)
    status_text = st.sidebar.empty()
    
    total = len(tabelas_config)
    
    for idx, (key, config) in enumerate(tabelas_config.items()):
        try:
            query = config['query']
            tipo = config['tipo']
            
            status_text.text(f"📥 Carregando {key} ({tipo})...")
            progress_bar.progress((idx + 1) / total)
            
            # Executar query
            df = pd.read_sql(query, _engine)
            df.columns = [col.lower() for col in df.columns]
            
            # Converter tipos numéricos
            for col in df.select_dtypes(include=['object']).columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
            
            dados[key] = df
            
            # Log do tamanho carregado
            mem_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
            status_text.text(f"✅ {key}: {len(df):,} registros ({mem_mb:.1f} MB)")
            
        except Exception as e:
            st.sidebar.warning(f"⚠️ Erro em {key}: {str(e)[:80]}")
            dados[key] = pd.DataFrame()
    
    progress_bar.empty()
    status_text.empty()
    
    # Resumo do carregamento
    total_registros = sum(len(df) for df in dados.values() if not df.empty)
    total_mem = sum(df.memory_usage(deep=True).sum() / 1024 / 1024 for df in dados.values() if not df.empty)
    
    st.sidebar.success(f"✅ {total_registros:,} registros ({total_mem:.1f} MB)")
    
    return dados

@st.cache_data(ttl=1800)
def carregar_inconsistencias_empresa(_engine, cnpj):
    """Carrega inconsistências DETALHADAS de uma empresa específica - SOB DEMANDA."""
    try:
        query = f"""
            SELECT i.* 
            FROM {DATABASE}.mlh_inconsistencias_detalhadas i
            INNER JOIN {DATABASE}.mlh_empresas_base e 
                ON i.nu_cnpj = e.nu_cnpj
            WHERE i.nu_cnpj = '{cnpj}'
              AND e.flag_atualmente_em_malha = 1
              AND e.id_equipe IS NOT NULL
        """
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Erro ao carregar inconsistências: {str(e)[:100]}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_inconsistencias_daf(_engine, id_equipe):
    """Carrega inconsistências DETALHADAS de uma DAF específica - SOB DEMANDA."""
    try:
        query = f"""
            SELECT i.*
            FROM {DATABASE}.mlh_inconsistencias_detalhadas i
            INNER JOIN {DATABASE}.mlh_empresas_base e 
                ON i.nu_cnpj = e.nu_cnpj
            WHERE e.id_equipe = {id_equipe}
              AND e.flag_atualmente_em_malha = 1
              AND e.id_equipe IS NOT NULL
        """
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Erro ao carregar inconsistências da DAF: {str(e)[:100]}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_empresas_daf(_engine, id_equipe):
    """Carrega empresas detalhadas de uma DAF específica - SOB DEMANDA."""
    try:
        query = f"""
            SELECT * 
            FROM {DATABASE}.mlh_empresas_base
            WHERE id_equipe = {id_equipe}
              AND flag_atualmente_em_malha = 1
              AND id_equipe IS NOT NULL
        """
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Erro ao carregar empresas: {str(e)[:100]}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_amostra_inconsistencias(_engine, limit=50000):
    """Carrega uma amostra de inconsistências para análises gerais."""
    try:
        query = f"""
            SELECT * FROM {DATABASE}.mlh_inconsistencias_detalhadas
            ORDER BY dt_identificada DESC
            LIMIT {limit}
        """
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Erro ao carregar amostra: {str(e)[:100]}")
        return pd.DataFrame()

@st.cache_data(ttl=1800)
def carregar_dataset_ml(_engine):
    """Carrega dataset de ML sob demanda - NÃO carrega no início."""
    try:
        query = f"SELECT * FROM {DATABASE}.mlh_dataset_ml_exclusoes"
        df = pd.read_sql(query, _engine)
        df.columns = [col.lower() for col in df.columns]
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dataset ML: {str(e)[:100]}")
        return pd.DataFrame()

# =============================================================================
# 5. FUNÇÕES AUXILIARES PARA VISUALIZAÇÃO MULTIDIMENSIONAL
# =============================================================================

def get_color_indicador(nivel: str, invertido: bool = False) -> str:
    """
    Retorna cor hex baseada no nível do indicador.
    
    Args:
        nivel: Nível do indicador ('EXCELENTE', 'ALTO/BOM', 'MEDIO', 'BAIXO/ALTO', 'CRITICO')
        invertido: Se True, inverte a lógica de cores (para pendência e exclusão)
    
    Returns:
        String com código hexadecimal da cor
    """
    cores_normais = {
        'EXCELENTE': '#10b981',  # Verde
        'ALTO': '#84cc16',       # Verde claro
        'BOM': '#84cc16',        # Verde claro
        'MEDIO': '#fbbf24',      # Amarelo
        'BAIXO': '#f97316',      # Laranja
        'CRITICO': '#ef4444'     # Vermelho
    }
    
    cores_invertidas = {
        'EXCELENTE': '#10b981',  # Verde
        'BOM': '#84cc16',        # Verde claro
        'MEDIO': '#fbbf24',      # Amarelo
        'ALTO': '#f97316',       # Laranja
        'CRITICO': '#ef4444'     # Vermelho
    }
    
    nivel_upper = nivel.upper()
    cores = cores_invertidas if invertido else cores_normais
    
    return cores.get(nivel_upper, '#6b7280')  # Cinza como default

def get_emoji_indicador(indicador: str) -> str:
    """Retorna emoji para cada tipo de indicador."""
    emojis = {
        'autonomia': '🎯',
        'pendencia': '⏳',
        'exclusao': '🗑️',
        'autuacao': '⚖️',
        'geral': '📊'
    }
    return emojis.get(indicador.lower(), '📈')

def criar_card_metrica(label: str, valor: float, nivel: str, icone: str = "📊", invertido: bool = False):
    """Cria um card de métrica colorido baseado no nível."""
    cor = get_color_indicador(nivel, invertido)
    
    st.markdown(f"""
    <div style='background: linear-gradient(135deg, {cor} 0%, {cor}dd 100%); 
                padding: 1.5rem; border-radius: 10px; color: white; 
                box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin: 10px 0;'>
        <div style='font-size: 2rem; margin-bottom: 0.5rem;'>{icone}</div>
        <div style='font-size: 0.9rem; opacity: 0.9;'>{label}</div>
        <div style='font-size: 2.5rem; font-weight: bold;'>{valor:.1f}</div>
        <div style='font-size: 0.85rem; opacity: 0.8; margin-top: 0.5rem;'>{nivel}</div>
    </div>
    """, unsafe_allow_html=True)

def criar_radar_chart_daf(df_daf, id_equipe=None, mostrar_media=True):
    """Cria radar chart com os 4 scores de uma DAF."""
    fig = go.Figure()
    
    if id_equipe is not None:
        df_plot = df_daf[df_daf['id_equipe'] == id_equipe].copy()
    else:
        df_plot = df_daf.copy()
    
    categorias = ['Autonomia', 'Fiscalização', 'Exclusão', 'Autuação']
    
    for idx, row in df_plot.iterrows():
        valores = [
            float(row.get('score_autonomia', 0)),
            float(row.get('score_fiscalizacao', 0)),  # CORRIGIDO
            float(row.get('score_exclusao', 0)),
            float(row.get('score_autuacao', 0))
        ]
        
        valores_fechado = valores + [valores[0]]
        categorias_fechado = categorias + [categorias[0]]
        
        fig.add_trace(go.Scatterpolar(
            r=valores_fechado,
            theta=categorias_fechado,
            fill='toself',
            name=f"DAF {row['id_equipe']}",
            line=dict(width=2),
            opacity=0.7
        ))
    
    if mostrar_media and len(df_daf) > 0:
        media_valores = [
            float(df_daf['score_autonomia'].mean()),
            float(df_daf['score_fiscalizacao'].mean()),  # CORRIGIDO
            float(df_daf['score_exclusao'].mean()),
            float(df_daf['score_autuacao'].mean())
        ]
        media_valores_fechado = media_valores + [media_valores[0]]
        
        fig.add_trace(go.Scatterpolar(
            r=media_valores_fechado,
            theta=categorias_fechado,
            fill='toself',
            name='Média SC',
            line=dict(color='gray', width=2, dash='dash'),
            opacity=0.3
        ))
    
    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                tickvals=[0, 25, 50, 75, 100]
            )
        ),
        showlegend=True,
        height=500
    )
    
    return fig

# =============================================================================
# 6. FUNÇÕES DE CÁLCULO E ANÁLISE
# =============================================================================

def calcular_kpis_gerais(dados):
    """Calcula KPIs principais do sistema usando agregações."""
    df_agregado = dados.get('inconsistencias_agregadas', pd.DataFrame())
    df_empresas = dados.get('empresas_base_resumo', pd.DataFrame())
    df_por_empresa = dados.get('inconsistencias_por_empresa', pd.DataFrame())
    
    if df_agregado.empty:
        return {k: 0 for k in ['total_empresas', 'total_inconsistencias', 
                                'taxa_autonomia', 'taxa_exclusao', 'taxa_fiscalizacao',
                                'dias_medio_malha', 'valor_total', 'tipos_unicos',
                                'empresas_ativas', 'inconsistencias_ativas', 
                                'inconsistencias_pendentes', 'contadores_sistema']}
    
    total_incons = df_agregado['qtd_total'].sum()
    total_autonoma = df_agregado['qtd_autonoma'].sum()
    total_exclusao = df_agregado['qtd_exclusao'].sum()
    
    # Somar todas inconsistências em fiscalização (EM_FISCALIZACAO + FISCALIZACAO_CONCLUIDA)
    total_fiscalizacao = df_agregado[
        df_agregado['canal_resolucao'].isin(['EM_FISCALIZACAO', 'FISCALIZACAO_CONCLUIDA'])
    ]['qtd_total'].sum() if 'canal_resolucao' in df_agregado.columns else 0
    
    # Empresas (já filtradas na query)
    if not df_empresas.empty:
        total_empresas_unicas = df_empresas['nu_cnpj'].nunique()
        empresas_ativas = total_empresas_unicas
    else:
        total_empresas_unicas = 0
        empresas_ativas = 0
    
    # Valor Total
    if not df_por_empresa.empty and 'valor_total' in df_por_empresa.columns:
        valor_total_corrigido = df_por_empresa['valor_total'].sum()
    else:
        valor_total_corrigido = df_agregado['valor_total'].sum()
    
    # Inconsistências Ativas (aguardando prazo - NORMAL)
    if not df_por_empresa.empty and 'qtd_ativas' in df_por_empresa.columns:
        inconsistencias_ativas = df_por_empresa['qtd_ativas'].sum()
    else:
        df_ativas = df_agregado[df_agregado.get('canal_resolucao', '') == 'ATIVA']
        inconsistencias_ativas = df_ativas['qtd_total'].sum() if not df_ativas.empty else 0
    
    # Inconsistências Pendentes (IDENTIFICADA - aguardando entrar na malha)
    df_identificadas = df_agregado[df_agregado.get('canal_resolucao', '') == 'IDENTIFICADA']
    inconsistencias_pendentes = df_identificadas['qtd_total'].sum() if not df_identificadas.empty else 0
    
    return {
        'total_empresas': int(total_empresas_unicas),
        'total_inconsistencias': int(total_incons),
        'taxa_autonomia': float(total_autonoma / total_incons * 100) if total_incons > 0 else 0,
        'taxa_exclusao': float(total_exclusao / total_incons * 100) if total_incons > 0 else 0,
        'taxa_fiscalizacao': float(total_fiscalizacao / total_incons * 100) if total_incons > 0 else 0,
        'dias_medio_malha': float(df_agregado['media_dias_malha'].mean()),
        'valor_total': float(valor_total_corrigido),
        'tipos_unicos': df_agregado['cd_inconsistencia'].nunique(),
        'empresas_ativas': int(empresas_ativas),
        'inconsistencias_ativas': int(inconsistencias_ativas),
        'inconsistencias_pendentes': int(inconsistencias_pendentes),
        'contadores_sistema': df_empresas['nu_cpf_cnpj_contador'].nunique() if not df_empresas.empty else 0
    }

def calcular_distribuicao_canais(dados):
    """Calcula distribuição por canal usando agregações."""
    df_agregado = dados.get('inconsistencias_agregadas', pd.DataFrame())
    
    if df_agregado.empty or 'canal_resolucao' not in df_agregado.columns:
        return pd.DataFrame()
    
    dist = df_agregado.groupby('canal_resolucao')['qtd_total'].sum().reset_index()
    dist.columns = ['Canal', 'Quantidade']
    dist['Percentual'] = (dist['Quantidade'] / dist['Quantidade'].sum() * 100).round(2)
    
    return dist

def preparar_dados_ml(dados, engine):
    """Prepara dados para modelos de Machine Learning."""
    # Carregar dataset ML sob demanda
    with st.spinner("📥 Carregando dataset de Machine Learning..."):
        df = carregar_dataset_ml(engine)
    
    if df.empty:
        st.error("Dataset de ML não disponível.")
        return None, None, None, None
    
    # Features
    features = [
        'taxa_exclusao_esperada_pct', 'taxa_autuacao_esperada_pct',
        'taxa_autonomia_esperada_pct', 'score_efetividade_tipo',
        'facilidade_num', 'legitimidade_num', 'natureza_num',
        'regime_normal', 'simples_nacional', 'qtd_tipos_inconsistencia_historico',
        'contador_taxa_autonomia', 'contador_taxa_autuacao', 'contador_score',
        'log_valor', 'dias_malha'
    ]
    
    # Filtrar features disponíveis
    features_disponiveis = [f for f in features if f in df.columns]
    
    if not features_disponiveis:
        return None, None, None, None
    
    X = df[features_disponiveis].fillna(0)
    y = df['target_exclusao'].fillna(0)
    
    # Remover NaN restantes
    mask_valid = ~X.isna().any(axis=1) & ~y.isna()
    X = X[mask_valid]
    y = y[mask_valid]
    
    # Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=42, stratify=y
    )
    
    # Normalizar
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)
    
    return X_train_scaled, X_test_scaled, y_train, y_test

# =============================================================================
# 6. CRIAÇÃO DE FILTROS
# =============================================================================

def criar_filtros_sidebar(dados):
    """Cria painel de filtros na sidebar."""
    filtros = {}
    
    with st.sidebar.expander("🔍 Filtros Globais", expanded=True):
        
        # Período
        df_evolucao = dados.get('evolucao_mensal', pd.DataFrame())
        if not df_evolucao.empty and 'periodo' in df_evolucao.columns:
            periodos = sorted(df_evolucao['periodo'].unique())
            if len(periodos) > 0:
                filtros['periodo_inicio'] = st.selectbox(
                    "Período Inicial",
                    periodos,
                    index=0
                )
                filtros['periodo_fim'] = st.selectbox(
                    "Período Final",
                    periodos,
                    index=len(periodos)-1
                )
        
        # Canal de resolução
        filtros['canais'] = st.multiselect(
            "Canais de Resolução",
            ['AUTONOMO_DDE', 'AUTONOMO_MALHA', 'FISCALIZACAO', 'EXCLUSAO_AUDITOR', 'ATIVA'],
            default=['AUTONOMO_DDE', 'AUTONOMO_MALHA', 'FISCALIZACAO', 'EXCLUSAO_AUDITOR', 'ATIVA']
        )
        
        # Natureza
        filtros['naturezas'] = st.multiselect(
            "Natureza da Inconsistência",
            ['OMISSAO', 'CREDITO_INDEVIDO', 'DIVERGENCIA_VALOR_MENOR', 'DIVERGENCIA_VALOR_MAIOR', 'OUTROS'],
            default=['OMISSAO', 'CREDITO_INDEVIDO', 'DIVERGENCIA_VALOR_MENOR', 'DIVERGENCIA_VALOR_MAIOR', 'OUTROS']
        )
        
        # Valor mínimo
        filtros['valor_minimo'] = st.number_input(
            "Valor Mínimo (R$)",
            min_value=0,
            max_value=1000000,
            value=0,
            step=1000
        )
        
        st.divider()
        
        # Visualização
        st.subheader("🎨 Visualização")
        filtros['tema'] = st.selectbox(
            "Tema dos Gráficos",
            ["plotly", "plotly_white", "plotly_dark"],
            index=1
        )
        
        filtros['mostrar_valores'] = st.checkbox("Mostrar valores", value=True)
    
    return filtros

def aplicar_filtros(df, filtros):
    """Aplica filtros no DataFrame."""
    if df.empty:
        return df
    
    df_filtrado = df.copy()
    
    # Canal - funciona tanto para dados detalhados quanto agregados
    if filtros.get('canais') and 'canal_resolucao' in df_filtrado.columns:
        if len(filtros['canais']) < 5:  # Se não selecionou todos
            df_filtrado = df_filtrado[df_filtrado['canal_resolucao'].isin(filtros['canais'])]
    
    # Natureza
    if filtros.get('naturezas') and 'natureza_inconsistencia' in df_filtrado.columns:
        if len(filtros['naturezas']) < 5:  # Se não selecionou todos
            df_filtrado = df_filtrado[df_filtrado['natureza_inconsistencia'].isin(filtros['naturezas'])]
    
    # Valor - para dados agregados, filtrar pelo valor_total
    valor_min = filtros.get('valor_minimo', 0)
    if valor_min > 0:
        if 'valor_total' in df_filtrado.columns:
            # Para dados agregados
            df_filtrado = df_filtrado[df_filtrado['valor_total'] >= valor_min]
        elif 'vl_inconsistencia' in df_filtrado.columns:
            # Para dados detalhados
            df_filtrado = df_filtrado[df_filtrado['vl_inconsistencia'] >= valor_min]
    
    # Período
    if 'nu_per_ref' in df_filtrado.columns:
        if filtros.get('periodo_inicio'):
            df_filtrado = df_filtrado[df_filtrado['nu_per_ref'] >= filtros['periodo_inicio']]
        if filtros.get('periodo_fim'):
            df_filtrado = df_filtrado[df_filtrado['nu_per_ref'] <= filtros['periodo_fim']]
    elif 'periodo' in df_filtrado.columns:
        if filtros.get('periodo_inicio'):
            df_filtrado = df_filtrado[df_filtrado['periodo'] >= filtros['periodo_inicio']]
        if filtros.get('periodo_fim'):
            df_filtrado = df_filtrado[df_filtrado['periodo'] <= filtros['periodo_fim']]
    
    return df_filtrado

def mostrar_filtros_ativos(filtros, suporta_periodo=True):
    """Exibe os filtros ativos de forma visual."""
    filtros_ativos = []
    
    if filtros.get('canais') and len(filtros['canais']) < 5:
        filtros_ativos.append(f"📍 Canais: {', '.join(filtros['canais'])}")
    
    if filtros.get('naturezas') and len(filtros['naturezas']) < 5:
        filtros_ativos.append(f"🎯 Naturezas: {', '.join(filtros['naturezas'])}")
    
    if filtros.get('valor_minimo', 0) > 0:
        filtros_ativos.append(f"💰 Valor mínimo: R$ {filtros['valor_minimo']:,.2f}")
    
    if filtros.get('periodo_inicio') or filtros.get('periodo_fim'):
        inicio = filtros.get('periodo_inicio', 'início')
        fim = filtros.get('periodo_fim', 'fim')
        
        if suporta_periodo:
            filtros_ativos.append(f"📅 Período: {inicio} a {fim}")
        else:
            st.warning(f"⚠️ Filtro de período ({inicio} a {fim}) não se aplica a esta página (dados agregados)")
    
    if filtros_ativos:
        st.markdown(
            f"<div class='info-box'>"
            f"<b>🔍 Filtros Ativos</b><br>"
            f"{' | '.join(filtros_ativos)}"
            f"</div>",
            unsafe_allow_html=True
        )
        return True
    return False
    
# =============================================================================
# 7. PÁGINAS DO DASHBOARD
# =============================================================================

def pagina_dashboard_executivo(dados, filtros):
    """Dashboard executivo principal usando dados agregados."""
    st.markdown("<h1 class='main-header'>📊 Dashboard Executivo - Gerenciamento de DAFs</h1>", unsafe_allow_html=True)
    
    # APLICAR FILTROS
    df_agregado = dados.get('inconsistencias_agregadas', pd.DataFrame())
    if not df_agregado.empty:
        df_agregado = aplicar_filtros(df_agregado, filtros)
        
        if df_agregado.empty:
            st.warning("⚠️ Nenhum dado encontrado com os filtros aplicados.")
            return
        
        dados['inconsistencias_agregadas'] = df_agregado
    
    # Mostrar filtros ativos
    mostrar_filtros_ativos(filtros, suporta_periodo=False)
    
    # KPIs principais
    kpis = calcular_kpis_gerais(dados)
    
    st.markdown("<div class='sub-header'>📈 Indicadores Principais</div>", unsafe_allow_html=True)
    
# Primeira linha de KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "🏢 Empresas",
            f"{kpis['total_empresas']:,}",
            help="Total de empresas no sistema"
        )
    
    with col2:
        st.metric(
            "📋 Inconsistências",
            f"{kpis['total_inconsistencias']:,}",
            help="Total de inconsistências identificadas"
        )
    
    with col3:
        st.metric(
            "✅ Taxa Autonomia",
            f"{kpis['taxa_autonomia']:.1f}%",
            delta=f"{kpis['taxa_autonomia']-60:.1f}pp" if kpis['taxa_autonomia'] > 60 else None,
            help="Percentual resolvido autonomamente (DDE + Malha)"
        )
    
    with col4:
        st.metric(
            "⚠️ Taxa Exclusão",
            f"{kpis['taxa_exclusao']:.1f}%",
            delta=f"{30-kpis['taxa_exclusao']:.1f}pp" if kpis['taxa_exclusao'] < 30 else None,
            delta_color="inverse",
            help="Percentual excluído por auditores"
        )
    
    with col5:
        st.metric(
            "🚨 Taxa Fiscalização",
            f"{kpis['taxa_fiscalizacao']:.1f}%",
            delta=f"{20-kpis['taxa_fiscalizacao']:.1f}pp" if kpis['taxa_fiscalizacao'] < 20 else None,
            delta_color="inverse",
            help="Percentual que precisou de PAF (não regularizou no prazo)"
        )
    
    # Segunda linha
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            "⏱️ Dias Médios",
            f"{kpis['dias_medio_malha']:.0f}",
            help="Tempo médio na malha"
        )
    
    with col2:
        st.metric(
            "💰 Valor Total",
            f"R$ {kpis['valor_total']/1e9:.2f}B",
            help="Valor total das inconsistências"
        )
    
    with col3:
        st.metric(
            "🔢 Tipos Únicos",
            f"{kpis['tipos_unicos']}",
            help="Tipos de inconsistência diferentes"
        )
    
    with col4:
        st.metric(
            "🔴 Ativas (Prazo)",
            f"{kpis['inconsistencias_ativas']:,}",
            help="Aguardando regularização no prazo - NORMAL"
        )
    
    with col5:
        st.metric(
            "🆕 Pendentes",
            f"{kpis['inconsistencias_pendentes']:,}",
            help="Identificadas, aguardando entrar na malha"
        )
    
    st.divider()
    
    # Info Box Explicativo do Fluxo
    st.markdown("""
    <div class='info-box'>
    <b>📊 Entendendo o Fluxo das Inconsistências</b><br><br>
    
    <b>1. IDENTIFICADA (🆕 Pendentes)</b>: Recém detectada pelo sistema, aguardando entrar na malha<br>
    <b>2. ATIVA (🔴)</b>: Entrou na malha, prazo para regularização autônoma - <i>Situação NORMAL</i><br>
    <b>3. Resoluções durante prazo ativo:</b><br>
    &nbsp;&nbsp;&nbsp;• <b>AUTONOMO_DDE</b> ✅ - Melhor cenário: regularizou via DDE<br>
    &nbsp;&nbsp;&nbsp;• <b>AUTONOMO_MALHA</b> ✅ - Bom: retificou declaração/nota<br>
    &nbsp;&nbsp;&nbsp;• <b>EXCLUSAO_AUDITOR</b> ⚠️ - Excluído após análise (legítima ou suspeita)<br>
    <b>4. FISCALIZAÇÃO (🚨)</b>: NÃO regularizou no prazo → PAF aberto - <i>Situação PROBLEMA</i><br>
    &nbsp;&nbsp;&nbsp;• <b>EM_FISCALIZACAO</b>: PAF em andamento<br>
    &nbsp;&nbsp;&nbsp;• <b>FISCALIZACAO_CONCLUIDA</b>: PAF finalizado (pode ter gerado autuação ou não)<br>
    <b>5. AUTUAÇÃO (⚖️)</b>: Gerou infração após fiscalização - <i>Situação MAIS GRAVE</i><br>
    
    <br><b>💡 Metas:</b><br>
    • Autonomia ≥ 60% (quanto mais, melhor)<br>
    • Fiscalização ≤ 20% (quanto menos, melhor - indica que resolvem no prazo)<br>
    • Autuação entre 5-15% (equilíbrio ideal)<br>
    • Exclusão ≤ 30% (quanto menos, melhor - pode ser suspeita)
    </div>
    """, unsafe_allow_html=True)
    
    st.divider()
    
    # Gráficos principais usando dados agregados
    df_agregado = dados.get('inconsistencias_agregadas', pd.DataFrame())
    
    if not df_agregado.empty:
        
        st.markdown("<div class='sub-header'>📊 Análises Visuais</div>", unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Distribuição por canal
            dist_canal = calcular_distribuicao_canais(dados)
            if not dist_canal.empty:
                fig = px.pie(
                    dist_canal,
                    values='Quantidade',
                    names='Canal',
                    title='Distribuição por Canal de Resolução',
                    template=filtros['tema'],
                    hole=0.4,
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Distribuição por natureza
            if 'natureza_inconsistencia' in df_agregado.columns:
                dist_nat = df_agregado.groupby('natureza_inconsistencia')['qtd_total'].sum().reset_index()
                dist_nat.columns = ['Natureza', 'Quantidade']
                
                fig = px.bar(
                    dist_nat,
                    x='Natureza',
                    y='Quantidade',
                    title='Distribuição por Natureza',
                    template=filtros['tema'],
                    color='Natureza'
                )
                st.plotly_chart(fig, use_container_width=True)
        
        # Gráfico de evolução temporal
        df_evolucao = dados.get('evolucao_mensal', pd.DataFrame())
        
        if not df_evolucao.empty:
            st.markdown("<div class='sub-header'>📈 Evolução Temporal</div>", unsafe_allow_html=True)
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df_evolucao['periodo'],
                y=df_evolucao['taxa_autonomia_pct'],
                name='Taxa Autonomia',
                mode='lines+markers',
                line=dict(color='#2ecc71', width=3)
            ))
            
            fig.add_trace(go.Scatter(
                x=df_evolucao['periodo'],
                y=df_evolucao['taxa_exclusao_pct'],
                name='Taxa Exclusão',
                mode='lines+markers',
                line=dict(color='#e74c3c', width=3)
            ))
            
            fig.add_trace(go.Scatter(
                x=df_evolucao['periodo'],
                y=df_evolucao['taxa_autuacao_pct'],
                name='Taxa Autuação',
                mode='lines+markers',
                line=dict(color='#3498db', width=3)
            ))
            
            fig.update_layout(
                title='Evolução das Taxas',
                template=filtros['tema'],
                height=400,
                xaxis_title='Período',
                yaxis_title='Taxa (%)',
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)

def pagina_tipos_inconsistencia(dados, filtros):
    """Análise detalhada dos tipos de inconsistência."""
    st.markdown("<h1 class='main-header'>🔍 Análise de Tipos de Inconsistência</h1>", unsafe_allow_html=True)
    
    df_catalogo = dados.get('catalogo_tipos', pd.DataFrame())
    df_benchmark = dados.get('benchmark_tipos', pd.DataFrame())
    df_ranking = dados.get('ranking_tipos', pd.DataFrame())
    df_agregado = dados.get('inconsistencias_agregadas', pd.DataFrame())

        # APLICAR FILTROS
    if not df_agregado.empty:
        df_agregado = aplicar_filtros(df_agregado, filtros)
        
        if df_agregado.empty:
            st.warning("⚠️ Nenhum dado encontrado com os filtros aplicados.")
            return
        
        # Mostrar filtros ativos
        mostrar_filtros_ativos(filtros, suporta_periodo=True)
        
    if df_catalogo.empty:
        st.error("Dados do catálogo não disponíveis.")
        return
    
    # KPIs
    st.markdown("<div class='sub-header'>📊 Visão Geral dos Tipos</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total de Tipos", f"{len(df_catalogo)}")
    
    with col2:
        if not df_ranking.empty and 'classificacao_efetividade' in df_ranking.columns:
            excelentes = len(df_ranking[df_ranking['classificacao_efetividade']=='EXCELENTE'])
            st.metric("Excelentes", f"{excelentes}")
    
    with col3:
        if not df_ranking.empty and 'classificacao_efetividade' in df_ranking.columns:
            problematicos = len(df_ranking[df_ranking['classificacao_efetividade']=='PROBLEMATICO'])
            st.metric("Problemáticos", f"{problematicos}")
    
    with col4:
        if not df_benchmark.empty:
            media_autonomia = df_benchmark['taxa_autonomia_esperada_pct'].mean()
            st.metric("Autonomia Média", f"{media_autonomia:.1f}%")
    
    with col5:
        if not df_benchmark.empty:
            media_exclusao = df_benchmark['taxa_exclusao_esperada_pct'].mean()
            st.metric("Exclusão Média", f"{media_exclusao:.1f}%")
    
    st.divider()
    
    # Ranking completo
    if not df_ranking.empty:
        st.markdown("<div class='sub-header'>🏆 Ranking de Efetividade</div>", unsafe_allow_html=True)
        
        col1, col2 = st.columns([1, 3])
        
        with col1:
            ordem = st.radio("Ordem:", ['Melhores', 'Piores'], index=0)
            top_n = st.slider("Mostrar top:", 5, 45, 20, 5)
        
        if ordem == 'Melhores':
            df_rank = df_ranking.nsmallest(top_n, 'ranking_efetividade')
        else:
            df_rank = df_ranking.nlargest(top_n, 'ranking_efetividade')
        
        # ========== CALCULAR TAXAS POR CANAL ==========
        if not df_agregado.empty:
            df_rank_enriquecido = df_rank.copy()
            
            # Inicializar colunas de taxas
            colunas_taxas = ['taxa_autonomo_dde_pct', 'taxa_autonomo_malha_pct', 'taxa_fiscalizacao_pct',
                           'taxa_exclusao_auditor_pct', 'taxa_ativa_pct', 'taxa_identificada_pct']
            
            for col in colunas_taxas:
                df_rank_enriquecido[col] = 0.0
            
            for idx, row in df_rank_enriquecido.iterrows():
                cd_incons = row['cd_inconsistencia']
                
                # Buscar dados agregados deste tipo
                dados_tipo = df_agregado[df_agregado['cd_inconsistencia'] == cd_incons]
                
                if not dados_tipo.empty:
                    # Total de inconsistências deste tipo
                    total_tipo = dados_tipo['qtd_total'].sum()
                    
                    if total_tipo > 0:
                        # Calcular taxa por canal
                        mapeamento_canais = {
                            'AUTONOMO_DDE': 'taxa_autonomo_dde_pct',
                            'AUTONOMO_MALHA': 'taxa_autonomo_malha_pct',
                            'FISCALIZACAO': 'taxa_fiscalizacao_pct',
                            'EXCLUSAO_AUDITOR': 'taxa_exclusao_auditor_pct',
                            'ATIVA': 'taxa_ativa_pct',
                            'IDENTIFICADA': 'taxa_identificada_pct'
                        }
                        
                        for canal, col_name in mapeamento_canais.items():
                            dados_canal = dados_tipo[dados_tipo['canal_resolucao'] == canal]
                            qtd_canal = dados_canal['qtd_total'].sum() if not dados_canal.empty else 0
                            df_rank_enriquecido.at[idx, col_name] = round((qtd_canal / total_tipo) * 100, 2)
            
            # Colunas para exibir
            cols_display = ['ranking_efetividade', 'cd_inconsistencia', 'nm_inconsistencia',
                          'qtd_ocorrencias_total', 
                          'taxa_autonomo_dde_pct', 'taxa_autonomo_malha_pct',
                          'taxa_fiscalizacao_pct', 'taxa_exclusao_auditor_pct',
                          'taxa_ativa_pct', 'taxa_identificada_pct',
                          'score_efetividade_tipo', 'classificacao_efetividade']
            
            # Filtrar apenas colunas existentes
            cols_existentes = [col for col in cols_display if col in df_rank_enriquecido.columns]
            
            # Formatação
            format_dict = {
                'qtd_ocorrencias_total': '{:,.0f}',
                'taxa_autonomo_dde_pct': '{:.2f}%',
                'taxa_autonomo_malha_pct': '{:.2f}%',
                'taxa_fiscalizacao_pct': '{:.2f}%',
                'taxa_exclusao_auditor_pct': '{:.2f}%',
                'taxa_ativa_pct': '{:.2f}%',
                'taxa_identificada_pct': '{:.2f}%',
                'score_efetividade_tipo': '{:.1f}'
            }
            
            st.dataframe(
                df_rank_enriquecido[cols_existentes].style.format(format_dict).background_gradient(
                    subset=['taxa_autonomo_dde_pct', 'taxa_autonomo_malha_pct'], 
                    cmap='Greens'
                ),
                use_container_width=True,
                height=600
            )
            
            # ========== LEGENDAS ==========
            with st.expander("📊 Entenda as Taxas por Canal"):
                st.markdown("""
                **Legenda das Taxas:**
                
                - **DDE (Declaração de Débitos Exercícios Anteriores)**: Resolvido via DDE
                  - ✅ Melhor cenário: contribuinte corrige
                
                - **Malha (Autônoma)**: Resolvido após entrar na malha, mas autonomamente pelo contribuinte
                  - ✅ Bom: contribuinte resolve sem precisar de fiscalização
                
                - **Fiscalização**: Resolvido via procedimento fiscal formal
                  - ⚖️ Necessitou intervenção de auditor
                
                - **Exclusão Auditor**: Excluído por auditor após análise
                  - ⚠️ Pode ser legítima ou suspeita (requer análise)
                
                - **Ativa**: Ainda aguardando resolução
                  - ⏳ Em andamento, sem resolução ainda
                
                - **Identificada**: Recém detectada pelo sistema, sem nenhuma ação ainda
                  - 🆕 Backlog - aguardando primeira análise
                
                **💡 Dica**: Taxas altas de DDE e Malha indicam boa efetividade do tipo. 
                Taxa alta de Identificada pode indicar gargalo no processamento.
                """)
        
        else:
            # Se não tiver dados agregados, mostrar tabela original
            cols_display = ['ranking_efetividade', 'cd_inconsistencia', 'nm_inconsistencia',
                           'qtd_ocorrencias_total', 'taxa_autonomia_esperada_pct',
                           'taxa_exclusao_esperada_pct', 'score_efetividade_tipo',
                           'classificacao_efetividade']
            
            cols_existentes = [col for col in cols_display if col in df_rank.columns]
            
            st.dataframe(
                df_rank[cols_existentes].style.format({
                    'qtd_ocorrencias_total': '{:,.0f}',
                    'taxa_autonomia_esperada_pct': '{:.1f}%',
                    'taxa_exclusao_esperada_pct': '{:.1f}%',
                    'score_efetividade_tipo': '{:.1f}'
                }),
                use_container_width=True,
                height=500
            )
    
    st.divider()
    
    # Análise de tipos problemáticos
    if not df_ranking.empty and 'classificacao_efetividade' in df_ranking.columns:
        tipos_problematicos = df_ranking[df_ranking['classificacao_efetividade']=='PROBLEMATICO']
        
        if not tipos_problematicos.empty:
            st.markdown("<div class='sub-header'>⚠️ Tipos Problemáticos</div>", unsafe_allow_html=True)
            
            st.markdown(
                f"<div class='alert-critico'>"
                f"<b>🚨 {len(tipos_problematicos)} tipos necessitam atenção especial!</b>"
                f"</div>",
                unsafe_allow_html=True
            )
            
            cols_prob = ['cd_inconsistencia', 'nm_inconsistencia', 'qtd_ocorrencias_total',
                        'taxa_exclusao_esperada_pct', 'taxa_autuacao_esperada_pct', 
                        'score_efetividade_tipo']
            
            cols_existentes = [col for col in cols_prob if col in tipos_problematicos.columns]
            
            st.dataframe(
                tipos_problematicos[cols_existentes].style.format({
                    'qtd_ocorrencias_total': '{:,.0f}',
                    'taxa_exclusao_esperada_pct': '{:.1f}%',
                    'taxa_autuacao_esperada_pct': '{:.1f}%',
                    'score_efetividade_tipo': '{:.1f}'
                }),
                use_container_width=True,
                height=300
            )
        else:
            st.markdown(
                "<div class='alert-positivo'>"
                "<b>✅ Nenhum tipo necessita revisão urgente!</b>"
                "</div>",
                unsafe_allow_html=True
            )

def pagina_performance_contadores(dados, filtros):
    """Análise de performance dos contadores."""
    st.markdown("<h1 class='main-header'>👥 Performance dos Contadores</h1>", unsafe_allow_html=True)
    
    df_contadores = dados.get('ranking_contadores', pd.DataFrame())
    df_perf = dados.get('performance_contadores', pd.DataFrame())
    
    if df_contadores.empty:
        st.error("Dados de contadores não disponíveis.")
        return
    
    # Estatísticas gerais
    st.markdown("<div class='sub-header'>📊 Estatísticas Gerais</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Contadores", f"{len(df_contadores):,}")
    
    with col2:
        top_perf = len(df_contadores[df_contadores.get('classificacao_performance', '')=='TOP_PERFORMER'])
        st.metric("Top Performers", f"{top_perf:,}")
    
    with col3:
        media_autonomia = df_contadores.get('taxa_autonomia_pct', pd.Series([0])).mean()
        st.metric("Autonomia Média", f"{media_autonomia:.1f}%")
    
    with col4:
        criticos = len(df_contadores[df_contadores.get('classificacao_performance', '').str.contains('CRITICO', na=False)])
        st.metric("Críticos", f"{criticos:,}")
    
    with col5:
        media_score = df_contadores.get('score_performance', pd.Series([0])).mean()
        st.metric("Score Médio", f"{media_score:.1f}")
    
    st.divider()
    
    # Ranking
    st.markdown("<div class='sub-header'>🏆 Ranking de Contadores</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        top_n = st.slider("Top N:", 10, 100, 50, 10)
        ordem = st.radio("Ordem:", ['Melhores', 'Piores'], index=0)
    
    if 'score_performance' in df_contadores.columns:
        if ordem == 'Melhores':
            df_rank = df_contadores.nlargest(top_n, 'score_performance')
        else:
            df_rank = df_contadores.nsmallest(top_n, 'score_performance')
        
        cols_display = ['nm_contador', 'nu_crc_contador', 'cd_uf_contador',
                        'qtd_clientes_total', 'qtd_empresas_com_incons',
                        'taxa_autonomia_pct', 'taxa_autuacao_pct', 'score_performance',
                        'classificacao_performance']
        
        cols_existentes = [col for col in cols_display if col in df_rank.columns]
        
        st.dataframe(
            df_rank[cols_existentes].style.format({
                'taxa_autonomia_pct': '{:.1f}%',
                'taxa_autuacao_pct': '{:.1f}%',
                'score_performance': '{:.1f}'
            }),
            use_container_width=True,
            height=500
        )
    else:
        st.warning("Coluna 'score_performance' não encontrada.")
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        if 'classificacao_performance' in df_contadores.columns:
            dist = df_contadores['classificacao_performance'].value_counts().reset_index()
            dist.columns = ['Classificação', 'Quantidade']
            
            fig = px.pie(
                dist,
                values='Quantidade',
                names='Classificação',
                title='Distribuição por Classificação',
                template=filtros['tema'],
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True, key="contadores_pizza_class")
    
    with col2:
        if 'taxa_autonomia_pct' in df_contadores.columns:
            fig = px.box(
                df_contadores,
                y='taxa_autonomia_pct',
                title='Distribuição da Taxa de Autonomia',
                template=filtros['tema']
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True, key="contadores_box_autonomia")

def pagina_performance_dafs(dados, filtros):
    """Análise de performance das DAFs com estrutura multidimensional."""
    st.markdown("<h1 class='main-header'>🏢 Performance das DAFs/Equipes</h1>", unsafe_allow_html=True)
    
    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    df_exclusoes = dados.get('exclusoes_auditores', pd.DataFrame())
    
    if df_dafs.empty:
        st.error("Dados de DAFs não disponíveis.")
        return
    
    # Info Box Explicativo
    st.markdown("""
    <div class='info-box'>
    <b>📊 Sistema de Avaliação Multidimensional de DAFs</b><br><br>
    
    <b>🎯 Autonomia</b>: % resolvidas pelo contribuinte (DDE + Malha) - <i>Quanto MAIOR, melhor</i><br>
    <b>⏳ Pendência</b>: % aguardando regularização (ATIVA) - <i>Neutro, mas se muito alta pode indicar lentidão</i><br>
    <b>🗑️ Exclusão</b>: % excluídas por auditor - <i>Quanto MENOR, melhor (pode ser suspeita)</i><br>
    <b>🚨 Fiscalização</b>: % que precisou de PAF - <i>Quanto MENOR, melhor (indica não regularização)</i><br>
    
    <br><b>Score Geral</b>: Ponderado (40% Autonomia + 30% Baixa Fiscalização + 20% Baixa Exclusão + 10% outros)
    </div>
    """, unsafe_allow_html=True)
    
    # ========== KPIs GERAIS ==========
    st.markdown("<div class='sub-header'>📊 Indicadores Gerais das DAFs</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total DAFs", f"{len(df_dafs):,}")
    
    with col2:
        excelentes = len(df_dafs[df_dafs['classificacao_geral']=='EXCELENTE']) if 'classificacao_geral' in df_dafs.columns else 0
        st.metric("🌟 Excelentes", f"{excelentes:,}")
    
    with col3:
        media_geral = df_dafs['score_geral_ponderado'].mean() if 'score_geral_ponderado' in df_dafs.columns else 0
        st.metric("📊 Score Médio Geral", f"{media_geral:.1f}")
    
    with col4:
        df_dafs['total_alertas'] = (
            df_dafs.get('flag_alerta_autonomia_critica', 0) +
            df_dafs.get('flag_alerta_pendencia_alta', 0) +
            df_dafs.get('flag_alerta_exclusao_alta', 0) +
            df_dafs.get('flag_alerta_autuacao_alta', 0)
        )
        dafs_alerta = len(df_dafs[df_dafs['total_alertas'] > 0])
        st.metric("🚨 DAFs com Alertas", f"{dafs_alerta:,}")
    
    with col5:
        criticas = len(df_dafs[df_dafs['classificacao_geral']=='CRITICO']) if 'classificacao_geral' in df_dafs.columns else 0
        st.metric("🔴 Críticas", f"{criticas:,}")
    
    # Segunda linha - Scores médios dos 4 indicadores
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        media_autonomia = df_dafs['score_autonomia'].mean() if 'score_autonomia' in df_dafs.columns else 0
        st.metric("🎯 Autonomia Média", f"{media_autonomia:.1f}")
    
    with col2:
        media_fiscalizacao = df_dafs['score_fiscalizacao'].mean() if 'score_fiscalizacao' in df_dafs.columns else 0
        st.metric("🚨 Fiscalização Média", f"{media_fiscalizacao:.1f}")
    
    with col3:
        media_exclusao = df_dafs['score_exclusao'].mean() if 'score_exclusao' in df_dafs.columns else 0
        st.metric("🗑️ Exclusão Média", f"{media_exclusao:.1f}")
    
    with col4:
        media_autuacao = df_dafs['score_autuacao'].mean() if 'score_autuacao' in df_dafs.columns else 0
        st.metric("⚖️ Autuação Média", f"{media_autuacao:.1f}")
    
    st.divider()
    
    # ========== DISTRIBUIÇÃO POR CLASSIFICAÇÃO GERAL ==========
    st.markdown("<div class='sub-header'>📊 Distribuição por Classificação Geral</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'classificacao_geral' in df_dafs.columns:
            dist_class = df_dafs['classificacao_geral'].value_counts().reset_index()
            dist_class.columns = ['Classificação', 'Quantidade']
            
            fig = px.pie(
                dist_class,
                values='Quantidade',
                names='Classificação',
                title='Distribuição das DAFs por Classificação',
                template=filtros['tema'],
                color='Classificação',
                color_discrete_map={
                    'EXCELENTE': '#10b981',
                    'BOM': '#84cc16',
                    'REGULAR': '#fbbf24',
                    'ATENCAO': '#f97316',
                    'CRITICO': '#ef4444'
                },
                hole=0.4
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True, key="grafico_pizza_classificacao")
    
    with col2:
        # Gráfico de scores médios por classificação
        if 'classificacao_geral' in df_dafs.columns and 'score_geral_ponderado' in df_dafs.columns:
            scores_por_class = df_dafs.groupby('classificacao_geral')['score_geral_ponderado'].mean().reset_index()
            scores_por_class.columns = ['Classificação', 'Score Médio']
            
            fig = px.bar(
                scores_por_class,
                x='Classificação',
                y='Score Médio',
                title='Score Geral Médio por Classificação',
                template=filtros['tema'],
                color='Classificação',
                color_discrete_map={
                    'EXCELENTE': '#10b981',
                    'BOM': '#84cc16',
                    'REGULAR': '#fbbf24',
                    'ATENCAO': '#f97316',
                    'CRITICO': '#ef4444'
                }
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True, key="grafico_barras_scores_class")
    
    st.divider()
    
    # ========== RANKING COMPLETO ==========
    st.markdown("<div class='sub-header'>🏆 Ranking Completo de DAFs</div>", unsafe_allow_html=True)
    
    # Adicionar coluna de total de alertas se não existir
    if 'total_alertas' not in df_dafs.columns:
        df_dafs['total_alertas'] = (
            df_dafs.get('flag_alerta_autonomia_critica', 0) +
            df_dafs.get('flag_alerta_pendencia_alta', 0) +
            df_dafs.get('flag_alerta_exclusao_alta', 0) +
            df_dafs.get('flag_alerta_autuacao_alta', 0)
        )
    
    cols_display = [
        'ranking_geral', 'id_equipe', 'score_geral_ponderado', 'classificacao_geral',
        'score_autonomia', 'score_pendencia', 'score_exclusao', 'score_autuacao',
        'total_alertas', 'qtd_empresas_acompanhadas', 'qtd_contadores_acompanhados'
    ]
    
    # Filtrar apenas colunas que existem
    cols_existentes = [col for col in cols_display if col in df_dafs.columns]
    
    st.dataframe(
        df_dafs[cols_existentes].style.format({
            'score_geral_ponderado': '{:.1f}',
            'score_autonomia': '{:.1f}',
            'score_pendencia': '{:.1f}',
            'score_exclusao': '{:.1f}',
            'score_autuacao': '{:.1f}'
        }).background_gradient(subset=['score_geral_ponderado'], cmap='RdYlGn'),
        use_container_width=True,
        height=600
    )
    
    st.divider()
    
    # ========== ANÁLISE DE ALERTAS ==========
    st.markdown("<div class='sub-header'>🚨 Análise de Alertas por Tipo</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Contagem de alertas por tipo
        alertas_counts = {
            '🎯 Autonomia Crítica': int(df_dafs.get('flag_alerta_autonomia_critica', pd.Series([0])).sum()),
            '⏳ Pendência Alta': int(df_dafs.get('flag_alerta_pendencia_alta', pd.Series([0])).sum()),
            '🗑️ Exclusão Alta': int(df_dafs.get('flag_alerta_exclusao_alta', pd.Series([0])).sum()),
            '⚖️ Autuação Alta': int(df_dafs.get('flag_alerta_autuacao_alta', pd.Series([0])).sum())
        }
        
        fig = px.bar(
            x=list(alertas_counts.keys()),
            y=list(alertas_counts.values()),
            title='Quantidade de DAFs com Cada Tipo de Alerta',
            template=filtros['tema'],
            labels={'x': 'Tipo de Alerta', 'y': 'Quantidade de DAFs'},
            color=list(alertas_counts.keys()),
            color_discrete_sequence=['#ef4444', '#f97316', '#fbbf24', '#fb923c']
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True, key="grafico_barras_alertas")
    
    with col2:
        # Distribuição de DAFs por número de alertas
        dist_alertas = df_dafs['total_alertas'].value_counts().sort_index().reset_index()
        dist_alertas.columns = ['Nº de Alertas', 'Quantidade de DAFs']
        
        fig = px.bar(
            dist_alertas,
            x='Nº de Alertas',
            y='Quantidade de DAFs',
            title='Distribuição de DAFs por Número de Alertas',
            template=filtros['tema'],
            color='Nº de Alertas',
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig, use_container_width=True, key="grafico_barras_dist_alertas")
    
# DAFs com múltiplos alertas
    df_multiplos_alertas = df_dafs[df_dafs['total_alertas'] >= 2].sort_values('total_alertas', ascending=False)
    
    if len(df_multiplos_alertas) > 0:
        st.markdown(f"""
        <div class='alert-critico'>
        <b>⚠️ {len(df_multiplos_alertas)} DAFs com Múltiplos Alertas Detectadas!</b><br>
        Estas equipes necessitam atenção prioritária.
        </div>
        """, unsafe_allow_html=True)
        
        cols_alertas = ['id_equipe', 'total_alertas', 'score_geral_ponderado', 'classificacao_geral',
                       'flag_alerta_autonomia_critica', 'flag_alerta_pendencia_alta',
                       'flag_alerta_exclusao_alta', 'flag_alerta_autuacao_alta']
        
        cols_exist_alertas = [col for col in cols_alertas if col in df_multiplos_alertas.columns]
        
        st.dataframe(
            df_multiplos_alertas[cols_exist_alertas].head(20).style.format({
                'score_geral_ponderado': '{:.1f}'
            }),
            use_container_width=True,
            height=400
        )
    else:
        st.markdown("""
        <div class='alert-positivo'>
        <b>✅ Nenhuma DAF com múltiplos alertas!</b>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # ========== ANÁLISE DE EXCLUSÕES (mantida da versão anterior) ==========
    if not df_exclusoes.empty:
        st.markdown("<div class='sub-header'>⚖️ Análise de Exclusões por Auditor</div>", unsafe_allow_html=True)
        
        colunas_necessarias = ['id_equipe_auditor', 'classificacao_exclusoes']
        
        if all(col in df_exclusoes.columns for col in colunas_necessarias):
            suspeitas = df_exclusoes[
                df_exclusoes['classificacao_exclusoes'].isin(['ALTA_SUSPEITA', 'MEDIA_SUSPEITA'])
            ]
            
            if not suspeitas.empty:
                st.markdown(
                    f"<div class='alert-critico'>"
                    f"<b>🚨 Equipes com Padrão Suspeito de Exclusão Detectadas!</b><br>"
                    f"Total: {len(suspeitas)} equipes"
                    f"</div>", 
                    unsafe_allow_html=True
                )
                
                # Colunas a exibir
                cols_display = []
                for col in ['id_equipe_auditor', 'qtd_total_inconsistencias', 'qtd_total_excluidas',
                           'taxa_exclusao_geral_pct', 'qtd_exclusoes_suspeitas', 
                           'taxa_exclusao_suspeita_pct', 'score_legitimidade', 'classificacao_exclusoes']:
                    if col in suspeitas.columns:
                        cols_display.append(col)
                
                if cols_display:
                    # Criar dicionário de formatação apenas para colunas existentes
                    format_dict = {}
                    if 'taxa_exclusao_geral_pct' in cols_display:
                        format_dict['taxa_exclusao_geral_pct'] = '{:.1f}%'
                    if 'taxa_exclusao_suspeita_pct' in cols_display:
                        format_dict['taxa_exclusao_suspeita_pct'] = '{:.1f}%'
                    if 'score_legitimidade' in cols_display:
                        format_dict['score_legitimidade'] = '{:.1f}'
                    
                    st.dataframe(
                        suspeitas[cols_display].style.format(format_dict),
                        use_container_width=True,
                        height=400
                    )
                else:
                    st.warning("Dados de exclusões incompletos.")
            else:
                st.markdown(
                    "<div class='alert-positivo'>"
                    "<b>✅ Nenhuma equipe com padrão suspeito detectada!</b>"
                    "</div>", 
                    unsafe_allow_html=True
                )
            
            # Estatísticas gerais de exclusões
            st.markdown("**📊 Estatísticas de Exclusões por Classificação:**")
            
            if 'classificacao_exclusoes' in df_exclusoes.columns:
                stats_excl = df_exclusoes['classificacao_exclusoes'].value_counts().reset_index()
                stats_excl.columns = ['Classificação', 'Quantidade']
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.dataframe(stats_excl, use_container_width=True)
                
                with col2:
                    fig = px.pie(
                        stats_excl,
                        values='Quantidade',
                        names='Classificação',
                        title='Distribuição de Classificações',
                        template=filtros['tema'],
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    st.plotly_chart(fig, use_container_width=True, key="grafico_pizza_exclusoes")
        else:
            st.warning("⚠️ Tabela de exclusões não possui as colunas esperadas.")
    else:
        st.warning("⚠️ Dados de análise de exclusões não disponíveis.")

    # ========== ANÁLISE DE EXCLUSÕES (mantida da versão anterior) ==========
    if not df_exclusoes.empty:
        st.markdown("<div class='sub-header'>⚖️ Análise de Exclusões por Auditor</div>", unsafe_allow_html=True)
        
        colunas_necessarias = ['id_equipe_auditor', 'classificacao_exclusoes']
        
        if all(col in df_exclusoes.columns for col in colunas_necessarias):
            suspeitas = df_exclusoes[
                df_exclusoes['classificacao_exclusoes'].isin(['ALTA_SUSPEITA', 'MEDIA_SUSPEITA'])
            ]
            
            if not suspeitas.empty:
                st.markdown(
                    f"<div class='alert-critico'>"
                    f"<b>🚨 Equipes com Padrão Suspeito de Exclusão Detectadas!</b><br>"
                    f"Total: {len(suspeitas)} equipes"
                    f"</div>", 
                    unsafe_allow_html=True
                )
                
                # Colunas a exibir
                cols_display = []
                for col in ['id_equipe_auditor', 'qtd_total_inconsistencias', 'qtd_total_excluidas',
                           'taxa_exclusao_geral_pct', 'qtd_exclusoes_suspeitas', 
                           'taxa_exclusao_suspeita_pct', 'score_legitimidade', 'classificacao_exclusoes']:
                    if col in suspeitas.columns:
                        cols_display.append(col)
                
                if cols_display:
                    format_dict = {}
                    if 'taxa_exclusao_geral_pct' in cols_display:
                        format_dict['taxa_exclusao_geral_pct'] = '{:.1f}%'
                    if 'taxa_exclusao_suspeita_pct' in cols_display:
                        format_dict['taxa_exclusao_suspeita_pct'] = '{:.1f}%'
                    if 'score_legitimidade' in cols_display:
                        format_dict['score_legitimidade'] = '{:.1f}'
                    
                    st.dataframe(
                        suspeitas[cols_display].style.format(format_dict),
                        use_container_width=True,
                        height=400
                    )
                else:
                    st.warning("Dados de exclusões incompletos.")
            else:
                st.markdown(
                    "<div class='alert-positivo'>"
                    "<b>✅ Nenhuma equipe com padrão suspeito detectada!</b>"
                    "</div>", 
                    unsafe_allow_html=True
                )
            
            # Estatísticas gerais de exclusões
            st.markdown("**📊 Estatísticas de Exclusões por Classificação:**")
            
            if 'classificacao_exclusoes' in df_exclusoes.columns:
                stats_excl = df_exclusoes['classificacao_exclusoes'].value_counts().reset_index()
                stats_excl.columns = ['Classificação', 'Quantidade']
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.dataframe(stats_excl, use_container_width=True)
                
                with col2:
                    fig = px.pie(
                        stats_excl,
                        values='Quantidade',
                        names='Classificação',
                        title='Distribuição de Classificações',
                        template=filtros['tema'],
                        color_discrete_sequence=px.colors.qualitative.Set3
                    )
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("⚠️ Tabela de exclusões não possui as colunas esperadas.")
    else:
        st.warning("⚠️ Dados de análise de exclusões não disponíveis.")

def pagina_drill_down_daf(dados, filtros):
    """Drill-down detalhado por DAF/Equipe."""
    st.markdown("<h1 class='main-header'>🔎 Drill-Down por DAF/Equipe</h1>", unsafe_allow_html=True)
    
    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    df_empresas_resumo = dados.get('empresas_base_resumo', pd.DataFrame())
    df_exclusoes = dados.get('exclusoes_auditores', pd.DataFrame())
    df_contadores = dados.get('performance_contadores', pd.DataFrame())
    
    if df_dafs.empty:
        st.error("Dados de DAFs não disponíveis.")
        return
    
    # Seleção da DAF
    st.markdown("<div class='sub-header'>🔍 Seleção de DAF/Equipe</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Lista de DAFs ordenada por ranking
        dafs_lista = df_dafs[['id_equipe', 'score_geral_ponderado', 'classificacao_geral']].sort_values('score_geral_ponderado', ascending=False)
        
        id_equipe_selecionada = st.selectbox(
            "Selecione a DAF/Equipe:",
            dafs_lista['id_equipe'].tolist(),
            format_func=lambda x: f"DAF {x} - {dafs_lista[dafs_lista['id_equipe']==x]['classificacao_geral'].iloc[0]} (Score: {dafs_lista[dafs_lista['id_equipe']==x]['score_geral_ponderado'].iloc[0]:.1f})"
        )
    
    if id_equipe_selecionada is None:
        st.info("👆 Selecione uma DAF para análise.")
        return
    
    # Buscar dados da DAF
    daf = df_dafs[df_dafs['id_equipe']==id_equipe_selecionada]
    
    if daf.empty:
        st.warning(f"DAF {id_equipe_selecionada} não encontrada.")
        return
    
    daf_info = daf.iloc[0]
    
    # Cabeçalho
    st.markdown(f"<h2 style='color: #1565c0;'>📊 DAF/Equipe: {id_equipe_selecionada}</h2>", unsafe_allow_html=True)
    
    # KPIs principais
    st.markdown("<div class='sub-header'>📈 Indicadores da DAF</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    # CALCULAR RANKING MANUALMENTE
    df_dafs_ordenado = df_dafs.sort_values('score_geral_ponderado', ascending=False).reset_index(drop=True)
    df_dafs_ordenado['ranking_manual'] = range(1, len(df_dafs_ordenado) + 1)
    ranking_atual = df_dafs_ordenado[df_dafs_ordenado['id_equipe'] == id_equipe_selecionada]['ranking_manual'].iloc[0] if len(df_dafs_ordenado[df_dafs_ordenado['id_equipe'] == id_equipe_selecionada]) > 0 else 0
    
    with col1:
        st.metric("Ranking", f"#{int(ranking_atual)}")
    
    with col2:
        st.metric("Score Geral", f"{daf_info.get('score_geral_ponderado', 0):.1f}")
    
    with col3:
        st.metric("Classificação", daf_info.get('classificacao_geral', 'N/A'))
    
    with col4:
        st.metric("Empresas", f"{int(daf_info.get('qtd_empresas_acompanhadas', 0)):,}")
    
    with col5:
        st.metric("Contadores", f"{int(daf_info.get('qtd_contadores_acompanhados', 0)):,}")
    
    # Segunda linha - Scores dos 4 Indicadores
    st.markdown("**📊 Scores dos 4 Indicadores:**")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        criar_card_metrica(
            "🎯 Autonomia",
            daf_info.get('score_autonomia', 0),
            daf_info.get('ind_autonomia_nivel', 'N/A'),
            "🎯"
        )
    
    with col2:
        criar_card_metrica(
            "🚨 Fiscalização",
            daf_info.get('score_fiscalizacao', 0),
            daf_info.get('ind_fiscalizacao_nivel', 'N/A'),
            "🚨",
            invertido=True
        )
    
    with col3:
        criar_card_metrica(
            "🗑️ Exclusão",
            daf_info.get('score_exclusao', 0),
            daf_info.get('ind_exclusao_nivel', 'N/A'),
            "🗑️",
            invertido=True
        )
    
    with col4:
        criar_card_metrica(
            "⚖️ Autuação",
            daf_info.get('score_autuacao', 0),
            daf_info.get('ind_autuacao_nivel', 'N/A'),
            "⚖️"
        )
    
    # Terceira linha - Taxas brutas e alertas
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Taxa Autonomia", f"{daf_info.get('taxa_autonomia_pct', 0):.1f}%")
    
    with col2:
        st.metric("Taxa Exclusão", f"{daf_info.get('taxa_exclusao_pct', 0):.1f}%")
    
    with col3:
        st.metric("Taxa Autuação", f"{daf_info.get('taxa_autuacao_pct', 0):.1f}%")
    
    with col4:
        st.metric("Taxa Pendência", f"{daf_info.get('taxa_pendencia_pct', 0):.1f}%")
    
    with col5:
        total_alertas = sum([
            daf_info.get('flag_alerta_autonomia_critica', 0),
            daf_info.get('flag_alerta_pendencia_alta', 0),
            daf_info.get('flag_alerta_exclusao_alta', 0),
            daf_info.get('flag_alerta_autuacao_alta', 0)
        ])
        st.metric("🚨 Alertas", int(total_alertas))
    
    st.divider()
    
    # Empresas da DAF
    df_empresas_resumo = dados.get('empresas_base_resumo', pd.DataFrame())
    
    if not df_empresas_resumo.empty and 'id_equipe' in df_empresas_resumo.columns:
        empresas_daf = df_empresas_resumo[df_empresas_resumo['id_equipe']==id_equipe_selecionada]
        
        if not empresas_daf.empty:
            st.markdown("<div class='sub-header'>🏢 Empresas Acompanhadas</div>", unsafe_allow_html=True)
            
            col1, col2 = st.columns([2, 1])
            
            with col1:
                st.metric("Total de Empresas", f"{len(empresas_daf):,}")
            
            with col2:
                com_incons = len(empresas_daf[empresas_daf['flag_atualmente_em_malha']==1]) if 'flag_atualmente_em_malha' in empresas_daf.columns else 0
                st.metric("Com Inconsistências Ativas", f"{com_incons:,}")
            
            # Mostrar resumo básico
            st.info(f"📋 Mostrando resumo de {len(empresas_daf):,} empresas. Clique no botão abaixo para ver detalhes completos.")
            
            # BOTÃO PARA CARREGAR DETALHES
            if st.button("🔍 Ver Detalhes Completos das Empresas", type="secondary", key="btn_empresas_detalhadas"):
                engine = st.session_state.get('engine')
                
                if engine:
                    with st.spinner(f"Carregando dados completos das empresas da DAF {id_equipe_selecionada}..."):
                        empresas_detalhadas = carregar_empresas_daf(engine, id_equipe_selecionada)
                    
                    if not empresas_detalhadas.empty:
                        st.success(f"✅ {len(empresas_detalhadas):,} empresas carregadas com todos os detalhes")
                        
                        # Tabela de empresas COMPLETA
                        cols_empresa = ['nu_cnpj', 'nm_razao_social', 'nm_munic', 'de_classe', 
                                       'nu_cpf_cnpj_contador', 'nm_contador']
                        
                        if 'qtd_inconsistencias_ativas' in empresas_detalhadas.columns:
                            cols_empresa.append('qtd_inconsistencias_ativas')
                        if 'valor_inconsistencias_ativas' in empresas_detalhadas.columns:
                            cols_empresa.append('valor_inconsistencias_ativas')
                        if 'dt_ultima_inconsistencia' in empresas_detalhadas.columns:
                            cols_empresa.append('dt_ultima_inconsistencia')
                        
                        cols_display = [col for col in cols_empresa if col in empresas_detalhadas.columns]
                        
                        st.dataframe(
                            empresas_detalhadas[cols_display],
                            use_container_width=True,
                            height=600
                        )
                        
                        # Estatísticas adicionais
                        col1, col2, col3, col4 = st.columns(4)
                        
                        with col1:
                            if 'de_classe' in empresas_detalhadas.columns:
                                st.metric("Optantes Simples", 
                                         len(empresas_detalhadas[empresas_detalhadas['de_classe']=='SIMPLES NACIONAL']))
                        
                        with col2:
                            if 'de_classe' in empresas_detalhadas.columns:
                                st.metric("Regime Normal", 
                                         len(empresas_detalhadas[empresas_detalhadas['de_classe']=='REGIME NORMAL']))
                        
                        with col3:
                            if 'nu_cpf_cnpj_contador' in empresas_detalhadas.columns:
                                st.metric("Contadores Distintos", 
                                         empresas_detalhadas['nu_cpf_cnpj_contador'].nunique())
                        
                        with col4:
                            if 'nm_munic' in empresas_detalhadas.columns:
                                st.metric("Municípios", 
                                         empresas_detalhadas['nm_munic'].nunique())
                        
                        # Gráfico de distribuição por município
                        if 'nm_munic' in empresas_detalhadas.columns:
                            st.markdown("**📍 Distribuição por Município (Top 15)**")
                            
                            dist_munic = empresas_detalhadas['nm_munic'].value_counts().head(15).reset_index()
                            dist_munic.columns = ['Município', 'Quantidade']
                            
                            fig = px.bar(
                                dist_munic,
                                x='Quantidade',
                                y='Município',
                                orientation='h',
                                title='Top 15 Municípios',
                                template=filtros['tema']
                            )
                            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                            st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.warning("Nenhuma empresa encontrada.")
                else:
                    st.error("Engine não disponível.")
            else:
                # Mostrar apenas resumo básico (CNPJs)
                st.dataframe(
                    empresas_daf[['nu_cnpj', 'flag_atualmente_em_malha']].head(100),
                    use_container_width=True,
                    height=300
                )
                st.caption("💡 Mostrando apenas primeiras 100 empresas do resumo. Clique no botão acima para ver todos os detalhes.")
    
    st.divider()
    
    # Contadores da DAF
    if not empresas_daf.empty and not df_contadores.empty:
        st.markdown("<div class='sub-header'>👥 Contadores da DAF</div>", unsafe_allow_html=True)
        
        # Pegar CNPJs dos contadores das empresas desta DAF
        cnpjs_contadores = empresas_daf['nu_cpf_cnpj_contador'].dropna().unique()
        
        contadores_daf = df_contadores[df_contadores['nu_cpf_cnpj_contador'].isin(cnpjs_contadores)]
        
        if not contadores_daf.empty:
            st.metric("Total de Contadores", f"{len(contadores_daf):,}")
            
            # Top 20 contadores
            cols_contador = ['nm_contador', 'nu_crc_contador', 'qtd_clientes_total', 
                           'taxa_autonomia_pct', 'taxa_autuacao_pct', 'score_performance',
                           'classificacao_performance']
            
            cols_display = [col for col in cols_contador if col in contadores_daf.columns]
            
            st.dataframe(
                contadores_daf[cols_display].head(20).style.format({
                    'taxa_autonomia_pct': '{:.1f}%',
                    'taxa_autuacao_pct': '{:.1f}%',
                    'score_performance': '{:.1f}'
                }),
                use_container_width=True,
                height=400
            )
            
            # Gráfico de distribuição
            col1, col2 = st.columns(2)
            
            with col1:
                if 'classificacao_performance' in contadores_daf.columns:
                    dist = contadores_daf['classificacao_performance'].value_counts().reset_index()
                    dist.columns = ['Classificação', 'Quantidade']
                    
                    fig = px.pie(
                        dist,
                        values='Quantidade',
                        names='Classificação',
                        title='Distribuição de Performance dos Contadores',
                        template=filtros['tema'],
                        hole=0.4
                    )
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                if 'taxa_autonomia_pct' in contadores_daf.columns:
                    fig = px.box(
                        contadores_daf,
                        y='taxa_autonomia_pct',
                        title='Distribuição da Taxa de Autonomia',
                        template=filtros['tema']
                    )
                    fig.update_layout(showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Análise de Exclusões da DAF
    if not df_exclusoes.empty and 'id_equipe_auditor' in df_exclusoes.columns:
        exclusoes_daf = df_exclusoes[df_exclusoes['id_equipe_auditor']==id_equipe_selecionada]
        
        if not exclusoes_daf.empty:
            st.markdown("<div class='sub-header'>⚖️ Análise de Exclusões</div>", unsafe_allow_html=True)
            
            excl = exclusoes_daf.iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                qtd_total = excl.get('qtd_total_excluidas', 0)
                st.metric("Total Excluídas", f"{int(qtd_total):,}")
            
            with col2:
                taxa = excl.get('taxa_exclusao_geral_pct', 0)
                st.metric("Taxa Exclusão", f"{taxa:.1f}%")
            
            with col3:
                suspeitas_qtd = excl.get('qtd_exclusoes_suspeitas', 0)
                st.metric("Exclusões Suspeitas", f"{int(suspeitas_qtd):,}")
            
            with col4:
                score = excl.get('score_legitimidade', 0)
                st.metric("Score Legitimidade", f"{score:.1f}")
            
            # Status
            classificacao = excl.get('classificacao_exclusoes', 'N/A')
            
            if classificacao in ['ALTA_SUSPEITA', 'MEDIA_SUSPEITA']:
                st.markdown(
                    f"<div class='alert-critico'>"
                    f"<b>🚨 Atenção: Classificação {classificacao}</b><br>"
                    f"Esta equipe apresenta padrão suspeito de exclusões."
                    f"</div>",
                    unsafe_allow_html=True
                )
            elif classificacao == 'EXCLUSOES_LEGITIMAS':
                st.markdown(
                    "<div class='alert-positivo'>"
                    "<b>✅ Exclusões dentro do padrão esperado</b>"
                    "</div>",
                    unsafe_allow_html=True
                )
            else:
                st.info(f"ℹ️ Classificação: {classificacao}")
    
    st.divider()
    
    # Inconsistências da DAF - CARREGAR SOB DEMANDA
    st.markdown("<div class='sub-header'>📋 Inconsistências da DAF</div>", unsafe_allow_html=True)
    
    # Usar dados agregados primeiro
    df_incons_por_daf = dados.get('inconsistencias_por_daf', pd.DataFrame())
    
    if not df_incons_por_daf.empty and 'id_equipe' in df_incons_por_daf.columns:
        incons_resumo = df_incons_por_daf[df_incons_por_daf['id_equipe']==id_equipe_selecionada]
        
        if not incons_resumo.empty:
            resumo = incons_resumo.iloc[0]
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Total", f"{int(resumo['qtd_total']):,}")
            
            with col2:
                st.metric("Ativas", f"{int(resumo['qtd_ativas']):,}")
            
            with col3:
                st.metric("Resolvidas", f"{int(resumo['qtd_autonoma']):,}")
            
            with col4:
                st.metric("Valor Total", f"R$ {resumo['valor_total']/1e6:.2f}M")
            
            # Botão para carregar detalhes
            if st.button("🔍 Carregar Detalhes e Análises Temporais Completas", type="primary"):
                engine = st.session_state.get('engine')
                
                if engine:
                    with st.spinner(f"Carregando inconsistências detalhadas da DAF {id_equipe_selecionada}... (pode demorar)"):
                        incons_detalhadas = carregar_inconsistencias_daf(engine, id_equipe_selecionada)
                    
                    if not incons_detalhadas.empty:
                        st.success(f"✅ {len(incons_detalhadas):,} inconsistências carregadas")
                        
                        # ========== ANÁLISE POR CANAL ==========
                        st.markdown("### 📊 Distribuição por Canal de Resolução")
                        
                        if 'canal_resolucao' in incons_detalhadas.columns:
                            dist_canal = incons_detalhadas['canal_resolucao'].value_counts().reset_index()
                            dist_canal.columns = ['Canal', 'Quantidade']
                            dist_canal['Percentual'] = (dist_canal['Quantidade'] / len(incons_detalhadas) * 100).round(2)
                            
                            col1, col2 = st.columns([1, 2])
                            
                            with col1:
                                st.dataframe(
                                    dist_canal.style.format({'Percentual': '{:.2f}%'}),
                                    use_container_width=True
                                )
                            
                            with col2:
                                fig = px.pie(
                                    dist_canal,
                                    values='Quantidade',
                                    names='Canal',
                                    title='Distribuição por Canal (Detalhado)',
                                    template=filtros['tema'],
                                    hole=0.4,
                                    color_discrete_sequence=px.colors.qualitative.Set3
                                )
                                fig.update_traces(textposition='inside', textinfo='percent+label')
                                st.plotly_chart(fig, use_container_width=True)
                        
                        st.divider()
                        
                        # ========== EVOLUÇÃO TEMPORAL POR CANAL ==========
                        st.markdown("### 📈 Evolução Temporal por Canal de Resolução")
                        
                        if all(col in incons_detalhadas.columns for col in ['nu_per_ref', 'canal_resolucao']):
                            evolucao_canal = incons_detalhadas.groupby(['nu_per_ref', 'canal_resolucao']).size().reset_index()
                            evolucao_canal.columns = ['Periodo', 'Canal', 'Quantidade']
                            
                            fig = px.line(
                                evolucao_canal,
                                x='Periodo',
                                y='Quantidade',
                                color='Canal',
                                title='Evolução de Inconsistências por Canal ao Longo do Tempo',
                                template=filtros['tema'],
                                markers=True,
                                height=500
                            )
                            
                            fig.update_layout(
                                xaxis_title='Período',
                                yaxis_title='Quantidade de Inconsistências',
                                hovermode='x unified'
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Gráfico de área empilhada
                            pivot_canal = evolucao_canal.pivot(index='Periodo', columns='Canal', values='Quantidade').fillna(0)
                            
                            fig = go.Figure()
                            
                            for canal in pivot_canal.columns:
                                fig.add_trace(go.Scatter(
                                    x=pivot_canal.index,
                                    y=pivot_canal[canal],
                                    name=canal,
                                    mode='lines',
                                    stackgroup='one',
                                    groupnorm='percent'  # normalizado em %
                                ))
                            
                            fig.update_layout(
                                title='Evolução Percentual por Canal (Empilhado)',
                                template=filtros['tema'],
                                height=400,
                                xaxis_title='Período',
                                yaxis_title='Percentual (%)',
                                hovermode='x unified'
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                        
                        st.divider()
                        
                        # ========== ANÁLISE TEMPORAL: NOVAS, EXCLUÍDAS, ATIVAS ==========
                        st.markdown("### 📊 Análise Temporal: Novas, Excluídas e Ativas")
                        
                        if 'nu_per_ref' in incons_detalhadas.columns:
                            # Novas por período (dt_identificada ou nu_per_ref)
                            novas_periodo = incons_detalhadas.groupby('nu_per_ref').size().reset_index()
                            novas_periodo.columns = ['Periodo', 'Novas']
                            
                            # Excluídas por período
                            if 'flag_exclusao_auditor' in incons_detalhadas.columns:
                                excluidas_periodo = incons_detalhadas[
                                    incons_detalhadas['flag_exclusao_auditor'] == 1
                                ].groupby('nu_per_ref').size().reset_index()
                                excluidas_periodo.columns = ['Periodo', 'Excluídas']
                                
                                # Merge
                                evolucao_completa = novas_periodo.merge(excluidas_periodo, on='Periodo', how='left').fillna(0)
                            else:
                                evolucao_completa = novas_periodo.copy()
                                evolucao_completa['Excluídas'] = 0
                            
                            # Ativas (acumulado)
                            if 'canal_resolucao' in incons_detalhadas.columns:
                                ativas_periodo = incons_detalhadas[
                                    incons_detalhadas['canal_resolucao'] == 'ATIVA'
                                ].groupby('nu_per_ref').size().reset_index()
                                ativas_periodo.columns = ['Periodo', 'Ativas']
                                
                                evolucao_completa = evolucao_completa.merge(ativas_periodo, on='Periodo', how='left').fillna(0)
                            else:
                                evolucao_completa['Ativas'] = 0
                            
                            # Gráfico combinado
                            fig = go.Figure()
                            
                            # Barras: Novas
                            fig.add_trace(go.Bar(
                                x=evolucao_completa['Periodo'],
                                y=evolucao_completa['Novas'],
                                name='Novas',
                                marker_color='#3498db'
                            ))
                            
                            # Barras: Excluídas
                            fig.add_trace(go.Bar(
                                x=evolucao_completa['Periodo'],
                                y=evolucao_completa['Excluídas'],
                                name='Excluídas',
                                marker_color='#e74c3c'
                            ))
                            
                            # Linha: Ativas
                            fig.add_trace(go.Scatter(
                                x=evolucao_completa['Periodo'],
                                y=evolucao_completa['Ativas'],
                                name='Ativas',
                                mode='lines+markers',
                                line=dict(color='#f39c12', width=3),
                                marker=dict(size=10),
                                yaxis='y2'
                            ))
                            
                            fig.update_layout(
                                title='Evolução: Novas, Excluídas e Ativas',
                                template=filtros['tema'],
                                height=500,
                                xaxis_title='Período',
                                yaxis_title='Quantidade (Novas e Excluídas)',
                                yaxis2=dict(
                                    title='Quantidade Ativas',
                                    overlaying='y',
                                    side='right'
                                ),
                                hovermode='x unified',
                                barmode='group'
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Estatísticas
                            col1, col2, col3, col4 = st.columns(4)
                            
                            with col1:
                                media_novas = evolucao_completa['Novas'].mean()
                                st.metric("Média Novas/Mês", f"{media_novas:.0f}")
                            
                            with col2:
                                media_excluidas = evolucao_completa['Excluídas'].mean()
                                st.metric("Média Excluídas/Mês", f"{media_excluidas:.0f}")
                            
                            with col3:
                                media_ativas = evolucao_completa['Ativas'].mean()
                                st.metric("Média Ativas/Mês", f"{media_ativas:.0f}")
                            
                            with col4:
                                if len(evolucao_completa) > 1:
                                    tendencia = ((evolucao_completa['Ativas'].iloc[-1] / 
                                                 evolucao_completa['Ativas'].iloc[0]) - 1) * 100 if evolucao_completa['Ativas'].iloc[0] > 0 else 0
                                    st.metric("Tendência Ativas", f"{tendencia:+.1f}%")
                        
                        st.divider()
                        
                        # ========== DISTRIBUIÇÃO POR NATUREZA ==========
                        st.markdown("### 🎯 Análise por Natureza e Tipo")
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            if 'natureza_inconsistencia' in incons_detalhadas.columns:
                                dist_nat = incons_detalhadas['natureza_inconsistencia'].value_counts().reset_index()
                                dist_nat.columns = ['Natureza', 'Quantidade']
                                
                                fig = px.bar(
                                    dist_nat,
                                    x='Natureza',
                                    y='Quantidade',
                                    title='Distribuição por Natureza',
                                    template=filtros['tema'],
                                    color='Natureza'
                                )
                                st.plotly_chart(fig, use_container_width=True)
                        
                        with col2:
                            if 'cd_inconsistencia' in incons_detalhadas.columns:
                                top_tipos = incons_detalhadas['cd_inconsistencia'].value_counts().head(10).reset_index()
                                top_tipos.columns = ['Código', 'Quantidade']
                                
                                fig = px.bar(
                                    top_tipos,
                                    y='Código',
                                    x='Quantidade',
                                    orientation='h',
                                    title='Top 10 Tipos',
                                    template=filtros['tema']
                                )
                                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                                st.plotly_chart(fig, use_container_width=True)
                        
                        st.divider()
                        
                        # ========== TABELA RESUMIDA ==========
                        st.markdown("### 📋 Amostra dos Dados (Top 1000)")
                        
                        cols_tabela = ['nu_cnpj', 'cd_inconsistencia', 'nm_inconsistencia',
                                      'nu_per_ref', 'vl_inconsistencia', 'canal_resolucao', 'dias_na_malha']
                        
                        cols_existentes = [col for col in cols_tabela if col in incons_detalhadas.columns]
                        
                        format_dict = {}
                        if 'vl_inconsistencia' in cols_existentes:
                            format_dict['vl_inconsistencia'] = 'R$ {:,.2f}'
                        
                        st.dataframe(
                            incons_detalhadas[cols_existentes].head(1000).style.format(format_dict),
                            use_container_width=True,
                            height=400
                        )
                    else:
                        st.warning("Nenhuma inconsistência detalhada encontrada.")
                else:
                    st.error("Engine não disponível.")
            else:
                st.info("👆 Clique no botão acima para carregar análises detalhadas e temporais completas")
        else:
            st.warning("Dados agregados não encontrados para esta DAF.")
    else:
        st.warning("Dados de inconsistências por DAF não disponíveis.")

def pagina_drill_down_inconsistencias(dados, filtros):
    """Drill-down detalhado por tipo de inconsistência."""
    st.markdown("<h1 class='main-header'>🔎 Drill-Down: Análise de Inconsistências</h1>", unsafe_allow_html=True)
    
    df_catalogo = dados.get('catalogo_tipos', pd.DataFrame())
    df_agregado = dados.get('inconsistencias_agregadas', pd.DataFrame())
    
    if df_catalogo.empty or df_agregado.empty:
        st.error("Dados de inconsistências não disponíveis.")
        return
    
    st.markdown("""
    <div class='info-box'>
    <b>🔍 Análise Detalhada por Tipo de Inconsistência</b><br>
    Explore cada tipo de malha fiscal: volume, efetividade, valor e padrões de resolução.
    </div>
    """, unsafe_allow_html=True)
    
    # ========== VISÃO GERAL ==========
    st.markdown("<div class='sub-header'>📊 Visão Geral</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_tipos = df_catalogo['cd_inconsistencia'].nunique()
        st.metric("📋 Total de Tipos", f"{total_tipos}")
    
    with col2:
        tipos_ativos = len(df_catalogo[df_catalogo.get('flag_tipo_ativo', 1) == 1])
        st.metric("✅ Tipos Ativos", f"{tipos_ativos}")
    
    with col3:
        total_incons = df_agregado['qtd_total'].sum()
        st.metric("📊 Total Inconsistências", f"{int(total_incons):,}")
    
    with col4:
        empresas_afetadas = df_agregado['qtd_empresas'].sum()
        st.metric("🏢 Empresas Afetadas", f"{int(empresas_afetadas):,}")
    
    with col5:
        valor_total = df_agregado['valor_total'].sum()
        st.metric("💰 Valor Total", f"R$ {valor_total/1e9:.2f}B")
    
    st.divider()
    
    # ========== ANÁLISE POR NATUREZA ==========
    st.markdown("<div class='sub-header'>🎯 Análise por Natureza de Inconsistência</div>", unsafe_allow_html=True)
    
    if 'natureza_inconsistencia' in df_agregado.columns:
        natureza_stats = df_agregado.groupby('natureza_inconsistencia').agg({
            'cd_inconsistencia': 'nunique',
            'qtd_empresas': 'sum',
            'qtd_total': 'sum',
            'qtd_autonoma': 'sum',
            'qtd_exclusao': 'sum',
            'qtd_infracao': 'sum',
            'valor_total': 'sum'
        }).reset_index()
        
        natureza_stats.columns = ['Natureza', 'Tipos', 'Empresas', 'Total', 
                                  'Autônomas', 'Exclusões', 'Autuações', 'Valor']
        
        # Calcular taxas
        natureza_stats['Taxa Autonomia %'] = (natureza_stats['Autônomas'] / natureza_stats['Total'] * 100).round(2)
        natureza_stats['Taxa Exclusão %'] = (natureza_stats['Exclusões'] / natureza_stats['Total'] * 100).round(2)
        natureza_stats['Taxa Autuação %'] = (natureza_stats['Autuações'] / natureza_stats['Total'] * 100).round(2)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(
                natureza_stats,
                x='Natureza',
                y='Total',
                title='Volume de Inconsistências por Natureza',
                template=filtros['tema'],
                color='Natureza',
                text='Total'
            )
            fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
            st.plotly_chart(fig, use_container_width=True, key="drill_nat_volume")
        
        with col2:
            fig = px.bar(
                natureza_stats,
                x='Natureza',
                y=['Taxa Autonomia %', 'Taxa Exclusão %', 'Taxa Autuação %'],
                title='Taxas de Resolução por Natureza',
                template=filtros['tema'],
                barmode='group'
            )
            st.plotly_chart(fig, use_container_width=True, key="drill_nat_taxas")
        
        # Tabela resumo
        st.dataframe(
            natureza_stats.style.format({
                'Total': '{:,.0f}',
                'Empresas': '{:,.0f}',
                'Valor': 'R$ {:,.2f}',
                'Taxa Autonomia %': '{:.2f}%',
                'Taxa Exclusão %': '{:.2f}%',
                'Taxa Autuação %': '{:.2f}%'
            }).background_gradient(subset=['Taxa Autonomia %'], cmap='Greens'),
            use_container_width=True
        )
    
    st.divider()
    
    # ========== ANÁLISE POR GRAVIDADE ==========
    st.markdown("<div class='sub-header'>⚠️ Análise por Gravidade Presumida</div>", unsafe_allow_html=True)
    
    if 'gravidade_presumida' in df_agregado.columns:
        gravidade_stats = df_agregado.groupby('gravidade_presumida').agg({
            'cd_inconsistencia': 'nunique',
            'qtd_empresas': 'sum',
            'qtd_total': 'sum',
            'qtd_infracao': 'sum',
            'valor_total': 'sum'
        }).reset_index()
        
        gravidade_stats.columns = ['Gravidade', 'Tipos', 'Empresas', 'Total', 'Autuações', 'Valor']
        gravidade_stats['Taxa Autuação %'] = (gravidade_stats['Autuações'] / gravidade_stats['Total'] * 100).round(2)
        
        # Ordenar por gravidade
        ordem_gravidade = {'ALTA': 1, 'MEDIA': 2, 'BAIXA': 3}
        gravidade_stats['ordem'] = gravidade_stats['Gravidade'].map(ordem_gravidade)
        gravidade_stats = gravidade_stats.sort_values('ordem').drop('ordem', axis=1)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.pie(
                gravidade_stats,
                values='Total',
                names='Gravidade',
                title='Distribuição por Gravidade',
                template=filtros['tema'],
                color='Gravidade',
                color_discrete_map={
                    'ALTA': '#ef4444',
                    'MEDIA': '#fbbf24',
                    'BAIXA': '#10b981'
                },
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True, key="drill_grav_dist")
        
        with col2:
            fig = px.bar(
                gravidade_stats,
                x='Gravidade',
                y='Taxa Autuação %',
                title='Taxa de Autuação por Gravidade',
                template=filtros['tema'],
                color='Gravidade',
                color_discrete_map={
                    'ALTA': '#ef4444',
                    'MEDIA': '#fbbf24',
                    'BAIXA': '#10b981'
                }
            )
            st.plotly_chart(fig, use_container_width=True, key="drill_grav_aut")
        
        st.dataframe(
            gravidade_stats.style.format({
                'Total': '{:,.0f}',
                'Empresas': '{:,.0f}',
                'Valor': 'R$ {:,.2f}',
                'Taxa Autuação %': '{:.2f}%'
            }),
            use_container_width=True
        )
    
    st.divider()
    
    # ========== TOP 20 TIPOS ==========
    st.markdown("<div class='sub-header'>🏆 Top 20 Tipos com Mais Inconsistências</div>", unsafe_allow_html=True)
    
    top_20 = df_agregado.nlargest(20, 'qtd_total').copy()
    
    # Calcular taxas
    top_20['taxa_autonomia'] = (top_20['qtd_autonoma'] / top_20['qtd_total'] * 100).round(2)
    top_20['taxa_exclusao'] = (top_20['qtd_exclusao'] / top_20['qtd_total'] * 100).round(2)
    top_20['taxa_autuacao'] = (top_20['qtd_infracao'] / top_20['qtd_total'] * 100).round(2)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = px.bar(
            top_20,
            y='cd_inconsistencia',
            x='qtd_total',
            orientation='h',
            title='Top 20 por Volume',
            template=filtros['tema'],
            color='natureza_inconsistencia',
            hover_data=['nm_inconsistencia']
        )
        fig.update_layout(yaxis={'categoryorder': 'total ascending'}, showlegend=False)
        st.plotly_chart(fig, use_container_width=True, key="drill_top20_vol")
    
    with col2:
        fig = px.scatter(
            top_20,
            x='taxa_autonomia',
            y='taxa_autuacao',
            size='qtd_total',
            color='gravidade_presumida',
            hover_name='cd_inconsistencia',
            hover_data=['nm_inconsistencia', 'qtd_empresas'],
            title='Autonomia vs Autuação (tamanho = volume)',
            template=filtros['tema'],
            color_discrete_map={
                'ALTA': '#ef4444',
                'MEDIA': '#fbbf24',
                'BAIXA': '#10b981'
            }
        )
        fig.add_hline(y=10, line_dash="dash", line_color="red", annotation_text="Meta Autuação")
        fig.add_vline(x=60, line_dash="dash", line_color="green", annotation_text="Meta Autonomia")
        st.plotly_chart(fig, use_container_width=True, key="drill_top20_scatter")
    
    # Tabela Top 20
    cols_display = ['cd_inconsistencia', 'nm_inconsistencia', 'natureza_inconsistencia',
                    'qtd_empresas', 'qtd_total', 'taxa_autonomia', 'taxa_exclusao', 'taxa_autuacao']
    
    st.dataframe(
        top_20[cols_display].style.format({
            'qtd_empresas': '{:,.0f}',
            'qtd_total': '{:,.0f}',
            'taxa_autonomia': '{:.2f}%',
            'taxa_exclusao': '{:.2f}%',
            'taxa_autuacao': '{:.2f}%'
        }).background_gradient(subset=['taxa_autonomia'], cmap='RdYlGn'),
        use_container_width=True,
        height=500
    )
    
    st.divider()
    
    # ========== SELEÇÃO DE TIPO ESPECÍFICO ==========
    st.markdown("<div class='sub-header'>🔍 Análise Detalhada de Tipo Específico</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Agregar por tipo (soma de todos os canais)
        tipos_agregados = df_agregado.groupby(['cd_inconsistencia', 'nm_inconsistencia']).agg({
            'qtd_total': 'sum',
            'qtd_empresas': 'sum',
            'qtd_autonoma': 'sum',
            'qtd_exclusao': 'sum',
            'qtd_infracao': 'sum',
            'valor_total': 'sum'
        }).reset_index()
        
        # Lista de tipos ordenada por volume (top 100)
        tipos_lista = tipos_agregados.nlargest(100, 'qtd_total')
        
        tipo_selecionado = st.selectbox(
            "Selecione um tipo de inconsistência:",
            tipos_lista['cd_inconsistencia'].tolist(),
            format_func=lambda x: f"{x} - {tipos_lista[tipos_lista['cd_inconsistencia']==x]['nm_inconsistencia'].iloc[0]} ({int(tipos_lista[tipos_lista['cd_inconsistencia']==x]['qtd_total'].iloc[0]):,} inconsistências)"
        )
    
    if tipo_selecionado:
        # Buscar dados do tipo (agregado)
        tipo_data = tipos_agregados[tipos_agregados['cd_inconsistencia']==tipo_selecionado]
        
        if not tipo_data.empty:
            tipo_info = tipo_data.iloc[0]
            
            # Buscar informações adicionais do catálogo
            if not df_catalogo.empty:
                info_catalogo = df_catalogo[df_catalogo['cd_inconsistencia']==tipo_selecionado]
                if not info_catalogo.empty:
                    tipo_info_catalogo = info_catalogo.iloc[0]
                    # Adicionar campos do catálogo que não estão no agregado
                    tipo_info = pd.concat([tipo_info, tipo_info_catalogo[['natureza_inconsistencia', 'gravidade_presumida']]])
            
            st.markdown(f"### 📋 {tipo_selecionado} - {tipo_info['nm_inconsistencia']}")
            
            # KPIs do tipo
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Empresas Afetadas", f"{int(tipo_info['qtd_empresas']):,}")
            
            with col2:
                st.metric("Total Inconsistências", f"{int(tipo_info['qtd_total']):,}")
            
            with col3:
                taxa_aut = (tipo_info['qtd_autonoma'] / tipo_info['qtd_total'] * 100)
                st.metric("Taxa Autonomia", f"{taxa_aut:.1f}%", 
                         delta=f"{taxa_aut-60:.1f}pp" if taxa_aut > 60 else None)
            
            with col4:
                taxa_exc = (tipo_info['qtd_exclusao'] / tipo_info['qtd_total'] * 100)
                st.metric("Taxa Exclusão", f"{taxa_exc:.1f}%",
                         delta=f"{30-taxa_exc:.1f}pp" if taxa_exc < 30 else None,
                         delta_color="inverse")
            
            with col5:
                taxa_aut_calc = (tipo_info['qtd_infracao'] / tipo_info['qtd_total'] * 100)
                st.metric("Taxa Autuação", f"{taxa_aut_calc:.1f}%")
            
            # Características
            col1, col2, col3 = st.columns(3)
            
            with col1:
                natureza = 'N/A'
                if not df_catalogo.empty:
                    info_cat = df_catalogo[df_catalogo['cd_inconsistencia']==tipo_selecionado]
                    if not info_cat.empty:
                        natureza = info_cat.iloc[0].get('natureza_inconsistencia', 'N/A')
                st.info(f"**Natureza:** {natureza}")
            
            with col2:
                gravidade = 'N/A'
                if not df_catalogo.empty:
                    info_cat = df_catalogo[df_catalogo['cd_inconsistencia']==tipo_selecionado]
                    if not info_cat.empty:
                        gravidade = info_cat.iloc[0].get('gravidade_presumida', 'N/A')
                cor_gravidade = {'ALTA': '🔴', 'MEDIA': '🟡', 'BAIXA': '🟢'}.get(gravidade, '⚪')
                st.info(f"**Gravidade:** {cor_gravidade} {gravidade}")
            
            with col3:
                # Identificar canal predominante
                tipo_canais = df_agregado[df_agregado['cd_inconsistencia']==tipo_selecionado]
                if not tipo_canais.empty:
                    canal_principal = tipo_canais.nlargest(1, 'qtd_total')['canal_resolucao'].iloc[0] if len(tipo_canais) > 0 else 'N/A'
                else:
                    canal_principal = 'N/A'
                st.info(f"**Canal Principal:** {canal_principal}")
            
            # Distribuição de resoluções
            st.markdown("**📊 Distribuição de Resoluções:**")
            
            resolucoes = {
                'Autônomas': int(tipo_info['qtd_autonoma']),
                'Exclusões': int(tipo_info['qtd_exclusao']),
                'Autuações': int(tipo_info['qtd_infracao']),
                'Outras': int(tipo_info['qtd_total'] - tipo_info['qtd_autonoma'] - tipo_info['qtd_exclusao'] - tipo_info['qtd_infracao'])
            }
            
            fig = px.pie(
                values=list(resolucoes.values()),
                names=list(resolucoes.keys()),
                title=f'Como são resolvidas as inconsistências {tipo_selecionado}',
                template=filtros['tema'],
                hole=0.4,
                color_discrete_sequence=['#10b981', '#ef4444', '#3b82f6', '#6b7280']
            )
            st.plotly_chart(fig, use_container_width=True, key="drill_tipo_resolucoes")
            
            # Alertas e recomendações
            if taxa_exc > 40:
                st.markdown(
                    f"<div class='alert-critico'>"
                    f"<b>🚨 ALERTA: Taxa de Exclusão Crítica ({taxa_exc:.1f}%)</b><br>"
                    f"Este tipo apresenta exclusão excessiva. Recomenda-se:<br>"
                    f"• Revisar legitimidade das exclusões<br>"
                    f"• Treinar auditores sobre este tipo<br>"
                    f"• Investigar padrões de exclusão"
                    f"</div>",
                    unsafe_allow_html=True
                )
            elif taxa_aut < 40:
                st.markdown(
                    f"<div class='alert-alto'>"
                    f"<b>⚠️ ATENÇÃO: Baixa Taxa de Autonomia ({taxa_aut:.1f}%)</b><br>"
                    f"Este tipo tem baixa resolução autônoma. Recomenda-se:<br>"
                    f"• Simplificar processo de regularização<br>"
                    f"• Capacitar contadores<br>"
                    f"• Revisar complexidade da exigência"
                    f"</div>",
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    "<div class='alert-positivo'>"
                    "<b>✅ Tipo com performance adequada</b><br>"
                    "Taxas de resolução dentro dos padrões esperados."
                    "</div>",
                    unsafe_allow_html=True
                )
            
            # Botão para carregar dados detalhados
            if st.button("📥 Carregar Análise Temporal Detalhada", type="secondary", key="btn_temporal_tipo"):
                engine = st.session_state.get('engine')
                
                if engine:
                    with st.spinner(f"Carregando dados temporais do tipo {tipo_selecionado}..."):
                        query = f"""
                            SELECT 
                                nu_per_ref as periodo,
                                canal_resolucao,
                                COUNT(*) as quantidade,
                                SUM(CASE WHEN flag_tem_valor_fiscal = 1 THEN vl_inconsistencia ELSE 0 END) as valor_total
                            FROM niat.mlh_inconsistencias_detalhadas
                            WHERE cd_inconsistencia = {tipo_selecionado}
                            GROUP BY nu_per_ref, canal_resolucao
                            ORDER BY nu_per_ref, canal_resolucao
                        """
                        
                        try:
                            df_temporal = pd.read_sql(query, engine)
                            df_temporal.columns = [col.lower() for col in df_temporal.columns]
                            
                            if not df_temporal.empty:
                                st.success(f"✅ {len(df_temporal):,} registros carregados")
                                
                                # Gráfico de evolução temporal
                                fig = px.line(
                                    df_temporal,
                                    x='periodo',
                                    y='quantidade',
                                    color='canal_resolucao',
                                    title=f'Evolução Temporal - {tipo_selecionado}',
                                    template=filtros['tema'],
                                    markers=True
                                )
                                st.plotly_chart(fig, use_container_width=True, key="drill_tipo_temporal")
                                
                                # Tabela de dados temporais
                                st.dataframe(
                                    df_temporal.style.format({
                                        'quantidade': '{:,.0f}',
                                        'valor_total': 'R$ {:,.2f}'
                                    }),
                                    use_container_width=True
                                )
                            else:
                                st.warning("Nenhum dado temporal encontrado.")
                        except Exception as e:
                            st.error(f"Erro ao carregar dados temporais: {str(e)[:200]}")
                else:
                    st.error("Engine não disponível.")

def pagina_machine_learning(dados, filtros):
    """Sistema de Machine Learning."""
    st.markdown("<h1 class='main-header'>🤖 Sistema de Machine Learning</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    <div class='info-box'>
    <b>📊 Sistema de Predição de Exclusões Suspeitas</b><br>
    Este módulo utiliza algoritmos de Machine Learning para identificar padrões e prever 
    a probabilidade de exclusões suspeitas com base em características históricas.
    </div>
    """, unsafe_allow_html=True)
    
    # Obter engine
    engine = st.session_state.get('engine')
    if not engine:
        st.error("❌ Engine de conexão não disponível.")
        return
    
    # Preparar dados
    st.markdown("<div class='sub-header'>⚙️ Configuração do Modelo</div>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        algoritmo = st.selectbox(
            "Algoritmo:",
            ['Random Forest', 'Gradient Boosting'],
            index=0
        )
    
    with col2:
        test_size = st.slider("% Teste:", 10, 40, 30, 5)
    
    with col3:
        threshold = st.slider("Threshold:", 0.3, 0.7, 0.5, 0.05)
    
    if st.button("🚀 Treinar Modelo", type="primary"):
        
        with st.spinner("Preparando dados..."):
            resultado = preparar_dados_ml(dados, engine)
        
        if resultado[0] is None:
            st.error("❌ Dados insuficientes para treinar o modelo.")
            return
        
        X_train, X_test, y_train, y_test = resultado
        
        with st.spinner(f"Treinando {algoritmo}..."):
            if algoritmo == 'Random Forest':
                modelo = RandomForestClassifier(
                    n_estimators=100,
                    max_depth=10,
                    random_state=42,
                    n_jobs=-1
                )
            else:
                modelo = GradientBoostingClassifier(
                    n_estimators=100,
                    max_depth=5,
                    random_state=42
                )
            
            modelo.fit(X_train, y_train)
            y_pred = modelo.predict(X_test)
            y_proba = modelo.predict_proba(X_test)[:, 1]
        
        st.success("✅ Modelo treinado com sucesso!")
        
        # Métricas
        st.markdown("<div class='sub-header'>📊 Performance do Modelo</div>", unsafe_allow_html=True)
        
        col1, col2, col3, col4 = st.columns(4)
        
        from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
        
        with col1:
            acc = accuracy_score(y_test, y_pred)
            st.metric("Acurácia", f"{acc:.2%}")
        
        with col2:
            prec = precision_score(y_test, y_pred)
            st.metric("Precisão", f"{prec:.2%}")
        
        with col3:
            rec = recall_score(y_test, y_pred)
            st.metric("Recall", f"{rec:.2%}")
        
        with col4:
            f1 = f1_score(y_test, y_pred)
            st.metric("F1-Score", f"{f1:.2%}")
        
        # Matriz de confusão
        col1, col2 = st.columns(2)
        
        with col1:
            cm = confusion_matrix(y_test, y_pred)
            
            fig = px.imshow(
                cm,
                labels=dict(x="Predito", y="Real", color="Quantidade"),
                x=['Não Exclusão', 'Exclusão'],
                y=['Não Exclusão', 'Exclusão'],
                title='Matriz de Confusão',
                template=filtros['tema'],
                color_continuous_scale='Blues',
                text_auto=True
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Curva ROC
            from sklearn.metrics import roc_curve
            
            fpr, tpr, _ = roc_curve(y_test, y_proba)
            auc = roc_auc_score(y_test, y_proba)
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=fpr,
                y=tpr,
                mode='lines',
                name=f'ROC (AUC={auc:.3f})',
                line=dict(color='#e74c3c', width=3)
            ))
            
            fig.add_trace(go.Scatter(
                x=[0, 1],
                y=[0, 1],
                mode='lines',
                name='Random',
                line=dict(color='gray', width=1, dash='dash')
            ))
            
            fig.update_layout(
                title='Curva ROC',
                xaxis_title='Taxa de Falsos Positivos',
                yaxis_title='Taxa de Verdadeiros Positivos',
                template=filtros['tema'],
                showlegend=True
            )
            
            st.plotly_chart(fig, use_container_width=True)
        
        # Feature importance
        if hasattr(modelo, 'feature_importances_'):
            st.markdown("<div class='sub-header'>🎯 Importância das Features</div>", unsafe_allow_html=True)
            
            # Carregar dataset ML novamente para pegar as features
            df_ml = carregar_dataset_ml(engine)
            
            features = [
                'taxa_exclusao_esperada_pct', 'taxa_autuacao_esperada_pct',
                'taxa_autonomia_esperada_pct', 'score_efetividade_tipo',
                'facilidade_num', 'legitimidade_num', 'natureza_num',
                'regime_normal', 'simples_nacional', 'qtd_tipos_inconsistencia_historico',
                'contador_taxa_autonomia', 'contador_taxa_autuacao', 'contador_score',
                'log_valor', 'dias_malha'
            ]
            
            features_disp = [f for f in features if f in df_ml.columns]
            
            if len(features_disp) == len(modelo.feature_importances_):
                importances = pd.DataFrame({
                    'Feature': features_disp,
                    'Importância': modelo.feature_importances_
                }).sort_values('Importância', ascending=False)
                
                fig = px.bar(
                    importances.head(15),
                    x='Importância',
                    y='Feature',
                    orientation='h',
                    title='Top 15 Features Mais Importantes',
                    template=filtros['tema'],
                    color='Importância',
                    color_continuous_scale='Viridis'
                )
                
                st.plotly_chart(fig, use_container_width=True)

def pagina_analise_temporal(dados, filtros):
    """Análise temporal completa com todas as métricas."""
    st.markdown("<h1 class='main-header'>📈 Análise Temporal Completa</h1>", unsafe_allow_html=True)
    
    df_evolucao = dados.get('evolucao_mensal', pd.DataFrame())
    
    if df_evolucao.empty:
        st.error("Dados de evolução temporal não disponíveis.")
        return
    
    # APLICAR FILTROS DE PERÍODO
    df_evolucao = aplicar_filtros(df_evolucao, filtros)
    
    if df_evolucao.empty:
        st.warning("⚠️ Nenhum dado encontrado com os filtros aplicados.")
        return
    
    # Mostrar filtros ativos
    mostrar_filtros_ativos(filtros, suporta_periodo=True)
    
    # ========== RESUMO DO PERÍODO ==========
    st.markdown("<div class='sub-header'>📅 Período Analisado</div>", unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Meses Analisados", f"{len(df_evolucao)}")
    
    with col2:
        periodo_inicio = df_evolucao['periodo'].min()
        st.metric("Período Inicial", f"{periodo_inicio}")
    
    with col3:
        periodo_fim = df_evolucao['periodo'].max()
        st.metric("Período Final", f"{periodo_fim}")
    
    with col4:
        total_incons = df_evolucao['qtd_inconsistencias_identificadas'].sum()
        st.metric("Total Inconsistências", f"{int(total_incons):,}")
    
    with col5:
        media_mes = df_evolucao['qtd_inconsistencias_identificadas'].mean()
        st.metric("Média Mensal", f"{int(media_mes):,}")
    
    st.divider()
    
    # ========== EVOLUÇÃO DAS TAXAS PRINCIPAIS ==========
    st.markdown("<div class='sub-header'>📊 Evolução das Taxas de Resolução</div>", unsafe_allow_html=True)
    
    fig = go.Figure()
    
    # Taxa Autonomia
    fig.add_trace(go.Scatter(
        x=df_evolucao['periodo'],
        y=df_evolucao['taxa_autonomia_pct'],
        name='Taxa Autonomia',
        mode='lines+markers',
        line=dict(color='#2ecc71', width=3),
        marker=dict(size=8),
        fill='tozeroy',
        fillcolor='rgba(46, 204, 113, 0.2)'
    ))
    
    # Taxa Exclusão
    fig.add_trace(go.Scatter(
        x=df_evolucao['periodo'],
        y=df_evolucao['taxa_exclusao_pct'],
        name='Taxa Exclusão',
        mode='lines+markers',
        line=dict(color='#e74c3c', width=3),
        marker=dict(size=8),
        fill='tozeroy',
        fillcolor='rgba(231, 76, 60, 0.2)'
    ))
    
    # Taxa Autuação
    fig.add_trace(go.Scatter(
        x=df_evolucao['periodo'],
        y=df_evolucao['taxa_autuacao_pct'],
        name='Taxa Autuação',
        mode='lines+markers',
        line=dict(color='#3498db', width=3),
        marker=dict(size=8),
        fill='tozeroy',
        fillcolor='rgba(52, 152, 219, 0.2)'
    ))
    
    # Linhas de referência
    fig.add_hline(y=60, line_dash="dash", line_color="green", annotation_text="Meta Autonomia (60%)")
    fig.add_hline(y=30, line_dash="dash", line_color="red", annotation_text="Limite Exclusão (30%)")
    
    fig.update_layout(
        title='Evolução das Taxas ao Longo do Tempo',
        template=filtros['tema'],
        height=500,
        xaxis_title='Período',
        yaxis_title='Taxa (%)',
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # ========== VOLUME DE INCONSISTÊNCIAS ==========
    st.markdown("<div class='sub-header'>📊 Volume de Inconsistências por Período</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=df_evolucao['periodo'],
            y=df_evolucao['qtd_inconsistencias_identificadas'],
            name='Identificadas',
            marker_color='#9b59b6'
        ))
        
        if 'qtd_ativas' in df_evolucao.columns:
            fig.add_trace(go.Scatter(
                x=df_evolucao['periodo'],
                y=df_evolucao['qtd_ativas'],
                name='Ativas',
                mode='lines+markers',
                line=dict(color='#e74c3c', width=3),
                yaxis='y2'
            ))
        
        fig.update_layout(
            title='Inconsistências Identificadas vs Ativas',
            template=filtros['tema'],
            height=400,
            xaxis_title='Período',
            yaxis_title='Quantidade Identificadas',
            yaxis2=dict(title='Quantidade Ativas', overlaying='y', side='right'),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Gráfico de área empilhada
        if all(col in df_evolucao.columns for col in ['qtd_resolvidas_dde', 'qtd_resolvidas_malha_auto', 'qtd_excluidas_auditor']):
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=df_evolucao['periodo'],
                y=df_evolucao['qtd_resolvidas_dde'],
                name='Resolvidas DDE',
                mode='lines',
                stackgroup='one',
                fillcolor='rgba(46, 204, 113, 0.7)'
            ))
            
            fig.add_trace(go.Scatter(
                x=df_evolucao['periodo'],
                y=df_evolucao['qtd_resolvidas_malha_auto'],
                name='Resolvidas Malha Auto',
                mode='lines',
                stackgroup='one',
                fillcolor='rgba(39, 174, 96, 0.7)'
            ))
            
            fig.add_trace(go.Scatter(
                x=df_evolucao['periodo'],
                y=df_evolucao['qtd_excluidas_auditor'],
                name='Excluídas Auditor',
                mode='lines',
                stackgroup='one',
                fillcolor='rgba(231, 76, 60, 0.7)'
            ))
            
            fig.update_layout(
                title='Composição da Resolução (Empilhado)',
                template=filtros['tema'],
                height=400,
                xaxis_title='Período',
                yaxis_title='Quantidade',
                hovermode='x unified'
            )
            
            st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # ========== TEMPO MÉDIO NA MALHA ==========
    st.markdown("<div class='sub-header'>⏱️ Tempo Médio de Permanência</div>", unsafe_allow_html=True)
    
    if 'media_dias_malha' in df_evolucao.columns:
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df_evolucao['periodo'],
            y=df_evolucao['media_dias_malha'],
            name='Dias Médios na Malha',
            mode='lines+markers',
            line=dict(color='#f39c12', width=3),
            marker=dict(size=10)
        ))
        
        # Média geral
        media_geral = df_evolucao['media_dias_malha'].mean()
        fig.add_hline(y=media_geral, line_dash="dash", line_color="gray", 
                     annotation_text=f"Média Geral: {media_geral:.0f} dias")
        
        fig.update_layout(
            title='Evolução do Tempo Médio na Malha',
            template=filtros['tema'],
            height=400,
            xaxis_title='Período',
            yaxis_title='Dias',
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # ========== ANÁLISE POR NATUREZA ==========
    st.markdown("<div class='sub-header'>🎯 Evolução por Natureza de Inconsistência</div>", unsafe_allow_html=True)
    
    if all(col in df_evolucao.columns for col in ['qtd_omissao', 'qtd_credito_indevido', 'qtd_divergencia']):
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df_evolucao['periodo'],
            y=df_evolucao['qtd_omissao'],
            name='Omissão',
            mode='lines+markers',
            line=dict(width=2)
        ))
        
        fig.add_trace(go.Scatter(
            x=df_evolucao['periodo'],
            y=df_evolucao['qtd_credito_indevido'],
            name='Crédito Indevido',
            mode='lines+markers',
            line=dict(width=2)
        ))
        
        fig.add_trace(go.Scatter(
            x=df_evolucao['periodo'],
            y=df_evolucao['qtd_divergencia'],
            name='Divergência',
            mode='lines+markers',
            line=dict(width=2)
        ))
        
        fig.update_layout(
            title='Evolução por Natureza',
            template=filtros['tema'],
            height=400,
            xaxis_title='Período',
            yaxis_title='Quantidade',
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # ========== TENDÊNCIAS E PREVISÕES ==========
    st.markdown("<div class='sub-header'>📈 Análise de Tendências</div>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Variação da taxa de autonomia
        if len(df_evolucao) > 1:
            var_autonomia = df_evolucao['taxa_autonomia_pct'].iloc[-1] - df_evolucao['taxa_autonomia_pct'].iloc[0]
            st.metric(
                "Variação Autonomia",
                f"{df_evolucao['taxa_autonomia_pct'].iloc[-1]:.1f}%",
                delta=f"{var_autonomia:+.1f}pp",
                help="Comparação último vs primeiro período"
            )
    
    with col2:
        # Variação da taxa de exclusão
        if len(df_evolucao) > 1:
            var_exclusao = df_evolucao['taxa_exclusao_pct'].iloc[-1] - df_evolucao['taxa_exclusao_pct'].iloc[0]
            st.metric(
                "Variação Exclusão",
                f"{df_evolucao['taxa_exclusao_pct'].iloc[-1]:.1f}%",
                delta=f"{var_exclusao:+.1f}pp",
                delta_color="inverse",
                help="Comparação último vs primeiro período"
            )
    
    with col3:
        # Tendência de volume
        if len(df_evolucao) > 1:
            var_volume = ((df_evolucao['qtd_inconsistencias_identificadas'].iloc[-1] / 
                          df_evolucao['qtd_inconsistencias_identificadas'].iloc[0]) - 1) * 100
            st.metric(
                "Variação Volume",
                f"{int(df_evolucao['qtd_inconsistencias_identificadas'].iloc[-1]):,}",
                delta=f"{var_volume:+.1f}%",
                help="Comparação último vs primeiro período"
            )
    
    # Heatmap de correlação
    st.markdown("**🔥 Mapa de Calor: Correlação entre Métricas**")
    
    cols_numericas = df_evolucao.select_dtypes(include=[np.number]).columns.tolist()
    cols_interesse = [col for col in cols_numericas if col != 'periodo'][:10]  # Top 10 métricas
    
    if len(cols_interesse) > 1:
        corr_matrix = df_evolucao[cols_interesse].corr()
        
        fig = px.imshow(
            corr_matrix,
            labels=dict(color="Correlação"),
            x=corr_matrix.columns,
            y=corr_matrix.columns,
            color_continuous_scale='RdBu_r',
            aspect="auto",
            title='Correlação entre Métricas Temporais',
            template=filtros['tema']
        )
        
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

def pagina_analise_multidimensional(dados, filtros):
    """Análise multidimensional detalhada das DAFs."""
    st.markdown("<h1 class='main-header'>🔬 Análise Multidimensional de DAFs</h1>", unsafe_allow_html=True)
    
    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    
    if df_dafs.empty:
        st.error("Dados de DAFs não disponíveis.")
        return
    
    st.markdown("""
    <div class='info-box'>
    <b>📊 Análise dos 4 Indicadores de Performance</b><br>
    Sistema de avaliação multidimensional baseado em: Autonomia, Pendência, Exclusão e Autuação.
    </div>
    """, unsafe_allow_html=True)
    
    # ========== SELEÇÃO DE DAFs ==========
    st.markdown("<div class='sub-header'>🔍 Seleção de DAFs para Análise</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Multiselect para escolher DAFs
        dafs_disponiveis = sorted(df_dafs['id_equipe'].tolist())
        dafs_selecionadas = st.multiselect(
            "Selecione DAFs para comparar (até 5):",
            dafs_disponiveis,
            default=dafs_disponiveis[:3] if len(dafs_disponiveis) >= 3 else dafs_disponiveis,
            max_selections=5
        )
    
    with col2:
        mostrar_media = st.checkbox("Mostrar Média SC", value=True)
    
    if not dafs_selecionadas:
        st.warning("👆 Selecione pelo menos uma DAF para análise.")
        return
    
    # Filtrar DAFs selecionadas
    df_dafs_sel = df_dafs[df_dafs['id_equipe'].isin(dafs_selecionadas)]
    
    st.divider()
    
    # ========== RADAR CHART COMPARATIVO ==========
    st.markdown("<div class='sub-header'>📡 Radar Chart - Comparação Multidimensional</div>", unsafe_allow_html=True)
    
    fig_radar = criar_radar_chart_daf(df_dafs_sel, mostrar_media=mostrar_media)
    fig_radar.update_layout(template=filtros['tema'])
    st.plotly_chart(fig_radar, use_container_width=True)
    
    st.divider()
    
    # ========== CARDS DOS 4 INDICADORES ==========
    st.markdown("<div class='sub-header'>📊 Indicadores Detalhados por DAF</div>", unsafe_allow_html=True)
    
    for _, daf_row in df_dafs_sel.iterrows():
        st.markdown(f"### 🏢 DAF {daf_row['id_equipe']}")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            criar_card_metrica(
                "🎯 Autonomia",
                daf_row.get('score_autonomia', 0),
                daf_row.get('ind_autonomia_nivel', 'N/A'),
                "🎯"
            )
        
        with col2:
            criar_card_metrica(
                "🚨 Fiscalização",
                daf_row.get('ind_fiscalizacao_pct', 0),  # CORRIGIDO - usar taxa, não score
                daf_row.get('ind_fiscalizacao_nivel', 'N/A'),
                "🚨",
                invertido=True
            )
        
        with col3:
            criar_card_metrica(
                "🗑️ Exclusão",
                daf_row.get('score_exclusao', 0),
                daf_row.get('ind_exclusao_nivel', 'N/A'),
                "🗑️",
                invertido=True
            )
        
        with col4:
            criar_card_metrica(
                "⚖️ Autuação",
                daf_row.get('score_autuacao', 0),
                daf_row.get('ind_autuacao_nivel', 'N/A'),
                "⚖️"
            )
        
        # Score Geral e Alertas
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "📊 Score Geral",
                f"{daf_row.get('score_geral_ponderado', 0):.1f}",
                help="Score ponderado dos 4 indicadores"
            )
        
        with col2:
            st.metric(
                "🏷️ Classificação",
                daf_row.get('classificacao_geral', 'N/A')
            )
        
        with col3:
            total_alertas = sum([
                daf_row.get('flag_alerta_autonomia_critica', 0),
                daf_row.get('flag_alerta_pendencia_alta', 0),
                daf_row.get('flag_alerta_exclusao_alta', 0),
                daf_row.get('flag_alerta_autuacao_alta', 0)
            ])
            st.metric("🚨 Alertas Ativos", int(total_alertas))
        
        st.markdown("---")
    
    st.divider()
    
    # ========== HEATMAP DOS 4 INDICADORES ==========
    st.markdown("<div class='sub-header'>🔥 Heatmap - Todos os Indicadores</div>", unsafe_allow_html=True)
    
    # Preparar matriz - CORRIGIDO
    df_heatmap = df_dafs[['id_equipe', 'score_autonomia', 'score_fiscalizacao', 
                           'score_exclusao', 'score_autuacao']].set_index('id_equipe')
    
    fig = px.imshow(
        df_heatmap.T,
        labels=dict(x="DAF", y="Indicador", color="Score"),
        x=df_heatmap.index.astype(str),
        y=['Autonomia', 'Fiscalização', 'Exclusão', 'Autuação'],
        color_continuous_scale='RdYlGn',
        aspect="auto",
        title='Heatmap de Scores - Todas as DAFs',
        template=filtros['tema']
    )
    
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # ========== TABELA COMPARATIVA ==========
    st.markdown("<div class='sub-header'>📋 Tabela Comparativa</div>", unsafe_allow_html=True)
    
    cols_tabela = ['id_equipe', 'score_autonomia', 'score_fiscalizacao', 'score_exclusao', 
                   'score_autuacao', 'score_geral_ponderado', 'classificacao_geral']
    
    st.dataframe(
        df_dafs_sel[cols_tabela].style.format({
            'score_autonomia': '{:.1f}',
            'score_fiscalizacao': '{:.1f}',
            'score_exclusao': '{:.1f}',
            'score_autuacao': '{:.1f}',
            'score_geral_ponderado': '{:.1f}'
        }).background_gradient(subset=['score_geral_ponderado'], cmap='RdYlGn'),
        use_container_width=True,
        height=400
    )

def pagina_indicador_autonomia(dados, filtros):
    """Análise detalhada do indicador de autonomia."""
    st.markdown("<h1 class='main-header'>🎯 Indicador de Autonomia</h1>", unsafe_allow_html=True)
    
    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    
    if df_dafs.empty:
        st.error("Dados de DAFs não disponíveis.")
        return
    
    st.markdown("""
    <div class='info-box'>
    <b>🎯 Taxa de Autonomia</b><br>
    Percentual de inconsistências resolvidas autonomamente pelos contribuintes, 
    sem necessidade de intervenção de auditores.
    </div>
    """, unsafe_allow_html=True)
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        media_autonomia = df_dafs['score_autonomia'].mean()
        st.metric("📊 Score Médio", f"{media_autonomia:.1f}")
    
    with col2:
        excelentes = len(df_dafs[df_dafs.get('ind_autonomia_nivel', '') == 'EXCELENTE'])
        st.metric("🌟 Excelentes", f"{excelentes}")
    
    with col3:
        criticas = df_dafs.get('flag_alerta_autonomia_baixa', pd.Series([0])).sum()
        st.metric("🚨 Alertas Críticos", f"{int(criticas)}")
    
    with col4:
        mediana = df_dafs['score_autonomia'].median()
        st.metric("📈 Mediana", f"{mediana:.1f}")
    
    st.divider()
    
    # Distribuição por níveis
    st.markdown("<div class='sub-header'>📊 Distribuição por Níveis</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'ind_autonomia_nivel' in df_dafs.columns:
            dist_nivel = df_dafs['ind_autonomia_nivel'].value_counts().reset_index()
            dist_nivel.columns = ['Nível', 'Quantidade']
            
            fig = px.bar(
                dist_nivel,
                x='Nível',
                y='Quantidade',
                title='Distribuição por Nível de Autonomia',
                template=filtros['tema'],
                color='Nível',
                color_discrete_map={
                    'EXCELENTE': '#10b981',
                    'ALTO': '#84cc16',
                    'BOM': '#84cc16',
                    'MEDIO': '#fbbf24',
                    'BAIXO': '#f97316',
                    'CRITICO': '#ef4444'
                }
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.histogram(
            df_dafs,
            x='score_autonomia',
            nbins=20,
            title='Histograma de Scores de Autonomia',
            template=filtros['tema'],
            color_discrete_sequence=['#10b981']
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Ranking
    st.markdown("<div class='sub-header'>🏆 Ranking de DAFs por Autonomia</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        ordem = st.radio("Ordem:", ['Melhores', 'Piores'], index=0)
        top_n = st.slider("Mostrar top:", 5, 50, 20, 5)
    
    if ordem == 'Melhores':
        df_rank = df_dafs.nlargest(top_n, 'score_autonomia')
    else:
        df_rank = df_dafs.nsmallest(top_n, 'score_autonomia')
    
    cols_display = ['id_equipe', 'score_autonomia', 'ind_autonomia_nivel', 
                    'taxa_autonomia_pct', 'flag_alerta_autonomia_baixa']
    
    st.dataframe(
        df_rank[cols_display].style.format({
            'score_autonomia': '{:.1f}',
            'taxa_autonomia_pct': '{:.2f}%'
        }).background_gradient(subset=['score_autonomia'], cmap='RdYlGn'),
        use_container_width=True,
        height=500
    )

def pagina_indicador_pendencia(dados, filtros):
    """Análise detalhada do indicador de pendência (ATIVAS)."""
    st.markdown("<h1 class='main-header'>⏳ Indicador de Ativas (Pendência)</h1>", unsafe_allow_html=True)
    
    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    
    if df_dafs.empty:
        st.error("Dados de DAFs não disponíveis.")
        return
    
    st.markdown("""
    <div class='info-box'>
    <b>⏳ Taxa de Ativas (Pendência)</b><br>
    Percentual de inconsistências que estão aguardando regularização no prazo normal. 
    <b>Situação NEUTRA</b> - não é problema, é parte do fluxo. Apenas monitorar se está muito alta 
    (pode indicar lentidão no processamento).
    </div>
    """, unsafe_allow_html=True)
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        media_ativas = df_dafs['ind_ativas_pct'].mean()
        st.metric("📊 Taxa Média Ativas", f"{media_ativas:.1f}%")
    
    with col2:
        normais = len(df_dafs[df_dafs.get('ind_ativas_nivel', '') == 'NORMAL'])
        st.metric("✅ Normais", f"{normais}")
    
    with col3:
        alto_volume = len(df_dafs[df_dafs.get('ind_ativas_nivel', '') == 'ALTO_VOLUME'])
        st.metric("🚨 Alto Volume", f"{int(alto_volume)}")
    
    with col4:
        mediana = df_dafs['ind_ativas_pct'].median()
        st.metric("📈 Mediana", f"{mediana:.1f}%")
    
    st.divider()
    
    # Distribuição
    col1, col2 = st.columns(2)
    
    with col1:
        if 'ind_ativas_nivel' in df_dafs.columns:
            dist_nivel = df_dafs['ind_ativas_nivel'].value_counts().reset_index()
            dist_nivel.columns = ['Nível', 'Quantidade']
            
            fig = px.pie(
                dist_nivel,
                values='Quantidade',
                names='Nível',
                title='Distribuição por Nível de Ativas',
                template=filtros['tema'],
                color='Nível',
                color_discrete_map={
                    'NORMAL': '#10b981',
                    'ATENCAO_VOLUME': '#fbbf24',
                    'ALTO_VOLUME': '#ef4444'
                },
                hole=0.4
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.box(
            df_dafs,
            y='ind_ativas_pct',
            title='Distribuição de Taxa de Ativas',
            template=filtros['tema'],
            color_discrete_sequence=['#3498db']
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # DAFs com volume alto de ativas
    if alto_volume > 0:
        st.markdown("<div class='sub-header'>🚨 DAFs com Alto Volume de Ativas</div>", unsafe_allow_html=True)
        
        df_alertas = df_dafs[df_dafs['ind_ativas_nivel'] == 'ALTO_VOLUME'].sort_values('ind_ativas_pct', ascending=False)
        
        st.markdown(
            f"<div class='alert-critico'>"
            f"<b>⚠️ {len(df_alertas)} DAFs com volume crítico de ativas!</b><br>"
            f"Necessitam atenção para redução de backlog."
            f"</div>",
            unsafe_allow_html=True
        )
        
        cols_display = ['id_equipe', 'ind_ativas_pct', 'ind_ativas_nivel', 
                       'taxa_ativas_pct', 'qtd_ativas']
        
        # Filtrar apenas colunas existentes
        cols_existentes = [col for col in cols_display if col in df_alertas.columns]
        
        format_dict = {}
        if 'ind_ativas_pct' in cols_existentes:
            format_dict['ind_ativas_pct'] = '{:.2f}%'
        if 'taxa_ativas_pct' in cols_existentes:
            format_dict['taxa_ativas_pct'] = '{:.2f}%'
        if 'qtd_ativas' in cols_existentes:
            format_dict['qtd_ativas'] = '{:,.0f}'
        
        st.dataframe(
            df_alertas[cols_existentes].style.format(format_dict),
            use_container_width=True
        )
    else:
        st.markdown(
            "<div class='alert-positivo'>"
            "<b>✅ Nenhuma DAF com volume crítico de ativas!</b>"
            "</div>",
            unsafe_allow_html=True
        )
    
    st.divider()
    
    # Ranking
    st.markdown("<div class='sub-header'>🏆 Ranking por Taxa de Ativas</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([1, 3])
    
    with col1:
        ordem = st.radio("Ordem:", ['Menores (Melhor)', 'Maiores'], index=0, key="radio_ranking_ativas")
    
    if ordem == 'Menores (Melhor)':
        df_rank = df_dafs.nsmallest(20, 'ind_ativas_pct')
    else:
        df_rank = df_dafs.nlargest(20, 'ind_ativas_pct')
    
    cols_display = ['id_equipe', 'ind_ativas_pct', 'ind_ativas_nivel', 
                   'taxa_ativas_pct', 'qtd_ativas', 'qtd_total_inconsistencias']
    
    cols_existentes = [col for col in cols_display if col in df_rank.columns]
    
    format_dict = {}
    if 'ind_ativas_pct' in cols_existentes:
        format_dict['ind_ativas_pct'] = '{:.2f}%'
    if 'taxa_ativas_pct' in cols_existentes:
        format_dict['taxa_ativas_pct'] = '{:.2f}%'
    if 'qtd_ativas' in cols_existentes:
        format_dict['qtd_ativas'] = '{:,.0f}'
    if 'qtd_total_inconsistencias' in cols_existentes:
        format_dict['qtd_total_inconsistencias'] = '{:,.0f}'
    
    st.dataframe(
        df_rank[cols_existentes].style.format(format_dict).background_gradient(
            subset=['ind_ativas_pct'], cmap='RdYlGn_r'
        ),
        use_container_width=True,
        height=500
    )

def pagina_indicador_exclusao(dados, filtros):
    """Análise detalhada do indicador de exclusão."""
    st.markdown("<h1 class='main-header'>🗑️ Indicador de Exclusão</h1>", unsafe_allow_html=True)
    
    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    
    if df_dafs.empty:
        st.error("Dados de DAFs não disponíveis.")
        return
    
    st.markdown("""
    <div class='info-box'>
    <b>🗑️ Taxa de Exclusão</b><br>
    Percentual de inconsistências excluídas por auditores. 
    Valores altos podem indicar exclusões indevidas.
    </div>
    """, unsafe_allow_html=True)
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        media_ativas = df_dafs['ind_ativas_pct'].mean()
        st.metric("📊 Taxa Média", f"{media_ativas:.1f}%")
    
    with col2:
        baixas = len(df_dafs[df_dafs['ind_ativas_nivel'] == 'NORMAL'])
        st.metric("✅ Normais", f"{baixas}")
    
    with col3:
        alertas = len(df_dafs[df_dafs['ind_ativas_nivel'] == 'ALTO_VOLUME'])
        st.metric("🚨 Alto Volume", f"{int(alertas)}")
    
    with col4:
        mediana = df_dafs['ind_ativas_pct'].median()
        st.metric("📈 Mediana", f"{mediana:.1f}%")
    
    st.divider()
    
    # DAFs com exclusão alta
    if alertas > 0:
        st.markdown("<div class='sub-header'>🚨 DAFs com Exclusão Alta</div>", unsafe_allow_html=True)
        
        df_alertas = df_dafs[df_dafs['flag_alerta_exclusao_alta'] == 1].sort_values('score_exclusao')
        
        st.markdown(
            f"<div class='alert-critico'>"
            f"<b>⚠️ {len(df_alertas)} DAFs com exclusão suspeita!</b><br>"
            f"Necessitam revisão de legitimidade das exclusões."
            f"</div>",
            unsafe_allow_html=True
        )
        
        cols_display = ['id_equipe', 'score_exclusao', 'ind_exclusao_nivel', 'taxa_exclusao_pct']
        
        st.dataframe(
            df_alertas[cols_display].style.format({
                'score_exclusao': '{:.1f}',
                'taxa_exclusao_pct': '{:.2f}%'
            }),
            use_container_width=True
        )
    
    # Ranking
    st.markdown("<div class='sub-header'>🏆 Ranking por Exclusão</div>", unsafe_allow_html=True)
    
    df_rank = df_dafs.nlargest(20, 'score_exclusao')
    
    cols_display = ['id_equipe', 'score_exclusao', 'ind_exclusao_nivel', 'taxa_exclusao_pct']
    
    st.dataframe(
        df_rank[cols_display].style.format({
            'score_exclusao': '{:.1f}',
            'taxa_exclusao_pct': '{:.2f}%'
        }).background_gradient(subset=['score_exclusao'], cmap='RdYlGn'),
        use_container_width=True,
        height=500
    )

def pagina_indicador_fiscalizacao(dados, filtros):
    """Análise detalhada do indicador de fiscalização."""
    st.markdown("<h1 class='main-header'>🚨 Indicador de Fiscalização</h1>", unsafe_allow_html=True)
    
    # CORREÇÃO: Buscar da tabela performance_dafs, não ranking_dafs
    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    
    if df_dafs.empty:
        st.error("Dados de DAFs não disponíveis.")
        return
    
    st.markdown("""
    <div class='info-box'>
    <b>🚨 Taxa de Necessidade de Fiscalização</b><br>
    Percentual de inconsistências que NÃO foram regularizadas no prazo e precisaram de 
    Procedimento Administrativo de Fiscalização (PAF). <b>Quanto MENOR, melhor</b> - 
    indica que os contribuintes estão regularizando no prazo ativo.<br><br>
    
    <b>⚖️ Diferença entre Fiscalização e Autuação:</b><br>
    • <b>Fiscalização</b> = Abertura de PAF (não regularizou no prazo)<br>
    • <b>Autuação</b> = Gerou infração após fiscalização (mais grave)<br><br>
    
    <b>💡 Meta:</b> Taxa de Fiscalização ≤ 20% (80% resolvem no prazo ativo)
    </div>
    """, unsafe_allow_html=True)
    
    # KPIs
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        media_fiscalizacao = df_dafs['taxa_necessidade_fiscalizacao'].mean()
        st.metric("📊 Taxa Média Fiscalização", f"{media_fiscalizacao:.2f}%")
    
    with col2:
        excelentes = len(df_dafs[df_dafs['ind_fiscalizacao_nivel'] == 'EXCELENTE'])
        st.metric("🌟 Excelentes", f"{excelentes}")
    
    with col3:
        criticos = df_dafs['flag_alerta_fiscalizacao_alta'].sum()
        st.metric("🚨 Críticos", f"{int(criticos)}")
    
    with col4:
        mediana = df_dafs['taxa_necessidade_fiscalizacao'].median()
        st.metric("📈 Mediana", f"{mediana:.2f}%")
    
    st.divider()
    
    # Distribuição
    col1, col2 = st.columns(2)
    
    with col1:
        dist_nivel = df_dafs['ind_fiscalizacao_nivel'].value_counts().reset_index()
        dist_nivel.columns = ['Nível', 'Quantidade']
        
        fig = px.pie(
            dist_nivel,
            values='Quantidade',
            names='Nível',
            title='Distribuição por Nível de Fiscalização',
            template=filtros['tema'],
            color='Nível',
            color_discrete_map={
                'EXCELENTE': '#10b981',
                'BOM': '#84cc16',
                'REGULAR': '#fbbf24',
                'ALTO': '#f97316',
                'CRITICO': '#ef4444'
            },
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        fig = px.histogram(
            df_dafs,
            x='taxa_necessidade_fiscalizacao',
            nbins=30,
            title='Distribuição da Taxa de Fiscalização',
            template=filtros['tema'],
            color_discrete_sequence=['#ef4444']
        )
        fig.update_layout(
            xaxis_title='Taxa de Fiscalização (%)',
            yaxis_title='Quantidade de DAFs',
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Estatísticas gerais
    st.markdown("<div class='sub-header'>📊 Estatísticas por Nível</div>", unsafe_allow_html=True)
    
    stats_nivel = df_dafs.groupby('ind_fiscalizacao_nivel').agg({
        'id_equipe': 'count',
        'taxa_necessidade_fiscalizacao': ['mean', 'min', 'max'],
        'taxa_autonomia_pct': 'mean',
        'score_geral_ponderado': 'mean'
    }).round(2)
    
    stats_nivel.columns = ['Qtd DAFs', 'Taxa Média', 'Taxa Mín', 'Taxa Máx', 'Autonomia Média', 'Score Médio']
    stats_nivel = stats_nivel.reset_index()
    stats_nivel.columns = ['Nível', 'Qtd DAFs', 'Taxa Média (%)', 'Taxa Mín (%)', 'Taxa Máx (%)', 'Autonomia Média (%)', 'Score Médio']
    
    st.dataframe(stats_nivel, use_container_width=True)
    
    st.divider()
    
    # DAFs com fiscalização alta
    if criticos > 0:
        st.markdown("<div class='sub-header'>🚨 DAFs com Alerta de Fiscalização</div>", unsafe_allow_html=True)
        
        df_alertas = df_dafs[df_dafs['flag_alerta_fiscalizacao_alta'] == 1].sort_values(
            'taxa_necessidade_fiscalizacao', ascending=False
        )
        
        st.markdown(
            f"<div class='alert-critico'>"
            f"<b>⚠️ {len(df_alertas)} DAFs com taxa de fiscalização crítica (>40%)!</b><br>"
            f"Muitas inconsistências precisando de PAF - indicam baixa regularização no prazo."
            f"</div>",
            unsafe_allow_html=True
        )
        
        cols_display = ['id_equipe', 'taxa_necessidade_fiscalizacao', 'ind_fiscalizacao_nivel', 
                       'taxa_autonomia_pct', 'taxa_exclusao_pct', 'qtd_total_inconsistencias',
                       'qtd_em_fiscalizacao', 'qtd_fiscalizacao_total']
        
        st.dataframe(
            df_alertas[cols_display].style.format({
                'taxa_necessidade_fiscalizacao': '{:.2f}%',
                'taxa_autonomia_pct': '{:.2f}%',
                'taxa_exclusao_pct': '{:.2f}%',
                'qtd_total_inconsistencias': '{:,.0f}',
                'qtd_em_fiscalizacao': '{:,.0f}',
                'qtd_fiscalizacao_total': '{:,.0f}'
            }).background_gradient(subset=['taxa_necessidade_fiscalizacao'], cmap='Reds'),
            use_container_width=True,
            height=400
        )
    else:
        st.markdown(
            "<div class='alert-positivo'>"
            "<b>✅ Nenhuma DAF com fiscalização crítica!</b>"
            "</div>",
            unsafe_allow_html=True
        )
    
    st.divider()
    
    # Ranking - Melhores (menor fiscalização)
    st.markdown("<div class='sub-header'>🏆 Top 20 - Menor Taxa de Fiscalização</div>", unsafe_allow_html=True)
    
    df_rank_melhores = df_dafs.nsmallest(20, 'taxa_necessidade_fiscalizacao')
    
    cols_display = ['id_equipe', 'taxa_necessidade_fiscalizacao', 'ind_fiscalizacao_nivel', 
                   'taxa_autonomia_pct', 'score_fiscalizacao', 'score_geral_ponderado', 
                   'classificacao_geral']
    
    st.dataframe(
        df_rank_melhores[cols_display].style.format({
            'taxa_necessidade_fiscalizacao': '{:.2f}%',
            'taxa_autonomia_pct': '{:.2f}%',
            'score_fiscalizacao': '{:.1f}',
            'score_geral_ponderado': '{:.1f}'
        }).background_gradient(subset=['score_fiscalizacao'], cmap='Greens'),
        use_container_width=True,
        height=500
    )
    
    st.divider()
    
    # Ranking - Piores (maior fiscalização)
    st.markdown("<div class='sub-header'>⚠️ Top 20 - Maior Taxa de Fiscalização</div>", unsafe_allow_html=True)
    
    df_rank_piores = df_dafs.nlargest(20, 'taxa_necessidade_fiscalizacao')
    
    st.dataframe(
        df_rank_piores[cols_display].style.format({
            'taxa_necessidade_fiscalizacao': '{:.2f}%',
            'taxa_autonomia_pct': '{:.2f}%',
            'score_fiscalizacao': '{:.1f}',
            'score_geral_ponderado': '{:.1f}'
        }).background_gradient(subset=['taxa_necessidade_fiscalizacao'], cmap='Reds'),
        use_container_width=True,
        height=500
    )
    
    st.divider()
    
    # Análise correlacional
    st.markdown("<div class='sub-header'>📊 Análise Correlacional</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Scatter: Fiscalização vs Autonomia
        fig = px.scatter(
            df_dafs,
            x='taxa_autonomia_pct',
            y='taxa_necessidade_fiscalizacao',
            size='qtd_total_inconsistencias',
            color='ind_fiscalizacao_nivel',
            title='Fiscalização vs Autonomia',
            template=filtros['tema'],
            labels={
                'taxa_autonomia_pct': 'Taxa de Autonomia (%)',
                'taxa_necessidade_fiscalizacao': 'Taxa de Fiscalização (%)'
            },
            hover_data=['id_equipe', 'classificacao_geral'],
            color_discrete_map={
                'EXCELENTE': '#10b981',
                'BOM': '#84cc16',
                'REGULAR': '#fbbf24',
                'ALTO': '#f97316',
                'CRITICO': '#ef4444'
            }
        )
        fig.add_hline(y=20, line_dash="dash", line_color="orange", annotation_text="Meta: 20%")
        fig.add_vline(x=60, line_dash="dash", line_color="green", annotation_text="Meta: 60%")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Scatter: Fiscalização vs Exclusão
        fig = px.scatter(
            df_dafs,
            x='taxa_exclusao_pct',
            y='taxa_necessidade_fiscalizacao',
            size='qtd_total_inconsistencias',
            color='ind_fiscalizacao_nivel',
            title='Fiscalização vs Exclusão',
            template=filtros['tema'],
            labels={
                'taxa_exclusao_pct': 'Taxa de Exclusão (%)',
                'taxa_necessidade_fiscalizacao': 'Taxa de Fiscalização (%)'
            },
            hover_data=['id_equipe', 'classificacao_geral'],
            color_discrete_map={
                'EXCELENTE': '#10b981',
                'BOM': '#84cc16',
                'REGULAR': '#fbbf24',
                'ALTO': '#f97316',
                'CRITICO': '#ef4444'
            }
        )
        fig.add_hline(y=20, line_dash="dash", line_color="orange", annotation_text="Meta: 20%")
        fig.add_vline(x=30, line_dash="dash", line_color="red", annotation_text="Limite: 30%")
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # Box plot comparativo
    st.markdown("<div class='sub-header'>📦 Distribuição por Classificação Geral</div>", unsafe_allow_html=True)
    
    fig = px.box(
        df_dafs,
        x='classificacao_geral',
        y='taxa_necessidade_fiscalizacao',
        color='classificacao_geral',
        title='Taxa de Fiscalização por Classificação Geral da DAF',
        template=filtros['tema'],
        labels={
            'classificacao_geral': 'Classificação Geral',
            'taxa_necessidade_fiscalizacao': 'Taxa de Fiscalização (%)'
        }
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

def pagina_alertas(dados, filtros):
    """Dashboard consolidado de alertas."""
    st.markdown("<h1 class='main-header'>🚨 Dashboard de Alertas</h1>", unsafe_allow_html=True)
    
    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    
    if df_dafs.empty:
        st.error("Dados de DAFs não disponíveis.")
        return
    
    # Calcular total de alertas
    df_dafs['total_alertas'] = (
        df_dafs.get('flag_alerta_autonomia_critica', 0) +
        df_dafs.get('flag_alerta_pendencia_alta', 0) +
        df_dafs.get('flag_alerta_exclusao_alta', 0) +
        df_dafs.get('flag_alerta_autuacao_alta', 0)
    )
    
    # KPIs de alertas
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_dafs_alerta = len(df_dafs[df_dafs['total_alertas'] > 0])
        st.metric("🚨 DAFs com Alertas", f"{total_dafs_alerta}")
    
    with col2:
        alertas_autonomia = df_dafs.get('flag_alerta_autonomia_critica', pd.Series([0])).sum()
        st.metric("🎯 Autonomia Crítica", f"{int(alertas_autonomia)}")
    
    with col3:
        alertas_pendencia = df_dafs.get('flag_alerta_pendencia_alta', pd.Series([0])).sum()
        st.metric("⏳ Pendência Alta", f"{int(alertas_pendencia)}")
    
    with col4:
        alertas_exclusao = df_dafs.get('flag_alerta_exclusao_alta', pd.Series([0])).sum()
        st.metric("🗑️ Exclusão Alta", f"{int(alertas_exclusao)}")
    
    st.divider()
    
    # Gráfico de alertas
    st.markdown("<div class='sub-header'>📊 Distribuição de Alertas</div>", unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        alertas_dist = {
            'Autonomia Crítica': int(alertas_autonomia),
            'Pendência Alta': int(alertas_pendencia),
            'Exclusão Alta': int(alertas_exclusao),
            'Autuação Alta': int(df_dafs.get('flag_alerta_autuacao_alta', pd.Series([0])).sum())
        }
        
        fig = px.bar(
            x=list(alertas_dist.keys()),
            y=list(alertas_dist.values()),
            title='Quantidade de Alertas por Tipo',
            template=filtros['tema'],
            labels={'x': 'Tipo de Alerta', 'y': 'Quantidade'},
            color=list(alertas_dist.keys()),
            color_discrete_sequence=['#ef4444', '#f97316', '#fbbf24', '#fb923c']
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        dist_total = df_dafs['total_alertas'].value_counts().sort_index().reset_index()
        dist_total.columns = ['Nº de Alertas', 'Quantidade de DAFs']
        
        fig = px.pie(
            dist_total,
            values='Quantidade de DAFs',
            names='Nº de Alertas',
            title='DAFs por Número de Alertas',
            template=filtros['tema'],
            hole=0.4
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    # DAFs com múltiplos alertas
    st.markdown("<div class='sub-header'>🔴 DAFs com Múltiplos Alertas</div>", unsafe_allow_html=True)
    
    df_multiplos = df_dafs[df_dafs['total_alertas'] >= 2].sort_values('total_alertas', ascending=False)
    
    if len(df_multiplos) > 0:
        st.markdown(
            f"<div class='alert-critico'>"
            f"<b>⚠️ {len(df_multiplos)} DAFs com 2 ou mais alertas!</b><br>"
            f"Necessitam intervenção prioritária."
            f"</div>",
            unsafe_allow_html=True
        )
        
        for _, row in df_multiplos.head(10).iterrows():
            alertas_ativos = []
            if row.get('flag_alerta_autonomia_baixa', 0):
                alertas_ativos.append('🎯 Autonomia Baixa')
            if row.get('flag_alerta_fiscalizacao_alta', 0):
                alertas_ativos.append('🚨 Fiscalização Alta')
            if row.get('flag_alerta_exclusao_alta', 0):
                alertas_ativos.append('🗑️ Exclusão Alta')
            if row.get('flag_alerta_autuacao_alta', 0):
                alertas_ativos.append('⚖️ Autuação Alta')
            
            with st.expander(f"🔴 DAF {row['id_equipe']} - {int(row['total_alertas'])} alertas"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Score Geral", f"{row.get('score_geral_ponderado', 0):.1f}")
                
                with col2:
                    st.metric("Classificação", row.get('classificacao_geral', 'N/A'))
                
                with col3:
                    st.metric("Inconsistências", f"{int(row.get('qtd_total_inconsistencias', 0)):,}")
                
                st.markdown(f"**Alertas Ativos:** {', '.join(alertas_ativos)}")
    else:
        st.markdown(
            "<div class='alert-positivo'>"
            "<b>✅ Nenhuma DAF com múltiplos alertas!</b>"
            "</div>",
            unsafe_allow_html=True
        )
    
    st.divider()
    
    # Tabela completa de DAFs com alertas
    st.markdown("<div class='sub-header'>📋 Todas as DAFs com Alertas</div>", unsafe_allow_html=True)
    
    df_com_alertas = df_dafs[df_dafs['total_alertas'] > 0].sort_values('total_alertas', ascending=False)
    
    cols_display = ['id_equipe', 'total_alertas', 'score_geral_ponderado', 'classificacao_geral',
                    'flag_alerta_autonomia_baixa', 'flag_alerta_fiscalizacao_alta',
                    'flag_alerta_exclusao_alta', 'flag_alerta_autuacao_alta']
    
    st.dataframe(
        df_com_alertas[cols_display].style.format({
            'score_geral_ponderado': '{:.1f}'
        }),
        use_container_width=True,
        height=500
    )

def pagina_sobre(dados, filtros):
    """Informações sobre o sistema."""
    st.markdown("<h1 class='main-header'>ℹ️ Sobre o Sistema</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    ## Sistema de Análise de DAFs - V2.0
    
    ### 📝 Descrição
    
    O **Sistema DAF V2.0** é uma plataforma desenvolvida pela Receita Estadual de Santa Catarina 
    para monitoramento, análise e gestão inteligente das malhas fiscais de ICMS.
    
    ### ✨ Principais Funcionalidades
    
    #### 1. Dashboard Executivo
    - Visão consolidada do sistema
    - KPIs principais em tempo real
    - Análises temporais e distributivas
    
    #### 2. Análise de Tipos de Inconsistência
    - 45 tipos diferentes de malhas fiscais
    - Benchmark e scoring de efetividade
    - Identificação de tipos problemáticos
    
    #### 3. Performance de Contadores
    - Ranking por taxa de autonomia
    - Análise de padrões comportamentais
    - Identificação de top performers e críticos
    
    #### 4. Performance de DAFs/Equipes
    - Monitoramento de equipes de auditores
    - Análise de legitimidade de exclusões
    - Detecção de padrões suspeitos
    
    #### 5. Drill-Down por DAF
    - Análise detalhada de equipes
    - Histórico completo de inconsistências
    - Análises temporais sob demanda
    
    #### 6. Machine Learning
    - Predição de exclusões suspeitas
    - Random Forest e Gradient Boosting
    - Feature importance e métricas de performance
    
    ### 📊 Dados e Métricas
    
    - **Empresas Monitoradas:** Todas com inconsistências no sistema
    - **Período de Análise:** 13 meses históricos
    - **Tipos de Inconsistência:** 45 tipos distintos
    - **Canais de Resolução:** 5 canais principais
    - **Atualização:** Dados atualizados periodicamente
    
    ### 🎯 Objetivos
    
    1. **Aumentar a Taxa de Autonomia**
       - Meta: ≥ 60% de resolução autônoma
       - Reduzir trabalho manual de auditores
    
    2. **Reduzir Exclusões Injustificadas**
       - Meta: ≤ 30% de exclusões
       - Identificar padrões suspeitos
    
    3. **Otimizar Recursos**
       - Priorização inteligente
       - Foco em casos de maior impacto
    
    4. **Melhorar Capacitação**
       - Identificar necessidades de treinamento
       - Segmentação de contadores
    
    ### 🛠️ Tecnologias Utilizadas
    
    - **Frontend:** Streamlit
    - **Visualização:** Plotly
    - **Análise:** Pandas, NumPy
    - **Machine Learning:** Scikit-learn
    - **Banco de Dados:** Impala (Hadoop)
    
    ### 👨‍💼 Desenvolvimento
    
    **Versão:** 2.0  
    **Data:** Outubro 2025
    
    ### 📞 Contato e Suporte
    
    Para dúvidas, sugestões ou suporte técnico, entre em contato com o MLH.
    
    ---
    
    *Sistema desenvolvido com foco em eficiência, precisão e facilidade de uso.*
    """)
    
    # Estatísticas do sistema
    st.markdown("<div class='sub-header'>📊 Estatísticas Atuais</div>", unsafe_allow_html=True)
    
    kpis = calcular_kpis_gerais(dados)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Empresas", f"{kpis['total_empresas']:,}")
    
    with col2:
        st.metric("Inconsistências", f"{kpis['total_inconsistencias']:,}")
    
    with col3:
        st.metric("Taxa Autonomia", f"{kpis['taxa_autonomia']:.1f}%")
    
    with col4:
        st.metric("Contadores", f"{kpis['contadores_sistema']:,}")

# =============================================================================
# 8. FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    """Função principal do dashboard."""
    
    # Sidebar - Header
    st.sidebar.markdown("""
    <div style='text-align: center; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; margin-bottom: 20px;'>
        <h1 style='color: white; margin: 0;'>🎯</h1>
        <p style='color: white; margin: 0; font-size: 0.9rem;'>Sistema de DAFs</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Menu de navegação
    st.sidebar.markdown("### 📋 Menu de Navegação")
    
    paginas = {
        "📊 Dashboard Executivo": pagina_dashboard_executivo,
        "🔬 Análise Multidimensional": pagina_analise_multidimensional,
        "🎯 Indicador: Autonomia": pagina_indicador_autonomia,
        "⏳ Indicador: Pendência": pagina_indicador_pendencia,
        "🗑️ Indicador: Exclusão": pagina_indicador_exclusao,
        "🚨 Indicador: Fiscalização": pagina_indicador_fiscalizacao,
        "⚠️ Alertas": pagina_alertas,
        "🔍 Tipos de Inconsistência": pagina_tipos_inconsistencia,
        "🔎 Drill-Down: Inconsistências": pagina_drill_down_inconsistencias,
        "📈 Análise Temporal": pagina_analise_temporal,
        "👥 Performance Contadores": pagina_performance_contadores,
        "🏢 Performance DAFs": pagina_performance_dafs,
        "🔎 Drill-Down DAF": pagina_drill_down_daf,
#        "🤖 Machine Learning": pagina_machine_learning,
        "ℹ️ Sobre o Sistema": pagina_sobre
    }
    
    pagina_selecionada = st.sidebar.radio(
        "Selecione a página",
        list(paginas.keys()),
        label_visibility="collapsed"
    )
    
    st.sidebar.markdown("---")
    
    # Criar engine e carregar dados
    engine = get_impala_engine()
    
    # Salvar engine no session_state para uso posterior
    if 'engine' not in st.session_state:
        st.session_state['engine'] = engine
    
    if engine is None:
        st.error("❌ Não foi possível conectar ao banco de dados.")
        st.stop()
    
    with st.spinner('⏳ Carregando dados do sistema...'):
        dados = carregar_dados_sistema(engine)
    
    if not dados:
        st.error("❌ Falha no carregamento dos dados.")
        st.stop()
    
    # Info na sidebar
    df_agregado = dados.get('inconsistencias_agregadas', pd.DataFrame())
    
    if not df_agregado.empty:
        total_registros = df_agregado['qtd_total'].sum()
        
        st.sidebar.success(f"✅ {int(total_registros):,} inconsistências")
        
        st.sidebar.info(f"""
        **📊 Dados Carregados:**
        
        🏢 {df_agregado['qtd_empresas'].sum():,.0f} empresas  
        📋 {df_agregado['cd_inconsistencia'].nunique()} tipos  
        💰 R$ {df_agregado['valor_total'].sum()/1e9:.2f}B
        """)
    
    # Filtros
    filtros = criar_filtros_sidebar(dados)
    
    st.sidebar.markdown("---")
    
    # Rodapé sidebar
    with st.sidebar.expander("ℹ️ Informações do Sistema"):
        st.caption(f"""
        **Versão:** 2.0  
        **Atualização:** {datetime.now().strftime('%d/%m/%Y %H:%M')}   
        **Órgão:** SEFAZ/SC
        """)
    
    # Executar página selecionada
    try:
        paginas[pagina_selecionada](dados, filtros)
    except Exception as e:
        st.error(f"❌ Erro ao carregar a página: {str(e)}")
        with st.expander("🔍 Detalhes do erro"):
            st.exception(e)
    
    # Rodapé
    st.markdown("---")
    st.markdown(f"""
    <div style='text-align: center; color: #666; font-size: 0.85rem;'>
        <b>Sistema MLH v2.0</b> | Receita Estadual de Santa Catarina<br>
        {datetime.now().strftime('%d/%m/%Y %H:%M')}
    </div>
    """, unsafe_allow_html=True)

# =============================================================================
# 9. EXECUÇÃO
# =============================================================================

if __name__ == "__main__":
    main()