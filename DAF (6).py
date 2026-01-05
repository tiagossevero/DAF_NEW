# =============================================================================
# SISTEMA DE MONITORAMENTO DE MALHAS FISCAIS - DAFs V3.0
# Receita Estadual de Santa Catarina
# =============================================================================

import streamlit as st
import hashlib

SENHA = "tsevero555"

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

import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from sqlalchemy import create_engine
import warnings
import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context

warnings.filterwarnings('ignore')

st.set_page_config(page_title="Sistema de DAFs V3.0", page_icon="🎯", layout="wide", initial_sidebar_state="expanded")

# CSS com melhorias de UX
st.markdown("""
<style>
    .main-header {font-size: 2.3rem; font-weight: bold; text-align: center; margin-bottom: 1.5rem; padding: 18px; background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); border-radius: 12px; color: white;}
    .sub-header {font-size: 1.4rem; font-weight: bold; color: #1e3a5f; margin-top: 1.5rem; margin-bottom: 1rem; border-bottom: 2px solid #1e3a5f; padding-bottom: 8px;}
    div[data-testid="stPlotlyChart"] {border: 1px solid #e0e0e0; border-radius: 8px; padding: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05);}
    div[data-testid="stMetric"] {background-color: #ffffff; border: 1px solid #e0e0e0; border-radius: 8px; padding: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); transition: transform 0.2s, box-shadow 0.2s;}
    div[data-testid="stMetric"]:hover {transform: translateY(-2px); box-shadow: 0 4px 8px rgba(0,0,0,0.1);}
    .alert-critico {background-color: #fef2f2; border-left: 4px solid #dc2626; padding: 12px; border-radius: 6px; margin: 10px 0;}
    .alert-atencao {background-color: #fffbeb; border-left: 4px solid #f59e0b; padding: 12px; border-radius: 6px; margin: 10px 0;}
    .alert-positivo {background-color: #f0fdf4; border-left: 4px solid #22c55e; padding: 12px; border-radius: 6px; margin: 10px 0;}
    .info-box {background-color: #eff6ff; border-left: 4px solid #3b82f6; padding: 12px; border-radius: 6px; margin: 10px 0;}

    /* Tooltips e cards customizados */
    .card-indicador {
        position: relative;
        padding: 1rem;
        border-radius: 10px;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        margin: 5px 0;
        text-align: center;
        transition: transform 0.2s, box-shadow 0.2s;
        cursor: help;
    }
    .card-indicador:hover {
        transform: translateY(-3px);
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
    }
    .card-indicador .tooltip-text {
        visibility: hidden;
        background-color: #333;
        color: #fff;
        text-align: left;
        border-radius: 6px;
        padding: 10px 12px;
        position: absolute;
        z-index: 1000;
        bottom: 105%;
        left: 50%;
        transform: translateX(-50%);
        width: 220px;
        font-size: 0.75rem;
        opacity: 0;
        transition: opacity 0.3s;
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    .card-indicador:hover .tooltip-text {
        visibility: visible;
        opacity: 1;
    }
    .card-indicador .tooltip-text::after {
        content: "";
        position: absolute;
        top: 100%;
        left: 50%;
        margin-left: -5px;
        border-width: 5px;
        border-style: solid;
        border-color: #333 transparent transparent transparent;
    }

    /* Dica de contexto */
    .context-tip {
        background: linear-gradient(135deg, #f0f9ff 0%, #e0f2fe 100%);
        border: 1px solid #7dd3fc;
        border-radius: 8px;
        padding: 12px 16px;
        margin: 10px 0;
        font-size: 0.9rem;
        color: #0369a1;
    }
    .context-tip b {
        color: #0c4a6e;
    }

    /* Legenda de cores */
    .legenda-cores {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin: 10px 0;
        padding: 10px;
        background-color: #f8fafc;
        border-radius: 8px;
    }
    .legenda-item {
        display: flex;
        align-items: center;
        gap: 5px;
        font-size: 0.8rem;
    }
    .legenda-cor {
        width: 12px;
        height: 12px;
        border-radius: 3px;
    }
</style>
""", unsafe_allow_html=True)

# Conexão
IMPALA_HOST = 'bdaworkernode02.sef.sc.gov.br'
IMPALA_PORT = 21050
DATABASE = 'niat'
IMPALA_USER = st.secrets.get("impala_credentials", {}).get("user", "tsevero")
IMPALA_PASSWORD = st.secrets.get("impala_credentials", {}).get("password", "")

@st.cache_resource
def get_impala_engine():
    try:
        engine = create_engine(
            f'impala://{IMPALA_HOST}:{IMPALA_PORT}/{DATABASE}',
            connect_args={'user': IMPALA_USER, 'password': IMPALA_PASSWORD, 'auth_mechanism': 'LDAP', 'use_ssl': True}
        )
        return engine
    except Exception as e:
        st.sidebar.error(f"❌ Erro: {str(e)[:100]}")
        return None

@st.cache_data(ttl=3600)
def carregar_dados_sistema(_engine):
    dados = {}
    if _engine is None:
        return {}
    try:
        with _engine.connect() as conn:
            st.sidebar.success("✅ Conexão OK!")
    except Exception as e:
        st.sidebar.error(f"❌ Falha: {str(e)[:100]}")
        return {}
    
    tabelas = {
        'tipos_inconsistencia': f"SELECT * FROM {DATABASE}.mlh_tipos_inconsistencia",
        'equipes': f"SELECT * FROM {DATABASE}.mlh_equipes",
        'fiscais': f"SELECT * FROM {DATABASE}.mlh_fiscais",
        'performance_dafs': f"SELECT * FROM {DATABASE}.mlh_performance_dafs",
        'evolucao_mensal': f"SELECT * FROM {DATABASE}.mlh_evolucao_mensal ORDER BY nu_per_ref DESC",
        'evolucao_mensal_daf': f"SELECT * FROM {DATABASE}.mlh_evolucao_mensal_daf",
        'ranking_tipos': f"SELECT * FROM {DATABASE}.mlh_ranking_tipos ORDER BY qtd_existentes DESC",
        'performance_contadores': f"SELECT * FROM {DATABASE}.mlh_performance_contadores ORDER BY score_performance DESC",
        'kpis_sistema': f"SELECT * FROM {DATABASE}.mlh_kpis_sistema",
        'empresas_resumo': f"SELECT * FROM {DATABASE}.mlh_empresas_resumo",
        'analise_resolucao_fiscal': f"SELECT * FROM {DATABASE}.mlh_analise_resolucao_fiscal",
        'exclusoes_por_daf': f"SELECT * FROM {DATABASE}.mlh_exclusoes_por_daf",
        # Novas tabelas de exclusão detalhadas (V3.1)
        'exclusoes_por_daf_v2': f"SELECT * FROM {DATABASE}.mlh_exclusoes_por_daf_v2",
        'exclusoes_por_fiscal': f"SELECT * FROM {DATABASE}.mlh_exclusoes_por_fiscal",
        'exclusoes_detalhadas': f"SELECT * FROM {DATABASE}.mlh_exclusoes_detalhadas",
        'top_exclusoes': f"SELECT * FROM {DATABASE}.mlh_top_exclusoes ORDER BY qtd_incons_excluidas DESC LIMIT 500"
    }
    
    progress_bar = st.sidebar.progress(0)
    total = len(tabelas)
    
    for idx, (key, query) in enumerate(tabelas.items()):
        try:
            progress_bar.progress((idx + 1) / total)
            df = pd.read_sql(query, _engine)
            df.columns = [col.lower() for col in df.columns]
            for col in df.select_dtypes(include=['object']).columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except:
                    pass
            dados[key] = df
        except Exception as e:
            st.sidebar.warning(f"⚠️ {key}: {str(e)[:50]}")
            dados[key] = pd.DataFrame()
    
    progress_bar.empty()
    return dados

# =============================================================================
# FUNÇÕES AUXILIARES
# =============================================================================

def formatar_valor_br(valor):
    if pd.isna(valor) or valor is None:
        return "R$ 0"
    valor = float(valor)
    if valor >= 1e9:
        return f"R$ {valor/1e9:.2f}B"
    elif valor >= 1e6:
        return f"R$ {valor/1e6:.2f}M"
    elif valor >= 1e3:
        return f"R$ {valor/1e3:.1f}K"
    else:
        return f"R$ {valor:,.2f}"

def get_color_nivel(nivel):
    cores = {'EXCELENTE': '#10b981', 'BOM': '#84cc16', 'MEDIO': '#fbbf24', 
             'BAIXO': '#f97316', 'CRITICO': '#ef4444', 'ALTO': '#f97316', 'N/A': '#6b7280'}
    return cores.get(str(nivel).upper() if nivel else 'N/A', '#6b7280')

def criar_card_indicador(label, valor, nivel, icone="📊", tooltip=""):
    """Cria um card de indicador com tooltip explicativo."""
    cor = get_color_nivel(nivel)
    tooltip_html = f'<span class="tooltip-text">{tooltip}</span>' if tooltip else ''
    st.markdown(f"""
    <div class='card-indicador' style='background: linear-gradient(135deg, {cor} 0%, {cor}dd 100%);'>
        {tooltip_html}
        <div style='font-size: 1.5rem;'>{icone}</div>
        <div style='font-size: 0.8rem; opacity: 0.9;'>{label}</div>
        <div style='font-size: 1.8rem; font-weight: bold;'>{valor}</div>
        <div style='font-size: 0.75rem; opacity: 0.8;'>{nivel}</div>
    </div>
    """, unsafe_allow_html=True)

# Dicionário de tooltips para KPIs - facilita manutenção e consistência
TOOLTIPS = {
    # KPIs Principais
    'empresas': 'Total de empresas monitoradas pelo sistema de malhas fiscais. Inclui todas as empresas com pelo menos uma inconsistência detectada.',
    'dafs': 'Delegacias de Fiscalização responsáveis pelo acompanhamento das empresas. Cada DAF monitora um conjunto específico de contribuintes.',
    'contadores': 'Profissionais contábeis responsáveis pelas empresas monitoradas. Um contador pode atender várias empresas.',
    'tipos': 'Quantidade de tipos distintos de inconsistências fiscais identificadas pelo sistema (ex: divergência DIME x NFe, omissão de receitas, etc).',
    'valor_total': 'Soma de todos os valores de ICMS envolvidos nas inconsistências (existentes + resolvidas). Representa o potencial total de recuperação.',

    # Status das Inconsistências
    'existentes': 'Inconsistências ativas que aguardam regularização pelo contribuinte. São as pendências atuais que precisam de ação.',
    'resolvida_malha': 'Inconsistências resolvidas de forma autônoma pelo contribuinte (via DDE ou retificação). Não exigiu ação fiscal.',
    'resolvida_fiscal': 'Inconsistências resolvidas via PAF (Processo Administrativo Fiscal) ou exclusão por auditor. Exigiu intervenção do fisco.',
    'em_fiscalizacao': 'Inconsistências que estão atualmente em processo de fiscalização (PAF aberto). Aguardando conclusão.',

    # Taxas e Metas
    'taxa_autonomia': 'Percentual de inconsistências resolvidas pelo próprio contribuinte, sem necessidade de fiscalização. META: ≥ 60%. Quanto maior, melhor a autorregularização.',
    'taxa_fiscalizacao': 'Percentual de inconsistências que necessitaram de fiscalização para resolução. META: ≤ 20%. Quanto menor, melhor.',

    # Valores
    'valor_existentes': 'Soma dos valores de ICMS das inconsistências ativas/pendentes. Representa o potencial de arrecadação a recuperar.',
    'valor_resolvida_malha': 'Valor de ICMS recuperado de forma autônoma pelos contribuintes. Indica eficácia do sistema de autorregularização.',
    'valor_em_fiscalizacao': 'Valor de ICMS das inconsistências atualmente em processo de fiscalização.',

    # Exclusões
    'exclusoes': 'Inconsistências removidas manualmente por auditores fiscais (cd_situacao = 11). Diferente de resolvida pela malha. Requer monitoramento.',
    'taxa_exclusao': 'Percentual de inconsistências excluídas manualmente sobre o total resolvido. Taxas altas (>10%) merecem investigação.',
    'nivel_risco': 'Classificação de risco baseada na taxa de exclusão: CRÍTICO (≥20%), ALTO (10-20%), MÉDIO (5-10%), BAIXO (<5%).',

    # Performance
    'score_autonomia': 'Pontuação calculada considerando taxa de autonomia e volume. Varia de 0 a 100. Quanto maior, melhor a performance.',
    'nivel_autonomia': 'Classificação baseada no score: EXCELENTE (≥80), BOM (60-79), MÉDIO (40-59), BAIXO (20-39), CRÍTICO (<20).',
}

def calcular_kpis_gerais(dados):
    df_kpis = dados.get('kpis_sistema', pd.DataFrame())
    if df_kpis.empty:
        return {k: 0 for k in ['total_empresas', 'total_dafs', 'total_contadores', 'total_tipos', 
                               'total_existentes', 'total_valor', 'total_resolvida_malha', 
                               'total_resolvida_fiscal', 'total_em_fiscalizacao', 
                               'taxa_autonomia', 'taxa_fiscalizacao', 'valor_existentes',
                               'valor_resolvida_malha', 'valor_em_fiscalizacao']}
    row = df_kpis.iloc[0]
    return {
        'total_empresas': int(row.get('total_empresas', 0) or 0),
        'total_dafs': int(row.get('total_dafs', 0) or 0),
        'total_contadores': int(row.get('total_contadores', 0) or 0),
        'total_tipos': int(row.get('total_tipos_incons', 0) or 0),
        'total_existentes': int(row.get('total_existentes', 0) or 0),
        'total_valor': float(row.get('total_valor', 0) or 0),
        'total_resolvida_malha': int(row.get('total_resolvida_malha', 0) or 0),
        'total_resolvida_fiscal': int(row.get('total_resolvida_fiscal', 0) or 0),
        'total_em_fiscalizacao': int(row.get('total_em_fiscalizacao', 0) or 0),
        'taxa_autonomia': float(row.get('taxa_autonomia_geral', 0) or 0),
        'taxa_fiscalizacao': float(row.get('taxa_fiscalizacao_geral', 0) or 0),
        'valor_existentes': float(row.get('total_valor_existentes', 0) or 0),
        'valor_resolvida_malha': float(row.get('total_valor_resolvida_malha', 0) or 0),
        'valor_em_fiscalizacao': float(row.get('total_valor_em_fiscalizacao', 0) or 0)
    }

def criar_filtros_sidebar(dados):
    filtros = {}
    with st.sidebar.expander("🔍 Filtros", expanded=False):
        df_equipes = dados.get('equipes', pd.DataFrame())
        if not df_equipes.empty:
            equipes_opcoes = ['Todas'] + sorted(df_equipes['nm_equipe'].dropna().unique().tolist())
            filtros['equipe'] = st.selectbox("Equipe/DAF", equipes_opcoes, index=0)
        filtros['classificacao'] = st.multiselect("Classificação", 
            ['EXCELENTE', 'BOM', 'MEDIO', 'BAIXO', 'CRITICO'], 
            default=['EXCELENTE', 'BOM', 'MEDIO', 'BAIXO', 'CRITICO'])
        filtros['tema'] = st.selectbox("Tema Gráficos", ["plotly_white", "plotly", "plotly_dark"], index=0)
    return filtros

# =============================================================================
# PÁGINA: DASHBOARD EXECUTIVO
# =============================================================================

def pagina_dashboard_executivo(dados, filtros):
    st.markdown("<h1 class='main-header'>📊 Dashboard Executivo - Sistema de Malhas V3.0</h1>", unsafe_allow_html=True)

    # Dica de contexto inicial
    st.markdown("""
    <div class='context-tip'>
    <b>💡 Dica:</b> Passe o mouse sobre os indicadores para ver explicações detalhadas.
    Os cards coloridos indicam o nível de performance: <span style='color:#10b981'>■</span> Excelente
    <span style='color:#84cc16'>■</span> Bom <span style='color:#fbbf24'>■</span> Médio
    <span style='color:#f97316'>■</span> Baixo <span style='color:#ef4444'>■</span> Crítico
    </div>
    """, unsafe_allow_html=True)

    kpis = calcular_kpis_gerais(dados)

    # KPIs - Linha 1
    st.markdown("<div class='sub-header'>📈 Indicadores Principais</div>", unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("🏢 Empresas", f"{kpis['total_empresas']:,}",
                  help=TOOLTIPS['empresas'])
    with col2:
        st.metric("👥 DAFs", f"{kpis['total_dafs']}",
                  help=TOOLTIPS['dafs'])
    with col3:
        st.metric("📋 Contadores", f"{kpis['total_contadores']:,}",
                  help=TOOLTIPS['contadores'])
    with col4:
        st.metric("🔢 Tipos", f"{kpis['total_tipos']}",
                  help=TOOLTIPS['tipos'])
    with col5:
        st.metric("💰 Valor Total", formatar_valor_br(kpis['total_valor']),
                  help=TOOLTIPS['valor_total'])

    # KPIs - Linha 2
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("🔴 Existentes", f"{kpis['total_existentes']:,}",
                  help=TOOLTIPS['existentes'])
    with col2:
        st.metric("✅ Resol. Malha", f"{kpis['total_resolvida_malha']:,}",
                  help=TOOLTIPS['resolvida_malha'])
    with col3:
        st.metric("⚖️ Resol. Fiscal", f"{kpis['total_resolvida_fiscal']:,}",
                  help=TOOLTIPS['resolvida_fiscal'])
    with col4:
        delta = "✓ Meta 60%" if kpis['taxa_autonomia'] >= 60 else f"↓ {60-kpis['taxa_autonomia']:.1f}pp"
        st.metric("🎯 Taxa Autonomia", f"{kpis['taxa_autonomia']:.1f}%", delta=delta,
                  help=TOOLTIPS['taxa_autonomia'])
    with col5:
        delta = "✓ Meta ≤20%" if kpis['taxa_fiscalizacao'] <= 20 else f"↑ {kpis['taxa_fiscalizacao']-20:.1f}pp"
        st.metric("🚨 Taxa Fiscalização", f"{kpis['taxa_fiscalizacao']:.1f}%", delta=delta, delta_color="inverse",
                  help=TOOLTIPS['taxa_fiscalizacao'])

    # KPIs - Linha 3 (VALORES)
    st.markdown("<div class='sub-header'>💰 Valores por Status</div>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("💵 Valor Existentes", formatar_valor_br(kpis['valor_existentes']),
                  help=TOOLTIPS['valor_existentes'])
    with col2:
        st.metric("💵 Valor Resol. Malha", formatar_valor_br(kpis['valor_resolvida_malha']),
                  help=TOOLTIPS['valor_resolvida_malha'])
    with col3:
        st.metric("💵 Valor Em Fiscal.", formatar_valor_br(kpis['valor_em_fiscalizacao']),
                  help=TOOLTIPS['valor_em_fiscalizacao'])
    with col4:
        st.metric("💵 Valor Total", formatar_valor_br(kpis['total_valor']),
                  help=TOOLTIPS['valor_total'])
    
    st.divider()
    
    # Gráficos
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("<div class='sub-header'>📊 Distribuição por Status</div>", unsafe_allow_html=True)
        df_dist = pd.DataFrame({
            'Status': ['Existentes', 'Resol. Malha', 'Resol. Fiscal', 'Em Fiscalização'],
            'Quantidade': [kpis['total_existentes'], kpis['total_resolvida_malha'], 
                          kpis['total_resolvida_fiscal'], kpis['total_em_fiscalizacao']]
        })
        fig_pie = px.pie(df_dist, values='Quantidade', names='Status', 
                        color_discrete_sequence=px.colors.qualitative.Set2, hole=0.4)
        fig_pie.update_layout(height=350, margin=dict(l=20, r=20, t=30, b=20))
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        st.markdown("<div class='sub-header'>📈 Evolução Taxa Autonomia</div>", unsafe_allow_html=True)
        df_evol = dados.get('evolucao_mensal', pd.DataFrame())
        if not df_evol.empty:
            df_evol_sorted = df_evol.sort_values('nu_per_ref')
            fig_line = px.line(df_evol_sorted, x='nu_per_ref', y='taxa_autonomia_mensal',
                              markers=True, color_discrete_sequence=['#3b82f6'])
            fig_line.add_hline(y=60, line_dash="dash", line_color="green", annotation_text="Meta 60%")
            fig_line.update_layout(height=350, xaxis_title="Período", yaxis_title="Taxa (%)",
                                  margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_line, use_container_width=True)
    
    st.divider()
    
    # Top DAFs
    st.markdown("<div class='sub-header'>🏆 Top 10 DAFs</div>", unsafe_allow_html=True)
    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    if not df_dafs.empty:
        df_top = df_dafs.nlargest(10, 'score_autonomia').copy()
        col_config = {
            'id_equipe': st.column_config.NumberColumn('ID', width='small'),
            'nm_equipe': st.column_config.TextColumn('Equipe', width='medium'),
            'qtd_empresas_acompanhadas': st.column_config.NumberColumn('Empresas', format='%d'),
            'valor_incons_existentes': st.column_config.NumberColumn('Valor Existentes', format='R$ %.2f'),
            'taxa_autonomia_pct': st.column_config.ProgressColumn('Taxa Autonomia', format='%.1f%%', min_value=0, max_value=100),
            'taxa_em_fiscalizacao_pct': st.column_config.ProgressColumn('Taxa Fiscal.', format='%.1f%%', min_value=0, max_value=50),
            'score_autonomia': st.column_config.ProgressColumn('Score', format='%d', min_value=0, max_value=100),
            'ind_autonomia_nivel': st.column_config.TextColumn('Nível')
        }
        cols = ['id_equipe', 'nm_equipe', 'qtd_empresas_acompanhadas', 'valor_incons_existentes',
                'taxa_autonomia_pct', 'taxa_em_fiscalizacao_pct', 'score_autonomia', 'ind_autonomia_nivel']
        cols_exist = [c for c in cols if c in df_top.columns]
        st.dataframe(df_top[cols_exist], column_config=col_config, use_container_width=True, hide_index=True, height=400)
    
    # Info
    st.markdown("""
    <div class='info-box'>
    <b>📊 Fluxo das Inconsistências:</b><br>
    <b>1. IDENTIFICADA</b> → Sistema detecta<br>
    <b>2. ATIVA</b> → Contribuinte tem prazo para regularizar (DDE/Retificação)<br>
    <b>3. Se não regulariza</b> → PAF (Em Fiscalização) → Resolvida Fiscal<br>
    <b>⚠️ ATENÇÃO:</b> "Resolvida Fiscal" pode incluir EXCLUSÕES por auditores - monitorar!
    </div>
    """, unsafe_allow_html=True)

# =============================================================================
# PÁGINA: ANÁLISE DE EXCLUSÕES POR FISCAL (cd_situacao = 11)
# =============================================================================

def pagina_analise_exclusoes(dados, filtros):
    st.markdown("<h1 class='main-header'>🔍 Análise de Exclusões por Fiscal</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    <div class='alert-critico'>
    <b>⚠️ EXCLUSÕES MANUAIS (cd_situacao = 11)</b><br>
    Esta análise identifica inconsistências que foram <b>EXCLUÍDAS MANUALMENTE</b> por fiscais.<br>
    <b>Diferente de:</b> Resolvida pela Malha (10) = contribuinte regularizou | Resolvida DDE (12) = pagamento DDE<br>
    <b>DAFs com alta taxa de exclusão manual precisam de investigação!</b>
    </div>
    """, unsafe_allow_html=True)
    
    # Tentar carregar tabela específica de exclusões
    df_exclusoes = dados.get('exclusoes_por_daf', pd.DataFrame())
    df_analise = dados.get('analise_resolucao_fiscal', pd.DataFrame())
    
    # Usar a melhor fonte disponível
    if not df_exclusoes.empty:
        df_principal = df_exclusoes.copy()
        col_exclusao = 'qtd_exclusoes'
        col_taxa = 'taxa_exclusao_pct'
    elif not df_analise.empty:
        df_principal = df_analise.copy()
        col_exclusao = 'qtd_resolvida_fiscal'
        col_taxa = 'taxa_resolvida_fiscal_pct'
    else:
        st.error("Dados não disponíveis. Execute: 01f_tabela_exclusoes_por_daf.sql")
        return
    
    # Dica de contexto
    st.markdown("""
    <div class='context-tip'>
    <b>💡 O que são exclusões?</b> São inconsistências removidas manualmente por auditores (cd_situacao=11).
    Diferente de "Resolvida Malha" (contribuinte regularizou) ou "Resolvida DDE" (pagamento).
    <b>Taxas altas de exclusão podem indicar uso inadequado e merecem investigação.</b>
    </div>
    """, unsafe_allow_html=True)

    # KPIs de Risco
    st.markdown("<div class='sub-header'>📊 Visão Geral de Exclusões</div>", unsafe_allow_html=True)

    col1, col2, col3, col4, col5 = st.columns(5)

    total_dafs = len(df_principal)
    total_exclusoes = df_principal[col_exclusao].sum() if col_exclusao in df_principal.columns else 0

    # Calcular níveis de risco
    if 'nivel_risco_exclusao' in df_principal.columns:
        criticos = len(df_principal[df_principal['nivel_risco_exclusao'] == 'CRITICO'])
        altos = len(df_principal[df_principal['nivel_risco_exclusao'] == 'ALTO'])
        medios = len(df_principal[df_principal['nivel_risco_exclusao'] == 'MEDIO'])
    elif col_taxa in df_principal.columns:
        criticos = len(df_principal[df_principal[col_taxa] >= 20])
        altos = len(df_principal[(df_principal[col_taxa] >= 10) & (df_principal[col_taxa] < 20)])
        medios = len(df_principal[(df_principal[col_taxa] >= 5) & (df_principal[col_taxa] < 10)])
    else:
        criticos, altos, medios = 0, 0, 0

    with col1:
        st.metric("Total Exclusões", f"{int(total_exclusoes):,}",
                  help=TOOLTIPS['exclusoes'])
    with col2:
        st.metric("🔴 CRÍTICOS (≥20%)", criticos,
                  help="DAFs com taxa de exclusão igual ou superior a 20%. Requer investigação imediata.")
    with col3:
        st.metric("🟠 ALTOS (10-20%)", altos,
                  help="DAFs com taxa de exclusão entre 10% e 20%. Monitoramento recomendado.")
    with col4:
        st.metric("🟡 MÉDIOS (5-10%)", medios,
                  help="DAFs com taxa de exclusão entre 5% e 10%. Dentro do aceitável, mas atenção.")
    with col5:
        pct_alerta = ((criticos + altos) / total_dafs * 100) if total_dafs > 0 else 0
        st.metric("% DAFs em Alerta", f"{pct_alerta:.1f}%",
                  help="Percentual de DAFs classificadas como risco CRÍTICO ou ALTO.")

    # Segunda linha de KPIs
    if 'valor_exclusoes' in df_principal.columns:
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            valor_total = df_principal['valor_exclusoes'].sum()
            st.metric("💰 Valor Total Excluído", formatar_valor_br(valor_total),
                      help="Soma dos valores de ICMS de todas as inconsistências excluídas. Representa potencial perda de arrecadação.")
        with col2:
            if 'qtd_empresas_afetadas' in df_principal.columns:
                emp_afetadas = df_principal['qtd_empresas_afetadas'].sum()
                st.metric("🏢 Empresas Afetadas", f"{int(emp_afetadas):,}",
                          help="Número de empresas distintas que tiveram inconsistências excluídas.")
        with col3:
            if 'qtd_motivos_distintos' in df_principal.columns:
                motivos = df_principal['qtd_motivos_distintos'].sum()
                st.metric("📋 Motivos Distintos", f"{int(motivos):,}",
                          help="Quantidade de justificativas diferentes utilizadas para exclusões.")
        with col4:
            st.metric("👥 DAFs com Exclusões", total_dafs,
                      help="Número de DAFs que realizaram ao menos uma exclusão manual.")
    
    st.divider()
    
    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["🚨 DAFs em Risco", "📊 Análise Gráfica", "🔍 Padrões Suspeitos", "📋 Tabela Completa"])
    
    with tab1:
        st.markdown("<div class='sub-header'>🚨 DAFs com Alta Taxa de Exclusão Manual</div>", unsafe_allow_html=True)
        
        # Filtrar DAFs em risco
        if 'nivel_risco_exclusao' in df_principal.columns:
            df_risco = df_principal[df_principal['nivel_risco_exclusao'].isin(['CRITICO', 'ALTO'])].copy()
        elif col_taxa in df_principal.columns:
            df_risco = df_principal[df_principal[col_taxa] >= 10].copy()
        else:
            df_risco = pd.DataFrame()
        
        if not df_risco.empty:
            df_risco = df_risco.sort_values(col_taxa, ascending=False)
            
            st.markdown(f"""
            <div class='alert-critico'>
            <b>⚠️ {len(df_risco)} DAFs com taxa de exclusão ≥ 10%</b><br>
            Estas equipes têm proporção elevada de exclusões manuais sobre o total resolvido!
            </div>
            """, unsafe_allow_html=True)
            
            col_config = {
                'id_equipe': st.column_config.NumberColumn('ID'),
                'nm_equipe': st.column_config.TextColumn('Equipe'),
                col_exclusao: st.column_config.NumberColumn('Qtd Exclusões', format='%d'),
                'valor_exclusoes': st.column_config.NumberColumn('Valor Excluído', format='R$ %.2f'),
                'qtd_resolvida_malha': st.column_config.NumberColumn('Resol. Malha', format='%d'),
                col_taxa: st.column_config.ProgressColumn('Taxa Exclusão', format='%.1f%%', min_value=0, max_value=50),
                'nivel_risco_exclusao': st.column_config.TextColumn('Risco'),
                'qtd_motivos_distintos': st.column_config.NumberColumn('Motivos', format='%d'),
                'qtd_empresas_afetadas': st.column_config.NumberColumn('Empresas', format='%d')
            }
            
            cols = ['id_equipe', 'nm_equipe', col_exclusao, 'valor_exclusoes', 'qtd_resolvida_malha',
                   col_taxa, 'nivel_risco_exclusao', 'qtd_motivos_distintos', 'qtd_empresas_afetadas']
            cols_exist = [c for c in cols if c in df_risco.columns]
            
            st.dataframe(df_risco[cols_exist], column_config=col_config, use_container_width=True, hide_index=True, height=400)
        else:
            st.success("✅ Nenhuma DAF com risco crítico ou alto identificada.")
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("<div class='sub-header'>📊 Distribuição de Risco</div>", unsafe_allow_html=True)
            
            if 'nivel_risco_exclusao' in df_principal.columns:
                dist = df_principal['nivel_risco_exclusao'].value_counts().reset_index()
                dist.columns = ['Nível', 'Quantidade']
                cores = {'CRITICO': '#ef4444', 'ALTO': '#f97316', 'MEDIO': '#fbbf24', 'BAIXO': '#10b981', 'N/A': '#6b7280'}
                fig = px.pie(dist, values='Quantidade', names='Nível', color='Nível', color_discrete_map=cores, hole=0.4)
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("<div class='sub-header'>📈 Top 15 DAFs por Exclusões</div>", unsafe_allow_html=True)
            
            df_top = df_principal.nlargest(15, col_exclusao)
            if not df_top.empty:
                fig = px.bar(df_top, x='nm_equipe' if 'nm_equipe' in df_top.columns else 'id_equipe', 
                           y=col_exclusao, color='nivel_risco_exclusao' if 'nivel_risco_exclusao' in df_top.columns else None,
                           color_discrete_map={'CRITICO': '#ef4444', 'ALTO': '#f97316', 'MEDIO': '#fbbf24', 'BAIXO': '#10b981'})
                fig.update_layout(height=350, xaxis_title="DAF", yaxis_title="Qtd Exclusões", showlegend=True)
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.markdown("<div class='sub-header'>🔍 Padrões Suspeitos de Exclusão</div>", unsafe_allow_html=True)
        
        st.markdown("""
        <div class='alert-atencao'>
        <b>O que procurar:</b><br>
        • DAFs com poucas PKs de exclusão mas muitos registros (uso repetitivo do mesmo motivo)<br>
        • Alta média de exclusões por motivo (indica possível uso indevido de um mesmo motivo)<br>
        • Exclusões concentradas em poucos tipos de inconsistência
        </div>
        """, unsafe_allow_html=True)
        
        if 'media_exclusoes_por_motivo' in df_principal.columns:
            df_suspeito = df_principal[df_principal['media_exclusoes_por_motivo'] > 100].sort_values('media_exclusoes_por_motivo', ascending=False)
            
            if not df_suspeito.empty:
                st.warning(f"⚠️ {len(df_suspeito)} DAFs com média > 100 exclusões por motivo!")
                
                col_config = {
                    'id_equipe': st.column_config.NumberColumn('ID'),
                    'nm_equipe': st.column_config.TextColumn('Equipe'),
                    col_exclusao: st.column_config.NumberColumn('Total Exclusões', format='%d'),
                    'qtd_motivos_distintos': st.column_config.NumberColumn('Motivos Usados', format='%d'),
                    'media_exclusoes_por_motivo': st.column_config.NumberColumn('Média/Motivo', format='%.1f'),
                    'nivel_risco_exclusao': st.column_config.TextColumn('Risco')
                }
                
                cols = ['id_equipe', 'nm_equipe', col_exclusao, 'qtd_motivos_distintos', 'media_exclusoes_por_motivo', 'nivel_risco_exclusao']
                cols_exist = [c for c in cols if c in df_suspeito.columns]
                
                st.dataframe(df_suspeito[cols_exist], column_config=col_config, use_container_width=True, hide_index=True)
            else:
                st.success("✅ Nenhum padrão suspeito identificado.")
        else:
            st.info("Execute o SQL 01f_tabela_exclusoes_por_daf.sql para ver padrões suspeitos.")
    
    with tab4:
        st.markdown("<div class='sub-header'>📋 Tabela Completa de Exclusões</div>", unsafe_allow_html=True)
        
        col_config = {
            'id_equipe': st.column_config.NumberColumn('ID'),
            'nm_equipe': st.column_config.TextColumn('Equipe'),
            col_exclusao: st.column_config.NumberColumn('Exclusões', format='%d'),
            'valor_exclusoes': st.column_config.NumberColumn('Valor', format='R$ %.0f'),
            'qtd_resolvida_malha': st.column_config.NumberColumn('Resol. Malha', format='%d'),
            'qtd_resolvida_dde': st.column_config.NumberColumn('Resol. DDE', format='%d'),
            'total_resolvido': st.column_config.NumberColumn('Total Resol.', format='%d'),
            col_taxa: st.column_config.ProgressColumn('Taxa Exclusão', format='%.1f%%', min_value=0, max_value=30),
            'nivel_risco_exclusao': st.column_config.TextColumn('Risco'),
            'qtd_empresas_afetadas': st.column_config.NumberColumn('Empresas', format='%d')
        }
        
        cols = ['id_equipe', 'nm_equipe', col_exclusao, 'valor_exclusoes', 'qtd_resolvida_malha', 
               'qtd_resolvida_dde', 'total_resolvido', col_taxa, 'nivel_risco_exclusao', 'qtd_empresas_afetadas']
        cols_exist = [c for c in cols if c in df_principal.columns]
        
        # Ordenação
        ordem = st.radio("Ordenar por:", ['Maior Taxa', 'Maior Volume', 'Maior Valor', 'Nome'], horizontal=True)
        if ordem == 'Maior Taxa' and col_taxa in df_principal.columns:
            df_exibir = df_principal.sort_values(col_taxa, ascending=False)
        elif ordem == 'Maior Volume' and col_exclusao in df_principal.columns:
            df_exibir = df_principal.sort_values(col_exclusao, ascending=False)
        elif ordem == 'Maior Valor' and 'valor_exclusoes' in df_principal.columns:
            df_exibir = df_principal.sort_values('valor_exclusoes', ascending=False)
        else:
            df_exibir = df_principal.sort_values('nm_equipe') if 'nm_equipe' in df_principal.columns else df_principal
        
        st.dataframe(df_exibir[cols_exist], column_config=col_config, use_container_width=True, hide_index=True, height=500)


def pagina_exclusoes_detalhada(dados, filtros):
    """Página detalhada de análise de exclusões - QUEM excluiu e MOTIVOS"""
    st.markdown("<h1 class='main-header'>🔎 Análise Detalhada de Exclusões</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    <div class='info-box'>
    <b>📋 Fonte dos dados:</b> Tabela <code>mlh_exclusao_incons</code><br>
    Cada registro representa uma SOLICITAÇÃO DE EXCLUSÃO feita por um fiscal, que pode afetar múltiplas inconsistências.<br>
    <b>id_exclusao</b> = cd_motivo_resolvido_fiscal na tabela de inconsistências
    </div>
    """, unsafe_allow_html=True)
    
    # Carregar dados das novas tabelas
    df_fiscais = dados.get('exclusoes_por_fiscal', pd.DataFrame())
    df_top_exclusoes = dados.get('top_exclusoes', pd.DataFrame())
    df_dafs_v2 = dados.get('exclusoes_por_daf_v2', pd.DataFrame())
    df_detalhadas = dados.get('exclusoes_detalhadas', pd.DataFrame())
    
    # Fallback para tabelas antigas
    if df_fiscais.empty:
        df_fiscais = dados.get('exclusoes_por_usuario', pd.DataFrame())
    if df_dafs_v2.empty:
        df_dafs_v2 = dados.get('exclusoes_por_daf', pd.DataFrame())
    
    if df_fiscais.empty and df_top_exclusoes.empty:
        st.error("Dados não disponíveis. Execute: 01g_tabelas_exclusoes_detalhadas.sql")
        return
    
    # KPIs Gerais
    st.markdown("<div class='sub-header'>📊 KPIs Gerais de Exclusões</div>", unsafe_allow_html=True)

    col1, col2, col3, col4, col5 = st.columns(5)

    if not df_fiscais.empty:
        total_fiscais = len(df_fiscais)
        total_exclusoes = df_fiscais['qtd_exclusoes_criadas'].sum() if 'qtd_exclusoes_criadas' in df_fiscais.columns else 0
        total_incons = df_fiscais['qtd_incons_excluidas'].sum() if 'qtd_incons_excluidas' in df_fiscais.columns else 0
        total_valor = df_fiscais['valor_incons_excluidas'].sum() if 'valor_incons_excluidas' in df_fiscais.columns else 0
        total_empresas = df_fiscais['qtd_empresas_afetadas'].sum() if 'qtd_empresas_afetadas' in df_fiscais.columns else 0

        with col1:
            st.metric("👤 Fiscais Excluindo", f"{total_fiscais:,}",
                      help="Quantidade de fiscais que criaram ao menos uma solicitação de exclusão.")
        with col2:
            st.metric("📋 Exclusões Criadas", f"{int(total_exclusoes):,}",
                      help="Número total de solicitações de exclusão registradas. Cada exclusão pode afetar múltiplas inconsistências.")
        with col3:
            st.metric("📊 Incons. Excluídas", f"{int(total_incons):,}",
                      help="Total de inconsistências que foram excluídas pelas solicitações. Uma exclusão pode abranger várias inconsistências.")
        with col4:
            st.metric("💰 Valor Excluído", formatar_valor_br(total_valor),
                      help="Soma do valor de ICMS das inconsistências excluídas. Representa potencial não arrecadado.")
        with col5:
            st.metric("🏢 Empresas Afetadas", f"{int(total_empresas):,}",
                      help="Quantidade de empresas distintas que tiveram inconsistências excluídas.")
    
    st.divider()
    
    tab1, tab2, tab3, tab4 = st.tabs(["👤 Por Fiscal", "🔝 Top Exclusões", "🏢 Por DAF", "📊 Categorias"])
    
    with tab1:
        st.markdown("<div class='sub-header'>👤 Ranking de Fiscais que Mais Excluem</div>", unsafe_allow_html=True)
        
        if not df_fiscais.empty:
            st.markdown("""
            <div class='alert-atencao'>
            <b>Atenção:</b> Fiscais com muitas exclusões devem ter suas justificativas revisadas.
            O campo <b>matricula_fiscal</b> identifica quem criou cada exclusão.
            </div>
            """, unsafe_allow_html=True)
            
            df_fiscais_sorted = df_fiscais.sort_values('qtd_incons_excluidas', ascending=False)
            
            col_config = {
                'matricula_fiscal': st.column_config.TextColumn('Matrícula'),
                'qtd_exclusoes_criadas': st.column_config.NumberColumn('Exclusões Criadas', format='%d'),
                'qtd_incons_excluidas': st.column_config.NumberColumn('Incons. Excluídas', format='%d'),
                'valor_incons_excluidas': st.column_config.NumberColumn('Valor Excluído', format='R$ %.0f'),
                'qtd_empresas_afetadas': st.column_config.NumberColumn('Empresas', format='%d'),
                'qtd_exclusoes_ativas': st.column_config.NumberColumn('Ativas', format='%d'),
                'qtd_exclusoes_canceladas': st.column_config.NumberColumn('Canceladas', format='%d'),
                'taxa_cancelamento_pct': st.column_config.ProgressColumn('% Canceladas', format='%.1f%%', min_value=0, max_value=100)
            }
            
            cols = ['matricula_fiscal', 'qtd_exclusoes_criadas', 'qtd_incons_excluidas', 
                   'valor_incons_excluidas', 'qtd_empresas_afetadas', 
                   'qtd_exclusoes_ativas', 'qtd_exclusoes_canceladas', 'taxa_cancelamento_pct']
            cols_exist = [c for c in cols if c in df_fiscais_sorted.columns]
            
            st.dataframe(df_fiscais_sorted[cols_exist].head(50), column_config=col_config, 
                        use_container_width=True, hide_index=True, height=400)
        else:
            st.info("Tabela mlh_exclusoes_por_fiscal não disponível.")
    
    with tab2:
        st.markdown("<div class='sub-header'>🔝 Top Exclusões Mais Usadas</div>", unsafe_allow_html=True)
        
        if not df_top_exclusoes.empty:
            st.markdown("""
            <div class='alert-critico'>
            <b>⚠️ Exclusões com muitos registros merecem investigação!</b><br>
            Uma única exclusão afetando milhares de inconsistências pode indicar uso inadequado.
            </div>
            """, unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            with col1:
                # Gráfico de barras
                df_top10 = df_top_exclusoes.nlargest(10, 'qtd_incons_excluidas')
                fig = px.bar(df_top10, x='id_exclusao', y='qtd_incons_excluidas', 
                           color='categoria_motivo' if 'categoria_motivo' in df_top10.columns else None,
                           hover_data=['matricula_fiscal', 'motivo_exclusao'] if 'motivo_exclusao' in df_top10.columns else None)
                fig.update_layout(height=350, xaxis_title="ID Exclusão", yaxis_title="Qtd Incons")
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Distribuição por categoria
                if 'categoria_motivo' in df_top_exclusoes.columns:
                    cat_dist = df_top_exclusoes.groupby('categoria_motivo')['qtd_incons_excluidas'].sum().reset_index()
                    fig = px.pie(cat_dist, values='qtd_incons_excluidas', names='categoria_motivo', hole=0.4)
                    fig.update_layout(height=350)
                    st.plotly_chart(fig, use_container_width=True)
            
            st.markdown("### 📋 Detalhes das Top Exclusões")
            
            col_config = {
                'id_exclusao': st.column_config.NumberColumn('ID'),
                'matricula_fiscal': st.column_config.TextColumn('Matrícula'),
                'nm_equipe': st.column_config.TextColumn('DAF'),
                'categoria_motivo': st.column_config.TextColumn('Categoria'),
                'motivo_exclusao': st.column_config.TextColumn('Motivo'),
                'qtd_incons_excluidas': st.column_config.NumberColumn('Incons', format='%d'),
                'valor_incons_excluidas': st.column_config.NumberColumn('Valor', format='R$ %.0f'),
                'dt_criacao': st.column_config.DateColumn('Data')
            }
            
            cols = ['id_exclusao', 'matricula_fiscal', 'nm_equipe', 'categoria_motivo',
                   'qtd_incons_excluidas', 'valor_incons_excluidas', 'dt_criacao']
            cols_exist = [c for c in cols if c in df_top_exclusoes.columns]
            
            st.dataframe(df_top_exclusoes[cols_exist].head(30), column_config=col_config,
                        use_container_width=True, hide_index=True, height=400)
            
            # Expander para ver motivos
            with st.expander("📝 Ver Motivos das Top 10 Exclusões"):
                for _, row in df_top10.iterrows():
                    motivo = row.get('motivo_exclusao', 'Não informado')
                    st.markdown(f"**ID {row['id_exclusao']}** ({int(row['qtd_incons_excluidas']):,} incons): {motivo[:200]}...")
        else:
            st.info("Tabela mlh_top_exclusoes não disponível.")
    
    with tab3:
        st.markdown("<div class='sub-header'>🏢 Exclusões por DAF (Detalhado)</div>", unsafe_allow_html=True)
        
        if not df_dafs_v2.empty:
            # Filtrar DAFs em risco
            nivel_col = 'nivel_risco' if 'nivel_risco' in df_dafs_v2.columns else 'nivel_risco_exclusao'
            if nivel_col in df_dafs_v2.columns:
                df_risco = df_dafs_v2[df_dafs_v2[nivel_col].isin(['CRITICO', 'ALTO'])]
                if not df_risco.empty:
                    st.markdown(f"""
                    <div class='alert-critico'>
                    <b>⚠️ {len(df_risco)} DAFs em risco (taxa ≥ 10%)</b>
                    </div>
                    """, unsafe_allow_html=True)
            
            col_config = {
                'id_equipe': st.column_config.NumberColumn('ID'),
                'nm_equipe': st.column_config.TextColumn('DAF'),
                'qtd_exclusoes_criadas': st.column_config.NumberColumn('Exclusões', format='%d'),
                'qtd_fiscais_excluindo': st.column_config.NumberColumn('Fiscais', format='%d'),
                'qtd_incons_excluidas': st.column_config.NumberColumn('Incons', format='%d'),
                'valor_incons_excluidas': st.column_config.NumberColumn('Valor', format='R$ %.0f'),
                'taxa_exclusao_pct': st.column_config.ProgressColumn('Taxa Exclusão', format='%.1f%%', min_value=0, max_value=30),
                'media_incons_por_exclusao': st.column_config.NumberColumn('Média/Exclusão', format='%.1f'),
                nivel_col: st.column_config.TextColumn('Risco')
            }
            
            cols = ['id_equipe', 'nm_equipe', 'qtd_exclusoes_criadas', 'qtd_fiscais_excluindo',
                   'qtd_incons_excluidas', 'valor_incons_excluidas', 'taxa_exclusao_pct',
                   'media_incons_por_exclusao', nivel_col]
            cols_exist = [c for c in cols if c in df_dafs_v2.columns]
            
            df_sorted = df_dafs_v2.sort_values('taxa_exclusao_pct', ascending=False)
            st.dataframe(df_sorted[cols_exist], column_config=col_config,
                        use_container_width=True, hide_index=True, height=450)
        else:
            st.info("Tabela mlh_exclusoes_por_daf_v2 não disponível.")
    
    with tab4:
        st.markdown("<div class='sub-header'>📊 Análise por Categoria de Motivo</div>", unsafe_allow_html=True)
        
        if not df_top_exclusoes.empty and 'categoria_motivo' in df_top_exclusoes.columns:
            cat_analise = df_top_exclusoes.groupby('categoria_motivo').agg({
                'id_exclusao': 'count',
                'qtd_incons_excluidas': 'sum',
                'valor_incons_excluidas': 'sum'
            }).reset_index()
            cat_analise.columns = ['Categoria', 'Qtd Exclusões', 'Incons Excluídas', 'Valor Excluído']
            cat_analise = cat_analise.sort_values('Incons Excluídas', ascending=False)
            
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(cat_analise, x='Categoria', y='Incons Excluídas', color='Categoria')
                fig.update_layout(height=350, showlegend=False)
                fig.update_xaxes(tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.dataframe(cat_analise, use_container_width=True, hide_index=True)
            
            st.markdown("""
            <div class='info-box'>
            <b>Categorias de Motivo:</b><br>
            • <b>TTD:</b> Tratamento Tributário Diferenciado (exclusão legítima por benefício fiscal)<br>
            • <b>EFD_ESCRITURACAO:</b> Notas lançadas corretamente na EFD<br>
            • <b>ESTORNO_DEVOLUCAO:</b> Notas estornadas ou devolvidas<br>
            • <b>CONTINGENCIA:</b> Notas emitidas em contingência<br>
            • <b>MANIFESTACAO:</b> Operação não realizada / manifestação do destinatário<br>
            • <b>RETIFICACAO:</b> Contribuinte retificou declaração<br>
            • <b>CANCELAMENTO:</b> NF-e ou documento cancelado<br>
            • <b>DARE_PAGO:</b> Diferença paga via DARE<br>
            • <b>MUTUO_EMPRESTIMO:</b> Operações de mútuo entre empresas/sócios<br>
            • <b>PIX_TRANSFERENCIA:</b> PIX entre contas da mesma titularidade<br>
            • <b>SEM_MOTIVO:</b> ⚠️ Exclusões sem justificativa - <b>INVESTIGAR!</b>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("Dados de categorização não disponíveis.")


# =============================================================================
# PÁGINA: PERFORMANCE DAFs
# =============================================================================

def pagina_performance_dafs(dados, filtros):
    st.markdown("<h1 class='main-header'>🏢 Performance das DAFs</h1>", unsafe_allow_html=True)

    # Dica de contexto
    st.markdown("""
    <div class='context-tip'>
    <b>💡 Como interpretar:</b> Cada DAF é avaliada pela sua <b>Taxa de Autonomia</b> (% de inconsistências resolvidas pelo contribuinte)
    e <b>Taxa de Fiscalização</b> (% que exigiu ação fiscal). O <b>Score</b> combina esses fatores em uma nota de 0-100.
    </div>
    """, unsafe_allow_html=True)

    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    if df_dafs.empty:
        st.error("Dados não disponíveis.")
        return

    # KPIs
    st.markdown("<div class='sub-header'>📊 Visão Geral</div>", unsafe_allow_html=True)
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total DAFs", len(df_dafs),
                  help="Número total de Delegacias de Fiscalização monitoradas.")
    with col2:
        excelentes = len(df_dafs[df_dafs['ind_autonomia_nivel'] == 'EXCELENTE']) if 'ind_autonomia_nivel' in df_dafs.columns else 0
        st.metric("Excelentes", excelentes,
                  help="DAFs com score de autonomia ≥ 80. Alta taxa de autorregularização pelos contribuintes.")
    with col3:
        criticos = len(df_dafs[df_dafs['ind_autonomia_nivel'] == 'CRITICO']) if 'ind_autonomia_nivel' in df_dafs.columns else 0
        st.metric("Críticos", criticos,
                  help="DAFs com score de autonomia < 20. Baixa autorregularização, pode indicar problemas.")
    with col4:
        media_autonomia = df_dafs['taxa_autonomia_pct'].mean() if 'taxa_autonomia_pct' in df_dafs.columns else 0
        st.metric("Média Autonomia", f"{media_autonomia:.1f}%",
                  help=TOOLTIPS['taxa_autonomia'])
    with col5:
        total_valor = df_dafs['valor_total_inconsistencias'].sum() if 'valor_total_inconsistencias' in df_dafs.columns else 0
        st.metric("Valor Total", formatar_valor_br(total_valor),
                  help="Soma de todos os valores de ICMS em inconsistências de todas as DAFs.")
    
    st.divider()
    
    tab1, tab2 = st.tabs(["📋 Tabela Completa", "📊 Gráficos"])
    
    with tab1:
        col1, col2 = st.columns([1, 4])
        with col1:
            ordem = st.radio("Ordenar:", ['Score (Melhor)', 'Score (Pior)', 'Valor (Maior)'])
        
        if ordem == 'Score (Melhor)':
            df_exibir = df_dafs.sort_values('score_autonomia', ascending=False)
        elif ordem == 'Score (Pior)':
            df_exibir = df_dafs.sort_values('score_autonomia', ascending=True)
        else:
            df_exibir = df_dafs.sort_values('valor_total_inconsistencias', ascending=False) if 'valor_total_inconsistencias' in df_dafs.columns else df_dafs
        
        col_config = {
            'id_equipe': st.column_config.NumberColumn('ID'),
            'nm_equipe': st.column_config.TextColumn('Equipe'),
            'qtd_empresas_acompanhadas': st.column_config.NumberColumn('Empresas', format='%d'),
            'qtd_incons_existentes': st.column_config.NumberColumn('Existentes', format='%d'),
            'qtd_resolucao_autonoma': st.column_config.NumberColumn('Resol. Autônoma', format='%d'),
            'qtd_resolvidas_fiscalizacao': st.column_config.NumberColumn('Resol. Fiscal', format='%d'),
            'valor_incons_existentes': st.column_config.NumberColumn('Valor Exist.', format='R$ %.0f'),
            'valor_resolucao_autonoma': st.column_config.NumberColumn('Valor Auton.', format='R$ %.0f'),
            'taxa_autonomia_pct': st.column_config.ProgressColumn('Taxa Auton.', format='%.1f%%', min_value=0, max_value=100),
            'taxa_em_fiscalizacao_pct': st.column_config.ProgressColumn('Taxa Fiscal.', format='%.1f%%', min_value=0, max_value=50),
            'score_autonomia': st.column_config.ProgressColumn('Score', format='%d', min_value=0, max_value=100),
            'ind_autonomia_nivel': st.column_config.TextColumn('Nível'),
            'flag_alerta_autonomia_baixa': st.column_config.CheckboxColumn('⚠️'),
            'flag_alerta_fiscalizacao_alta': st.column_config.CheckboxColumn('🚨')
        }
        
        cols = ['id_equipe', 'nm_equipe', 'qtd_empresas_acompanhadas', 'qtd_incons_existentes',
               'qtd_resolucao_autonoma', 'qtd_resolvidas_fiscalizacao', 'valor_incons_existentes',
               'valor_resolucao_autonoma', 'taxa_autonomia_pct', 'taxa_em_fiscalizacao_pct',
               'score_autonomia', 'ind_autonomia_nivel', 'flag_alerta_autonomia_baixa', 'flag_alerta_fiscalizacao_alta']
        cols_exist = [c for c in cols if c in df_exibir.columns]
        
        st.dataframe(df_exibir[cols_exist], column_config=col_config, use_container_width=True, hide_index=True, height=600)
    
    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("<div class='sub-header'>📊 Distribuição por Nível</div>", unsafe_allow_html=True)
            if 'ind_autonomia_nivel' in df_dafs.columns:
                dist = df_dafs['ind_autonomia_nivel'].value_counts().reset_index()
                dist.columns = ['Nível', 'Quantidade']
                cores = {'EXCELENTE': '#10b981', 'BOM': '#84cc16', 'MEDIO': '#fbbf24', 'BAIXO': '#f97316', 'CRITICO': '#ef4444', 'N/A': '#6b7280'}
                fig = px.bar(dist, x='Nível', y='Quantidade', color='Nível', color_discrete_map=cores)
                fig.update_layout(height=350, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("<div class='sub-header'>📈 Scatter: Autonomia vs Fiscalização</div>", unsafe_allow_html=True)
            if 'taxa_autonomia_pct' in df_dafs.columns and 'taxa_em_fiscalizacao_pct' in df_dafs.columns:
                fig = px.scatter(df_dafs, x='taxa_autonomia_pct', y='taxa_em_fiscalizacao_pct',
                               size='qtd_empresas_acompanhadas' if 'qtd_empresas_acompanhadas' in df_dafs.columns else None,
                               color='ind_autonomia_nivel' if 'ind_autonomia_nivel' in df_dafs.columns else None,
                               hover_data=['nm_equipe'] if 'nm_equipe' in df_dafs.columns else None,
                               color_discrete_map={'EXCELENTE': '#10b981', 'BOM': '#84cc16', 'MEDIO': '#fbbf24', 'BAIXO': '#f97316', 'CRITICO': '#ef4444'})
                fig.add_vline(x=60, line_dash="dash", line_color="green")
                fig.add_hline(y=20, line_dash="dash", line_color="red")
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
        var_autonomia = ultimo['taxa_autonomia_mensal'] - penultimo['taxa_autonomia_mensal']
        with col1:
            st.metric("Taxa Atual", f"{ultimo['taxa_autonomia_mensal']:.1f}%", delta=f"{var_autonomia:+.1f}pp")
        with col2:
            var_exist = ultimo['qtd_existentes'] - penultimo['qtd_existentes']
            st.metric("Existentes", f"{int(ultimo['qtd_existentes']):,}", delta=f"{int(var_exist):+,}", delta_color="inverse")
        with col3:
            var_resol = ultimo['qtd_resolvida_malha'] - penultimo['qtd_resolvida_malha']
            st.metric("Resol. Malha", f"{int(ultimo['qtd_resolvida_malha']):,}", delta=f"{int(var_resol):+,}")
        with col4:
            st.metric("Período", f"{int(ultimo['nu_per_ref'])}")
    
    st.divider()
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("<div class='sub-header'>📈 Evolução Taxa Autonomia</div>", unsafe_allow_html=True)
        fig = px.line(df_evol_sorted, x='nu_per_ref', y='taxa_autonomia_mensal', markers=True)
        fig.add_hline(y=60, line_dash="dash", line_color="green", annotation_text="Meta 60%")
        fig.update_layout(height=350, xaxis_title="Período", yaxis_title="Taxa (%)")
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("<div class='sub-header'>📊 Evolução Volumes</div>", unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df_evol_sorted['nu_per_ref'], y=df_evol_sorted['qtd_existentes'],
                                fill='tozeroy', name='Existentes', line=dict(color='#f59e0b')))
        if 'qtd_resolvida_malha' in df_evol_sorted.columns:
            fig.add_trace(go.Scatter(x=df_evol_sorted['nu_per_ref'], y=df_evol_sorted['qtd_resolvida_malha'],
                                    fill='tozeroy', name='Resol. Malha', line=dict(color='#10b981')))
        fig.update_layout(height=350, xaxis_title="Período", yaxis_title="Quantidade")
        st.plotly_chart(fig, use_container_width=True)
    
    with st.expander("📋 Dados Mensais"):
        st.dataframe(df_evol_sorted, hide_index=True, use_container_width=True)


# =============================================================================
# PÁGINA: TIPOS DE INCONSISTÊNCIA
# =============================================================================

def pagina_tipos_inconsistencia(dados, filtros):
    st.markdown("<h1 class='main-header'>🔍 Tipos de Inconsistência</h1>", unsafe_allow_html=True)

    # Dica de contexto
    st.markdown("""
    <div class='context-tip'>
    <b>💡 O que são tipos de inconsistência?</b> São as categorias de divergências fiscais detectadas pelo sistema,
    como: divergência entre DIME e NFe, omissão de receitas, crédito indevido, etc.
    Cada tipo tem comportamentos diferentes de regularização.
    </div>
    """, unsafe_allow_html=True)

    df_tipos = dados.get('ranking_tipos', pd.DataFrame())
    if df_tipos.empty:
        df_tipos = dados.get('tipos_inconsistencia', pd.DataFrame())

    if df_tipos.empty:
        st.error("Dados não disponíveis.")
        return

    # KPIs
    st.markdown("<div class='sub-header'>📊 Visão Geral</div>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Tipos", len(df_tipos),
                  help="Quantidade de tipos distintos de inconsistências fiscais cadastradas no sistema.")
    with col2:
        if 'qtd_existentes' in df_tipos.columns:
            total = df_tipos['qtd_existentes'].sum()
            st.metric("Total Inconsistências", f"{int(total):,}",
                      help="Soma de todas as inconsistências existentes de todos os tipos.")
    with col3:
        if 'valor_existentes' in df_tipos.columns:
            valor = df_tipos['valor_existentes'].sum()
            st.metric("Valor Total", formatar_valor_br(valor),
                      help="Soma dos valores de ICMS de todas as inconsistências existentes.")
    with col4:
        if 'taxa_autonomia_pct' in df_tipos.columns:
            media = df_tipos['taxa_autonomia_pct'].mean()
            st.metric("Média Autonomia", f"{media:.1f}%",
                      help="Média da taxa de autonomia entre todos os tipos. Tipos com baixa autonomia podem exigir mais fiscalização.")
    
    st.divider()
    
    tab1, tab2 = st.tabs(["📋 Ranking", "📊 Gráficos"])
    
    with tab1:
        st.markdown("<div class='sub-header'>📋 Ranking de Tipos</div>", unsafe_allow_html=True)
        
        col_config = {
            'cd_inconsistencia': st.column_config.NumberColumn('Código'),
            'nm_inconsistencia': st.column_config.TextColumn('Descrição'),
            'qtd_existentes': st.column_config.NumberColumn('Qtd Existentes', format='%d'),
            'valor_existentes': st.column_config.NumberColumn('Valor', format='R$ %.0f'),
            'qtd_resolvidas_malha': st.column_config.NumberColumn('Resol. Malha', format='%d'),
            'qtd_resolvidas_fiscalizacao': st.column_config.NumberColumn('Em Fiscal.', format='%d'),
            'taxa_autonomia_pct': st.column_config.ProgressColumn('Autonomia', format='%.1f%%', min_value=0, max_value=100),
            'taxa_em_fiscalizacao_pct': st.column_config.ProgressColumn('% Fiscal.', format='%.1f%%', min_value=0, max_value=50)
        }
        
        cols = ['cd_inconsistencia', 'nm_inconsistencia', 'qtd_existentes', 'valor_existentes',
               'qtd_resolvidas_malha', 'qtd_resolvidas_fiscalizacao', 'taxa_autonomia_pct', 'taxa_em_fiscalizacao_pct']
        cols_exist = [c for c in cols if c in df_tipos.columns]
        
        ordem = st.radio("Ordenar por:", ['Maior Volume', 'Maior Valor', 'Menor Autonomia'], horizontal=True, key='ord_tipos')
        if ordem == 'Maior Volume' and 'qtd_existentes' in df_tipos.columns:
            df_ord = df_tipos.sort_values('qtd_existentes', ascending=False)
        elif ordem == 'Maior Valor' and 'valor_existentes' in df_tipos.columns:
            df_ord = df_tipos.sort_values('valor_existentes', ascending=False)
        elif ordem == 'Menor Autonomia' and 'taxa_autonomia_pct' in df_tipos.columns:
            df_ord = df_tipos.sort_values('taxa_autonomia_pct', ascending=True)
        else:
            df_ord = df_tipos
        
        st.dataframe(df_ord[cols_exist], column_config=col_config, use_container_width=True, hide_index=True, height=500)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("<div class='sub-header'>📊 Top 10 por Volume</div>", unsafe_allow_html=True)
            if 'qtd_existentes' in df_tipos.columns and 'nm_inconsistencia' in df_tipos.columns:
                df_top = df_tipos.nlargest(10, 'qtd_existentes')
                fig = px.bar(df_top, x='qtd_existentes', y='nm_inconsistencia', orientation='h')
                fig.update_layout(height=400, yaxis_title="", xaxis_title="Quantidade")
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("<div class='sub-header'>📊 Top 10 por Valor</div>", unsafe_allow_html=True)
            if 'valor_existentes' in df_tipos.columns and 'nm_inconsistencia' in df_tipos.columns:
                df_top = df_tipos.nlargest(10, 'valor_existentes')
                fig = px.bar(df_top, x='valor_existentes', y='nm_inconsistencia', orientation='h', color_discrete_sequence=['#f59e0b'])
                fig.update_layout(height=400, yaxis_title="", xaxis_title="Valor (R$)")
                st.plotly_chart(fig, use_container_width=True)


# =============================================================================
# PÁGINA: PERFORMANCE CONTADORES
# =============================================================================

def pagina_performance_contadores(dados, filtros):
    st.markdown("<h1 class='main-header'>👥 Performance dos Contadores</h1>", unsafe_allow_html=True)

    # Dica de contexto
    st.markdown("""
    <div class='context-tip'>
    <b>💡 Sobre os contadores:</b> Este ranking mostra a performance dos profissionais contábeis responsáveis pelas empresas.
    Contadores com alta taxa de autonomia indicam boa prática fiscal e orientação aos clientes.
    Contadores com baixa performance podem precisar de capacitação ou orientação.
    </div>
    """, unsafe_allow_html=True)

    df_cont = dados.get('performance_contadores', pd.DataFrame())
    if df_cont.empty:
        st.error("Dados não disponíveis.")
        return

    # KPIs
    st.markdown("<div class='sub-header'>📊 Visão Geral</div>", unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Contadores", len(df_cont),
                  help="Quantidade de contadores/escritórios que atendem empresas com inconsistências.")
    with col2:
        if 'qtd_empresas' in df_cont.columns:
            st.metric("Total Empresas", f"{df_cont['qtd_empresas'].sum():,}",
                      help="Soma de todas as empresas atendidas pelos contadores listados.")
    with col3:
        if 'taxa_autonomia_pct' in df_cont.columns:
            media = df_cont['taxa_autonomia_pct'].mean()
            st.metric("Média Autonomia", f"{media:.1f}%",
                      help="Média da taxa de autonomia dos contadores. Indica capacidade geral de regularização.")
    with col4:
        if 'score_performance' in df_cont.columns:
            media_score = df_cont['score_performance'].mean()
            st.metric("Score Médio", f"{media_score:.1f}",
                      help="Pontuação média de performance dos contadores. Varia de 0 a 100.")
    
    st.divider()
    
    tab1, tab2 = st.tabs(["📋 Ranking", "📊 Distribuição"])
    
    with tab1:
        st.markdown("<div class='sub-header'>📋 Ranking de Contadores</div>", unsafe_allow_html=True)
        
        col_config = {
            'nu_cpf_cnpj_contador': st.column_config.TextColumn('CPF/CNPJ'),
            'nm_contador': st.column_config.TextColumn('Nome'),
            'qtd_empresas': st.column_config.NumberColumn('Empresas', format='%d'),
            'qtd_inconsistencias': st.column_config.NumberColumn('Inconsist.', format='%d'),
            'valor_inconsistencias': st.column_config.NumberColumn('Valor', format='R$ %.0f'),
            'taxa_autonomia_pct': st.column_config.ProgressColumn('Autonomia', format='%.1f%%', min_value=0, max_value=100),
            'taxa_em_fiscalizacao_pct': st.column_config.ProgressColumn('% Fiscal.', format='%.1f%%', min_value=0, max_value=50),
            'score_performance': st.column_config.ProgressColumn('Score', format='%.0f', min_value=0, max_value=100),
            'nivel_performance': st.column_config.TextColumn('Nível')
        }
        
        cols = ['nu_cpf_cnpj_contador', 'nm_contador', 'qtd_empresas', 'qtd_inconsistencias',
               'valor_inconsistencias', 'taxa_autonomia_pct', 'taxa_em_fiscalizacao_pct', 'score_performance', 'nivel_performance']
        cols_exist = [c for c in cols if c in df_cont.columns]
        
        ordem = st.radio("Ordenar por:", ['Maior Score', 'Mais Empresas', 'Menor Autonomia'], horizontal=True, key='ord_cont')
        if ordem == 'Maior Score' and 'score_performance' in df_cont.columns:
            df_ord = df_cont.sort_values('score_performance', ascending=False)
        elif ordem == 'Mais Empresas' and 'qtd_empresas' in df_cont.columns:
            df_ord = df_cont.sort_values('qtd_empresas', ascending=False)
        elif ordem == 'Menor Autonomia' and 'taxa_autonomia_pct' in df_cont.columns:
            df_ord = df_cont.sort_values('taxa_autonomia_pct', ascending=True)
        else:
            df_ord = df_cont
        
        st.dataframe(df_ord[cols_exist].head(100), column_config=col_config, use_container_width=True, hide_index=True, height=500)
    
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("<div class='sub-header'>📊 Distribuição por Nível</div>", unsafe_allow_html=True)
            if 'nivel_performance' in df_cont.columns:
                dist = df_cont['nivel_performance'].value_counts().reset_index()
                dist.columns = ['Nível', 'Quantidade']
                cores = {'EXCELENTE': '#10b981', 'BOM': '#3b82f6', 'REGULAR': '#fbbf24', 'BAIXO': '#f97316', 'CRITICO': '#ef4444'}
                fig = px.pie(dist, values='Quantidade', names='Nível', color='Nível', color_discrete_map=cores, hole=0.4)
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("<div class='sub-header'>📈 Distribuição de Score</div>", unsafe_allow_html=True)
            if 'score_performance' in df_cont.columns:
                fig = px.histogram(df_cont, x='score_performance', nbins=20, color_discrete_sequence=['#3b82f6'])
                fig.update_layout(height=350, xaxis_title="Score", yaxis_title="Quantidade")
                st.plotly_chart(fig, use_container_width=True)


# =============================================================================
# PÁGINA: ANÁLISE TEMPORAL
# =============================================================================

def pagina_analise_temporal(dados, filtros):
    st.markdown("<h1 class='main-header'>📈 Análise Temporal</h1>", unsafe_allow_html=True)

    # Dica de contexto
    st.markdown("""
    <div class='context-tip'>
    <b>💡 Análise de tendências:</b> Acompanhe a evolução mensal das inconsistências e taxas.
    Os deltas (▲▼) mostram a variação em relação ao mês anterior.
    Tendências positivas na taxa de autonomia indicam melhoria no processo de autorregularização.
    </div>
    """, unsafe_allow_html=True)

    df_evol = dados.get('evolucao_mensal', pd.DataFrame())
    df_evol_daf = dados.get('evolucao_mensal_daf', pd.DataFrame())

    if df_evol.empty:
        st.error("Dados não disponíveis.")
        return

    # KPIs
    st.markdown("<div class='sub-header'>📊 Evolução Geral</div>", unsafe_allow_html=True)

    if 'nu_per_ref' in df_evol.columns:
        df_evol_sorted = df_evol.sort_values('nu_per_ref', ascending=False)

        if len(df_evol_sorted) >= 2:
            ultimo = df_evol_sorted.iloc[0]
            penultimo = df_evol_sorted.iloc[1]

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                qtd_atual = ultimo.get('qtd_ativas', 0)
                qtd_ant = penultimo.get('qtd_ativas', 0)
                delta = qtd_atual - qtd_ant if qtd_ant > 0 else 0
                st.metric("Ativas (Último Mês)", f"{int(qtd_atual):,}", delta=f"{int(delta):,}",
                          help="Quantidade de inconsistências ativas no último mês. Delta mostra variação vs mês anterior.")

            with col2:
                res_atual = ultimo.get('qtd_resolvidas_malha', 0)
                res_ant = penultimo.get('qtd_resolvidas_malha', 0)
                delta = res_atual - res_ant if res_ant > 0 else 0
                st.metric("Resol. Malha", f"{int(res_atual):,}", delta=f"{int(delta):,}",
                          help="Inconsistências resolvidas pelo contribuinte no último mês. Aumento é positivo.")

            with col3:
                taxa_atual = ultimo.get('taxa_autonomia_pct', 0)
                taxa_ant = penultimo.get('taxa_autonomia_pct', 0)
                delta = taxa_atual - taxa_ant
                st.metric("Taxa Autonomia", f"{taxa_atual:.1f}%", delta=f"{delta:.1f}%",
                          help="Taxa de autonomia do último mês. Meta: ≥60%. Aumento indica melhoria.")

            with col4:
                valor_atual = ultimo.get('valor_ativas', 0)
                st.metric("Valor Ativas", formatar_valor_br(valor_atual),
                          help="Valor total de ICMS das inconsistências ativas no último mês.")
    
    st.divider()
    
    tab1, tab2 = st.tabs(["📈 Evolução Geral", "🏢 Por DAF"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("<div class='sub-header'>📈 Inconsistências Ativas</div>", unsafe_allow_html=True)
            if 'nu_per_ref' in df_evol.columns and 'qtd_ativas' in df_evol.columns:
                df_plot = df_evol.sort_values('nu_per_ref').tail(24)
                fig = px.line(df_plot, x='nu_per_ref', y='qtd_ativas', markers=True)
                fig.update_layout(height=300, xaxis_title="Período", yaxis_title="Quantidade")
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("<div class='sub-header'>📈 Taxa de Autonomia</div>", unsafe_allow_html=True)
            if 'nu_per_ref' in df_evol.columns and 'taxa_autonomia_pct' in df_evol.columns:
                df_plot = df_evol.sort_values('nu_per_ref').tail(24)
                fig = px.line(df_plot, x='nu_per_ref', y='taxa_autonomia_pct', markers=True, color_discrete_sequence=['#10b981'])
                fig.add_hline(y=60, line_dash="dash", line_color="green", annotation_text="Meta 60%")
                fig.update_layout(height=300, xaxis_title="Período", yaxis_title="Taxa (%)")
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("<div class='sub-header'>📊 Comparativo Mensal</div>", unsafe_allow_html=True)
        if 'nu_per_ref' in df_evol.columns:
            df_plot = df_evol.sort_values('nu_per_ref').tail(12)
            fig = go.Figure()
            if 'qtd_ativas' in df_plot.columns:
                fig.add_trace(go.Bar(x=df_plot['nu_per_ref'].astype(str), y=df_plot['qtd_ativas'], name='Ativas', marker_color='#f59e0b'))
            if 'qtd_resolvidas_malha' in df_plot.columns:
                fig.add_trace(go.Bar(x=df_plot['nu_per_ref'].astype(str), y=df_plot['qtd_resolvidas_malha'], name='Resolvidas', marker_color='#10b981'))
            fig.update_layout(height=350, barmode='group', xaxis_title="Período", yaxis_title="Quantidade")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.markdown("<div class='sub-header'>🏢 Evolução por DAF</div>", unsafe_allow_html=True)
        
        if not df_evol_daf.empty and 'id_equipe' in df_evol_daf.columns:
            equipes = df_evol_daf['id_equipe'].unique()
            equipe_sel = st.selectbox("Selecione a DAF:", equipes, key='evol_daf_sel')
            
            df_daf = df_evol_daf[df_evol_daf['id_equipe'] == equipe_sel].sort_values('nu_per_ref')
            
            if not df_daf.empty:
                col1, col2 = st.columns(2)
                
                with col1:
                    if 'qtd_ativas' in df_daf.columns:
                        fig = px.line(df_daf, x='nu_per_ref', y='qtd_ativas', markers=True, title="Ativas")
                        fig.update_layout(height=300)
                        st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    if 'taxa_autonomia_pct' in df_daf.columns:
                        fig = px.line(df_daf, x='nu_per_ref', y='taxa_autonomia_pct', markers=True, title="Taxa Autonomia", color_discrete_sequence=['#10b981'])
                        fig.add_hline(y=60, line_dash="dash", line_color="green")
                        fig.update_layout(height=300)
                        st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Dados de evolução por DAF não disponíveis.")


def pagina_alertas(dados, filtros):
    st.markdown("<h1 class='main-header'>⚠️ Central de Alertas</h1>", unsafe_allow_html=True)

    # Dica de contexto
    st.markdown("""
    <div class='context-tip'>
    <b>💡 Sobre os alertas:</b> O sistema monitora automaticamente duas situações críticas:
    <b>Autonomia Baixa</b> (taxa < 40%) indica que contribuintes não estão regularizando espontaneamente.
    <b>Fiscalização Alta</b> (taxa > 25%) indica excesso de intervenção fiscal.
    </div>
    """, unsafe_allow_html=True)

    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    if df_dafs.empty:
        st.error("Dados não disponíveis.")
        return

    alertas_autonomia = df_dafs[df_dafs.get('flag_alerta_autonomia_baixa', 0) == 1] if 'flag_alerta_autonomia_baixa' in df_dafs.columns else pd.DataFrame()
    alertas_fiscalizacao = df_dafs[df_dafs.get('flag_alerta_fiscalizacao_alta', 0) == 1] if 'flag_alerta_fiscalizacao_alta' in df_dafs.columns else pd.DataFrame()

    col1, col2, col3, col4 = st.columns(4)
    total_alertas = len(alertas_autonomia) + len(alertas_fiscalizacao)
    with col1:
        st.metric("Total Alertas", total_alertas,
                  help="Soma de todos os alertas ativos (autonomia baixa + fiscalização alta).")
    with col2:
        st.metric("🎯 Autonomia Baixa", len(alertas_autonomia),
                  help="DAFs com taxa de autonomia inferior a 40%. Contribuintes não estão regularizando espontaneamente.")
    with col3:
        st.metric("🚨 Fiscalização Alta", len(alertas_fiscalizacao),
                  help="DAFs com taxa de fiscalização superior a 25%. Muitas inconsistências exigindo ação fiscal.")
    with col4:
        pct = (total_alertas / len(df_dafs) * 100) if len(df_dafs) > 0 else 0
        st.metric("% em Alerta", f"{pct:.1f}%",
                  help="Percentual de DAFs que possuem ao menos um alerta ativo.")
    
    st.divider()
    
    tab1, tab2 = st.tabs(["🎯 Autonomia Baixa", "🚨 Fiscalização Alta"])
    
    with tab1:
        if not alertas_autonomia.empty:
            st.markdown(f"<div class='alert-critico'><b>⚠️ {len(alertas_autonomia)} DAFs com autonomia < 40%</b></div>", unsafe_allow_html=True)
            cols = ['id_equipe', 'nm_equipe', 'taxa_autonomia_pct', 'qtd_empresas_acompanhadas', 'ind_autonomia_nivel']
            cols_exist = [c for c in cols if c in alertas_autonomia.columns]
            st.dataframe(alertas_autonomia[cols_exist].sort_values('taxa_autonomia_pct'), hide_index=True, use_container_width=True)
        else:
            st.success("✅ Nenhum alerta de autonomia baixa!")
    
    with tab2:
        if not alertas_fiscalizacao.empty:
            st.markdown(f"<div class='alert-atencao'><b>🚨 {len(alertas_fiscalizacao)} DAFs com fiscalização > 25%</b></div>", unsafe_allow_html=True)
            cols = ['id_equipe', 'nm_equipe', 'taxa_em_fiscalizacao_pct', 'qtd_em_fiscalizacao', 'qtd_empresas_acompanhadas']
            cols_exist = [c for c in cols if c in alertas_fiscalizacao.columns]
            st.dataframe(alertas_fiscalizacao[cols_exist].sort_values('taxa_em_fiscalizacao_pct', ascending=False), hide_index=True, use_container_width=True)
        else:
            st.success("✅ Nenhum alerta de fiscalização alta!")


def pagina_drill_down_daf(dados, filtros):
    st.markdown("<h1 class='main-header'>🔎 Drill-Down por DAF</h1>", unsafe_allow_html=True)

    # Dica de contexto
    st.markdown("""
    <div class='context-tip'>
    <b>💡 Análise detalhada:</b> Selecione uma DAF para ver seus indicadores específicos.
    Os cards coloridos indicam o nível de performance. Veja a evolução temporal e identifique tendências.
    </div>
    """, unsafe_allow_html=True)

    df_dafs = dados.get('performance_dafs', pd.DataFrame())
    df_evol_daf = dados.get('evolucao_mensal_daf', pd.DataFrame())

    if df_dafs.empty:
        st.error("Dados não disponíveis.")
        return

    dafs_opcoes = df_dafs[['id_equipe', 'nm_equipe']].drop_duplicates()
    dafs_opcoes['label'] = dafs_opcoes['id_equipe'].astype(str) + ' - ' + dafs_opcoes['nm_equipe'].fillna('')
    daf_sel = st.selectbox("Selecione DAF:", dafs_opcoes['label'].tolist(),
                           help="Escolha uma Delegacia de Fiscalização para ver seus indicadores detalhados.")

    if not daf_sel:
        return

    id_equipe = int(daf_sel.split(' - ')[0])
    df_daf = df_dafs[df_dafs['id_equipe'] == id_equipe].iloc[0]

    st.divider()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        nivel = df_daf.get('ind_autonomia_nivel', 'N/A')
        criar_card_indicador("Taxa Autonomia", f"{df_daf.get('taxa_autonomia_pct', 0):.1f}%", nivel, "🎯",
                             tooltip=TOOLTIPS['taxa_autonomia'])
    with col2:
        taxa_fisc = df_daf.get('taxa_em_fiscalizacao_pct', 0)
        nivel_fisc = 'EXCELENTE' if taxa_fisc <= 10 else 'BOM' if taxa_fisc <= 20 else 'MEDIO' if taxa_fisc <= 30 else 'CRITICO'
        criar_card_indicador("Taxa Fiscalização", f"{taxa_fisc:.1f}%", nivel_fisc, "🚨",
                             tooltip=TOOLTIPS['taxa_fiscalizacao'])
    with col3:
        st.metric("Empresas", f"{int(df_daf.get('qtd_empresas_acompanhadas', 0)):,}",
                  help="Quantidade de empresas acompanhadas por esta DAF.")
        st.metric("Contadores", f"{int(df_daf.get('qtd_contadores_acompanhados', 0)):,}",
                  help="Quantidade de contadores que atendem empresas desta DAF.")
    with col4:
        st.metric("Valor Existentes", formatar_valor_br(df_daf.get('valor_incons_existentes', 0)),
                  help="Valor de ICMS das inconsistências ativas desta DAF.")
        st.metric("Valor Total", formatar_valor_br(df_daf.get('valor_total_inconsistencias', 0)),
                  help="Valor total de ICMS em inconsistências (ativas + resolvidas) desta DAF.")
    
    st.divider()
    
    tab1, tab2 = st.tabs(["📋 Detalhes", "📈 Evolução"])
    
    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**📊 Quantidades**")
            st.write(f"• Existentes: {int(df_daf.get('qtd_incons_existentes', 0)):,}")
            st.write(f"• Resol. Autônoma: {int(df_daf.get('qtd_resolucao_autonoma', 0)):,}")
            st.write(f"• Resol. Fiscal: {int(df_daf.get('qtd_resolvidas_fiscalizacao', 0)):,}")
            st.write(f"• Em Fiscalização: {int(df_daf.get('qtd_em_fiscalizacao', 0)):,}")
        with col2:
            st.markdown("**💰 Valores**")
            st.write(f"• Existentes: {formatar_valor_br(df_daf.get('valor_incons_existentes', 0))}")
            st.write(f"• Resol. Autônoma: {formatar_valor_br(df_daf.get('valor_resolucao_autonoma', 0))}")
            st.write(f"• Em Fiscalização: {formatar_valor_br(df_daf.get('valor_em_fiscalizacao', 0))}")
        
        alertas = []
        if df_daf.get('flag_alerta_autonomia_baixa', 0) == 1:
            alertas.append("🎯 Autonomia baixa")
        if df_daf.get('flag_alerta_fiscalizacao_alta', 0) == 1:
            alertas.append("🚨 Fiscalização alta")
        
        if alertas:
            st.markdown(f"<div class='alert-atencao'><b>⚠️ Alertas:</b> {', '.join(alertas)}</div>", unsafe_allow_html=True)
        else:
            st.markdown("<div class='alert-positivo'><b>✅ Sem alertas</b></div>", unsafe_allow_html=True)
    
    with tab2:
        if not df_evol_daf.empty:
            df_evol_sel = df_evol_daf[df_evol_daf['id_equipe'] == id_equipe].sort_values('nu_per_ref')
            if not df_evol_sel.empty:
                fig = px.line(df_evol_sel, x='nu_per_ref', y='taxa_autonomia_mensal', markers=True)
                fig.add_hline(y=60, line_dash="dash", line_color="green")
                fig.update_layout(height=300, xaxis_title="Período", yaxis_title="Taxa (%)")
                st.plotly_chart(fig, use_container_width=True)


def pagina_sobre(dados, filtros):
    st.markdown("<h1 class='main-header'>ℹ️ Sobre o Sistema</h1>", unsafe_allow_html=True)

    st.markdown("""
    ## Sistema de Monitoramento de Malhas Fiscais - V3.0

    ### 📊 Fluxo das Inconsistências:

    ```
    ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
    │   IDENTIFICADA  │ ──► │      ATIVA      │ ──► │    RESOLVIDA    │
    │ Sistema detecta │     │ Prazo p/ regular│     │   Conclusão     │
    └─────────────────┘     └────────┬────────┘     └─────────────────┘
                                     │
                          ┌──────────┴──────────┐
                          ▼                     ▼
                ┌─────────────────┐   ┌─────────────────┐
                │  Autorregular.  │   │      PAF        │
                │  (DDE/Retific.) │   │  Fiscalização   │
                └─────────────────┘   └─────────────────┘
    ```

    ### 📈 Métricas e Metas:
    """)

    col1, col2 = st.columns(2)
    with col1:
        st.markdown("""
        <div class='alert-positivo'>
        <b>🎯 Taxa de Autonomia</b><br>
        <b>Meta: ≥ 60%</b><br>
        Percentual de inconsistências resolvidas pelo próprio contribuinte.
        Quanto maior, melhor a autorregularização.
        </div>
        """, unsafe_allow_html=True)
    with col2:
        st.markdown("""
        <div class='alert-atencao'>
        <b>🚨 Taxa de Fiscalização</b><br>
        <b>Meta: ≤ 20%</b><br>
        Percentual que exigiu ação fiscal.
        Quanto menor, melhor a eficiência do sistema.
        </div>
        """, unsafe_allow_html=True)

    st.markdown("""
    ### ⚠️ Pontos de Atenção:
    """)

    st.markdown("""
    <div class='alert-critico'>
    <b>Exclusões Manuais (cd_situacao = 11)</b><br>
    "Resolvida Fiscal" pode incluir <b>exclusões por auditores</b> que precisam ser monitoradas!
    Taxas de exclusão acima de 10% merecem investigação.
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    ### 📊 Legenda de Níveis:
    """)

    st.markdown("""
    <div class='legenda-cores'>
        <div class='legenda-item'><div class='legenda-cor' style='background:#10b981'></div><span>EXCELENTE (≥80)</span></div>
        <div class='legenda-item'><div class='legenda-cor' style='background:#84cc16'></div><span>BOM (60-79)</span></div>
        <div class='legenda-item'><div class='legenda-cor' style='background:#fbbf24'></div><span>MÉDIO (40-59)</span></div>
        <div class='legenda-item'><div class='legenda-cor' style='background:#f97316'></div><span>BAIXO (20-39)</span></div>
        <div class='legenda-item'><div class='legenda-cor' style='background:#ef4444'></div><span>CRÍTICO (<20)</span></div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("""
    ### 📋 Glossário de Termos:
    """)

    with st.expander("Ver glossário completo"):
        st.markdown("""
        | Termo | Descrição |
        |-------|-----------|
        | **DAF** | Delegacia de Fiscalização - unidade responsável pelo acompanhamento |
        | **Inconsistência** | Divergência fiscal detectada pelo sistema (ex: DIME x NFe) |
        | **Existente** | Inconsistência ativa aguardando regularização |
        | **Resolvida Malha** | Regularizada pelo contribuinte (autorregularização) |
        | **Resolvida Fiscal** | Resolvida via PAF ou exclusão por auditor |
        | **Em Fiscalização** | Em processo de fiscalização (PAF aberto) |
        | **DDE** | Declaração de Débitos Estaduais |
        | **PAF** | Processo Administrativo Fiscal |
        | **Score** | Pontuação de performance (0-100) |
        """)

    st.markdown("""
    ---
    **Versão:** 3.0 | **Schema:** niat.mlh_* | **Desenvolvido por:** SEFAZ/SC
    """)
    kpis = calcular_kpis_gerais(dados)
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Empresas", f"{kpis['total_empresas']:,}")
    with col2:
        st.metric("DAFs", f"{kpis['total_dafs']}")
    with col3:
        st.metric("Taxa Autonomia", f"{kpis['taxa_autonomia']:.1f}%")
    with col4:
        st.metric("Valor Total", formatar_valor_br(kpis['total_valor']))

# =============================================================================
# FUNÇÃO PRINCIPAL
# =============================================================================

def main():
    st.sidebar.markdown("""
    <div style='text-align: center; padding: 12px; background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%); border-radius: 10px; margin-bottom: 12px;'>
        <h2 style='color: white; margin: 0;'>🎯</h2>
        <p style='color: white; margin: 0; font-size: 0.8rem;'>DAFs V3.0</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.sidebar.markdown("### 📋 Menu")
    
    paginas = {
        "📊 Dashboard Executivo": pagina_dashboard_executivo,
        "🔍 Análise Exclusões/Fiscal": pagina_analise_exclusoes,
        "🔎 Exclusões Detalhadas": pagina_exclusoes_detalhada,
        "🏢 Performance DAFs": pagina_performance_dafs,
        "🔍 Tipos de Inconsistência": pagina_tipos_inconsistencia,
        "👥 Performance Contadores": pagina_performance_contadores,
        "📈 Análise Temporal": pagina_analise_temporal,
        "⚠️ Alertas": pagina_alertas,
        "🔎 Drill-Down DAF": pagina_drill_down_daf,
        "ℹ️ Sobre": pagina_sobre
    }
    
    pagina_sel = st.sidebar.radio("Página", list(paginas.keys()), label_visibility="collapsed")
    st.sidebar.markdown("---")
    
    engine = get_impala_engine()
    if engine is None:
        st.error("❌ Sem conexão.")
        st.stop()
    
    with st.spinner('⏳ Carregando...'):
        dados = carregar_dados_sistema(engine)
    
    if not dados:
        st.error("❌ Falha no carregamento.")
        st.stop()
    
    kpis = calcular_kpis_gerais(dados)
    st.sidebar.info(f"🏢 {kpis['total_empresas']:,} empresas\n👥 {kpis['total_dafs']} DAFs\n🎯 {kpis['taxa_autonomia']:.1f}% autonomia")
    
    filtros = criar_filtros_sidebar(dados)
    st.sidebar.markdown("---")
    st.sidebar.caption(f"V3.0 | {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    
    try:
        paginas[pagina_sel](dados, filtros)
    except Exception as e:
        st.error(f"❌ Erro: {str(e)}")
        with st.expander("Detalhes"):
            st.exception(e)
    
    st.markdown("---")
    st.markdown(f"<div style='text-align: center; color: #666; font-size: 0.75rem;'>Sistema MLH V3.0 | SEFAZ/SC | {datetime.now().strftime('%d/%m/%Y %H:%M')}</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    main()