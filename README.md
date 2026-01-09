# DAFs V3.0 - Sistema de Monitoramento de Malhas Fiscais

Sistema de monitoramento e gestão de malhas fiscais para a Receita Estadual de Santa Catarina (SEFAZ/SC). O sistema rastreia inconsistências em declarações fiscais (DAFs - Delegacias de Fiscalização) e monitora taxas de resolução através de diversos canais.

## Sobre o Projeto

O DAFs V3.0 é uma aplicação web desenvolvida para acompanhar e analisar questões de conformidade fiscal em empresas de Santa Catarina, Brasil. O sistema fornece análises em tempo real sobre inconsistências em declarações tributárias e sua resolução através de conformidade autônoma ou intervenção fiscal.

### Principais Funcionalidades

- **Dashboard Executivo** - Visão geral de KPIs e métricas de performance
- **Análise de Exclusões** - Monitoramento de taxas de exclusão e avaliação de risco
- **Performance DAFs** - Métricas de produtividade por delegacia
- **Tipos de Inconsistência** - Classificação e análise por tipo
- **Performance Contadores** - Métricas individuais por contador
- **Análise Temporal** - Tendências históricas e comparações período a período
- **Sistema de Alertas** - Notificações de condições críticas e anomalias
- **Drill-Down Detalhado** - Análise granular por DAF específica

## Arquitetura

```
┌─────────────────────────────────────────────┐
│   Interface Web Streamlit (DAF V3.0)        │
├─────────────────────────────────────────────┤
│  ├─ Camada de Autenticação                  │
│  ├─ 10 Páginas de Dashboard                 │
│  ├─ Estilização CSS Customizada             │
│  └─ Filtros e Navegação via Sidebar         │
├─────────────────────────────────────────────┤
│   Camada de Processamento de Dados          │
│  ├─ SQLAlchemy ORM                          │
│  ├─ Pandas DataFrames                       │
│  ├─ Cache de Dados (TTL 1h)                 │
│  └─ Cálculos de KPIs                        │
├─────────────────────────────────────────────┤
│   Camada de Visualização                    │
│  ├─ Gráficos Plotly Express                 │
│  ├─ Métricas e Indicadores                  │
│  └─ Gráficos Interativos                    │
├─────────────────────────────────────────────┤
│   Backend Apache Impala                     │
│  ├─ Tabelas MLH_* (Malhas Fiscais)          │
│  ├─ 15+ Tabelas Analíticas                  │
│  └─ Autenticação LDAP                       │
└─────────────────────────────────────────────┘
```

## Tecnologias Utilizadas

### Framework Principal
- **Streamlit** - Framework web para dashboards de dados
- **Python 3.x** - Linguagem de programação principal

### Processamento e Análise de Dados
- **Pandas** - Manipulação e análise de dados
- **NumPy** - Computação numérica
- **SQLAlchemy** - ORM para operações de banco de dados

### Visualização
- **Plotly Express** - Gráficos e visualizações interativas
- **Plotly Graph Objects** - Recursos avançados de gráficos

### Banco de Dados
- **Apache Impala** - Motor de consulta SQL para big data

### Autenticação e Segurança
- **Hashlib** - Hashing de senhas
- **SSL/TLS** - Autenticação LDAP segura

## Instalação

### Pré-requisitos

- Python 3.8 ou superior
- Acesso ao banco de dados Impala da SEFAZ/SC
- Credenciais LDAP válidas

### Dependências

```bash
pip install streamlit pandas numpy plotly sqlalchemy impyla
```

Ou crie um arquivo `requirements.txt`:

```txt
streamlit>=1.28.0
pandas>=2.0.0
numpy>=1.24.0
plotly>=5.15.0
sqlalchemy>=2.0.0
impyla>=0.18.0
```

E instale com:

```bash
pip install -r requirements.txt
```

### Configuração

1. **Configure as credenciais do Streamlit**

   Crie o arquivo `.streamlit/secrets.toml`:

   ```toml
   [impala_credentials]
   user = "seu_usuario_ldap"
   password = "sua_senha_ldap"
   ```

2. **Verifique a conexão com o banco de dados**

   O sistema se conecta ao servidor:
   - **Host:** `bdaworkernode02.sef.sc.gov.br`
   - **Porta:** 21050
   - **Database:** `niat`

## Executando a Aplicação

```bash
streamlit run "DAF (6).py"
```

Acesse a aplicação em: `http://localhost:8501`

## Estrutura do Projeto

```
DAF_NEW/
├── DAF (6).py                    # Aplicação principal (V3.0)
├── DAF old projeto antigo.py     # Versão legada (referência)
├── DAFS MALHAS (1).json          # Metadados e configuração do banco
└── README.md                     # Este arquivo
```

### Descrição dos Arquivos

| Arquivo | Descrição |
|---------|-----------|
| `DAF (6).py` | Versão atual de produção (V3.0) com todas as funcionalidades do dashboard |
| `DAF old projeto antigo.py` | Versão anterior com features de ML (scikit-learn) mantida para referência |
| `DAFS MALHAS (1).json` | Definições de tabelas, queries SQL e schema do sistema MLH |

## Páginas do Dashboard

| Página | Descrição |
|--------|-----------|
| Dashboard Executivo | KPIs gerais, indicadores de performance e distribuição de status |
| Análise Exclusões/Fiscal | Análise de resolução fiscal e monitoramento de exclusões |
| Exclusões Detalhadas | Análise aprofundada de dados de exclusão |
| Performance DAFs | Métricas de performance por delegacia |
| Tipos de Inconsistência | Classificação e taxas de resolução por tipo |
| Performance Contadores | Métricas individuais por contador |
| Análise Temporal | Tendências históricas e séries temporais |
| Alertas | Alertas de condições críticas e detecção de anomalias |
| Drill-Down DAF | Investigação detalhada por DAF específica |
| Sobre | Documentação e informações do sistema |

## KPIs Principais

### Métricas Monitoradas

- **Total de Empresas** - Empresas com inconsistências monitoradas
- **Quantidade de DAFs** - Número de delegacias de fiscalização
- **Quantidade de Contadores** - Profissionais contábeis ativos
- **Tipos de Inconsistência** - Tipos distintos identificados
- **Valor Total** - Montantes de ICMS envolvidos
- **Taxa de Autonomia** (Meta: ≥60%) - Percentual de auto-resolução
- **Taxa de Intervenção Fiscal** (Meta: ≤20%) - Necessidade de PAF/auditoria

### Canais de Resolução de Inconsistências

| Canal | Descrição |
|-------|-----------|
| `AUTONOMO_DDE` | Resolução autônoma via entrada direta |
| `AUTONOMO_MALHA` | Resolução autônoma via retificação de malha |
| `EXCLUSAO_AUDITOR` | Exclusão por auditor (pré-fiscal) |
| `EM_FISCALIZACAO` | Processo fiscal ativo (PAF) |
| `FISCALIZACAO_CONCLUIDA` | Investigação fiscal concluída |
| `ATIVA` | Inconsistências ativas/pendentes |

### Classificação de Risco

| Nível | Cor | Descrição |
|-------|-----|-----------|
| CRÍTICO | Vermelho | Requer ação imediata |
| ALTO | Laranja | Alta prioridade |
| MÉDIO | Amarelo | Atenção necessária |
| BAIXO | Verde | Situação controlada |

## Recursos de UX

### Sistema de Cores

- **Verde (#10b981)** - Excelente performance
- **Amarelo (#84cc16)** - Boa performance
- **Laranja (#f97316)** - Performance média
- **Vermelho (#ef4444)** - Performance crítica

### Funcionalidades

- Tooltips interativos com explicações dos indicadores
- Cards de métricas com efeitos hover
- Layouts responsivos em grid
- Caixas de alerta customizadas
- Indicadores de carregamento
- Tratamento detalhado de erros
- Gerenciamento de sessão para autenticação

## Tabelas do Banco de Dados

O sistema utiliza as seguintes tabelas analíticas:

| Tabela | Descrição |
|--------|-----------|
| `mlh_tipos_inconsistencia` | Catálogo de tipos de inconsistência |
| `mlh_equipes` | Equipes e departamentos |
| `mlh_fiscais` | Auditores fiscais |
| `mlh_performance_dafs` | Métricas de performance das DAFs |
| `mlh_evolucao_mensal` | Dados de evolução mensal |
| `mlh_performance_contadores` | Tracking de performance dos contadores |
| `mlh_exclusoes_detalhadas` | Análise detalhada de exclusões |
| `mlh_kpis_sistema` | KPIs do sistema |
| `mlh_empresas_resumo` | Resumo de empresas |

## Segurança

- **Autenticação por Senha** - Proteção de acesso ao dashboard
- **LDAP Seguro** - Conexão criptografada com o banco de dados
- **SSL/TLS** - Contexto HTTPS configurado
- **Session State** - Autenticação gerenciada via estado de sessão do Streamlit
- **Credenciais** - Armazenadas no Streamlit Secrets (não no código)

## Contribuindo

1. Faça um fork do repositório
2. Crie uma branch para sua feature (`git checkout -b feature/nova-funcionalidade`)
3. Commit suas alterações (`git commit -m 'Adiciona nova funcionalidade'`)
4. Push para a branch (`git push origin feature/nova-funcionalidade`)
5. Abra um Pull Request

## Histórico de Versões

| Versão | Descrição |
|--------|-----------|
| V3.0 | Versão atual com tooltips e melhorias de UX |
| V2.x | Versão com features básicas de ML |
| V1.x | Versão inicial do dashboard |

## Autores

- **Receita Estadual de Santa Catarina** - SEFAZ/SC
- **Núcleo de Inteligência e Análise Tributária** - NIAT

## Licença

Este projeto é de uso interno da SEFAZ/SC. Todos os direitos reservados.

---

**Sistema de Monitoramento de Malhas Fiscais** - Receita Estadual de Santa Catarina
