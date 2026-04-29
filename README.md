# 📊 Data Analyzer — Excel & Database Inspector

> Ferramenta Python de análise exploratória automática para tabelas Excel, CSVs e bancos de dados relacionais.

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat-square&logo=python&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2.x-150458?style=flat-square&logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.x-D71F00?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-22c55e?style=flat-square)
![Status](https://img.shields.io/badge/Status-Em%20desenvolvimento-f59e0b?style=flat-square)

---

## 🎯 Sobre o projeto

O **Data Analyzer** é um script Python de linha de comando que realiza análise exploratória detalhada (EDA) de qualquer fonte tabular — arquivos `.xlsx`, `.csv`, bancos SQLite ou PostgreSQL — sem necessidade de configuração prévia.

Desenvolvido como parte do meu portfólio de transição para a área de **Dados & Business Intelligence**, com foco em automação de análises recorrentes que normalmente consomem tempo manual no Power BI ou Excel.

---

## ✨ Funcionalidades

| Recurso | Descrição |
|---|---|
| 🔍 **EDA Automática** | Dimensões, tipos, nulos, duplicatas e outliers em segundos |
| 📈 **Estatísticas numéricas** | Média, desvio padrão, min, max e contagem de outliers via IQR |
| 🏷️ **Análise categórica** | Cardinalidade e top-3 valores mais frequentes por coluna |
| 📅 **Detecção de datas** | Intervalo mínimo/máximo de colunas temporais |
| 📝 **Relatório HTML** | Geração de relatório interativo completo via `ydata-profiling` |
| 🗄️ **Multi-fonte** | Excel, CSV, SQLite e PostgreSQL com um único script |

---

## 🚀 Como usar

### 1. Clone o repositório

```bash
git clone https://github.com/seu-usuario/data-analyzer.git
cd data-analyzer
```

### 2. Instale as dependências

```bash
pip install pandas openpyxl sqlalchemy ydata-profiling
```

### 3. Execute

```bash
# Modo demo (dados fictícios para testar)
python data_analyzer.py

# Analisar um arquivo Excel
python data_analyzer.py --excel vendas.xlsx

# Analisar um CSV
python data_analyzer.py --csv clientes.csv

# Analisar banco SQLite
python data_analyzer.py --sqlite banco.db

# Analisar PostgreSQL
python data_analyzer.py --pg "postgresql://user:senha@localhost/meu_banco"

# Gerar relatório HTML interativo
python data_analyzer.py --excel vendas.xlsx --report --out ./relatorios
```

---

## 📂 Estrutura do projeto

```
data-analyzer/
├── data_analyzer.py     # Script principal
├── requirements.txt     # Dependências
├── README.md            # Documentação
└── exemplos/
    └── demo.xlsx        # Arquivo de exemplo para testes
```

---

## 🖥️ Exemplo de saída

```
════════════════════════════════════════════════════════════
  📊 ANÁLISE: VENDAS_2024
════════════════════════════════════════════════════════════

📐 DIMENSÕES
   Linhas:   12.480
   Colunas:  9

🚨 VALORES NULOS
   Coluna                         Nulos        %
   ────────────────────────────────────────────
   desconto_pct                     342     2.7%
   cliente                           18     0.1%

🔁 DUPLICATAS
   Linhas duplicadas: 5 (0.0%)

📈 ESTATÍSTICAS NUMÉRICAS
              count    mean     std    min    50%    max  outliers_iqr
   valor     12480  148.32   95.41   2.50  130.00  980.0            87
   quantidade 12480    4.21    2.89   1.00    4.00   10.0             0
```

---

## 🛠️ Tecnologias utilizadas

- **[Pandas](https://pandas.pydata.org/)** — manipulação e análise de dados
- **[OpenPyXL](https://openpyxl.readthedocs.io/)** — leitura de arquivos `.xlsx`
- **[SQLAlchemy](https://www.sqlalchemy.org/)** — conexão com bancos de dados relacionais
- **[ydata-profiling](https://docs.profiling.ydata.ai/)** — geração de relatórios HTML interativos
- **[NumPy](https://numpy.org/)** — cálculos estatísticos

---

## 📌 Roadmap

- [x] Leitura de Excel (múltiplas abas)
- [x] Leitura de CSV com detecção automática de separador
- [x] Conexão com SQLite e PostgreSQL
- [x] Análise de nulos, duplicatas e outliers
- [x] Geração de relatório HTML
- [ ] Exportação de resumo para Excel formatado
- [ ] Interface web simples com Streamlit
- [ ] Suporte a MySQL e SQL Server
- [ ] Detecção automática de colunas de ID e chaves candidatas

---

## 👤 Autor

**Nilvan Filipe**
Analista de Dados em formação | Power BI · DAX · Python · SQL

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Nilvan%20Filipe-0A66C2?style=flat-square&logo=linkedin)](https://linkedin.com/in/seu-perfil)
[![GitHub](https://img.shields.io/badge/GitHub-@seu--usuario-181717?style=flat-square&logo=github)](https://github.com/seu-usuario)

---

## 📄 Licença

Distribuído sob a licença MIT. Veja `LICENSE` para mais informações. 
