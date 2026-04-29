"""
╔══════════════════════════════════════════════════════════════╗
║        DATA ANALYZER — Excel & Banco de Dados               ║
║        Por: Nilvan Filipe | Stack: Python + Pandas           ║
╚══════════════════════════════════════════════════════════════╝

USO:
  1. Análise de Excel:   python data_analyzer.py --excel arquivo.xlsx
  2. Análise de SQLite:  python data_analyzer.py --sqlite banco.db
  3. Análise de Postgres: python data_analyzer.py --pg "postgresql://user:pass@host/db"
  4. Relatório completo: adicione --report para gerar HTML

INSTALAÇÃO DAS DEPENDÊNCIAS:
  pip install pandas openpyxl sqlalchemy ydata-profiling matplotlib seaborn
"""

import argparse
import sys
import os
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np


# ──────────────────────────────────────────────
# 1. LEITORES DE FONTE DE DADOS
# ──────────────────────────────────────────────

def ler_excel(caminho: str) -> dict[str, pd.DataFrame]:
    """Lê todas as abas de um arquivo Excel."""
    print(f"\n📂 Lendo Excel: {"C:\Users\Nilvan\OneDrive\01 - Nilvan Pessoal\Documentos\07 - PROJETOS\00 Base - Excell\Base de Dados - Kaggle.com\Olist - E-commerce Brasileiro"}")
    xl = pd.ExcelFile(caminho)
    abas = {}
    for aba in xl.sheet_names:    import os
    import pandas as pd
    
    pasta = r"C:\Users\Nilvan\OneDrive\01 - Nilvan Pessoal\Documentos\07 - PROJETOS\00 Base - Excell\Base de Dados - Kaggle.com\Olist - E-commerce Brasileiro"
    
    # Carregar todos os arquivos CSV
    arquivos = {}
    for arquivo in os.listdir(pasta):
        if arquivo.endswith('.csv'):
            caminho = os.path.join(pasta, arquivo)
            arquivos[arquivo] = pd.read_csv(caminho)
            print(f"✓ {arquivo} carregado ({len(arquivos[arquivo])} linhas)")
        df = xl.parse(aba)
        abas[aba] = df
        print(f"   ✅ Aba '{aba}' → {df.shape[0]} linhas × {df.shape[1]} colunas")
    return abas


def ler_sqlite(caminho: str) -> dict[str, pd.DataFrame]:
    """Lê todas as tabelas de um banco SQLite."""
    from sqlalchemy import create_engine, inspect
    print(f"\n🗄️  Conectando ao SQLite: {caminho}")
    engine = create_engine(f"sqlite:///{caminho}")
    inspector = inspect(engine)
    tabelas = {}
    for tabela in inspector.get_table_names():
        df = pd.read_sql_table(tabela, engine)
        tabelas[tabela] = df
        print(f"   ✅ Tabela '{tabela}' → {df.shape[0]} linhas × {df.shape[1]} colunas")
    return tabelas


def ler_postgres(conn_str: str) -> dict[str, pd.DataFrame]:
    """Lê tabelas públicas de um banco PostgreSQL."""
    from sqlalchemy import create_engine, inspect
    print(f"\n🐘 Conectando ao PostgreSQL...")
    engine = create_engine(conn_str)
    inspector = inspect(engine)
    tabelas = {}
    for tabela in inspector.get_table_names(schema="public"):
        df = pd.read_sql_table(tabela, engine, schema="public")
        tabelas[tabela] = df
        print(f"   ✅ Tabela '{tabela}' → {df.shape[0]} linhas × {df.shape[1]} colunas")
    return tabelas


def ler_csv(caminho: str) -> dict[str, pd.DataFrame]:
    """Lê um CSV com detecção automática de separador."""
    print(f"\n📄 Lendo CSV: {caminho}")
    df = pd.read_csv(caminho, sep=None, engine="python")
    nome = os.path.splitext(os.path.basename(caminho))[0]
    print(f"   ✅ '{nome}' → {df.shape[0]} linhas × {df.shape[1]} colunas")
    return {nome: df}


# ──────────────────────────────────────────────
# 2. ANÁLISE DETALHADA
# ──────────────────────────────────────────────

def analisar_tabela(nome: str, df: pd.DataFrame) -> None:
    """Imprime análise detalhada de uma tabela/aba."""
    sep = "═" * 60
    print(f"\n{sep}")
    print(f"  📊 ANÁLISE: {nome.upper()}")
    print(sep)

    total_linhas, total_colunas = df.shape
    print(f"\n📐 DIMENSÕES")
    print(f"   Linhas:   {total_linhas:,}")
    print(f"   Colunas:  {total_colunas}")

    # ── 2.1 Tipos de Dados ──
    print(f"\n🔠 TIPOS DE DADOS")
    tipos = df.dtypes.value_counts()
    for tipo, qtd in tipos.items():
        print(f"   {str(tipo):<15} → {qtd} coluna(s)")

    # ── 2.2 Nulos ──
    nulos = df.isnull().sum()
    nulos_pct = (nulos / total_linhas * 100).round(2)
    colunas_com_nulo = nulos[nulos > 0]

    print(f"\n🚨 VALORES NULOS")
    if colunas_com_nulo.empty:
        print("   ✅ Nenhuma coluna com valores nulos!")
    else:
        print(f"   {'Coluna':<30} {'Nulos':>8} {'%':>8}")
        print(f"   {'-'*48}")
        for col in colunas_com_nulo.index:
            print(f"   {col:<30} {nulos[col]:>8,} {nulos_pct[col]:>7.1f}%")

    # ── 2.3 Duplicatas ──
    dup = df.duplicated().sum()
    print(f"\n🔁 DUPLICATAS")
    print(f"   Linhas duplicadas: {dup:,} ({dup/total_linhas*100:.1f}%)")

    # ── 2.4 Estatísticas Numéricas ──
    numericas = df.select_dtypes(include=[np.number])
    if not numericas.empty:
        print(f"\n📈 ESTATÍSTICAS NUMÉRICAS")
        stats = numericas.describe().T
        stats["outliers_iqr"] = numericas.apply(_contar_outliers_iqr)
        print(stats[["count", "mean", "std", "min", "50%", "max", "outliers_iqr"]].to_string())

    # ── 2.5 Colunas Categóricas ──
    categoricas = df.select_dtypes(include=["object", "category"])
    if not categoricas.empty:
        print(f"\n🏷️  COLUNAS CATEGÓRICAS")
        for col in categoricas.columns:
            n_unicos = df[col].nunique()
            top3 = df[col].value_counts().head(3)
            print(f"\n   [{col}] — {n_unicos} valores únicos")
            for val, cnt in top3.items():
                print(f"      '{val}' → {cnt:,}x ({cnt/total_linhas*100:.1f}%)")

    # ── 2.6 Colunas de Data ──
    datas = df.select_dtypes(include=["datetime64"])
    if not datas.empty:
        print(f"\n📅 COLUNAS DE DATA")
        for col in datas.columns:
            print(f"   [{col}] min: {df[col].min()} | max: {df[col].max()}")

    print(f"\n{'─'*60}")
    print("  ✅ Análise concluída para esta tabela")
    print(f"{'─'*60}")


def _contar_outliers_iqr(serie: pd.Series) -> int:
    """Conta outliers usando regra IQR (1.5x)."""
    Q1 = serie.quantile(0.25)
    Q3 = serie.quantile(0.75)
    IQR = Q3 - Q1
    return int(((serie < Q1 - 1.5 * IQR) | (serie > Q3 + 1.5 * IQR)).sum())


# ──────────────────────────────────────────────
# 3. RELATÓRIO HTML (ydata-profiling)
# ──────────────────────────────────────────────

def gerar_relatorio_html(nome: str, df: pd.DataFrame, pasta_saida: str = ".") -> None:
    """Gera relatório HTML interativo com ydata-profiling."""
    try:
        from ydata_profiling import ProfileReport
        print(f"\n📝 Gerando relatório HTML para '{nome}'...")
        profile = ProfileReport(
            df,
            title=f"Análise — {nome}",
            explorative=True,
            minimal=False
        )
        caminho_html = os.path.join(pasta_saida, f"relatorio_{nome}.html")
        profile.to_file(caminho_html)
        print(f"   ✅ Relatório salvo em: {caminho_html}")
    except ImportError:
        print("   ⚠️  ydata-profiling não instalado.")
        print("       Execute: pip install ydata-profiling")


# ──────────────────────────────────────────────
# 4. ANÁLISE RÁPIDA (sem argumentos — modo demo)
# ──────────────────────────────────────────────

def modo_demo() -> None:
    """Cria e analisa um DataFrame de exemplo para demonstração."""
    print("\n🎯 MODO DEMO — Nenhuma fonte especificada. Usando dados fictícios.\n")
    np.random.seed(42)
    n = 200

    df_demo = pd.DataFrame({
        "id_pedido":     range(1, n + 1),
        "cliente":       np.random.choice(["Ana", "Carlos", "Maria", "João", None], n),
        "produto":       np.random.choice(["Camisa", "Calça", "Vestido", "Blusa"], n),
        "valor":         np.round(np.random.exponential(scale=150, size=n), 2),
        "quantidade":    np.random.randint(1, 10, n),
        "data_pedido":   pd.date_range("2024-01-01", periods=n, freq="D"),
        "status":        np.random.choice(["Pago", "Pendente", "Cancelado"], n, p=[0.7, 0.2, 0.1]),
        "desconto_pct":  np.random.choice([0, 5, 10, 15, None], n),
    })

    # Introduz algumas duplicatas
    df_demo = pd.concat([df_demo, df_demo.sample(5)], ignore_index=True)

    analisar_tabela("DEMO_CASA_PRADO", df_demo)


# ──────────────────────────────────────────────
# 5. CLI — Interface de Linha de Comando
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="🔍 Analisador de tabelas Excel/BD — Nilvan Filipe",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--excel",  metavar="ARQUIVO.xlsx", help="Analisa um arquivo Excel")
    parser.add_argument("--csv",    metavar="ARQUIVO.csv",  help="Analisa um arquivo CSV")
    parser.add_argument("--sqlite", metavar="BANCO.db",     help="Analisa um banco SQLite")
    parser.add_argument("--pg",     metavar="CONN_STR",     help="Analisa PostgreSQL (connection string)")
    parser.add_argument("--report", action="store_true",    help="Gera relatório HTML (requer ydata-profiling)")
    parser.add_argument("--out",    metavar="PASTA",        default=".", help="Pasta para salvar relatórios")

    args = parser.parse_args()

    # Nenhuma fonte → modo demo
    if not any([args.excel, args.csv, args.sqlite, args.pg]):
        modo_demo()
        return

    # Carrega dados conforme a fonte
    fontes: dict[str, pd.DataFrame] = {}

    if args.excel:
        fontes.update(ler_excel(args.excel))
    if args.csv:
        fontes.update(ler_csv(args.csv))
    if args.sqlite:
        fontes.update(ler_sqlite(args.sqlite))
    if args.pg:
        fontes.update(ler_postgres(args.pg))

    # Analisa cada tabela/aba
    for nome, df in fontes.items():
        analisar_tabela(nome, df)
        if args.report:
            gerar_relatorio_html(nome, df, args.out)

    print(f"\n🏁 Análise finalizada. {len(fontes)} tabela(s) processada(s).\n")


if __name__ == "__main__":
    main()
