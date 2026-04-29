"""
Olist E-commerce — Análise Completa + Dashboard HTML
"""

import os, io, base64, warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns

# ── Paleta ──────────────────────────────────────────────────────────────────
CORES = ["#2563EB", "#16A34A", "#DC2626", "#D97706", "#7C3AED",
         "#DB2777", "#0891B2", "#65A30D", "#EA580C", "#6366F1"]
sns.set_theme(style="whitegrid", palette=CORES, font_scale=1.0)
plt.rcParams["figure.dpi"] = 130
plt.rcParams["axes.spines.top"]   = False
plt.rcParams["axes.spines.right"] = False

PASTA = (r"C:\Users\Nilvan\OneDrive\01 - Nilvan Pessoal\Documentos"
         r"\07 - PROJETOS\00 Base - Excell\Base de Dados - Kaggle.com"
         r"\Olist - E-commerce Brasileiro")

OUT_HTML = os.path.join(
    r"C:\Users\Nilvan\Desktop\Projetos\.projeto - 24 (Data Analyzer)",
    "olist_dashboard.html"
)

# ── Helpers ──────────────────────────────────────────────────────────────────
def fig_to_b64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", facecolor="white")
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode()
    plt.close(fig)
    return b64

def brl(v):
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_n(v):
    return f"{v:,.0f}".replace(",", ".")

# ── 1. CARREGAMENTO ──────────────────────────────────────────────────────────
print("Carregando datasets...")
csv_map = {
    "customers":    "olist_customers_dataset.csv",
    "geolocation":  "olist_geolocation_dataset.csv",
    "order_items":  "olist_order_items_dataset.csv",
    "payments":     "olist_order_payments_dataset.csv",
    "reviews":      "olist_order_reviews_dataset.csv",
    "orders":       "olist_orders_dataset.csv",
    "products":     "olist_products_dataset.csv",
    "sellers":      "olist_sellers_dataset.csv",
    "cat_transl":   "product_category_name_translation.csv",
}
dfs = {k: pd.read_csv(os.path.join(PASTA, v), low_memory=False)
       for k, v in csv_map.items()}

# Parse de datas
date_cols = [
    "order_purchase_timestamp", "order_approved_at",
    "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date"
]
for c in date_cols:
    dfs["orders"][c] = pd.to_datetime(dfs["orders"][c], errors="coerce")

dfs["reviews"]["review_creation_date"] = pd.to_datetime(
    dfs["reviews"]["review_creation_date"], errors="coerce")
dfs["order_items"] = dfs["order_items"].copy()
dfs["order_items"]["shipping_limit_date"] = pd.to_datetime(
    dfs["order_items"]["shipping_limit_date"], errors="coerce")

print("  Datasets carregados.")

# ── 2. SUMÁRIO DE QUALIDADE ──────────────────────────────────────────────────
print("Gerando sumário de qualidade...")
quality_rows = []
for key, df in dfs.items():
    nulos_total = df.isnull().sum().sum()
    nulos_pct   = nulos_total / (df.shape[0] * df.shape[1]) * 100
    dups        = df.duplicated().sum()
    colunas_nulo = (df.isnull().sum() > 0).sum()
    quality_rows.append({
        "Dataset":           key,
        "Linhas":            df.shape[0],
        "Colunas":           df.shape[1],
        "Total Registros":   df.shape[0] * df.shape[1],
        "Nulos":             nulos_total,
        "% Nulos":           round(nulos_pct, 2),
        "Cols c/ Nulos":     colunas_nulo,
        "Duplicatas":        dups,
        "% Dups":            round(dups / df.shape[0] * 100, 2),
    })
quality_df = pd.DataFrame(quality_rows)

# Detalhe de nulos por coluna
nulos_detail = {}
for key, df in dfs.items():
    n = df.isnull().sum()
    n = n[n > 0]
    if not n.empty:
        nulos_detail[key] = n.reset_index()
        nulos_detail[key].columns = ["Coluna", "Nulos"]
        nulos_detail[key]["%"] = (nulos_detail[key]["Nulos"] / len(df) * 100).round(2)

# ── 3. JOINS ────────────────────────────────────────────────────────────────
print("Montando base analítica...")
orders      = dfs["orders"]
items       = dfs["order_items"]
payments    = dfs["payments"]
reviews     = dfs["reviews"]
customers   = dfs["customers"]
products    = dfs["products"]
sellers     = dfs["sellers"]
cat_transl  = dfs["cat_transl"]
geo         = dfs["geolocation"]

# Base principal
base = (orders
        .merge(customers[["customer_id", "customer_state", "customer_city"]], on="customer_id", how="left")
        .merge(payments.groupby("order_id").agg(
            total_payment=("payment_value", "sum"),
            n_payments=("payment_sequential", "max"),
            payment_type=("payment_type", lambda x: x.mode()[0] if len(x) else "unknown"),
            installments=("payment_installments", "max")
        ).reset_index(), on="order_id", how="left")
        .merge(items.groupby("order_id").agg(
            n_items=("order_item_id", "max"),
            revenue=("price", "sum"),
            freight=("freight_value", "sum")
        ).reset_index(), on="order_id", how="left")
        .merge(reviews[["order_id", "review_score"]].drop_duplicates("order_id"),
               on="order_id", how="left")
)

# Apenas pedidos entregues com datas válidas
delivered = base[base["order_status"] == "delivered"].copy()
delivered["delivery_days"] = (
    delivered["order_delivered_customer_date"] -
    delivered["order_purchase_timestamp"]
).dt.days
delivered["delay_days"] = (
    delivered["order_delivered_customer_date"] -
    delivered["order_estimated_delivery_date"]
).dt.days
delivered["on_time"] = delivered["delay_days"] <= 0

# Mês de compra
base["month"] = base["order_purchase_timestamp"].dt.to_period("M")
delivered["month"] = delivered["order_purchase_timestamp"].dt.to_period("M")

# Items enriquecidos
items_rich = (items
    .merge(products[["product_id", "product_category_name"]], on="product_id", how="left")
    .merge(cat_transl, on="product_category_name", how="left")
    .merge(orders[["order_id", "order_status"]], on="order_id", how="left")
)

print("  Base analítica pronta.\n")

# ════════════════════════════════════════════════════════════════════════════
# GRÁFICOS
# ════════════════════════════════════════════════════════════════════════════
charts = {}   # nome -> b64

# ── A1: Qualidade — Barras de nulos por dataset ──────────────────────────────
print("Gerando gráficos...")
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle("Qualidade dos Dados — Visão Geral", fontsize=14, fontweight="bold")

ax = axes[0]
bars = ax.barh(quality_df["Dataset"], quality_df["% Nulos"], color=CORES[:len(quality_df)])
ax.set_xlabel("% de Nulos")
ax.set_title("Percentual de Nulos por Dataset")
for bar, v in zip(bars, quality_df["% Nulos"]):
    ax.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
            f"{v:.1f}%", va="center", fontsize=8)

ax2 = axes[1]
bars2 = ax2.barh(quality_df["Dataset"], quality_df["% Dups"], color=CORES[:len(quality_df)])
ax2.set_xlabel("% de Duplicatas")
ax2.set_title("Percentual de Duplicatas por Dataset")
for bar, v in zip(bars2, quality_df["% Dups"]):
    ax2.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
             f"{v:.1f}%", va="center", fontsize=8)

plt.tight_layout()
charts["qualidade"] = fig_to_b64(fig)

# ── B1: Faturamento Mensal ───────────────────────────────────────────────────
faturamento_mensal = (
    base.dropna(subset=["month", "revenue"])
    .groupby("month")["revenue"].sum()
    .reset_index()
)
faturamento_mensal["month_str"] = faturamento_mensal["month"].astype(str)
faturamento_mensal = faturamento_mensal[
    faturamento_mensal["month_str"].between("2017-01", "2018-09")
]

fig, ax = plt.subplots(figsize=(14, 5))
ax.fill_between(range(len(faturamento_mensal)),
                faturamento_mensal["revenue"] / 1e6,
                alpha=0.3, color=CORES[0])
ax.plot(range(len(faturamento_mensal)),
        faturamento_mensal["revenue"] / 1e6,
        color=CORES[0], linewidth=2.5, marker="o", markersize=5)
ax.set_xticks(range(len(faturamento_mensal)))
ax.set_xticklabels(faturamento_mensal["month_str"], rotation=45, ha="right", fontsize=8)
ax.set_ylabel("Faturamento (R$ Milhões)")
ax.set_title("Faturamento Mensal — Evolução", fontsize=13, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"R${x:.1f}M"))
plt.tight_layout()
charts["faturamento_mensal"] = fig_to_b64(fig)

# ── B2: Top 15 Categorias por Receita ────────────────────────────────────────
cat_revenue = (
    items_rich[items_rich["order_status"] == "delivered"]
    .groupby("product_category_name_english")["price"].sum()
    .dropna().sort_values(ascending=False).head(15).reset_index()
)
cat_revenue.columns = ["categoria", "receita"]

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(cat_revenue["categoria"][::-1],
               cat_revenue["receita"][::-1] / 1e6,
               color=CORES[0])
ax.set_xlabel("Receita (R$ Milhões)")
ax.set_title("Top 15 Categorias por Receita", fontsize=13, fontweight="bold")
for bar, v in zip(bars, (cat_revenue["receita"][::-1] / 1e6)):
    ax.text(bar.get_width() + 0.02, bar.get_y() + bar.get_height()/2,
            f"R${v:.2f}M", va="center", fontsize=7.5)
plt.tight_layout()
charts["top_categorias_receita"] = fig_to_b64(fig)

# ── B3: Volume de Pedidos por Mês ─────────────────────────────────────────────
pedidos_mes = (
    base.dropna(subset=["month"])
    .groupby("month").size().reset_index(name="pedidos")
)
pedidos_mes["month_str"] = pedidos_mes["month"].astype(str)
pedidos_mes = pedidos_mes[pedidos_mes["month_str"].between("2017-01", "2018-09")]

fig, ax = plt.subplots(figsize=(14, 5))
ax.bar(pedidos_mes["month_str"], pedidos_mes["pedidos"],
       color=CORES[1], alpha=0.85)
ax.set_xticklabels(pedidos_mes["month_str"], rotation=45, ha="right", fontsize=8)
ax.set_ylabel("Número de Pedidos")
ax.set_title("Volume de Pedidos por Mês", fontsize=13, fontweight="bold")
ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
plt.tight_layout()
charts["pedidos_mes"] = fig_to_b64(fig)

# ── C1: Clientes por Estado ───────────────────────────────────────────────────
clientes_estado = (customers["customer_state"]
                   .value_counts().head(10).reset_index())
clientes_estado.columns = ["estado", "clientes"]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(clientes_estado["estado"], clientes_estado["clientes"],
              color=CORES[:10])
ax.set_xlabel("Estado")
ax.set_ylabel("Número de Clientes")
ax.set_title("Top 10 Estados por Número de Clientes", fontsize=13, fontweight="bold")
for bar, v in zip(bars, clientes_estado["clientes"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
            f"{v:,}", ha="center", fontsize=8)
plt.tight_layout()
charts["clientes_estado"] = fig_to_b64(fig)

# ── C2: Frequência de Compra por Cliente ──────────────────────────────────────
freq = (base.merge(customers[["customer_id", "customer_unique_id"]], on="customer_id", how="left")
        .groupby("customer_unique_id")["order_id"].nunique()
        .value_counts().head(6).reset_index())
freq.columns = ["pedidos", "clientes"]
freq["label"] = freq["pedidos"].apply(lambda x: f"{x} pedido{'s' if x > 1 else ''}")

fig, ax = plt.subplots(figsize=(8, 5))
wedges, texts, autotexts = ax.pie(
    freq["clientes"], labels=freq["label"],
    autopct=lambda p: f"{p:.1f}%",
    colors=CORES[:len(freq)], startangle=140,
    pctdistance=0.75
)
ax.set_title("Frequência de Compra por Cliente Único", fontsize=13, fontweight="bold")
plt.tight_layout()
charts["frequencia_compra"] = fig_to_b64(fig)

# ── C3: Ticket Médio por Estado ───────────────────────────────────────────────
ticket_estado = (
    delivered.groupby("customer_state")["revenue"]
    .mean().dropna().sort_values(ascending=False).head(12).reset_index()
)
ticket_estado.columns = ["estado", "ticket"]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(ticket_estado["estado"], ticket_estado["ticket"],
              color=CORES[2])
ax.set_xlabel("Estado")
ax.set_ylabel("Ticket Médio (R$)")
ax.set_title("Ticket Médio por Estado (Top 12)", fontsize=13, fontweight="bold")
for bar, v in zip(bars, ticket_estado["ticket"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
            f"R${v:.0f}", ha="center", fontsize=7.5)
plt.tight_layout()
charts["ticket_estado"] = fig_to_b64(fig)

# ── D1: Métodos de Pagamento ──────────────────────────────────────────────────
pay_method = payments["payment_type"].value_counts().reset_index()
pay_method.columns = ["metodo", "qtd"]
pay_method["metodo"] = pay_method["metodo"].str.replace("_", " ").str.title()

fig, ax = plt.subplots(figsize=(8, 5))
wedges, texts, autotexts = ax.pie(
    pay_method["qtd"], labels=pay_method["metodo"],
    autopct=lambda p: f"{p:.1f}%",
    colors=CORES[:len(pay_method)], startangle=140
)
ax.set_title("Métodos de Pagamento", fontsize=13, fontweight="bold")
plt.tight_layout()
charts["metodos_pagamento"] = fig_to_b64(fig)

# ── D2: Distribuição de Parcelas ─────────────────────────────────────────────
pay_cc = payments[payments["payment_type"] == "credit_card"]
parcelas = pay_cc["payment_installments"].value_counts().sort_index().head(15).reset_index()
parcelas.columns = ["parcelas", "qtd"]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(parcelas["parcelas"].astype(str), parcelas["qtd"],
              color=CORES[3])
ax.set_xlabel("Número de Parcelas")
ax.set_ylabel("Quantidade de Pedidos")
ax.set_title("Distribuição de Parcelamentos (Cartão de Crédito)",
             fontsize=13, fontweight="bold")
for bar, v in zip(bars, parcelas["qtd"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
            f"{v:,}", ha="center", fontsize=7.5)
plt.tight_layout()
charts["parcelas"] = fig_to_b64(fig)

# ── D3: Valor Médio por Método ───────────────────────────────────────────────
pay_avg = (payments.groupby("payment_type")["payment_value"]
           .mean().reset_index().sort_values("payment_value", ascending=False))
pay_avg.columns = ["metodo", "valor_medio"]
pay_avg["metodo"] = pay_avg["metodo"].str.replace("_", " ").str.title()

fig, ax = plt.subplots(figsize=(8, 4))
bars = ax.barh(pay_avg["metodo"], pay_avg["valor_medio"], color=CORES[:len(pay_avg)])
ax.set_xlabel("Valor Médio (R$)")
ax.set_title("Valor Médio de Transação por Método", fontsize=13, fontweight="bold")
for bar, v in zip(bars, pay_avg["valor_medio"]):
    ax.text(bar.get_width() + 1, bar.get_y() + bar.get_height()/2,
            f"R${v:.2f}", va="center", fontsize=9)
plt.tight_layout()
charts["valor_medio_metodo"] = fig_to_b64(fig)

# ── E1: Distribuição de Notas ─────────────────────────────────────────────────
score_dist = reviews["review_score"].value_counts().sort_index().reset_index()
score_dist.columns = ["nota", "qtd"]

fig, ax = plt.subplots(figsize=(8, 5))
color_map = {1: "#DC2626", 2: "#F97316", 3: "#EAB308", 4: "#84CC16", 5: "#16A34A"}
bars = ax.bar(score_dist["nota"].astype(str), score_dist["qtd"],
              color=[color_map[n] for n in score_dist["nota"]])
ax.set_xlabel("Nota")
ax.set_ylabel("Quantidade de Reviews")
ax.set_title("Distribuição de Notas nas Avaliações", fontsize=13, fontweight="bold")
for bar, v in zip(bars, score_dist["qtd"]):
    pct = v / score_dist["qtd"].sum() * 100
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 200,
            f"{v:,}\n({pct:.1f}%)", ha="center", fontsize=8.5)
plt.tight_layout()
charts["dist_notas"] = fig_to_b64(fig)

# ── E2: Nota Média por Categoria ─────────────────────────────────────────────
cat_score = (
    items_rich[items_rich["order_status"] == "delivered"]
    .merge(reviews[["order_id","review_score"]].drop_duplicates("order_id"), on="order_id", how="left")
    .groupby("product_category_name_english")["review_score"]
    .agg(["mean","count"]).reset_index()
    .query("count >= 100")
    .sort_values("mean", ascending=False).head(15)
)
cat_score.columns = ["categoria", "nota_media", "qtd"]

fig, ax = plt.subplots(figsize=(12, 6))
bars = ax.barh(cat_score["categoria"][::-1],
               cat_score["nota_media"][::-1],
               color=CORES[4])
ax.set_xlabel("Nota Média")
ax.set_title("Nota Média por Categoria (mín. 100 reviews)", fontsize=13, fontweight="bold")
ax.set_xlim(3, 5)
for bar, v in zip(bars, cat_score["nota_media"][::-1]):
    ax.text(bar.get_width() + 0.01, bar.get_y() + bar.get_height()/2,
            f"{v:.2f}", va="center", fontsize=8)
plt.tight_layout()
charts["nota_categoria"] = fig_to_b64(fig)

# ── E3: Score ao longo do tempo ──────────────────────────────────────────────
reviews_time = (
    reviews.dropna(subset=["review_creation_date"])
    .copy()
)
reviews_time["month"] = reviews_time["review_creation_date"].dt.to_period("M")
score_time = (reviews_time.groupby("month")["review_score"]
              .mean().reset_index())
score_time["month_str"] = score_time["month"].astype(str)
score_time = score_time[score_time["month_str"].between("2017-01", "2018-09")]

fig, ax = plt.subplots(figsize=(14, 4))
ax.plot(range(len(score_time)), score_time["review_score"],
        color=CORES[4], linewidth=2.5, marker="o", markersize=5)
ax.axhline(reviews["review_score"].mean(), color="gray", linestyle="--", alpha=0.6,
           label=f"Média geral: {reviews['review_score'].mean():.2f}")
ax.set_xticks(range(len(score_time)))
ax.set_xticklabels(score_time["month_str"], rotation=45, ha="right", fontsize=8)
ax.set_ylabel("Nota Média")
ax.set_title("Evolução da Nota Média Mensal", fontsize=13, fontweight="bold")
ax.set_ylim(3, 5)
ax.legend()
plt.tight_layout()
charts["score_tempo"] = fig_to_b64(fig)

# ── F1: Distribuição de Dias de Entrega ──────────────────────────────────────
valid_del = delivered.dropna(subset=["delivery_days"])
valid_del = valid_del[(valid_del["delivery_days"] > 0) &
                       (valid_del["delivery_days"] < 120)]

fig, ax = plt.subplots(figsize=(12, 5))
ax.hist(valid_del["delivery_days"], bins=60, color=CORES[5], alpha=0.85, edgecolor="white")
ax.axvline(valid_del["delivery_days"].median(), color="black", linestyle="--",
           label=f"Mediana: {valid_del['delivery_days'].median():.0f} dias")
ax.axvline(valid_del["delivery_days"].mean(), color="red", linestyle="--",
           label=f"Média: {valid_del['delivery_days'].mean():.1f} dias")
ax.set_xlabel("Dias até Entrega")
ax.set_ylabel("Quantidade de Pedidos")
ax.set_title("Distribuição de Tempo de Entrega (Compra → Entrega)", fontsize=13, fontweight="bold")
ax.legend()
plt.tight_layout()
charts["dist_entrega"] = fig_to_b64(fig)

# ── F2: % de Entregas no Prazo por Estado ────────────────────────────────────
on_time_estado = (
    delivered.dropna(subset=["on_time"])
    .groupby("customer_state")["on_time"]
    .agg(["sum", "count"])
    .assign(pct=lambda x: x["sum"] / x["count"] * 100)
    .query("count >= 100")
    .sort_values("pct").head(15)
    .reset_index()
)

fig, ax = plt.subplots(figsize=(10, 5))
colors_bar = [CORES[2] if p < 80 else CORES[1] for p in on_time_estado["pct"]]
bars = ax.barh(on_time_estado["customer_state"], on_time_estado["pct"],
               color=colors_bar)
ax.axvline(80, color="gray", linestyle="--", alpha=0.6, label="80% referência")
ax.set_xlabel("% Entregues no Prazo")
ax.set_title("% de Entregas no Prazo por Estado (mín. 100 pedidos)",
             fontsize=13, fontweight="bold")
for bar, v in zip(bars, on_time_estado["pct"]):
    ax.text(bar.get_width() + 0.3, bar.get_y() + bar.get_height()/2,
            f"{v:.1f}%", va="center", fontsize=8)
ax.legend()
plt.tight_layout()
charts["entrega_estado"] = fig_to_b64(fig)

# ── F3: Atraso Médio por Categoria ───────────────────────────────────────────
delay_cat = (
    delivered.dropna(subset=["delay_days"])
    .merge(items_rich[["order_id","product_category_name_english"]].drop_duplicates("order_id"),
           on="order_id", how="left")
    .groupby("product_category_name_english")["delay_days"]
    .mean().dropna().sort_values(ascending=True).head(15).reset_index()
)
delay_cat.columns = ["categoria", "atraso_medio"]

fig, ax = plt.subplots(figsize=(12, 6))
colors_d = [CORES[2] if v > 0 else CORES[1] for v in delay_cat["atraso_medio"]]
bars = ax.barh(delay_cat["categoria"], delay_cat["atraso_medio"], color=colors_d)
ax.axvline(0, color="black", linewidth=0.8)
ax.set_xlabel("Atraso Médio (dias) — negativo = adiantado")
ax.set_title("Atraso Médio por Categoria (Top piores pontualidade)",
             fontsize=13, fontweight="bold")
for bar, v in zip(bars, delay_cat["atraso_medio"]):
    ax.text(bar.get_width() + 0.1, bar.get_y() + bar.get_height()/2,
            f"{v:.1f}d", va="center", fontsize=8)
plt.tight_layout()
charts["atraso_categoria"] = fig_to_b64(fig)

# ── G1: Correlação Atraso × Nota ─────────────────────────────────────────────
corr_data = delivered.dropna(subset=["delay_days","review_score"])
delay_bins = pd.cut(corr_data["delay_days"],
                    bins=[-60,-14,-7,-1,0,7,14,30,100],
                    labels=["<-14","-14 a -7","-7 a -1","No dia","1-7","8-14","15-30",">30"])
corr_group = corr_data.groupby(delay_bins, observed=True)["review_score"].mean().reset_index()
corr_group.columns = ["atraso_faixa","nota_media"]

fig, ax = plt.subplots(figsize=(12, 5))
colors_c = [CORES[1] if "No" in str(f) or str(f).startswith("-") else CORES[2]
            for f in corr_group["atraso_faixa"]]
bars = ax.bar(corr_group["atraso_faixa"].astype(str),
              corr_group["nota_media"], color=colors_c)
ax.set_xlabel("Faixa de Atraso (dias)")
ax.set_ylabel("Nota Média")
ax.set_title("Nota Média × Pontualidade de Entrega", fontsize=13, fontweight="bold")
ax.set_ylim(1, 5)
for bar, v in zip(bars, corr_group["nota_media"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.03,
            f"{v:.2f}", ha="center", fontsize=9)
plt.tight_layout()
charts["atraso_nota"] = fig_to_b64(fig)

# ── G2: Ticket Médio × Nota ──────────────────────────────────────────────────
ticket_bins = pd.cut(delivered["revenue"],
                     bins=[0,50,100,200,500,1000,10000],
                     labels=["0-50","50-100","100-200","200-500","500-1k",">1k"])
ticket_score = delivered.groupby(ticket_bins, observed=True)["review_score"].mean().reset_index()
ticket_score.columns = ["ticket_faixa","nota_media"]

fig, ax = plt.subplots(figsize=(10, 5))
bars = ax.bar(ticket_score["ticket_faixa"].astype(str),
              ticket_score["nota_media"], color=CORES[6])
ax.set_xlabel("Faixa de Valor do Pedido (R$)")
ax.set_ylabel("Nota Média")
ax.set_title("Nota Média × Valor do Pedido", fontsize=13, fontweight="bold")
ax.set_ylim(1, 5)
for bar, v in zip(bars, ticket_score["nota_media"]):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.03,
            f"{v:.2f}", ha="center", fontsize=9)
plt.tight_layout()
charts["ticket_nota"] = fig_to_b64(fig)

# ── G3: Correlação numérica geral ─────────────────────────────────────────────
corr_cols = delivered[["revenue","freight","delivery_days","delay_days",
                        "review_score","n_items","installments","total_payment"]].copy()
corr_cols.columns = ["Receita","Frete","Dias entrega","Atraso",
                     "Nota","Itens","Parcelas","Total pago"]
corr_matrix = corr_cols.dropna().corr()

fig, ax = plt.subplots(figsize=(10, 8))
mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
sns.heatmap(corr_matrix, annot=True, fmt=".2f", cmap="RdYlGn",
            center=0, ax=ax, mask=mask, square=True,
            annot_kws={"size": 10}, linewidths=0.5)
ax.set_title("Matriz de Correlação entre Variáveis Principais",
             fontsize=13, fontweight="bold")
plt.tight_layout()
charts["correlacao"] = fig_to_b64(fig)

print("  Todos os gráficos gerados.")

# ════════════════════════════════════════════════════════════════════════════
# MÉTRICAS-CHAVE
# ════════════════════════════════════════════════════════════════════════════
total_rev        = items[items["order_id"].isin(orders[orders["order_status"]=="delivered"]["order_id"])]["price"].sum()
total_pedidos    = orders["order_id"].nunique()
total_clientes   = customers["customer_unique_id"].nunique()
total_sellers    = sellers["seller_id"].nunique()
avg_ticket       = delivered["revenue"].mean()
avg_score        = reviews["review_score"].mean()
pct_on_time      = delivered["on_time"].mean() * 100
avg_del_days     = valid_del["delivery_days"].median()
pct_devolucao    = (orders["order_status"] == "canceled").mean() * 100

# ── QUALIDADE DETALHADA ───────────────────────────────────────────────────────
total_linhas_global   = sum(df.shape[0] for df in dfs.values())
total_colunas_global  = sum(df.shape[1] for df in dfs.values())
total_registros_global = sum(df.shape[0] * df.shape[1] for df in dfs.values())
total_nulos_global    = sum(df.isnull().sum().sum() for df in dfs.values())
total_dups_global     = sum(df.duplicated().sum() for df in dfs.values())

# ════════════════════════════════════════════════════════════════════════════
# HTML
# ════════════════════════════════════════════════════════════════════════════
print("Montando HTML...")

def card(titulo, valor, sub="", cor="#2563EB"):
    return f"""
    <div class="card">
      <div class="card-label">{titulo}</div>
      <div class="card-value" style="color:{cor}">{valor}</div>
      <div class="card-sub">{sub}</div>
    </div>"""

def section(titulo, icone=""):
    return f'<h2 class="section-title">{icone} {titulo}</h2>'

def img(b64, titulo="", sub=""):
    caption = f'<p class="chart-caption"><strong>{titulo}</strong>{" — " + sub if sub else ""}</p>' if titulo else ""
    return f'<div class="chart-box">{caption}<img src="data:image/png;base64,{b64}" style="width:100%"/></div>'

def quality_table():
    rows = ""
    for _, r in quality_df.iterrows():
        dups_class = "bad" if r["% Dups"] > 5 else ("warn" if r["% Dups"] > 1 else "ok")
        nulos_class = "bad" if r["% Nulos"] > 5 else ("warn" if r["% Nulos"] > 0.5 else "ok")
        rows += f"""<tr>
          <td><strong>{r['Dataset']}</strong></td>
          <td>{fmt_n(r['Linhas'])}</td>
          <td>{r['Colunas']}</td>
          <td>{fmt_n(r['Total Registros'])}</td>
          <td class="{nulos_class}">{fmt_n(r['Nulos'])} ({r['% Nulos']}%)</td>
          <td>{r['Cols c/ Nulos']}</td>
          <td class="{dups_class}">{fmt_n(r['Duplicatas'])} ({r['% Dups']}%)</td>
        </tr>"""
    return rows

def null_detail_html():
    html = ""
    for key, df_n in nulos_detail.items():
        html += f"<h4 style='margin:10px 0 4px;color:#555'>{key}</h4><table class='small-table'>"
        html += "<tr><th>Coluna</th><th>Nulos</th><th>%</th></tr>"
        for _, row in df_n.iterrows():
            lvl = "bad" if row["%"] > 20 else ("warn" if row["%"] > 5 else "")
            html += f"<tr><td>{row['Coluna']}</td><td class='{lvl}'>{fmt_n(row['Nulos'])}</td><td class='{lvl}'>{row['%']}%</td></tr>"
        html += "</table>"
    return html

HTML = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Olist Dashboard — Análise Completa</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',system-ui,sans-serif;background:#F1F5F9;color:#1E293B;line-height:1.5}}
  header{{background:linear-gradient(135deg,#1E3A8A,#2563EB);color:white;padding:28px 40px}}
  header h1{{font-size:1.8rem;font-weight:700}}
  header p{{opacity:.85;font-size:.95rem;margin-top:4px}}
  .container{{max-width:1400px;margin:0 auto;padding:24px 32px}}
  .section-title{{font-size:1.25rem;font-weight:700;color:#1E3A8A;
    border-left:4px solid #2563EB;padding-left:12px;margin:32px 0 16px}}
  .cards{{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:14px;margin-bottom:8px}}
  .card{{background:white;border-radius:12px;padding:18px;
    box-shadow:0 1px 4px rgba(0,0,0,.08)}}
  .card-label{{font-size:.75rem;color:#64748B;text-transform:uppercase;letter-spacing:.04em}}
  .card-value{{font-size:1.55rem;font-weight:700;margin:4px 0}}
  .card-sub{{font-size:.78rem;color:#94A3B8}}
  .grid2{{display:grid;grid-template-columns:1fr 1fr;gap:20px}}
  .grid3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:20px}}
  .chart-box{{background:white;border-radius:12px;padding:16px;
    box-shadow:0 1px 4px rgba(0,0,0,.08);margin-bottom:4px}}
  .chart-caption{{font-size:.82rem;color:#475569;margin-bottom:8px}}
  table{{width:100%;border-collapse:collapse;background:white;
    border-radius:10px;overflow:hidden;box-shadow:0 1px 4px rgba(0,0,0,.08)}}
  th{{background:#1E3A8A;color:white;padding:10px 12px;text-align:left;font-size:.82rem}}
  td{{padding:9px 12px;font-size:.83rem;border-bottom:1px solid #E2E8F0}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:#F8FAFC}}
  .ok{{color:#16A34A;font-weight:600}}
  .warn{{color:#D97706;font-weight:600}}
  .bad{{color:#DC2626;font-weight:600}}
  .small-table{{font-size:.78rem;margin-bottom:10px}}
  .small-table th{{padding:6px 10px;font-size:.75rem}}
  .small-table td{{padding:5px 10px}}
  footer{{text-align:center;padding:20px;color:#94A3B8;font-size:.8rem}}
  @media(max-width:900px){{.grid2,.grid3{{grid-template-columns:1fr}}}}
</style>
</head>
<body>
<header>
  <h1>Olist E-commerce Brasileiro — Dashboard Analítico</h1>
  <p>Análise completa das 9 tabelas do dataset Kaggle &nbsp;|&nbsp; {total_pedidos:,} pedidos &nbsp;|&nbsp; Gerado em 28/04/2026</p>
</header>
<div class="container">

<!-- ═══════════ KPIs ═══════════ -->
{section("KPIs Principais", "📊")}
<div class="cards">
  {card("Faturamento Total", brl(total_rev), "pedidos entregues", "#16A34A")}
  {card("Total de Pedidos", fmt_n(total_pedidos), "orders únicos")}
  {card("Clientes Únicos", fmt_n(total_clientes), "customer_unique_id")}
  {card("Vendedores Ativos", fmt_n(total_sellers), "sellers")}
  {card("Ticket Médio", brl(avg_ticket), "por pedido entregue", "#D97706")}
  {card("Nota Média", f"{avg_score:.2f} / 5", "satisfação geral", "#7C3AED")}
  {card("Entregas no Prazo", f"{pct_on_time:.1f}%", "pedidos entregues", "#0891B2")}
  {card("Mediana de Entrega", f"{avg_del_days:.0f} dias", "compra → cliente", "#DB2777")}
  {card("Taxa de Cancelamento", f"{pct_devolucao:.1f}%", "do total de pedidos", "#DC2626")}
</div>

<!-- ═══════════ QUALIDADE ═══════════ -->
{section("1 — Qualidade dos Dados", "🔍")}
<div class="cards">
  {card("Total de Datasets", "9", "arquivos CSV")}
  {card("Total de Linhas", fmt_n(total_linhas_global), "soma de todas tabelas")}
  {card("Total de Colunas", fmt_n(total_colunas_global), "soma de todas tabelas")}
  {card("Total de Registros", fmt_n(total_registros_global), "linhas × colunas")}
  {card("Valores Nulos", fmt_n(total_nulos_global), f"{total_nulos_global/total_registros_global*100:.2f}% do total", "#DC2626")}
  {card("Linhas Duplicadas", fmt_n(total_dups_global), "soma geral", "#D97706")}
</div>
<br/>
<table>
  <thead>
    <tr>
      <th>Dataset</th><th>Linhas</th><th>Colunas</th><th>Registros</th>
      <th>Nulos</th><th>Cols c/ Nulos</th><th>Duplicatas</th>
    </tr>
  </thead>
  <tbody>{quality_table()}</tbody>
</table>
<br/>
<div class="grid2">
  {img(charts["qualidade"], "Visualização de Qualidade", "% nulos e duplicatas por dataset")}
  <div>
    <h4 style="margin-bottom:12px;color:#1E3A8A">Detalhe de Nulos por Coluna</h4>
    {null_detail_html()}
  </div>
</div>

<!-- ═══════════ VENDAS ═══════════ -->
{section("2 — Análise de Vendas e Receita", "💰")}
{img(charts["faturamento_mensal"], "Faturamento Mensal", "Evolução da receita ao longo do período")}
<div class="grid2" style="margin-top:20px">
  {img(charts["pedidos_mes"], "Volume de Pedidos por Mês")}
  {img(charts["top_categorias_receita"], "Top 15 Categorias por Receita")}
</div>

<!-- ═══════════ CLIENTES ═══════════ -->
{section("3 — Análise de Clientes", "👥")}
<div class="grid3">
  {img(charts["clientes_estado"], "Clientes por Estado")}
  {img(charts["frequencia_compra"], "Frequência de Compra")}
  {img(charts["ticket_estado"], "Ticket Médio por Estado")}
</div>

<!-- ═══════════ PAGAMENTOS ═══════════ -->
{section("4 — Análise de Pagamentos", "💳")}
<div class="grid3">
  {img(charts["metodos_pagamento"], "Métodos de Pagamento")}
  {img(charts["parcelas"], "Parcelamentos (Cartão de Crédito)")}
  {img(charts["valor_medio_metodo"], "Valor Médio por Método")}
</div>

<!-- ═══════════ REVIEWS ═══════════ -->
{section("5 — Análise de Avaliações", "⭐")}
<div class="grid2">
  {img(charts["dist_notas"], "Distribuição de Notas")}
  {img(charts["score_tempo"], "Evolução da Nota Média")}
</div>
{img(charts["nota_categoria"], "Nota Média por Categoria", "apenas categorias com ≥100 reviews")}

<!-- ═══════════ ENTREGA ═══════════ -->
{section("6 — Análise de Desempenho de Entrega", "🚚")}
{img(charts["dist_entrega"], "Distribuição de Tempo de Entrega")}
<div class="grid2" style="margin-top:20px">
  {img(charts["entrega_estado"], "% Entregas no Prazo por Estado")}
  {img(charts["atraso_categoria"], "Atraso Médio por Categoria")}
</div>

<!-- ═══════════ CORRELAÇÕES ═══════════ -->
{section("7 — Correlações entre Datasets", "🔗")}
<div class="grid2">
  {img(charts["atraso_nota"], "Nota × Pontualidade de Entrega", "quanto o atraso impacta a satisfação")}
  {img(charts["ticket_nota"], "Nota × Valor do Pedido", "ticket médio vs satisfação")}
</div>
{img(charts["correlacao"], "Matriz de Correlação", "correlação entre todas as variáveis numéricas principais")}

</div>
<footer>Olist E-commerce Dataset — Kaggle &nbsp;|&nbsp; Análise gerada com Python + Pandas + Matplotlib + Seaborn</footer>
</body>
</html>"""

with open(OUT_HTML, "w", encoding="utf-8") as f:
    f.write(HTML)

print(f"\nDashboard salvo em:\n{OUT_HTML}\n")
print("Abra o arquivo .html no navegador para visualizar.")
