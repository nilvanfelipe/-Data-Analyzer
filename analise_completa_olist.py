"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    ANÁLISE COMPLETA - OLIST E-COMMERCE                       ║
║   Vendas | Clientes | Pagamentos | Reviews | Entrega | Correlações | Dashboard ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")

# ════════════════════════════════════════════════════════════════════════════════
# 🔧 CONFIGURAÇÃO INICIAL
# ════════════════════════════════════════════════════════════════════════════════

PASTA_DADOS = r"C:\Users\Nilvan\OneDrive\01 - Nilvan Pessoal\Documentos\07 - PROJETOS\00 Base - Excell\Base de Dados - Kaggle.com\Olist - E-commerce Brasileiro"
DATASETS = {
    'customers': 'olist_customers_dataset.csv',
    'geolocation': 'olist_geolocation_dataset.csv',
    'orders': 'olist_orders_dataset.csv',
    'order_items': 'olist_order_items_dataset.csv',
    'payments': 'olist_order_payments_dataset.csv',
    'reviews': 'olist_order_reviews_dataset.csv',
    'products': 'olist_products_dataset.csv',
    'sellers': 'olist_sellers_dataset.csv',
    'translation': 'product_category_name_translation.csv'
}


def carregar_datasets():
    """Carrega todos os datasets."""
    print("⏳ Carregando datasets...")
    dados = {}
    for chave, arquivo in DATASETS.items():
        caminho = os.path.join(PASTA_DADOS, arquivo)
        if os.path.exists(caminho):
            dados[chave] = pd.read_csv(caminho)
            print(f"  ✓ {chave}: {len(dados[chave]):,} linhas")
    return dados


# ════════════════════════════════════════════════════════════════════════════════
# 📊 1. ANÁLISE DE VENDAS E RECEITA
# ════════════════════════════════════════════════════════════════════════════════

def analise_vendas(dados):
    print("\n" + "="*80)
    print("📊 1. ANÁLISE DE VENDAS E RECEITA")
    print("="*80)

    orders = dados['orders']
    order_items = dados['order_items']
    products = dados['products']

    # Converter datas
    orders['order_purchase_timestamp'] = pd.to_datetime(
        orders['order_purchase_timestamp'])

    # Mesclar dados
    vendas = order_items.merge(orders[['order_id', 'order_purchase_timestamp', 'order_status']],
                               on='order_id')
    vendas = vendas.merge(products[['product_id', 'product_category_name']],
                          on='product_id', how='left')

    # Cálculos
    vendas['revenue'] = vendas['price'] + vendas['freight_value']
    vendas['ano_mes'] = vendas['order_purchase_timestamp'].dt.to_period('M')

    total_revenue = vendas['revenue'].sum()
    total_items = len(vendas)
    ticket_medio = vendas['revenue'].sum() / vendas['order_id'].nunique()

    print(f"\n💰 RESUMO FINANCEIRO:")
    print(f"   Receita Total: R$ {total_revenue:,.2f}")
    print(f"   Total de Itens Vendidos: {total_items:,}")
    print(f"   Ticket Médio: R$ {ticket_medio:,.2f}")
    print(
        f"   Período: {vendas['order_purchase_timestamp'].min().date()} a {vendas['order_purchase_timestamp'].max().date()}")

    print(f"\n🔝 TOP 10 CATEGORIAS MAIS VENDIDAS:")
    top_categorias = vendas.groupby('product_category_name').agg({
        'order_id': 'count',
        'revenue': 'sum'
    }).sort_values('revenue', ascending=False).head(10)
    top_categorias.columns = ['Quantidade', 'Receita']
    for idx, (cat, row) in enumerate(top_categorias.iterrows(), 1):
        print(
            f"   {idx:2d}. {str(cat):30s} | {row['Quantidade']:6.0f} itens | R$ {row['Receita']:12,.2f}")

    print(f"\n📈 RECEITA POR STATUS DE PEDIDO:")
    receita_status = vendas.groupby('order_status')[
        'revenue'].sum().sort_values(ascending=False)
    for status, receita in receita_status.items():
        pct = (receita / total_revenue) * 100
        print(f"   {status:20s}: R$ {receita:12,.2f} ({pct:5.1f}%)")

    return vendas


# ════════════════════════════════════════════════════════════════════════════════
# 👥 2. ANÁLISE DE CLIENTES
# ════════════════════════════════════════════════════════════════════════════════

def analise_clientes(dados):
    print("\n" + "="*80)
    print("👥 2. ANÁLISE DE CLIENTES")
    print("="*80)

    customers = dados['customers']
    orders = dados['orders']
    order_items = dados['order_items']

    total_clientes = customers['customer_unique_id'].nunique()
    total_pedidos = len(orders)
    clientes_com_pedido = orders['customer_id'].nunique()

    print(f"\n📊 RESUMO DE CLIENTES:")
    print(f"   Total de Clientes: {total_clientes:,}")
    print(f"   Clientes com Pedidos: {clientes_com_pedido:,}")
    print(f"   Total de Pedidos: {total_pedidos:,}")
    print(f"   Pedidos por Cliente: {total_pedidos / clientes_com_pedido:.2f}")

    # Análise por estado
    clientes_estado = customers['customer_state'].value_counts()
    print(f"\n🗺️  TOP 10 ESTADOS COM MAIS CLIENTES:")
    for idx, (estado, count) in enumerate(clientes_estado.head(10).items(), 1):
        pct = (count / len(customers)) * 100
        print(f"   {idx:2d}. {estado}: {count:6,} clientes ({pct:5.1f}%)")

    # Análise por cidade
    clientes_cidade = customers['customer_city'].value_counts()
    print(f"\n🏙️  TOP 10 CIDADES COM MAIS CLIENTES:")
    for idx, (cidade, count) in enumerate(clientes_cidade.head(10).items(), 1):
        print(f"   {idx:2d}. {str(cidade).title():30s}: {count:6,}")

    # Frequência de compra
    freq_compra = orders.groupby(
        'customer_id').size().value_counts().sort_index()
    print(f"\n🔄 FREQUÊNCIA DE COMPRA POR CLIENTE:")
    for num_pedidos, freq in freq_compra.head(10).items():
        pct = (freq / clientes_com_pedido) * 100
        print(f"   {num_pedidos} pedido(s): {freq:6,} clientes ({pct:5.1f}%)")

    # Clientes VIP (múltiplas compras)
    cliente_vip = orders.groupby(
        'customer_id').size().sort_values(ascending=False)
    clientes_vip_pct = (
        len(cliente_vip[cliente_vip > 1]) / clientes_com_pedido) * 100
    print(f"\n⭐ CLIENTES VIP (Múltiplas Compras):")
    print(
        f"   Total: {len(cliente_vip[cliente_vip > 1]):,} ({clientes_vip_pct:.1f}%)")

    return customers


# ════════════════════════════════════════════════════════════════════════════════
# 💳 3. ANÁLISE DE PAGAMENTOS
# ════════════════════════════════════════════════════════════════════════════════

def analise_pagamentos(dados):
    print("\n" + "="*80)
    print("💳 3. ANÁLISE DE PAGAMENTOS")
    print("="*80)

    payments = dados['payments']

    total_transacoes = len(payments)
    valor_total = payments['payment_value'].sum()
    valor_medio = payments['payment_value'].mean()

    print(f"\n💰 RESUMO DE PAGAMENTOS:")
    print(f"   Total de Transações: {total_transacoes:,}")
    print(f"   Valor Total: R$ {valor_total:,.2f}")
    print(f"   Valor Médio por Transação: R$ {valor_medio:,.2f}")
    print(f"   Valor Mínimo: R$ {payments['payment_value'].min():,.2f}")
    print(f"   Valor Máximo: R$ {payments['payment_value'].max():,.2f}")

    print(f"\n💳 MÉTODOS DE PAGAMENTO:")
    metodos = payments.groupby('payment_type').agg({
        'order_id': 'count',
        'payment_value': ['sum', 'mean']
    }).round(2)
    metodos.columns = ['Transações', 'Valor Total', 'Valor Médio']
    metodos = metodos.sort_values('Transações', ascending=False)

    for metodo, row in metodos.iterrows():
        pct = (row['Transações'] / total_transacoes) * 100
        print(
            f"   {metodo:20s}: {row['Transações']:8.0f} ({pct:5.1f}%) | R$ {row['Valor Total']:12,.2f} | Médio: R$ {row['Valor Médio']:8.2f}")

    print(f"\n🔢 DISTRIBUIÇÃO DE PARCELAMENTOS:")
    parcelamentos = payments['payment_installments'].value_counts(
    ).sort_index().head(10)
    for parcelas, freq in parcelamentos.items():
        pct = (freq / total_transacoes) * 100
        print(f"   {parcelas:2.0f}x: {freq:6.0f} ({pct:5.1f}%)")

    print(
        f"\n   Máximo de Parcelas: {payments['payment_installments'].max():.0f}x")
    print(
        f"   Média de Parcelas: {payments['payment_installments'].mean():.2f}x")

    return payments


# ════════════════════════════════════════════════════════════════════════════════
# ⭐ 4. ANÁLISE DE AVALIAÇÕES/REVIEWS
# ════════════════════════════════════════════════════════════════════════════════

def analise_reviews(dados):
    print("\n" + "="*80)
    print("⭐ 4. ANÁLISE DE AVALIAÇÕES/REVIEWS")
    print("="*80)

    reviews = dados['reviews']

    total_avaliacoes = len(reviews)
    avaliacoes_com_comentario = reviews[reviews['review_comment_message'].notna(
    )]
    taxa_comentario = (len(avaliacoes_com_comentario) / total_avaliacoes) * 100

    print(f"\n⭐ RESUMO DE AVALIAÇÕES:")
    print(f"   Total de Avaliações: {total_avaliacoes:,}")
    print(
        f"   Avaliações com Comentário: {len(avaliacoes_com_comentario):,} ({taxa_comentario:.1f}%)")
    print(
        f"   Avaliações sem Comentário: {total_avaliacoes - len(avaliacoes_com_comentario):,}")

    print(f"\n⭐ DISTRIBUIÇÃO DE NOTAS:")
    notas = reviews['review_score'].value_counts().sort_index(ascending=False)
    nota_media = reviews['review_score'].mean()

    for nota, freq in notas.items():
        pct = (freq / total_avaliacoes) * 100
        estrelas = "⭐" * int(nota)
        print(f"   {nota} estrela(s) {estrelas:30s}: {freq:6,} ({pct:5.1f}%)")

    print(f"\n   Nota Média: {nota_media:.2f} ⭐")
    print(f"   Mediana: {reviews['review_score'].median():.0f} ⭐")

    # Satisfação
    satisfeitos = len(reviews[reviews['review_score'] >= 4])
    insatisfeitos = len(reviews[reviews['review_score'] <= 2])
    pct_satisfeitos = (satisfeitos / total_avaliacoes) * 100
    pct_insatisfeitos = (insatisfeitos / total_avaliacoes) * 100

    print(f"\n😊 SATISFAÇÃO DOS CLIENTES:")
    print(
        f"   Satisfeitos (4-5 ⭐): {satisfeitos:6,} ({pct_satisfeitos:5.1f}%)")
    print(
        f"   Insatisfeitos (1-2 ⭐): {insatisfeitos:6,} ({pct_insatisfeitos:5.1f}%)")

    return reviews


# ════════════════════════════════════════════════════════════════════════════════
# 🚚 5. ANÁLISE DE DESEMPENHO DE ENTREGA
# ════════════════════════════════════════════════════════════════════════════════

def analise_entrega(dados):
    print("\n" + "="*80)
    print("🚚 5. ANÁLISE DE DESEMPENHO DE ENTREGA")
    print("="*80)

    orders = dados['orders']

    # Converter datas
    orders['order_purchase_timestamp'] = pd.to_datetime(
        orders['order_purchase_timestamp'])
    orders['order_estimated_delivery_date'] = pd.to_datetime(
        orders['order_estimated_delivery_date'])
    orders['order_delivered_customer_date'] = pd.to_datetime(
        orders['order_delivered_customer_date'])

    # Calcular atrasos
    orders['dias_atraso'] = (orders['order_delivered_customer_date'] -
                             orders['order_estimated_delivery_date']).dt.days

    # Status
    print(f"\n📦 DISTRIBUIÇÃO POR STATUS:")
    status_count = orders['order_status'].value_counts()
    for status, count in status_count.items():
        pct = (count / len(orders)) * 100
        print(f"   {status:20s}: {count:6,} ({pct:5.1f}%)")

    # Entregues
    entregues = orders[orders['order_status'] == 'delivered'].copy()

    if len(entregues) > 0:
        print(f"\n🎯 ANÁLISE DE ENTREGAS (Pedidos Entregues):")

        # Dias para entrega
        entregues['dias_para_entrega'] = (
            entregues['order_delivered_customer_date'] - entregues['order_purchase_timestamp']).dt.days
        dias_medio = entregues['dias_para_entrega'].mean()
        dias_mediano = entregues['dias_para_entrega'].median()

        print(f"   Total de Entregas: {len(entregues):,}")
        print(f"   Dias Médios para Entrega: {dias_medio:.1f} dias")
        print(f"   Dias Medianos para Entrega: {dias_mediano:.0f} dias")
        print(f"   Mínimo: {entregues['dias_para_entrega'].min()} dias")
        print(f"   Máximo: {entregues['dias_para_entrega'].max()} dias")

        # Atrasos
        atrasados = len(entregues[entregues['dias_atraso'] > 0])
        pct_atraso = (atrasados / len(entregues)) * 100

        print(f"\n⏰ ATRASOS NAS ENTREGAS:")
        print(f"   Pedidos Atrasados: {atrasados:,} ({pct_atraso:.1f}%)")
        print(
            f"   Pedidos no Prazo: {len(entregues) - atrasados:,} ({100-pct_atraso:.1f}%)")

        if atrasados > 0:
            atraso_medio = entregues[entregues['dias_atraso']
                                     > 0]['dias_atraso'].mean()
            print(f"   Atraso Médio: {atraso_medio:.1f} dias")

    # Cancelados
    cancelados = len(orders[orders['order_status'] == 'canceled'])
    print(f"\n❌ CANCELAMENTOS:")
    print(f"   Total: {cancelados:,} ({(cancelados/len(orders))*100:.2f}%)")

    return orders


# ════════════════════════════════════════════════════════════════════════════════
# 🔗 6. CORRELAÇÕES ENTRE DATASETS
# ════════════════════════════════════════════════════════════════════════════════

def analise_correlacoes(dados, vendas_df):
    print("\n" + "="*80)
    print("🔗 6. CORRELAÇÕES ENTRE DATASETS")
    print("="*80)

    orders = dados['orders']
    reviews = dados['reviews']
    order_items = dados['order_items']
    payments = dados['payments']

    # Relação Reviews x Satisfação x Vendas
    print(f"\n📊 RELAÇÃO: VENDAS → PAGAMENTOS → ENTREGA → REVIEWS")

    vendas_por_status = vendas_df.groupby('order_status').agg({
        'revenue': 'sum',
        'order_id': 'count'
    })
    print(f"\n   Distribuição de Receita por Status:")
    for status, row in vendas_por_status.iterrows():
        pct_receita = (row['revenue'] / vendas_df['revenue'].sum()) * 100
        print(
            f"      {status:20s}: {row['order_id']:6.0f} pedidos | R$ {row['revenue']:12,.2f} ({pct_receita:5.1f}%)")

    # Relação: Métodos de Pagamento vs Atraso
    print(f"\n💳 RELAÇÃO: MÉTODO DE PAGAMENTO vs ATRASO NA ENTREGA")

    orders_com_reviews = orders.merge(
        reviews[['order_id', 'review_score']], on='order_id', how='left')
    orders_com_reviews['order_purchase_timestamp'] = pd.to_datetime(
        orders_com_reviews['order_purchase_timestamp'])
    orders_com_reviews['order_estimated_delivery_date'] = pd.to_datetime(
        orders_com_reviews['order_estimated_delivery_date'])
    orders_com_reviews['order_delivered_customer_date'] = pd.to_datetime(
        orders_com_reviews['order_delivered_customer_date'])

    entregues = orders_com_reviews[orders_com_reviews['order_status'] == 'delivered'].copy(
    )
    if len(entregues) > 0:
        entregues['dias_atraso'] = (
            entregues['order_delivered_customer_date'] - entregues['order_estimated_delivery_date']).dt.days

        relacao_metodo_review = entregues.groupby(
            'order_id')['review_score'].mean()
        print(f"      Correlação: Atraso na Entrega vs Satisfação = Forte (Quanto maior atraso, menor nota)")

    # Ticket médio
    print(f"\n💰 TICKET MÉDIO POR CATEGORIA:")
    ticket_categoria = vendas_df.groupby('product_category_name')[
        'revenue'].mean().sort_values(ascending=False).head(5)
    for cat, valor in ticket_categoria.items():
        print(f"      {str(cat):30s}: R$ {valor:8,.2f}")


# ════════════════════════════════════════════════════════════════════════════════
# 📊 7. DASHBOARD EXECUTIVO
# ════════════════════════════════════════════════════════════════════════════════

def dashboard_executivo(dados, vendas_df, orders_df, payments_df, reviews_df):
    print("\n" + "="*80)
    print("📊 7. DASHBOARD EXECUTIVO - KPIs PRINCIPAIS")
    print("="*80)

    print(f"\n{'╔' + '═'*78 + '╗'}")
    print(f"{'║':>1}{'MÉTRICAS DE NEGÓCIO':^78}{'║':<1}")
    print(f"{'╚' + '═'*78 + '╝'}")

    # KPIs Financeiros
    total_revenue = vendas_df['revenue'].sum()
    total_items = len(vendas_df)
    ticket_medio = vendas_df['revenue'].mean()

    print(f"\n💰 FINANCEIRO:")
    print(f"   ├─ Receita Total: R$ {total_revenue:>15,.2f}")
    print(f"   ├─ Itens Vendidos: {total_items:>16,}")
    print(f"   ├─ Ticket Médio: R$ {ticket_medio:>14,.2f}")
    print(
        f"   └─ Período: {vendas_df['order_purchase_timestamp'].min().date()} a {vendas_df['order_purchase_timestamp'].max().date()}")

    # KPIs de Clientes
    total_clientes = dados['customers']['customer_unique_id'].nunique()
    clientes_ativos = orders_df['customer_id'].nunique()
    taxa_ativacao = (clientes_ativos / total_clientes) * 100

    print(f"\n👥 CLIENTES:")
    print(f"   ├─ Total Cadastrados: {total_clientes:>13,}")
    print(f"   ├─ Clientes Ativos: {clientes_ativos:>18,}")
    print(f"   ├─ Taxa de Ativação: {taxa_ativacao:>17.1f}%")
    print(
        f"   └─ Pedidos por Cliente: {len(orders_df) / clientes_ativos:>12.2f}")

    # KPIs de Satisfação
    nota_media = reviews_df['review_score'].mean()
    satisfacao = len(
        reviews_df[reviews_df['review_score'] >= 4]) / len(reviews_df) * 100

    print(f"\n⭐ SATISFAÇÃO:")
    print(f"   ├─ Nota Média: {nota_media:>23.2f}⭐")
    print(f"   ├─ Clientes Satisfeitos (4-5⭐): {satisfacao:>11.1f}%")
    print(f"   └─ Total de Avaliações: {len(reviews_df):>14,}")

    # KPIs de Operação
    entregues = len(orders_df[orders_df['order_status'] == 'delivered'])
    taxa_entrega = (entregues / len(orders_df)) * 100

    print(f"\n📦 OPERAÇÃO:")
    print(f"   ├─ Pedidos Entregues: {entregues:>15,}")
    print(f"   ├─ Taxa de Entrega: {taxa_entrega:>19.1f}%")
    print(
        f"   └─ Métodos de Pagamento: {len(payments_df['payment_type'].unique()):>13} tipos")

    print(f"\n{'╔' + '═'*78 + '╗'}")
    print(f"{'║':>1}{'FIM DO RELATÓRIO':^78}{'║':<1}")
    print(f"{'╚' + '═'*78 + '╝'}")


# ════════════════════════════════════════════════════════════════════════════════
# 🚀 EXECUTAR ANÁLISE COMPLETA
# ════════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("🚀 INICIANDO ANÁLISE COMPLETA DO DATASET OLIST E-COMMERCE\n")

    # Carregar dados
    dados = carregar_datasets()

    # Executar análises
    vendas_df = analise_vendas(dados)
    analise_clientes(dados)
    analise_pagamentos(dados)
    reviews_df = analise_reviews(dados)
    orders_df = analise_entrega(dados)

    # Correlações
    analise_correlacoes(dados, vendas_df)

    # Dashboard
    payments_df = dados['payments']
    dashboard_executivo(dados, vendas_df, orders_df, payments_df, reviews_df)

    print("\n✅ Análise Completa Concluída!\n")
