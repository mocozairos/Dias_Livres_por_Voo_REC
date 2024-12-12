import streamlit as st
import mysql.connector
import decimal
import pandas as pd
from datetime import timedelta
import matplotlib.pyplot as plt

def gerar_df_sales(base_luck):
    # Parametros de Login AWS
    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    # Conexão as Views
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT `Cod_Reserva_Principal`, `Cod_Reserva`, `Data_Servico`, `Data Execucao` FROM vw_sales'

    # Script MySql para requests
    cursor.execute(
        request_name
    )
    # Coloca o request em uma variavel
    resultado = cursor.fetchall()
    # Busca apenas o cabecalhos do Banco
    cabecalho = [desc[0] for desc in cursor.description]

    # Fecha a conexão
    cursor.close()
    conexao.close()

    # Coloca em um dataframe e muda o tipo de decimal para float
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def gerar_df_router(base_luck):
    
    config = {
    'user': 'user_automation_jpa',
    'password': 'luck_jpa_2024',
    'host': 'comeia.cixat7j68g0n.us-east-1.rds.amazonaws.com',
    'database': base_luck
    }
    
    conexao = mysql.connector.connect(**config)
    cursor = conexao.cursor()

    request_name = f'SELECT `Reserva`, `Data Execucao`, `Status do Servico`, `Status da Reserva`, `Tipo de Servico`, `Servico` FROM vw_router'

    # Script MySql para requests
    cursor.execute(
        request_name
    )
    # Coloca o request em uma variavel
    resultado = cursor.fetchall()
    # Busca apenas o cabecalhos do Banco
    cabecalho = [desc[0] for desc in cursor.description]

    # Fecha a conexão
    cursor.close()
    conexao.close()

    # Coloca em um dataframe e muda o tipo de decimal para float
    df = pd.DataFrame(resultado, columns=cabecalho)
    df = df.applymap(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df

def calcular_media_estadia(df_ultimos_servicos_filtrado):

    df_ref = df_ultimos_servicos_filtrado[~(pd.isna(df_ultimos_servicos_filtrado['Data OUT']))].reset_index(drop=True)

    df_ref['Data IN'] = pd.to_datetime(df_ref['Data IN'])

    df_ref['Data OUT'] = pd.to_datetime(df_ref['Data OUT'])

    df_ref['Dias Estadia'] = (df_ref['Data OUT'] - df_ref['Data IN']).dt.days

    df_ref['Dias Estadia'] = df_ref['Dias Estadia'].astype(int)

    media_estadia = round(df_ref['Dias Estadia'].mean(), 0)

    return media_estadia

def puxar_dados_phoenix():

    st.session_state.df_sales = gerar_df_sales('test_phoenix_recife')

    st.session_state.df_sales = st.session_state.df_sales.rename(columns={'Cod_Reserva_Principal': 'Reserva Mae'})

    st.session_state.df_sales.loc[pd.isna(st.session_state.df_sales['Reserva Mae']), 'Reserva Mae'] = st.session_state.df_sales['Cod_Reserva']

    st.session_state.df_sales['Data_Servico'] = pd.to_datetime(st.session_state.df_sales['Data_Servico'], unit='s').dt.date

    st.session_state.df_router_2 = gerar_df_router('test_phoenix_recife')

    st.session_state.df_router_2 = st.session_state.df_router_2[(st.session_state.df_router_2['Status do Servico']!='CANCELADO') & (st.session_state.df_router_2['Status da Reserva']!='CANCELADO') & 
                                                            (st.session_state.df_router_2['Status da Reserva']!='PENDENCIA DE IMPORTAÇÃO') & 
                                                            (~pd.isna(st.session_state.df_router_2['Status do Servico'])) & 
                                                            (~st.session_state.df_router_2['Servico'].str.upper().str.contains('COMBO FLEX'))].reset_index(drop=True)

    st.session_state.df_router_2['Data Execucao'] = pd.to_datetime(st.session_state.df_router_2['Data Execucao'])

    st.session_state.df_router_2['Reserva Mae'] = st.session_state.df_router_2['Reserva'].str[:10]

def gerar_df_ultimos_servicos():

    df_ultimos_servicos = (st.session_state.df_router_2.groupby('Reserva Mae', as_index=False).agg({'Data Execucao': ['max', 'min']}))

    df_ultimos_servicos.columns = ['Reserva Mae', 'Data Ultimo Servico', 'Data Primeiro Servico']

    df_ultimos_servicos['Data Ultimo Servico'] = df_ultimos_servicos['Data Ultimo Servico'].dt.date

    df_ultimos_servicos['Data Primeiro Servico'] = df_ultimos_servicos['Data Primeiro Servico'].dt.date

    df_ultimos_servicos = df_ultimos_servicos.rename(columns={'Data Execucao': 'Data Ultimo Servico'})

    df_ultimos_servicos_filtrado = df_ultimos_servicos[(df_ultimos_servicos['Data Ultimo Servico'] >= st.session_state.data_inicial) & 
                                                       (df_ultimos_servicos['Data Ultimo Servico'] <= st.session_state.data_final)].reset_index(drop=True)
    
    return df_ultimos_servicos_filtrado

def incluir_data_in_out(df_ultimos_servicos_filtrado):

    df_in = st.session_state.df_router_2[st.session_state.df_router_2['Tipo de Servico']=='IN'].reset_index(drop=True)

    df_in['Data Execucao'] = df_in['Data Execucao'].dt.date

    df_in = df_in.rename(columns={'Data Execucao': 'Data IN'})

    df_ultimos_servicos_filtrado = pd.merge(df_ultimos_servicos_filtrado, df_in[['Reserva Mae', 'Data IN']], on='Reserva Mae', how='left')

    df_ultimos_servicos_filtrado = df_ultimos_servicos_filtrado[~pd.isna(df_ultimos_servicos_filtrado['Data IN'])].reset_index(drop=True)

    df_out = st.session_state.df_router_2[st.session_state.df_router_2['Tipo de Servico']=='OUT'].reset_index(drop=True)

    df_out['Data Execucao'] = df_out['Data Execucao'].dt.date

    df_out = df_out.rename(columns={'Data Execucao': 'Data OUT'})

    df_ultimos_servicos_filtrado = pd.merge(df_ultimos_servicos_filtrado, df_out[['Reserva Mae', 'Data OUT']], on='Reserva Mae', how='left')

    st.session_state.media_estadia = calcular_media_estadia(df_ultimos_servicos_filtrado)

    df_ultimos_servicos_filtrado.loc[pd.isna(df_ultimos_servicos_filtrado['Data OUT']), 'Data OUT'] = df_ultimos_servicos_filtrado['Data IN'] + timedelta(days=st.session_state.media_estadia)

    return df_ultimos_servicos_filtrado

def grafico_linha_percentual(referencia, eixo_x, eixo_y_1, ref_1_label, titulo):

    referencia[eixo_x] = referencia[eixo_x].astype(str)
    
    fig, ax = plt.subplots(figsize=(15, 8))
    
    plt.plot(referencia[eixo_x], referencia[eixo_y_1], label = ref_1_label, linewidth = 0.5, color = 'black')
    
    for i in range(len(referencia[eixo_x])):
        texto = str(int(referencia[eixo_y_1][i] * 100)) + "%"
        plt.text(referencia[eixo_x][i], referencia[eixo_y_1][i], texto, ha='center', va='bottom')

    plt.title(titulo, fontsize=30)
    plt.xlabel('Ano/Mês')
    ax.legend(loc='lower right', bbox_to_anchor=(1.2, 1))
    st.pyplot(fig)
    plt.close(fig)

def contabilizar_servicos_depois_in_e_total(df_ultimos_servicos_filtrado):

    for index, reserva in df_ultimos_servicos_filtrado['Reserva Mae'].items():

        data_in = df_ultimos_servicos_filtrado.at[index, 'Data IN']

        df_ultimos_servicos_filtrado.at[index, 'Servicos Depois do IN'] = len(st.session_state.df_sales[(st.session_state.df_sales['Reserva Mae']==reserva) & 
                                                                                                        (st.session_state.df_sales['Data_Servico']>=data_in)]['Data Execucao'].unique().tolist())

        df_ultimos_servicos_filtrado.at[index, 'Total Servicos'] = len(st.session_state.df_router_2[(st.session_state.df_router_2['Reserva Mae']==reserva)]['Data Execucao'].unique().tolist())

    return df_ultimos_servicos_filtrado

def contabilizar_dias_livres_chegada_e_saida(df_ultimos_servicos_filtrado):

    df_ultimos_servicos_filtrado['Dias Livres na Chegada'] = df_ultimos_servicos_filtrado['Dias Estadia'] - \
        (df_ultimos_servicos_filtrado['Total Servicos'] - df_ultimos_servicos_filtrado['Servicos Depois do IN'])

    df_ultimos_servicos_filtrado['Dias Livres na Saída'] = df_ultimos_servicos_filtrado['Dias Livres na Chegada'] - df_ultimos_servicos_filtrado['Servicos Depois do IN']

    df_ultimos_servicos_filtrado = df_ultimos_servicos_filtrado[df_ultimos_servicos_filtrado['Dias Livres na Saída']>-1].reset_index(drop=True)

    return df_ultimos_servicos_filtrado

def criar_colunas_ano_mes():

    st.session_state.df_salvo['ano'] = pd.to_datetime(st.session_state.df_salvo['Data OUT']).dt.year

    st.session_state.df_salvo['mes'] = pd.to_datetime(st.session_state.df_salvo['Data OUT']).dt.month

def ajustar_dataframe_group_mensal():

        df_group = st.session_state.df_salvo.groupby(['ano', 'mes'])[['Dias Livres na Chegada', 'Dias Livres na Saída']].sum().reset_index()

        df_group = df_group[(df_group['mes']>=data_inicial.month) & (df_group['mes']<=data_final.month)].reset_index(drop=True)

        df_group['mes/ano'] = pd.to_datetime(df_group['ano'].astype(str) + '-' + df_group['mes'].astype(str)).dt.to_period('M')

        df_group['Aproveitamento'] = round(-(df_group['Dias Livres na Saída']/df_group['Dias Livres na Chegada']-1), 2)

        st.session_state.df_group_salvo = df_group

st.set_page_config(layout='wide')

# Puxar dados do Phoenix

if not 'df_sales' in st.session_state:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

# Títulos da página

st.title('Aproveitamento Dias Livres - Recife')

st.subheader('*apenas paxs com TRF IN*')

st.divider()

row1 = st.columns(2)

row2 = st.columns(1)

# Botão pra atualizar dados do Phoenix

with row1[0]:

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

    if atualizar_phoenix:

        with st.spinner('Puxando dados do Phoenix...'):

            puxar_dados_phoenix()

# Container com botão de datas e botão de gerar análise

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Final', value=None ,format='DD/MM/YYYY', key='data_final')

    gerar_analise = container_datas.button('Gerar Análise')

# Script de geração de análise

if gerar_analise and data_final and data_inicial:

    # Tirando dados do dataframe usado pra mostrar gráfico

    if 'df_group_salvo' in st.session_state:

        st.session_state.df_group_salvo = st.session_state.df_group_salvo.iloc[0:0]

    # Inserir colunas de Data Ultimo Servico e Data Primeiro Servico e filtrar as reservas que tem ultimo serviço dentro do período especificado

    df_ultimos_servicos_filtrado = gerar_df_ultimos_servicos()

    # Inclusão de data de IN e data de OUT e definição de data de OUT em cima de média de estadia p/ reservas sem OUT

    df_ultimos_servicos_filtrado = incluir_data_in_out(df_ultimos_servicos_filtrado)

    # Criando coluna Dias Estadia

    df_ultimos_servicos_filtrado['Dias Estadia'] = (pd.to_datetime(df_ultimos_servicos_filtrado['Data OUT']) - pd.to_datetime(df_ultimos_servicos_filtrado['Data IN'])).dt.days + 1

    # Contabilizando Serviços Depois do IN e Total Serviços de cada reserva

    df_ultimos_servicos_filtrado = contabilizar_servicos_depois_in_e_total(df_ultimos_servicos_filtrado)

    # Contabilizar Dias Livres na Chegada e Dias Livres na Saída (e filtrando reservas que não tenham dias livres na saída > -1)

    df_ultimos_servicos_filtrado = contabilizar_dias_livres_chegada_e_saida(df_ultimos_servicos_filtrado)

    st.session_state.df_salvo = df_ultimos_servicos_filtrado

    if data_inicial.month != data_final.month:

        # Criando colunas ano e mes

        criar_colunas_ano_mes()

        # Agrupando dataframe por ano e mes, criando coluna mes_ano, filtrando período selecionado pelo usuário e calculando o aproveitamento de cada mês

        ajustar_dataframe_group_mensal()

# Gráfico de resultado de análise entre meses diferentes

if 'df_group_salvo' in st.session_state and len(st.session_state.df_group_salvo)>0:

    grafico_linha_percentual(st.session_state.df_group_salvo, 'mes/ano', 'Aproveitamento', 'Aproveitamento', 'Aproveitamento Dias Livres')

# Texto de resultado de análise

if 'df_salvo' in st.session_state:

    with row1[1]:

        dias_livres_na_chegada = st.session_state.df_salvo['Dias Livres na Chegada'].sum()

        dias_livres_na_saída = st.session_state.df_salvo['Dias Livres na Saída'].sum()

        aproveitamento = -(dias_livres_na_saída/dias_livres_na_chegada-1)

        st.subheader(f'Dias Livres na Chegada = {int(dias_livres_na_chegada)}')

        st.subheader(f'Dias Livres na Saída = {int(dias_livres_na_saída)}')

        st.subheader(f'Aproveitamento = {int(aproveitamento*100)}%')
