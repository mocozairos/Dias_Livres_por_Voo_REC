import streamlit as st
import mysql.connector
import decimal
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder
import gspread 
from google.cloud import secretmanager 
import json
from google.oauth2.service_account import Credentials

def gerar_df_phoenix(vw_name, base_luck):
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

    request_name = f'SELECT * FROM {vw_name}'

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

def puxar_dados_phoenix():

    st.session_state.df_router_bruto = gerar_df_phoenix('vw_router', st.session_state.base_luck)

    st.session_state.filtrar_servicos_geral = []

    st.session_state.filtrar_servicos_geral.extend(list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Serviços IN'].tolist())))

    st.session_state.filtrar_servicos_geral.extend(list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Serviços TOUR'].tolist())))

    st.session_state.df_router = \
        st.session_state.df_router_bruto[(~st.session_state.df_router_bruto['Status do Servico'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Status do Serviço'].tolist())))) & 
                                         (~st.session_state.df_router_bruto['Status da Reserva'].isin(list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Status da Reserva'].tolist())))) & 
                                         (~pd.isna(st.session_state.df_router_bruto[list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Colunas Vazias'].tolist()))]).any(axis=1)) &
                                         (~st.session_state.df_router_bruto['Servico'].isin(st.session_state.filtrar_servicos_geral))].reset_index(drop=True)

    st.session_state.df_router['Reserva Mae'] = st.session_state.df_router['Reserva'].str[:10]  
    
    st.session_state.df_router['Total Paxs'] = st.session_state.df_router[['Total ADT', 'Total CHD']].sum(axis=1)

def calcular_media_estadia():

    df_in_geral = st.session_state.df_router[(st.session_state.df_router['Tipo de Servico']=='IN')].reset_index(drop=True)

    df_in_geral = df_in_geral.drop_duplicates(subset=['Reserva Mae', 'Data Execucao']).reset_index(drop=True)

    df_in_geral = df_in_geral[~df_in_geral['Reserva Mae'].isin(df_in_geral[df_in_geral['Reserva Mae'].duplicated()]['Reserva Mae'].unique().tolist())].reset_index(drop=True)

    df_out_geral = st.session_state.df_router[(st.session_state.df_router['Tipo de Servico']=='OUT')].reset_index(drop=True)

    df_in_out_geral = pd.merge(df_in_geral[['Reserva Mae', 'Servico', 'Voo', 'Data Execucao']], df_out_geral[['Reserva Mae', 'Data Execucao']], on='Reserva Mae', how='left')

    df_in_out_geral = df_in_out_geral.rename(columns={'Data Execucao_x': 'Data IN', 'Data Execucao_y': 'Data OUT', 'Voo': 'Voo IN'})

    df_in_out_geral = df_in_out_geral[(~pd.isna(df_in_out_geral['Data OUT']))].reset_index(drop=True)

    df_in_out_geral['Dias Estadia'] = (pd.to_datetime(df_in_out_geral['Data OUT']) - pd.to_datetime(df_in_out_geral['Data IN'])).dt.days

    df_in_out_geral = df_in_out_geral[(df_in_out_geral['Dias Estadia']>=0)].reset_index(drop=True)

    df_in_out_geral['Dias Estadia'] = df_in_out_geral['Dias Estadia'].astype(int)

    df_in_out_geral = df_in_out_geral[~(pd.isna(df_in_out_geral['Voo IN']))].reset_index(drop=True)

    media_estadia = round(df_in_out_geral['Dias Estadia'].mean(), 0)

    return media_estadia

def inserir_datas_in_out_voo_in(df_in):

    lista_reservas_in = df_in['Reserva Mae'].unique().tolist()

    df_out = st.session_state.df_router[(st.session_state.df_router['Tipo de Servico']=='OUT') & (st.session_state.df_router['Reserva Mae'].isin(lista_reservas_in))].reset_index(drop=True)

    df_in_out = pd.merge(df_in[['Reserva Mae', 'Servico', 'Voo', 'Horario Voo', 'Data Execucao', 'Est Destino', 'Cliente', 'Telefone Cliente', 'Parceiro', 'Total Paxs']], 
                         df_out[['Reserva Mae', 'Data Execucao']], on='Reserva Mae', how='left')

    df_in_out = df_in_out.rename(columns={'Data Execucao_x': 'Data IN', 'Data Execucao_y': 'Data OUT', 'Voo': 'Voo IN'})

    return df_in_out, lista_reservas_in

def contabilizar_servicos_por_reserva(df_in_out, lista_reservas_in, data_relatorio):

    df_tour_transfer = st.session_state.df_router[(st.session_state.df_router['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])) & 
                                                  (st.session_state.df_router['Reserva Mae'].isin(lista_reservas_in)) & (st.session_state.df_router['Data Execucao']>=data_relatorio)]\
                                                    .reset_index(drop=True)

    df_tour_transfer_group = df_tour_transfer.groupby(['Data Execucao', 'Reserva Mae'])['Servico'].count().reset_index()

    df_tour_transfer_group = df_tour_transfer_group.groupby(['Reserva Mae'])['Servico'].count().reset_index()

    df_tour_transfer_group = df_tour_transfer_group.rename(columns={'Servico': 'Qtd. Servicos'})

    df_in_out = pd.merge(df_in_out, df_tour_transfer_group, on='Reserva Mae', how='left')

    df_in_out['Qtd. Servicos'] = df_in_out['Qtd. Servicos'].fillna(0)

    return df_in_out

def calcular_estadia_dias_livres(df_in_out):

    df_in_out['Dias Estadia'] = (pd.to_datetime(df_in_out['Data OUT']) - pd.to_datetime(df_in_out['Data IN'])).dt.days

    df_in_out['Dias Estadia'] = df_in_out['Dias Estadia'].fillna(media_estadia)

    df_in_out['Dias Estadia'] = df_in_out['Dias Estadia'].astype(int)

    df_in_out['Dias Estadia'] = df_in_out['Dias Estadia']-1

    st.session_state.df_reservas_negativas = df_in_out[df_in_out['Dias Estadia']<0].reset_index(drop=True)

    df_in_out = df_in_out[(df_in_out['Dias Estadia']>=0) & (df_in_out['Dias Estadia']<100)].reset_index(drop=True)

    df_in_out = df_in_out.drop_duplicates().reset_index(drop=True)

    df_in_out['Dias Livres'] = df_in_out['Dias Estadia']-df_in_out['Qtd. Servicos']

    return df_in_out

def plotar_tabela_com_voos_dias_livres(df_in_out):

    df_final = df_in_out.groupby('Voo IN').agg({'Horario Voo': 'first', 'Dias Livres': 'sum'}).reset_index()

    df_final = df_final.sort_values(by=['Dias Livres'], ascending=False).reset_index(drop=True)

    gb = GridOptionsBuilder.from_dataframe(df_final)
    gb.configure_selection('multiple', use_checkbox=True, header_checkbox=True)
    gb.configure_grid_options(domLayout='autoHeight')
    gb.configure_grid_options(domLayout='autoWidth')
    gridOptions = gb.build()

    with row1[1]:

        grid_response = AgGrid(df_final, gridOptions=gridOptions, enable_enterprise_modules=False, fit_columns_on_grid_load=True)

    selected_rows = grid_response['selected_rows']

    return selected_rows

def plotar_tabela_dias_livres_por_hotel(df_ref_2):
            
    gb = GridOptionsBuilder.from_dataframe(df_ref_2)
    gb.configure_selection('multiple', use_checkbox=True, header_checkbox=True)
    gb.configure_grid_options(domLayout='autoWidth')
    gridOptions = gb.build()

    with row1[0]:

        grid_response = AgGrid(df_ref_2, gridOptions=gridOptions, enable_enterprise_modules=False, fit_columns_on_grid_load=True)

    selected_rows_3 = grid_response['selected_rows']

    return selected_rows_3

def plotar_tabela_row_servico_especifico(df_ref_3, row2):

    with row2[0]:

        container_dataframe = st.container()

        container_dataframe.dataframe(df_ref_3[['Reserva Mae', 'Cliente', 'Telefone Cliente', 'Servico', 'Est Destino', 'Voo IN', 'Data IN', 'Data OUT', 'Total Paxs', 'Qtd. Servicos', 'Dias Estadia', 
                                                'Dias Livres']].sort_values(by='Voo IN'), hide_index=True, use_container_width=True)

def plotar_tabela_servicos_no_voo(df_ref):

    gb = GridOptionsBuilder.from_dataframe(df_ref)
    gb.configure_selection('multiple', use_checkbox=True, header_checkbox=True)
    gb.configure_grid_options(domLayout='autoWidth')
    gridOptions = gb.build()

    with row1[1]:

        grid_response = AgGrid(df_ref, gridOptions=gridOptions, enable_enterprise_modules=False, fit_columns_on_grid_load=True)

    selected_rows_2 = grid_response['selected_rows']

    return selected_rows_2

def recalcular_servicos_reservas_diferentes(df_in_out, data_relatorio):

    df_in_out['Data IN'] = pd.to_datetime(df_in_out['Data IN'])
    df_in_out['Data OUT'] = pd.to_datetime(df_in_out['Data OUT'])

    df_router_2 = st.session_state.df_router[(st.session_state.df_router['Data Execucao']>=data_relatorio) & (st.session_state.df_router['Tipo de Servico'].isin(['TOUR', 'TRANSFER']))]\
        .reset_index(drop=True)
    df_router_2['Chave'] = df_router_2['Cliente'] + "|" + df_router_2['Parceiro']

    df_in_out['Chave'] = df_in_out['Cliente'] + "|" + df_in_out['Parceiro']

    dias_por_chave = df_router_2.groupby('Chave')['Data Execucao'].nunique()
    
    df_in_out['Qtd. Servicos'] = df_in_out['Chave'].map(dias_por_chave).fillna(df_in_out['Qtd. Servicos'])

    df_in_out['Dias Livres'] = df_in_out['Dias Estadia'] - df_in_out['Qtd. Servicos']

    return df_in_out

def puxar_aba_simples(id_gsheet, nome_aba, nome_df):

    # GCP projeto onde está a chave credencial
    project_id = "grupoluck"

    # ID da chave credencial do google.
    secret_id = "cred-luck-aracaju"

    # Cria o cliente.
    secret_client = secretmanager.SecretManagerServiceClient()

    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})

    secret_payload = response.payload.data.decode("UTF-8")

    credentials_info = json.loads(secret_payload)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Use the credentials to authorize the gspread client
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(id_gsheet)
    
    sheet = spreadsheet.worksheet(nome_aba)

    sheet_data = sheet.get_all_values()

    st.session_state[nome_df] = pd.DataFrame(sheet_data[1:], columns=sheet_data[0])

def inserir_config(df_itens_faltantes, id_gsheet, nome_aba):

    # GCP projeto onde está a chave credencial
    project_id = "grupoluck"

    # ID da chave credencial do google.
    secret_id = "cred-luck-aracaju"

    # Cria o cliente.
    secret_client = secretmanager.SecretManagerServiceClient()

    secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_name})

    secret_payload = response.payload.data.decode("UTF-8")

    credentials_info = json.loads(secret_payload)

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    # Use the credentials to authorize the gspread client
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scopes)
    client = gspread.authorize(credentials)
    
    spreadsheet = client.open_by_key(id_gsheet)

    sheet = spreadsheet.worksheet(nome_aba)

    sheet.batch_clear(["A2:Z100"])

    data = df_itens_faltantes.values.tolist()
    sheet.update('A2', data)

    st.success('Configurações salvas com sucesso!')
st.set_page_config(layout='wide')

st.session_state.base_luck = 'test_phoenix_recife'

st.session_state.id_sheet = '1d3EkHqSuGgMERs_JsUsHSJ83od91Un7DfINFtsFs8yM'

st.session_state.aba_sheet = 'Configurações Recife'

st.session_state.titulo = 'Dias Livres por Hotel - Recife'

if not 'df_config' in st.session_state:

    puxar_aba_simples(st.session_state.id_sheet, st.session_state.aba_sheet, 'df_config')

if not 'mostrar_config' in st.session_state:

    st.session_state.mostrar_config = False

st.title(st.session_state.titulo)

row0 = st.columns(1)

st.divider()

st.header('Configurações')

alterar_configuracoes = st.button('Visualizar Configurações')

if alterar_configuracoes:

    if st.session_state.mostrar_config == True:

        st.session_state.mostrar_config = False

    else:

        st.session_state.mostrar_config = True

row01 = st.columns(3)

if st.session_state.mostrar_config == True:

    with row01[0]:

        filtrar_status_servico = st.multiselect('Excluir Status do Serviço', sorted(st.session_state.df_router_bruto['Status do Servico'].unique().tolist()), key='filtrar_status_servico', 
                                                default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Status do Serviço'].tolist())))

        filtrar_status_reserva = st.multiselect('Excluir Status da Reserva', sorted(st.session_state.df_router_bruto['Status da Reserva'].unique().tolist()), key='filtrar_status_reserva', 
                                                default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Status da Reserva'].tolist())))

    with row01[1]:
        
        filtrar_servicos_in = st.multiselect('Excluir Serviços IN', sorted(st.session_state.df_router_bruto[st.session_state.df_router_bruto['Tipo de Servico']=='IN']['Servico'].unique().tolist()), 
                                            key='filtrar_servicos_in', default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Serviços IN'].tolist())))
        
        filtrar_servicos_tt = st.multiselect('Excluir Serviços TOUR', 
                                            sorted(st.session_state.df_router_bruto[st.session_state.df_router_bruto['Tipo de Servico'].isin(['TOUR', 'TRANSFER'])]['Servico'].unique().tolist()), 
                                            key='filtrar_servicos_tt', default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Serviços TOUR'].tolist())))
        
    with row01[2]:

        filtrar_colunas_vazias = st.multiselect('Não Permitir Valor Vazio', sorted(st.session_state.df_router_bruto.columns.tolist()), key='filtrar_colunas_vazias', 
                                                default=list(filter(lambda x: x != '', st.session_state.df_config['Filtrar Colunas Vazias'].tolist())))
        
        hoteis_all_inclusive = st.multiselect('Hoteis All Inclusive', 
                                            sorted(st.session_state.df_router_bruto[st.session_state.df_router_bruto['Tipo de Servico']=='IN']['Est Destino'].dropna().unique().tolist()), 
                                            key='hoteis_all_inclusive', default=list(filter(lambda x: x != '', st.session_state.df_config['Hoteis All Inclusive'].tolist())))
        
        st.session_state.filtrar_servicos_geral = []

        st.session_state.filtrar_servicos_geral.extend(filtrar_servicos_in)

        st.session_state.filtrar_servicos_geral.extend(filtrar_servicos_tt)

    salvar_config = st.button('Salvar Configurações')

    if salvar_config:

        lista_escolhas = [filtrar_status_servico, filtrar_status_reserva, filtrar_colunas_vazias, filtrar_servicos_in, filtrar_servicos_tt, hoteis_all_inclusive]

        st.session_state.df_config = pd.DataFrame({f'Coluna{i+1}': pd.Series(lista) for i, lista in enumerate(lista_escolhas)})

        st.session_state.df_config = st.session_state.df_config.fillna('')

        inserir_config(st.session_state.df_config, st.session_state.id_sheet, st.session_state.aba_sheet)

        puxar_aba_simples(st.session_state.id_sheet, st.session_state.aba_sheet, 'df_config')

# Puxando dados do Phoenix

if not 'df_router' in st.session_state:

    with st.spinner('Puxando dados do Phoenix...'):

        puxar_dados_phoenix()

if not 'df_final' in st.session_state:

    st.session_state.df_final = pd.DataFrame(columns=['Reserva Mae', 'Servico', 'Voo IN', 'Horario Voo', 'Data IN', 'Est Destino', 'Cliente', 'Telefone Cliente', 'Parceiro', 'Total Paxs', 'Data OUT', 
                                                      'Qtd. Servicos', 'Dias Estadia', 'Dias Livres', 'Chave'])

st.divider()

row1 = st.columns(2)

row2 = st.columns(1)

# Botão pra puxar dados do Phoenix manualmente

with row1[0]:

    atualizar_phoenix = st.button('Atualizar Dados Phoenix')

    if atualizar_phoenix:

        with st.spinner('Puxando dados do Phoenix...'):

            puxar_dados_phoenix()

# Botões de input de datas

with row1[0]:

    container_datas = st.container(border=True)

    container_datas.subheader('Período')

    data_inicial = container_datas.date_input('Data Inicial', value=None ,format='DD/MM/YYYY', key='data_inicial')

    data_final = container_datas.date_input('Data Final', value=None ,format='DD/MM/YYYY', key='data_final')

    gerar_relatorio = container_datas.button('Gerar Relatório')

if gerar_relatorio and data_inicial and data_final and data_inicial==data_final:

    # Pegando reservas que tenham TRF IN dentro do período

    df_in = st.session_state.df_router[(st.session_state.df_router['Data Execucao'] == data_inicial) & (st.session_state.df_router['Tipo de Servico']=='IN')].reset_index(drop=True)
    
    # Retirando linhas que tem mais de um IN na mesma data

    df_in = df_in.drop_duplicates(subset=['Reserva Mae', 'Data Execucao']).reset_index(drop=True)

    # Calculando média de estadia

    media_estadia = calcular_media_estadia()

    # Inserindo colunas Data IN, Data OUT e Voo IN

    df_in_out, lista_reservas_in = inserir_datas_in_out_voo_in(df_in)

    # Inserindo contabilização de serviços por reserva

    df_in_out = contabilizar_servicos_por_reserva(df_in_out, lista_reservas_in, data_inicial)

    # Calculando Estadia de reservas e Dias Livres

    df_in_out = calcular_estadia_dias_livres(df_in_out)

    # Recalcular número de serviços de reservas diferentes que deveriam ser de uma mesma reserva

    df_in_out = recalcular_servicos_reservas_diferentes(df_in_out, data_inicial)

    df_in_out = df_in_out[df_in_out['Dias Livres']>=0].reset_index(drop=True)

    st.session_state.df_final = df_in_out

    st.session_state.df_final['Data IN'] = st.session_state.df_final['Data IN'].dt.date

    st.session_state.df_final['Data OUT'] = st.session_state.df_final['Data OUT'].dt.date

elif gerar_relatorio and data_inicial and data_final and data_inicial<data_final:

    barra_progresso = st.progress(0, text='Gerando dias livres...')

    progresso = 0

    st.session_state.df_final = pd.DataFrame(columns=['Reserva Mae', 'Servico', 'Voo IN', 'Horario Voo', 'Data IN', 'Est Destino', 'Cliente', 'Telefone Cliente', 'Parceiro', 'Total Paxs', 'Data OUT', 
                                                      'Qtd. Servicos', 'Dias Estadia', 'Dias Livres', 'Chave'])

    intervalo_datas = pd.date_range(start=data_inicial, end=data_final)

    for data_ref in intervalo_datas:

        progresso+=1/len(intervalo_datas)

        data_ref = data_ref.date()

        # Pegando reservas que tenham TRF IN dentro do período

        df_in = st.session_state.df_router[(st.session_state.df_router['Data Execucao'] == data_ref) & (st.session_state.df_router['Tipo de Servico']=='IN')].reset_index(drop=True)
        
        # Retirando linhas que tem mais de um IN na mesma data

        df_in = df_in.drop_duplicates(subset=['Reserva Mae', 'Data Execucao']).reset_index(drop=True)

        # Calculando média de estadia

        media_estadia = calcular_media_estadia()

        # Inserindo colunas Data IN, Data OUT e Voo IN

        df_in_out, lista_reservas_in = inserir_datas_in_out_voo_in(df_in)

        # Inserindo contabilização de serviços por reserva

        df_in_out = contabilizar_servicos_por_reserva(df_in_out, lista_reservas_in, data_ref)

        # Calculando Estadia de reservas e Dias Livres

        df_in_out = calcular_estadia_dias_livres(df_in_out)

        # Recalcular número de serviços de reservas diferentes que deveriam ser de uma mesma reserva

        df_in_out = recalcular_servicos_reservas_diferentes(df_in_out, data_ref)

        df_in_out = df_in_out[df_in_out['Dias Livres']>=0].reset_index(drop=True)

        st.session_state.df_final = pd.concat([st.session_state.df_final, df_in_out], ignore_index=True)

        barra_progresso.progress(progresso, text='Gerando dias livres...')

    st.session_state.df_final['Data IN'] = st.session_state.df_final['Data IN'].dt.date

    st.session_state.df_final['Data OUT'] = st.session_state.df_final['Data OUT'].dt.date

    barra_progresso.empty()

if 'df_reservas_negativas' in st.session_state:

    nomes_reservas = ', '.join(st.session_state.df_reservas_negativas['Reserva Mae'].unique().tolist())

    n_reservas = len(st.session_state.df_reservas_negativas['Reserva Mae'].unique().tolist())

    if n_reservas>0:

        with row0[0]:

            with st.expander(f'*Existem {n_reservas} reservas com data de OUT antes do IN e, portanto, foram desconsideradas da análise*'):

                st.markdown(f'*{nomes_reservas}*')

# Plotando tabela com voos e pegando a seleção do usuário

if len(st.session_state.df_final)>0:

    selected_rows = plotar_tabela_com_voos_dias_livres(st.session_state.df_final)

    # Segunda plotagem de tabelas depois do usuário selecionar voos e serviços

    if selected_rows is not None and len(selected_rows)>0:

        df_ref = st.session_state.df_final[st.session_state.df_final['Voo IN'].isin(selected_rows['Voo IN'].unique().tolist())].reset_index(drop=True)

        total_dias_livres = df_ref['Dias Livres'].sum()

        total_paxs_ref = df_ref['Total Paxs'].sum()

        with row1[1]:

            st.subheader(f'Total de dias livres dos voos selecionados = {int(total_dias_livres)}')

            st.subheader(f'Total de paxs dos voos selecionados = {int(total_paxs_ref)}')

        df_ref = st.session_state.df_final[st.session_state.df_final['Voo IN'].isin(selected_rows['Voo IN'].unique().tolist())].groupby(['Servico'])['Dias Livres'].sum().reset_index()   

        selected_rows_2 = plotar_tabela_servicos_no_voo(df_ref)

        if selected_rows_2 is not None and len(selected_rows_2)>0:

            df_ref_2 = st.session_state.df_final[(st.session_state.df_final['Voo IN'].isin(selected_rows['Voo IN'].unique().tolist())) & 
                                                 (st.session_state.df_final['Servico'].isin(selected_rows_2['Servico'].unique().tolist()))].groupby('Est Destino')['Dias Livres'].sum().reset_index()
            
            selected_rows_3 = plotar_tabela_dias_livres_por_hotel(df_ref_2)

            if selected_rows_3 is not None and len(selected_rows_3)>0:

                df_ref_3 = st.session_state.df_final[(st.session_state.df_final['Voo IN'].isin(selected_rows['Voo IN'].unique().tolist())) & 
                                                     (st.session_state.df_final['Servico'].isin(selected_rows_2['Servico'].unique().tolist())) & 
                                                     (st.session_state.df_final['Est Destino'].isin(selected_rows_3['Est Destino'].unique().tolist()))].reset_index(drop=True)
            
                plotar_tabela_row_servico_especifico(df_ref_3, row2)
