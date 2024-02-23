#-----------------------------------------------------------------------
import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import asyncio
import aiohttp
from datetime import datetime
import re
import csv
import os
#------------------------------------------------------ 
separador = "-----------------------------------"
#------------------------------------------------------
print(' INICIO DE COMUNICAÇÃO ')
#------------------------------------------------------
def dolar():
    global cotacao_dolar
    global bid_value

    print(separador)
    global inicio_geral, inicio_str
    inicio_binance = datetime.now().time()
    inicio_str = inicio_binance.strftime(" %H:%M:%S")
    inicio_geral = datetime.strptime(inicio_str, " %H:%M:%S")
    print(" 📜 - INI:", inicio_str)
    link = 'https://economia.awesomeapi.com.br/last/USD-BRL'
    response = requests.get(link)
    if response.status_code == 200:
        data = response.json()
        usd_brl_data = data.get('USDBRL', {})
        bid_value = float(usd_brl_data.get('bid', 0))
        cotacao_dolar = bid_value
        with open('dolar.html', 'w', encoding='utf-8') as file:
            file.write(f"USD to BRL: {bid_value}")
    print(f' 💵 - DOL: R$ {bid_value:.2f}')
dolar()
########################################################
#  001 -  BINANCE    
#######################################################
def full_bin():
    def binance_api():
        print(separador)
        print(" ✅ - 001:  API Binance")
        global inicio_datetime, inicio_str
        inicio_binance = datetime.now().time()
        inicio_str = inicio_binance.strftime(" %H:%M:%S")
        inicio_datetime = datetime.strptime(inicio_str, " %H:%M:%S")
        print(" 🔍 - INI:", inicio_str)
        #link = 'https://api.binance.com/api/v3/ticker/24hr
        link = 'https://api.binance.com/api/v3/ticker/price'
        response = requests.get(link)
        if response.status_code == 200:
            page_content = response.content
            soup = BeautifulSoup(page_content, 'html.parser')
            with open('dom_binance.json', 'w', encoding='utf-8') as file:
                file.write(soup.prettify())
    binance_api()
    #------------------------------------------------------
    def binance_csv():
        with open('dom_binance.json') as f:
            data = json.load(f)
        df = pd.DataFrame(data)
        df.to_csv('tabela_binance.csv', index=False)
        #--------------------------------------------------
        fim_binance = datetime.now().time()
        fim_str = fim_binance.strftime(" %H:%M:%S")
        print(" 🔍 - FIM:", fim_str)
        fim_datetime = datetime.strptime(fim_str, " %H:%M:%S")
        tempo_decorrido = fim_datetime - inicio_datetime
        print(" 🕰️  - TEM: ", tempo_decorrido)
        print(separador)
        #--------------------------------------------------
    binance_csv()
    #------------------------------------------------------
    def header_binance():
        file = 'tabela_binance.csv'
        df = pd.read_csv(file)
        df.columns = [coluna if coluna != 'symbol' else 'symbol_bin' for coluna in df.columns]
        df.columns = [coluna if coluna != 'price' else 'buy_bin' for coluna in df.columns]
        df.to_csv(file, index=False)
    header_binance()
    #------------------------------------------------------
    def binance_usdt():
        import pandas as pd
        df = pd.read_csv('tabela_binance.csv')
        df = df[df['symbol_bin'].str.endswith('USDT')]
        df.to_csv('tabela_binance.csv', index=False)
    binance_usdt()
    #------------------------------------------------------
    def binance_kill():
        import pandas as pd
        df = pd.read_csv('tabela_binance.csv')
        for index, row in df.iterrows():
            df.at[index, 'symbol_bin'] = row['symbol_bin'].replace('USDT', '')
        df.to_csv('tabela_binance.csv', index=False)
    binance_kill()
    #------------------------------------------------------
    def bin_brl():
        nome_arquivo = 'tabela_binance.csv'
        df = pd.read_csv(nome_arquivo)
        df['brl_bin'] = None
        df.to_csv('tabela_binance.csv', index=False)
    bin_brl()
    #------------------------------------------------------
    def brl_bin():
        global cotacao_dolar
        nome_arquivo = 'tabela_binance.csv'
        df = pd.read_csv(nome_arquivo)
        df['buy_bin'] = pd.to_numeric(df['buy_bin'], errors='coerce')
        df['buy_bin'].fillna(0, inplace=True)
        df['brl_bin'] = df['buy_bin'] * cotacao_dolar
        df.to_csv('tabela_binance.csv', index=False)
    brl_bin()
    #------------------------------------------------------
    def binance_end():
        import pandas as pd
        arquivo_entrada = 'tabela_binance.csv'
        nome_coluna_apagar = 'buy_bin'  
        dados_csv = pd.read_csv(arquivo_entrada)
        dados_csv = dados_csv.drop(columns=[nome_coluna_apagar], axis=1)
        dados_csv.to_csv(arquivo_entrada, index=False)
    binance_end()
full_bin()
#######################################################
#  002 - ERCADO BITCOIN    
#######################################################
def full_mb():
    global contador_qtt_mb
    contador_qtt_mb = 0
    def mb_symbols():
        global bloco_symbolsmc
        print(" ✅ - 002:  API Mercado Bitcoin")
        #-------------------------------------------------
        inicio_binance = datetime.now().time()
        inicio_str = inicio_binance.strftime(" %H:%M:%S")
        inicio_datetime = datetime.strptime(inicio_str, " %H:%M:%S")
        print(" 🔍 - INI:", inicio_str)
        #-------------------------------------------------
        link = 'https://api.mercadobitcoin.net/api/v4/symbols'
        response = requests.get(link)
        if response.status_code == 200:
            page_content = response.content
            soup = BeautifulSoup(page_content, 'html.parser')
            with open('mb_symbols.json', 'w', encoding='utf-8') as file:
                file.write(soup.prettify())
        #-------------------------------------------------
        with open('mb_symbols.json', 'r') as file:
            symbols_json = file.read()
        pattern = re.compile(r'\[(.*?)\]')
        matches = re.findall(pattern, symbols_json)
        if matches:
            bloco_symbolsmc = matches[0]
            symbols_list = bloco_symbolsmc.split(',')
            symbols_list = [symbol.strip() for symbol in symbols_list]
            with open('mb_symbols.csv', 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['symbols'])
                for symbol in symbols_list:
                    symbol = symbol.replace('"', '')
                    #print(symbol)
                    writer.writerow([symbol])
            #print('CSV criado com sucesso.')
        else:
            print("Nenhuma correspondência encontrada entre colchetes.")
    mb_symbols()
    #------------------------------------------------------
    def mb_csv():
        caminho_arquivo = 'mb_symbols.csv'
        df = pd.read_csv(caminho_arquivo)
        contador_save = 0
        contador_linhas = len(df)
        #print(f'O arquivo CSV tem {contador_linhas} linhas ')
        tamanho_bloco = 100
        num_consultas = (contador_linhas // tamanho_bloco) + (contador_linhas % tamanho_bloco > 0)
        for i in range(num_consultas):
            inicio = i * tamanho_bloco
            fim = min((i + 1) * tamanho_bloco, contador_linhas)
            symbols_mc = df['symbols'].iloc[inicio:fim].tolist()
            params = {'symbols': symbols_mc}
            headers = {'Content-Type': 'application/json'}
            url = "https://api.mercadobitcoin.net/api/v4/tickers"
            try:
                response = requests.get(url, params=params, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    nome_arquivo = f'mb_bloco{i + 1}.json'
                    with open(nome_arquivo, 'w', encoding='utf-8') as arquivo:
                        json.dump(data, arquivo, ensure_ascii=False, indent=2)
                    #print(f"bloco {i + 1} salva como {nome_arquivo}")
                    contador_save += 1
                else:
                    print(f"Erro na consulta do bloco {i + 1}. Status code: {response.status_code}")
            except Exception as e:
                print(f"Erro na consulta do bloco {i + 1}: {str(e)}")
        #------------------------------------------------------
        dados_unificados = []
        for numero_arquivo in range(1, i + 1):
            nome_arquivo = f'mb_bloco{numero_arquivo}.json'
            try:
                with open(nome_arquivo, 'r') as arquivo:
                    dados_arquivo = json.load(arquivo)
                    dados_unificados.extend(dados_arquivo)
            except FileNotFoundError:
                print(f'Arquivo {nome_arquivo} não encontrado.')
        #------------------------------------------------------
        with open('dom_mercadobitcoin.json', 'w') as arquivo_unificado:
            json.dump(dados_unificados, arquivo_unificado, indent=2)
        #------------------------------------------------------
        with open('dom_mercadobitcoin.json', 'r') as file:
            data = json.load(file)
        pairs_data = []
        for item in data:
            pair = item.get('pair', '')
            buy = item.get('buy', '')
            #sell = item.get('sell', '')
            #vol = item.get('vol', '')
            #pairs_data.append([pair, buy, sell, vol])
            pairs_data.append([pair, buy])
        #df = pd.DataFrame(pairs_data, columns=['pair', 'buy', 'sell', 'vol'])
        df = pd.DataFrame(pairs_data, columns=['symbol_mb', 'buy'])
        df.to_csv('tabela_mercadobitcoin.csv', index=False)
        #------------------------------------------------------
        fim_binance = datetime.now().time()
        fim_str = fim_binance.strftime(" %H:%M:%S")
        print(" 🔍 - FIM:", fim_str)
        fim_datetime = datetime.strptime(fim_str, " %H:%M:%S")
        tempo_decorrido = fim_datetime - inicio_datetime
        print(" 🕰️  - TEM: ", tempo_decorrido)
        print(separador)
    mb_csv()
    #------------------------------------------------------
    def mb_header():
        file = 'tabela_mercadobitcoin.csv'
        df = pd.read_csv(file)
        df.columns = [coluna if coluna != 'buy' else 'brl_mb' for coluna in df.columns]
        df.to_csv(file, index=False)
    mb_header()
    #------------------------------------------------------
    def mb_brlkill():
        import pandas as pd
        df = pd.read_csv('tabela_mercadobitcoin.csv')
        for index, row in df.iterrows():
            df.at[index, 'symbol_mb'] = row['symbol_mb'].replace('-BRL', '')
        df.to_csv('tabela_mercadobitcoin.csv', index=False)
    mb_brlkill()
    #------------------------------------------------------
full_mb()
#######################################################
#  003 - GATE.IO   
#######################################################
def full_gat():
    def dom_gateio():
        global inicio_gt2
        print(' ✅ - 003:  API Gate.io')
        inicio_gate = datetime.now().time()
        inicio_g1 = inicio_gate.strftime(" %H:%M:%S")
        inicio_gt2 = datetime.strptime(inicio_g1, " %H:%M:%S")
        print(" 🔍 - INI:", inicio_g1)
        #------------------------------------------------------
        link = 'https://data.gateapi.io/api2/1/marketlist'
        #------------------------------------------------------
        response = requests.get(link)
        #------------------------------------------------------
        if response.status_code == 200:
            page_content = response.content
            soup = BeautifulSoup(page_content, 'html.parser')
            with open('dom_gateio.json', 'w', encoding='utf-8') as file:
                file.write(soup.prettify())
        #------------------------------------------------------
    dom_gateio()
    #------------------------------------------------------
    def csv_gateio():
        import pandas as pd
        with open('dom_gateio.json', 'r') as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError:
                file.seek(0)  # Volta para o início do arquivo
                data = json.loads(file.read())
        if isinstance(data.get("data"), list):
            symbols = []
            rates = []
            for item in data["data"]:
                symbol = item.get('symbol', '')
                rate = item.get('rate', '')
                symbols.append(symbol)
                rates.append(rate)
            df = pd.DataFrame({'Symbol': symbols, 'Rate': rates})
            df.to_csv('tabela_gateio.csv', index=False)
        else:
            print("A chave 'data' não foi encontrada ou seu valor não é uma lista JSON.")
    csv_gateio()
    #------------------------------------------------------
    def header_gateio():
        try:
            file_gateio = 'tabela_gateio.csv'
            df = pd.read_csv(file_gateio)
            df.columns = [coluna if coluna != 'Symbol' else 'symbol_gat' for coluna in df.columns]
            df.columns = [coluna if coluna != 'Rate' else 'buy_gat' for coluna in df.columns]
            df.to_csv(file_gateio, index=False)
        except Exception as e:
            print(f"An error occurred: {e}")
    header_gateio()
    #------------------------------------------------------
    def gate_iofim():
        fim_gateio = datetime.now().time()
        fim_gt1 = fim_gateio.strftime(" %H:%M:%S")
        print(" 🔍 - FIM:", fim_gt1)
        fim_gt2 = datetime.strptime(fim_gt1, " %H:%M:%S")
        tempo_decorrido = fim_gt2 - inicio_gt2
        print(" 🕰️  - TEM: ", tempo_decorrido)
        print(separador)
    gate_iofim()
    #------------------------------------------------------
    def brl_gateio():
        nome_arquivo = 'tabela_gateio.csv'
        df = pd.read_csv(nome_arquivo)
        df['brl_gat'] = None
        df.to_csv('tabela_gateio.csv', index=False)
    brl_gateio()
    #------------------------------------------------------
    def brl_gat():
        global cotacao_dolar
        nome_arquivo = 'tabela_gateio.csv'
        df = pd.read_csv(nome_arquivo)
        df['buy_gat'] = pd.to_numeric(df['buy_gat'], errors='coerce')
        df['buy_gat'].fillna(0, inplace=True)
        df['brl_gat'] = df['buy_gat'] * cotacao_dolar
        df.to_csv('tabela_gateio.csv', index=False)
    brl_gat()
    #------------------------------------------------------
    def gateio_end():
        import pandas as pd
        arquivo_entrada = 'tabela_gateio.csv'
        nome_coluna_apagar = 'buy_gat'  
        dados_csv = pd.read_csv(arquivo_entrada)
        dados_csv = dados_csv.drop(columns=[nome_coluna_apagar], axis=1)
        dados_csv.to_csv(arquivo_entrada, index=False)
    gateio_end()
    #------------------------------------------------------
    def gate_io_clean():
        df = pd.read_csv('tabela_gateio.csv')
        df = df[df['brl_gat'] != 0]
        df.to_csv('tabela_gateio.csv', index=False)
    gate_io_clean()
    #------------------------------------------------------
    def gateio_duplicadasfix():
        import pandas as pd
        df = pd.read_csv('tabela_gateio.csv')
        idx_max = df.groupby('symbol_gat')['brl_gat'].idxmax()
        df = df.loc[idx_max]
        df.to_csv('tabela_gateio.csv', index=False)
    gateio_duplicadasfix()
    #------------------------------------------------------
    def gateio_zeroclean():
        import pandas as pd
        df = pd.read_csv('tabela_gateio.csv')
        df = df[df['brl_gat'] != 0]
        df.to_csv('tabela_gateio.csv', index=False)
    gateio_zeroclean()
    #------------------------------------------------------
full_gat()
#######################################################
#  004 - BITSO   
#######################################################
def full_bitso():
    def bitso_api():
        global inicio_gt2
        print(' ✅ - 004:  API Bitso ')
        inicio_gate = datetime.now().time()
        inicio_g1 = inicio_gate.strftime(" %H:%M:%S")
        inicio_gt2 = datetime.strptime(inicio_g1, " %H:%M:%S")
        print(" 🔍 - INI:", inicio_g1)
        url = "https://sandbox.bitso.com/api/v3/ticker/"
        response = requests.get(url)
        if response.status_code == 200:
            page_content = response.content
            soup = BeautifulSoup(page_content, 'html.parser')
            with open('dom_bitso.json', 'w', encoding='utf-8') as file:
                file.write(soup.prettify())
    bitso_api()
    #------------------------------------------------------
    def bitso_csv():
        json_filename = 'dom_bitso.json'
        with open(json_filename, 'r') as json_file:
            data = json.load(json_file)
        csv_filename = 'tabela_bitso.csv'
        with open(csv_filename, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['book', 'bid'])
            for item in data['payload']:
                csv_writer.writerow([item['book'], item['bid']])
        #print(f'CSV gerado com sucesso a partir do arquivo JSON: {csv_filename}')
        fim_gateio = datetime.now().time()
        fim_gt1 = fim_gateio.strftime(" %H:%M:%S")
        print(" 🔍 - FIM:", fim_gt1)
        fim_gt2 = datetime.strptime(fim_gt1, " %H:%M:%S")
        tempo_decorrido = fim_gt2 - inicio_gt2
        print(" 🕰️  - TEM: ", tempo_decorrido)
        print(separador)
    bitso_csv()
    #------------------------------------------------------
    def header_bitso():
        try:
            file_gateio = 'tabela_bitso.csv'
            df = pd.read_csv(file_gateio)
            df.columns = [coluna if coluna != 'book' else 'symbol_bit' for coluna in df.columns]
            df.columns = [coluna if coluna != 'bid' else 'buy_bit' for coluna in df.columns]
            df.to_csv(file_gateio, index=False)
        except Exception as e:
            print(f"An error occurred: {e}")
    header_bitso()
    #------------------------------------------------------
    def bitso_usd():
        import pandas as pd
        df = pd.read_csv('tabela_bitso.csv')
        df = df[df['symbol_bit'].str.endswith('_usd')]
        df.to_csv('tabela_bitso.csv', index=False)
    bitso_usd()
    #------------------------------------------------------
    def bitso_kill():
        import pandas as pd
        df = pd.read_csv('tabela_bitso.csv')
        for index, row in df.iterrows():
            df.at[index, 'symbol_bit'] = row['symbol_bit'].replace('_usd', '')
        df.to_csv('tabela_bitso.csv', index=False)
    bitso_kill()
    #------------------------------------------------------
    def bitso_caps():
        df = pd.read_csv('tabela_bitso.csv')
        df['symbol_bit'] = df['symbol_bit'].str.upper()
        df.to_csv('tabela_bitso.csv', index=False)
    bitso_caps()
    #------------------------------------------------------
    def brl_bitso():
        nome_arquivo = 'tabela_bitso.csv'
        df = pd.read_csv(nome_arquivo)
        df['brl_bit'] = None
        df.to_csv('tabela_bitso.csv', index=False)
    brl_bitso()
    #------------------------------------------------------
    def brl_bit():
        global cotacao_dolar
        nome_arquivo = 'tabela_bitso.csv'
        df = pd.read_csv(nome_arquivo)
        df['buy_bit'] = pd.to_numeric(df['buy_bit'], errors='coerce')
        df['buy_bit'].fillna(0, inplace=True)
        df['brl_bit'] = df['buy_bit'] * cotacao_dolar
        df.to_csv('tabela_bitso.csv', index=False)
    brl_bit()
    #------------------------------------------------------
    def bitso_end():
        import pandas as pd
        arquivo_entrada = 'tabela_bitso.csv'
        nome_coluna_apagar = 'buy_bit'  
        dados_csv = pd.read_csv(arquivo_entrada)
        dados_csv = dados_csv.drop(columns=[nome_coluna_apagar], axis=1)
        dados_csv.to_csv(arquivo_entrada, index=False)
    bitso_end()
full_bitso()
#######################################################
#  005 - MEXC   
#######################################################
def full_mex():
    def mex_capi():
        global inicio_gt2
        print(' ✅ - 005:  API MEXC ')
        inicio_gate = datetime.now().time()
        inicio_g1 = inicio_gate.strftime(" %H:%M:%S")
        inicio_gt2 = datetime.strptime(inicio_g1, " %H:%M:%S")
        print(" 🔍 - INI:", inicio_g1)
        url = "https://api.mexc.com/api/v3/ticker/price"
        response = requests.get(url)
        if response.status_code == 200:
            page_content = response.content
            soup = BeautifulSoup(page_content, 'html.parser')
            with open('dom_mexc.json', 'w', encoding='utf-8') as file:
                file.write(soup.prettify())
    mex_capi()
    #------------------------------------------------------
    def mex_csv():
        json_filename = 'dom_mexc.json'
        with open(json_filename, 'r') as json_file:
            data = json.load(json_file)
        csv_filename = 'tabela_mexc.csv'
        with open(csv_filename, 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(['symbol', 'price'])
            for item in data:
                csv_writer.writerow([item['symbol'], item['price']])
        fim_gateio = datetime.now().time()
        fim_gt1 = fim_gateio.strftime(" %H:%M:%S")
        print(" 🔍 - FIM:", fim_gt1)
        fim_gt2 = datetime.strptime(fim_gt1, " %H:%M:%S")
        tempo_decorrido = fim_gt2 - inicio_gt2
        print(" 🕰️  - TEM: ", tempo_decorrido)
        print(separador)
    mex_csv()
    #------------------------------------------------------
    def header_mex():
        try:
            file_gateio = 'tabela_mexc.csv'
            df = pd.read_csv(file_gateio)
            df.columns = [coluna if coluna != 'symbol' else 'symbol_mex' for coluna in df.columns]
            df.columns = [coluna if coluna != 'price' else 'buy_mex' for coluna in df.columns]
            df.to_csv(file_gateio, index=False)
        except Exception as e:
            print(f"An error occurred: {e}")
    header_mex()
    #------------------------------------------------------
    def mex_usdt():
        import pandas as pd
        df = pd.read_csv('tabela_mexc.csv')
        df = df[df['symbol_mex'].str.endswith('USDT')]
        df.to_csv('tabela_mexc.csv', index=False)
    mex_usdt()
    #------------------------------------------------------
    def mex_kill():
        df = pd.read_csv('tabela_mexc.csv')
        for index, row in df.iterrows():
            df.at[index, 'symbol_mex'] = row['symbol_mex'].replace('USDT', '')
        df.to_csv('tabela_mexc.csv', index=False)
    mex_kill()
    #------------------------------------------------------
    def mex_brl():
        nome_arquivo = 'tabela_mexc.csv'
        df = pd.read_csv(nome_arquivo)
        df['brl_mex'] = None
        df.to_csv('tabela_mexc.csv', index=False)
    mex_brl()
    #------------------------------------------------------
    def brl_mex():
        global cotacao_dolar
        nome_arquivo = 'tabela_mexc.csv'
        df = pd.read_csv(nome_arquivo)
        df['buy_mex'] = pd.to_numeric(df['buy_mex'], errors='coerce')
        df['buy_mex'].fillna(0, inplace=True)
        df['brl_mex'] = df['buy_mex'] * cotacao_dolar
        df.to_csv('tabela_mexc.csv', index=False)
    brl_mex()
    #------------------------------------------------------
    def mexc_end():
        import pandas as pd
        arquivo_entrada = 'tabela_mexc.csv'
        nome_coluna_apagar = 'buy_mex'  
        dados_csv = pd.read_csv(arquivo_entrada)
        dados_csv = dados_csv.drop(columns=[nome_coluna_apagar], axis=1)
        dados_csv.to_csv(arquivo_entrada, index=False)
    mexc_end()
full_mex()
#######################################################
#  006 - KUCOIN  
#######################################################
def full_kucoin():
    #-----------------------------------------------------------------------------------------
    def kucoin_list():
        global inicio_kuc
        print(' ✅ - 006:  API KUCOIN ')
        inicio_gate = datetime.now().time()
        inicio_g1 = inicio_gate.strftime(" %H:%M:%S")
        inicio_kuc = datetime.strptime(inicio_g1, " %H:%M:%S")
        print(" 🔍 - INI:", inicio_kuc)
        base_url = "https://api.kucoin.com"
        endpoint = "/api/v1/market/allTickers"
        response = requests.get(base_url + endpoint)
        if response.status_code == 200:
            data = response.json()
            with open("dom/dxm_kucoin.json", "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=2)
        else:
            print(f"Error: API request failed with status code {response.status_code}")
    kucoin_list()
    #-----------------------------------------------------------------------------------------
    def kucoin_csv():
        with open('dom/dxm_kucoin.json') as file:
            data = json.load(file)
        ticker_data = data['data']['ticker']
        filtered_symbols = [entry['symbol'] for entry in ticker_data if '-USDT' in entry['symbol']]
        csv_file_path = 'tabela_kuc.csv'
        with open(csv_file_path, 'w', newline='') as csvfile:
            fieldnames = ['symbol_kuc', 'buy_kuc', 'sell_kuc']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for entry in ticker_data:
                symbol = entry['symbol']
                if symbol in filtered_symbols:
                    buy_price = entry['buy']
                    sell_price = entry['sell']
                    writer.writerow({
                        'symbol_kuc': symbol,
                        'buy_kuc': buy_price,
                        'sell_kuc': sell_price
                    })
    kucoin_csv()
    #-----------------------------------------------------------------------------------------
    def CUK_kill():
        df = pd.read_csv('tabela_kuc.csv')
        for index, row in df.iterrows():
            df.at[index, 'symbol_kuc'] = row['symbol_kuc'].replace('-USDT', '')
        df.to_csv('tabela_kuc.csv', index=False)
    CUK_kill()
    #-----------------------------------------------------------------------------------------
    def brl_cuk():
        global cotacao_dolar
        nome_arquivo = 'tabela_kuc.csv'
        df = pd.read_csv(nome_arquivo)
        df['buy_kuc'] = pd.to_numeric(df['buy_kuc'], errors='coerce')
        df['buy_kuc'].fillna(0, inplace=True)
        df['brl_kuc'] = df['buy_kuc'] * cotacao_dolar
        df.to_csv('tabela_kuc.csv', index=False)
    brl_cuk()
full_kucoin()
#######################################################
#  007 - FOXBIT   
#######################################################
def full_foxbit():
    #-----------------------------------------------------------------------------------------
    def foxbit_list():
        print(separador)
        global inicio_kuc
        print(' ✅ - 007:  API FOXBIT ')
        inicio_gate = datetime.now().time()
        inicio_g1 = inicio_gate.strftime(" %H:%M:%S")
        inicio_kuc = datetime.strptime(inicio_g1, " %H:%M:%S")
        print(" 🔍 - INI:", inicio_kuc)
        base_url = "https://api.foxbit.com.br/rest/v3/markets"
        response = requests.get(base_url)
        if response.status_code == 200:
            data = response.json()
            with open("dom_foxbit.json", "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=2)
        else:
            print(f"Error: API request failed with status code {response.status_code}")
    foxbit_list()
    #-----------------------------------------------------------------------------------------
    def salvar_nomes_usdt_em_csv():
        caminho_arquivo_json = 'dom_foxbit.json'
        caminho_arquivo_csv = 'tabela_fox.csv'
        with open(caminho_arquivo_json, 'r') as arquivo:
            dados = json.load(arquivo)
        nomes_usdt = [item['base']['symbol'] for item in dados['data'] if 'usdt' in item['symbol']]
        with open(caminho_arquivo_csv, 'w', newline='') as arquivo_csv:
            escritor_csv = csv.writer(arquivo_csv)
            escritor_csv.writerow(['symbols_fox'])
            escritor_csv.writerows(map(lambda x: [x], nomes_usdt))
    salvar_nomes_usdt_em_csv()
    #-----------------------------------------------------------------------------------------
    def add_usdt():
        import pandas as pd
        nome_arquivo = 'tabela_fox.csv'
        valor_a_adicionar = 'usdt'
        df = pd.read_csv(nome_arquivo)
        df = df.apply(lambda x: x.astype(str) + valor_a_adicionar)
        df.to_csv(nome_arquivo, index=False)
    add_usdt()
    #-----------------------------------------------------------------------------------------
    def listar_moedas():
        table_fox = []
        nome_arquivo = 'tabela_fox.csv'
        with open(nome_arquivo, 'r') as arquivo_csv:
            leitor_csv = csv.reader(arquivo_csv)
            for linha in leitor_csv:
                table_fox.append(linha)
        return table_fox
    #-----------------------------------------------------------------------------------------
    def foxbit_list(market_symbol):
    #------------------------------------------------------
        if market_symbol == 'symbols_fox':
            return
        elif market_symbol == 'usdtusdt':
            return
    #------------------------------------------------------
        base_url = f'https://api.foxbit.com.br/rest/v3/markets/{market_symbol}/orderbook'
        response = requests.get(base_url)
        
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed to fetch data for market symbol {market_symbol}. Status code: {response.status_code}")
            return None
    #------------------------------------------------------
    def main():
        table_fox = listar_moedas()
        resultado = []
        for coin in table_fox:
            market_symbol = coin[0]
            data = foxbit_list(market_symbol)
            if data is not None:
                resultado.append({
                    'symbol_fox': market_symbol,
                    'dados': data
                })
        nome_arquivo_atualizado = 'tabela_fox.csv'
        with open(nome_arquivo_atualizado, 'w', newline='') as arquivo_csv_atualizado:
            escritor_csv = csv.DictWriter(arquivo_csv_atualizado, fieldnames=['symbol_fox', 'dados'])
            escritor_csv.writeheader()
            escritor_csv.writerows(resultado)
    main()
    #-----------------------------------------------------------------------------------------
    def fox_price():
        import pandas as pd
        import ast
        df = pd.read_csv('tabela_fox.csv')
        df['dados'] = df['dados'].apply(ast.literal_eval)
        df['usd_fox'] = df['dados'].apply(lambda x: float(x['asks'][0][0]))
        df.to_csv('tabela_fox.csv', index=False)
    fox_price()
    #-----------------------------------------------------------------------------------------
    def coluna_brl_fox():
        nome_arquivo = 'tabela_fox.csv'
        df = pd.read_csv(nome_arquivo)
        df['brl_fox'] = None
        df.to_csv('tabela_fox.csv', index=False)
    coluna_brl_fox()
    #------------------------------------------------------
    def for_brl():
        global cotacao_dolar
        nome_arquivo = 'tabela_fox.csv'
        df = pd.read_csv(nome_arquivo)
        df['usd_fox'] = pd.to_numeric(df['usd_fox'], errors='coerce')
        df['usd_fox'].fillna(0, inplace=True)
        df['brl_fox'] = df['usd_fox'] * cotacao_dolar
        df.to_csv('tabela_fox.csv', index=False)
    for_brl()
    #------------------------------------------------------
    def usdt_clean():
        global cotacao_dolar
        nome_arquivo = 'tabela_fox.csv'
        df = pd.read_csv(nome_arquivo)
        df['symbol_fox'] = df['symbol_fox'].str.replace('usdt', '')
        df['symbol_fox'] = df['symbol_fox'].str.upper()

        df.to_csv(nome_arquivo, index=False)
    usdt_clean()
full_foxbit()
#######################################################
#  008 - CHILIC   
#######################################################
def full_chillz():
    #-----------------------------------------------------------------------------------------
    def chillz_list():
        print(separador)
        print(' ✅ - 008:  API CHILIZ ')
        inicio_gate = datetime.now().time()
        inicio_g1 = inicio_gate.strftime(" %H:%M:%S")
        inicio_kuc = datetime.strptime(inicio_g1, " %H:%M:%S")
        print(" 🔍 - INI:", inicio_kuc)
        base_url = "https://api.chiliz.net/openapi/quote/v1/ticker/24hr"
        response = requests.get(base_url)
        if response.status_code == 200:
            data = response.json()
            with open("dom_chiliz.json", "w", encoding="utf-8") as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=2)
        else:
            print(f"Error: API request failed with status code {response.status_code}")
    chillz_list()
    #--------------------------------------------------------------------------
    def chiliz_csv():
        import pandas as pd
        with open('dom_chiliz.json', 'r') as file:
            data = pd.read_json(file)
        filtered_data = data[data['symbol'].str.upper().str.endswith('USDT')]
        result_df = pd.DataFrame({
            'symbol_chi': filtered_data['symbol'],
            'buy_chi': filtered_data['lastPrice'].astype(float)
        })
        result_df.to_csv('tabela_chiliz.csv', index=False)
    chiliz_csv()
    #--------------------------------------------------------------------------
    def chiliz_zero():
        import pandas as pd
        df = pd.read_csv('tabela_chiliz.csv')
        df = df[df['buy_chi'] != 0]
        df.to_csv('tabela_chiliz.csv', index=False)
    chiliz_zero()

    def chiliz_usdt():
        df = pd.read_csv('tabela_chiliz.csv')
        for index, row in df.iterrows():
            df.at[index, 'symbol_chi'] = row['symbol_chi'].replace('USDT', '')
        df.to_csv('tabela_chiliz.csv', index=False)
    chiliz_usdt()
    #-----------------------------------------------------------------------------------------
    def chiliz_brl():
        global cotacao_dolar
        nome_arquivo = 'tabela_chiliz.csv'
        df = pd.read_csv(nome_arquivo)
        df['buy_chi'] = pd.to_numeric(df['buy_chi'], errors='coerce')
        df['buy_chi'].fillna(0, inplace=True)
        df['brl_chi'] = df['buy_chi'] * cotacao_dolar
        df.to_csv('tabela_chiliz.csv', index=False)
    chiliz_brl()
    """
    #--------------------------------------------------------------------------
    def listar_moedas_chi():
        table_fox = []
        nome_arquivo = 'tabela_chiliz.csv'
        with open(nome_arquivo, 'r') as arquivo_csv:
            leitor_csv = csv.reader(arquivo_csv)
            for linha in leitor_csv:
                table_fox.append(linha)
        print(table_fox)
        return table_fox
    #--------------------------------------------------------------------------  
    def chi_list(market_symbol_chi):
        base_url = f'https://api.foxbit.com.br/rest/v3/markets/{market_symbol_chi}/orderbook'
        response = requests.get(base_url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            print(f"Failed to fetch data for market symbol {market_symbol_chi}. Status code: {response.status_code}")
            return None
    #--------------------------------------------------------------------------
    def main_chi():
        table_chi = listar_moedas_chi()
        resultado = []
        for coin in table_chi:
            market_symbol_chi = coin[0]
            data = chi_list(market_symbol_chi)
            if data is not None:
                resultado.append({
                    'symbol_chi': market_symbol_chi,
                    'dados': data
                })
        nome_arquivo_atualizado = 'tabela_chiliz2.csv'
        with open(nome_arquivo_atualizado, 'w', newline='') as arquivo_csv_atualizado:
            escritor_csv = csv.DictWriter(arquivo_csv_atualizado, fieldnames=['symbol_chi', 'dados'])
            escritor_csv.writeheader()
            escritor_csv.writerows(resultado)
    main_chi()
    #--------------------------------------------------------------------------
    def chiliz_orderbook():
            url = "https://api.exchange.com/openapi/quote/v1/depth"
            params = {
                'symbol': 'seu_simbolo',
                'limit': 100  # Você pode ajustar o limite conforme necessário
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                order_book_data = response.json()
                print(order_book_data)
            else:
                print(f"Erro na solicitação. Código de status: {response.status_code}")
                print(response.text)
    chiliz_orderbook()
    """
full_chillz()
#######################################################
print(' FIM DE COMUNICAÇÃO ')
print(separador)
#######################################################
#  CRUZAR DADOS  
#######################################################
print('f1')
#------------------------------------------------------
def cleaner():
    diretorio = os.getcwd()
    arquivos = os.listdir(diretorio)
    for arquivo in arquivos:
        if arquivo.startswith('mb'):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            os.remove(caminho_arquivo)
    """
    for arquivo in arquivos:
        if arquivo.startswith('dom_'):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            os.remove(caminho_arquivo)
    """
    for arquivo in arquivos:
        if arquivo.startswith('dolar'):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            os.remove(caminho_arquivo)
    for arquivo in arquivos:
        if arquivo.startswith('ab'):
            caminho_arquivo = os.path.join(diretorio, arquivo)
            os.remove(caminho_arquivo)
cleaner()
print('f2')
#------------------------------------------------------
def merge():
    #--------------------------------------------------
    tabela_binance = pd.read_csv('tabela_binance.csv')
    tabela_mercadobitcoin = pd.read_csv('tabela_mercadobitcoin.csv')
    tabela_gateio = pd.read_csv("tabela_gateio.csv")
    tabela_bitso = pd.read_csv("tabela_bitso.csv")
    tabela_mexc = pd.read_csv("tabela_mexc.csv")
    tabela_kuc = pd.read_csv("tabela_kuc.csv")
    tabela_fox = pd.read_csv("tabela_fox.csv")
    tabela_chiliz = pd.read_csv("tabela_chiliz.csv")
    #--------------------------------------------------
    tabela_unida = pd.merge(tabela_binance, tabela_mercadobitcoin, left_on='symbol_bin', right_on='symbol_mb', how='left', suffixes=('_bin', '_mb'))
    tabela_unida = pd.merge(tabela_unida, tabela_gateio, left_on='symbol_bin', right_on='symbol_gat', how='left')
    tabela_unida = pd.merge(tabela_unida, tabela_bitso, left_on='symbol_bin', right_on='symbol_bit', how='left')
    tabela_unida = pd.merge(tabela_unida, tabela_mexc, left_on='symbol_bin', right_on='symbol_mex', how='left')
    tabela_unida = pd.merge(tabela_unida, tabela_kuc, left_on='symbol_bin', right_on='symbol_kuc', how='left')
    tabela_unida = pd.merge(tabela_unida, tabela_fox, left_on='symbol_bin', right_on='symbol_fox', how='left')
    tabela_unida = pd.merge(tabela_unida, tabela_chiliz, left_on='symbol_bin', right_on='symbol_chi', how='left')
    tabela_unida.to_csv('merge.csv', index=False)
merge()
#------------------------------------------------------
def merge_clean():
    caminho_arquivo = 'merge.csv'
    df = pd.read_csv(caminho_arquivo)
    for index, row in df.iterrows():
        num_nao_nulos = row.count()
        if num_nao_nulos < 3:
            df = df.drop(index, axis=0)
    df.to_csv('merge.csv', index=False)
merge_clean()
#------------------------------------------------------
def merge_columns():
    nome_arquivo = 'merge.csv'
    df = pd.read_csv(nome_arquivo)
    #--------------------------------------------------
    df['d_bin_mb'] = None
    df['d_bin_gat'] = None
    df['d_bin_bit'] = None
    df['d_bin_mex'] = None
    df['d_bin_kuc'] = None
    df['d_bin_fox'] = None
    df['d_bin_chi'] = None
    #--------------------------------------------------
    df['d_mb_bin'] = None 
    df['d_mb_gat'] = None
    df['d_mb_bit'] = None
    df['d_mb_mex'] = None
    df['d_mb_kuc'] = None
    df['d_mb_fox'] = None
    df['d_mb_chi'] = None
    #--------------------------------------------------
    df['d_gat_bin'] = None 
    df['d_gat_mb'] = None
    df['d_gat_bit'] = None
    df['d_gat_mex'] = None
    df['d_gat_kuc'] = None
    df['d_gat_fox'] = None
    df['d_gat_chi'] = None
    #--------------------------------------------------
    df['d_bit_bin'] = None 
    df['d_bit_mb'] = None
    df['d_bit_gat'] = None
    df['d_bit_mex'] = None
    df['d_bit_kuc'] = None
    df['d_bit_fox'] = None
    df['d_bit_chi'] = None
    #--------------------------------------------------
    df['d_mex_bin'] = None 
    df['d_mex_mb'] = None
    df['d_mex_gat'] = None
    df['d_mex_bit'] = None
    df['d_mex_kuc'] = None
    df['d_mex_fox'] = None
    df['d_mex_chi'] = None
    #--------------------------------------------------
    df['d_kuc_bin'] = None 
    df['d_kuc_mb'] = None
    df['d_kuc_gat'] = None
    df['d_kuc_bit'] = None
    df['d_kuc_mex'] = None
    df['d_kuc_fox'] = None
    df['d_kuc_chi'] = None
    #--------------------------------------------------
    df['d_fox_bin'] = None 
    df['d_fox_mb'] = None
    df['d_fox_gat'] = None
    df['d_fox_bit'] = None
    df['d_fox_mex'] = None
    df['d_fox_kuc'] = None
    df['d_fox_chi'] = None  
    #--------------------------------------------------
    df['d_chi_bin'] = None
    df['d_chi_mb'] = None
    df['d_chi_gat'] = None
    df['d_chi_bit'] = None
    df['d_chi_mex'] = None
    df['d_chi_kuc'] = None
    df['d_chi_fox'] = None
    #--------------------------------------------------
    df.to_csv('merge.csv', index=False)
    #--------------------------------------------------
merge_columns()
#------------------------------------------------------
def clean_data():
    import pandas as pd
    df = pd.read_csv('merge.csv')
    df = df.drop(columns=['dados', 'usd_fox', 'buy_chi', 'buy_kuc', 'sell_kuc'])
    df.to_csv('merge.csv', index=False)
    print('f3')
clean_data()
#------------------------------------------------------
def diferenca_percentual():
    #--------------------------------------------------
    caminho_arquivo = 'merge.csv'
    df = pd.read_csv(caminho_arquivo)
    #############################################################
    df['d_bin_mb'] = ((df['brl_mb'] - df['brl_bin']) / df['brl_bin']) * 100 
    df['d_bin_mb'] = df['d_bin_mb'].apply(lambda x: max(0, x))
    df['d_bin_mb'] = df['d_bin_mb'].map(lambda x: f'{x:.2f}%')
    #-------------------------------------------------- 3
    df['d_bin_gat'] = ((df['brl_gat'] - df['brl_bin']) / df['brl_bin']) * 100
    df['d_bin_gat'] = df['d_bin_gat'].apply(lambda x: max(0, x))
    df['d_bin_gat'] = df['d_bin_gat'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_bin_bit'] = ((df['brl_bit'] - df['brl_bin']) / df['brl_bin']) * 100
    df['d_bin_bit'] = df['d_bin_bit'].apply(lambda x: max(0, x))
    df['d_bin_bit'] = df['d_bin_bit'].map(lambda x: f'{x:.2f}%')
    #-------------------------------------------------- 4
    df['d_bin_mex'] = ((df['brl_mex'] - df['brl_bin']) / df['brl_bin']) * 100
    df['d_bin_mex'] = df['d_bin_mex'].apply(lambda x: max(0, x))
    df['d_bin_mex'] = df['d_bin_mex'].map(lambda x: f'{x:.2f}%')
    #-------------------------------------------------- 5
    df['d_bin_fox'] = ((df['brl_fox'] - df['brl_bin']) / df['brl_bin']) * 100
    df['d_bin_fox'] = df['d_bin_fox'].apply(lambda x: max(0, x))
    df['d_bin_fox'] = df['d_bin_fox'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------6
    df['d_bin_kuc'] = ((df['brl_kuc'] - df['brl_bin']) / df['brl_bin']) * 100
    df['d_bin_kuc'] = df['d_bin_kuc'].apply(lambda x: max(0, x))
    df['d_bin_kuc'] = df['d_bin_kuc'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------7
    df['d_bin_chi'] = ((df['brl_chi'] - df['brl_bin']) / df['brl_bin']) * 100
    df['d_bin_chi'] = df['d_bin_chi'].apply(lambda x: max(0, x))
    df['d_bin_chi'] = df['d_bin_chi'].map(lambda x: f'{x:.2f}%')
    #############################################################
    df['d_mb_bin'] = ((df['brl_bin'] - df['brl_mb']) / df['brl_mb']) * 100 
    df['d_mb_bin'] = df['d_mb_bin'].apply(lambda x: max(0, x))
    df['d_mb_bin'] = df['d_mb_bin'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mb_gat'] = ((df['brl_gat'] - df['brl_mb']) / df['brl_mb']) * 100
    df['d_mb_gat'] = df['d_mb_gat'].apply(lambda x: max(0, x))
    df['d_mb_gat'] = df['d_mb_gat'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mb_bit'] = ((df['brl_mb'] - df['brl_bit']) / df['brl_bit']) * 100
    df['d_mb_bit'] = df['d_mb_bit'].apply(lambda x: max(0, x))
    df['d_mb_bit'] = df['d_mb_bit'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mb_mex'] = ((df['brl_mex'] - df['brl_mb']) / df['brl_mb']) * 100
    df['d_mb_mex'] = df['d_mb_mex'].apply(lambda x: max(0, x))
    df['d_mb_mex'] = df['d_mb_mex'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mb_kuc'] = ((df['brl_kuc'] - df['brl_mb']) / df['brl_mb']) * 100
    df['d_mb_kuc'] = df['d_mb_kuc'].apply(lambda x: max(0, x))
    df['d_mb_kuc'] = df['d_mb_kuc'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mb_fox'] = ((df['brl_fox'] - df['brl_mb']) / df['brl_mb']) * 100
    df['d_mb_fox'] = df['d_mb_fox'].apply(lambda x: max(0, x))
    df['d_mb_fox'] = df['d_mb_fox'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mb_chi'] = ((df['brl_chi'] - df['brl_mb']) / df['brl_mb']) * 100
    df['d_mb_chi'] = df['d_mb_chi'].apply(lambda x: max(0, x))
    df['d_mb_chi'] = df['d_mb_chi'].map(lambda x: f'{x:.2f}%')
    #############################################################
    df['d_gat_bin'] = ((df['brl_bin'] - df['brl_gat']) / df['brl_gat']) * 100
    df['d_gat_bin'] = df['d_gat_bin'].apply(lambda x: max(0, x))
    df['d_gat_bin'] = df['d_gat_bin'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_gat_mb'] = ((df['brl_mb'] - df['brl_gat']) / df['brl_gat']) * 100
    df['d_gat_mb'] = df['d_gat_mb'].apply(lambda x: max(0, x))
    df['d_gat_mb'] = df['d_gat_mb'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_gat_bit'] = ((df['brl_bit'] - df['brl_gat']) / df['brl_gat']) * 100
    df['d_gat_bit'] = df['d_gat_bit'].apply(lambda x: max(0, x))
    df['d_gat_bit'] = df['d_gat_bit'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_gat_mex'] = ((df['brl_mex'] - df['brl_gat']) / df['brl_gat']) * 100
    df['d_gat_mex'] = df['d_gat_mex'].apply(lambda x: max(0, x))
    df['d_gat_mex'] = df['d_gat_mex'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_gat_kuc'] = ((df['brl_kuc'] - df['brl_gat']) / df['brl_gat']) * 100
    df['d_gat_kuc'] = df['d_gat_kuc'].apply(lambda x: max(0, x))
    df['d_gat_kuc'] = df['d_gat_kuc'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_gat_fox'] = ((df['brl_fox'] - df['brl_gat']) / df['brl_gat']) * 100
    df['d_gat_fox'] = df['d_gat_fox'].apply(lambda x: max(0, x))
    df['d_gat_fox'] = df['d_gat_fox'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_gat_chi'] = ((df['brl_chi'] - df['brl_gat']) / df['brl_gat']) * 100
    df['d_gat_chi'] = df['d_gat_chi'].apply(lambda x: max(0, x))
    df['d_gat_chi'] = df['d_gat_chi'].map(lambda x: f'{x:.2f}%')
    #############################################################
    df['d_bit_bin'] = ((df['brl_bin'] - df['brl_bit']) / df['brl_bit']) * 100
    df['d_bit_bin'] = df['d_bit_bin'].apply(lambda x: max(0, x))
    df['d_bit_bin'] = df['d_bit_bin'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_bit_mb'] = ((df['brl_bit'] - df['brl_mb']) / df['brl_mb']) * 100
    df['d_bit_mb'] = df['d_bit_mb'].apply(lambda x: max(0, x))
    df['d_bit_mb'] = df['d_bit_mb'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_bit_gat'] = ((df['brl_gat'] - df['brl_bit']) / df['brl_bit']) * 100
    df['d_bit_gat'] = df['d_bit_gat'].apply(lambda x: max(0, x))
    df['d_bit_gat'] = df['d_bit_gat'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_bit_mex'] = ((df['brl_bit'] - df['brl_mex']) / df['brl_mex']) * 100   
    df['d_bit_mex'] = df['d_bit_mex'].apply(lambda x: max(0, x))
    df['d_bit_mex'] = df['d_bit_mex'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_bit_kuc'] = ((df['brl_kuc'] - df['brl_bit']) / df['brl_bit']) * 100
    df['d_bit_kuc'] = df['d_bit_kuc'].apply(lambda x: max(0, x))
    df['d_bit_kuc'] = df['d_bit_kuc'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_bit_fox'] = ((df['brl_fox'] - df['brl_bit']) / df['brl_bit']) * 100
    df['d_bit_fox'] = df['d_bit_fox'].apply(lambda x: max(0, x))
    df['d_bit_fox'] = df['d_bit_fox'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_bit_chi'] = ((df['brl_chi'] - df['brl_bit']) / df['brl_bit']) * 100
    df['d_bit_chi'] = df['d_bit_chi'].apply(lambda x: max(0, x))
    df['d_bit_chi'] = df['d_bit_chi'].map(lambda x: f'{x:.2f}%')
    #############################################################
    df['d_mex_bin'] = ((df['brl_bin'] - df['brl_mex']) / df['brl_mex']) * 100
    df['d_mex_bin'] = df['d_mex_bin'].apply(lambda x: max(0, x))
    df['d_mex_bin'] = df['d_mex_bin'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mex_mb'] = ((df['brl_mb'] - df['brl_mex']) / df['brl_mex']) * 100
    df['d_mex_mb'] = df['d_mex_mb'].apply(lambda x: max(0, x))
    df['d_mex_mb'] = df['d_mex_mb'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mex_bit'] = ((df['brl_mex'] - df['brl_bit']) / df['brl_bit']) * 100   
    df['d_mex_bit'] = df['d_mex_bit'].apply(lambda x: max(0, x))
    df['d_mex_bit'] = df['d_mex_bit'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mex_gat'] = ((df['brl_gat'] - df['brl_mex']) / df['brl_mex']) * 100
    df['d_mex_gat'] = df['d_mex_gat'].apply(lambda x: max(0, x))
    df['d_mex_gat'] = df['d_mex_gat'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mex_kuc'] = ((df['brl_kuc'] - df['brl_mex']) / df['brl_mex']) * 100
    df['d_mex_kuc'] = df['d_mex_kuc'].apply(lambda x: max(0, x))
    df['d_mex_kuc'] = df['d_mex_kuc'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mex_fox'] = ((df['brl_fox'] - df['brl_mex']) / df['brl_mex']) * 100
    df['d_mex_fox'] = df['d_mex_fox'].apply(lambda x: max(0, x))
    df['d_mex_fox'] = df['d_mex_fox'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_mex_chi'] = ((df['brl_chi'] - df['brl_mex']) / df['brl_mex']) * 100
    df['d_mex_chi'] = df['d_mex_chi'].apply(lambda x: max(0, x))
    df['d_mex_chi'] = df['d_mex_chi'].map(lambda x: f'{x:.2f}%')
    #############################################################
    df['d_kuc_bin'] = ((df['brl_bin'] - df['brl_kuc']) / df['brl_kuc']) * 100
    df['d_kuc_bin'] = df['d_kuc_bin'].apply(lambda x: max(0, x))
    df['d_kuc_bin'] = df['d_kuc_bin'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_kuc_mb'] = ((df['brl_mb'] - df['brl_kuc']) / df['brl_kuc']) * 100
    df['d_kuc_mb'] = df['d_kuc_mb'].apply(lambda x: max(0, x))
    df['d_kuc_mb'] = df['d_kuc_mb'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_kuc_gat'] = ((df['brl_gat'] - df['brl_kuc']) / df['brl_kuc']) * 100
    df['d_kuc_gat'] = df['d_kuc_gat'].apply(lambda x: max(0, x))
    df['d_kuc_gat'] = df['d_kuc_gat'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_kuc_bit'] = ((df['brl_bit'] - df['brl_kuc']) / df['brl_kuc']) * 100
    df['d_kuc_bit'] = df['d_kuc_bit'].apply(lambda x: max(0, x))
    df['d_kuc_bit'] = df['d_kuc_bit'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_kuc_mex'] = ((df['brl_mex'] - df['brl_kuc']) / df['brl_kuc']) * 100
    df['d_kuc_mex'] = df['d_kuc_mex'].apply(lambda x: max(0, x))
    df['d_kuc_mex'] = df['d_kuc_mex'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_kuc_fox'] = ((df['brl_fox'] - df['brl_kuc']) / df['brl_kuc']) * 100
    df['d_kuc_fox'] = df['d_kuc_fox'].apply(lambda x: max(0, x))
    df['d_kuc_fox'] = df['d_kuc_fox'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_kuc_chi'] = ((df['brl_chi'] - df['brl_kuc']) / df['brl_kuc']) * 100
    df['d_kuc_chi'] = df['d_kuc_chi'].apply(lambda x: max(0, x))
    df['d_kuc_chi'] = df['d_kuc_chi'].map(lambda x: f'{x:.2f}%')
    #############################################################
    df['d_fox_bin'] = ((df['brl_bin'] - df['brl_fox']) / df['brl_fox']) * 100
    df['d_fox_bin'] = df['d_fox_bin'].apply(lambda x: max(0, x))
    df['d_fox_bin'] = df['d_fox_bin'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_fox_mb'] = ((df['brl_mb'] - df['brl_fox']) / df['brl_fox']) * 100
    df['d_fox_mb'] = df['d_fox_mb'].apply(lambda x: max(0, x))
    df['d_fox_mb'] = df['d_fox_mb'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_fox_gat'] = ((df['brl_gat'] - df['brl_fox']) / df['brl_fox']) * 100
    df['d_fox_gat'] = df['d_fox_gat'].apply(lambda x: max(0, x))
    df['d_fox_gat'] = df['d_fox_gat'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_fox_bit'] = ((df['brl_bit'] - df['brl_fox']) / df['brl_fox']) * 100
    df['d_fox_bit'] = df['d_fox_bit'].apply(lambda x: max(0, x))
    df['d_fox_bit'] = df['d_fox_bit'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_fox_mex'] = ((df['brl_mex'] - df['brl_fox']) / df['brl_fox']) * 100
    df['d_fox_mex'] = df['d_fox_mex'].apply(lambda x: max(0, x))
    df['d_fox_mex'] = df['d_fox_mex'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_fox_kuc'] = ((df['brl_kuc'] - df['brl_fox']) / df['brl_fox']) * 100
    df['d_fox_kuc'] = df['d_fox_kuc'].apply(lambda x: max(0, x))
    df['d_fox_kuc'] = df['d_fox_kuc'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_fox_chi'] = ((df['brl_chi'] - df['brl_fox']) / df['brl_fox']) * 100
    df['d_fox_chi'] = df['d_fox_chi'].apply(lambda x: max(0, x))
    df['d_fox_chi'] = df['d_fox_chi'].map(lambda x: f'{x:.2f}%')
    #############################################################
    df['d_chi_bin'] = ((df['brl_bin'] - df['brl_chi']) / df['brl_chi']) * 100
    df['d_chi_bin'] = df['d_chi_bin'].apply(lambda x: max(0, x))
    df['d_chi_bin'] = df['d_chi_bin'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_chi_mb'] = ((df['brl_mb'] - df['brl_chi']) / df['brl_chi']) * 100
    df['d_chi_mb'] = df['d_chi_mb'].apply(lambda x: max(0, x))
    df['d_chi_mb'] = df['d_chi_mb'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_chi_gat'] = ((df['brl_gat'] - df['brl_chi']) / df['brl_chi']) * 100
    df['d_chi_gat'] = df['d_chi_gat'].apply(lambda x: max(0, x))
    df['d_chi_gat'] = df['d_chi_gat'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_chi_bit'] = ((df['brl_bit'] - df['brl_chi']) / df['brl_chi']) * 100
    df['d_chi_bit'] = df['d_chi_bit'].apply(lambda x: max(0, x))
    df['d_chi_bit'] = df['d_chi_bit'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_chi_mex'] = ((df['brl_mex'] - df['brl_chi']) / df['brl_chi']) * 100
    df['d_chi_mex'] = df['d_chi_mex'].apply(lambda x: max(0, x))
    df['d_chi_mex'] = df['d_chi_mex'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_chi_kuc'] = ((df['brl_kuc'] - df['brl_chi']) / df['brl_chi']) * 100
    df['d_chi_kuc'] = df['d_chi_kuc'].apply(lambda x: max(0, x))
    df['d_chi_kuc'] = df['d_chi_kuc'].map(lambda x: f'{x:.2f}%')
    #--------------------------------------------------
    df['d_chi_fox'] = ((df['brl_fox'] - df['brl_chi']) / df['brl_chi']) * 100
    df['d_chi_fox'] = df['d_chi_fox'].apply(lambda x: max(0, x))
    df['d_chi_fox'] = df['d_chi_fox'].map(lambda x: f'{x:.2f}%')
    #############################################################
    df.to_csv(caminho_arquivo, index=False)
diferenca_percentual()
#------------------------------------------------------
def inf_fix():
    df = pd.read_csv('merge.csv')
    df['d_bin_mb'] = df['d_bin_mb'].replace('inf%', '0.00%')
    df['d_bin_gat'] = df['d_bin_gat'].replace('inf%', '0.00%')
    df['d_bin_bit'] = df['d_bin_bit'].replace('inf%', '0.00%')
    df['d_bin_mex'] = df['d_bin_mex'].replace('inf%', '0.00%')
    df['d_bin_kuc'] = df['d_bin_kuc'].replace('inf%', '0.00%')
    df['d_bin_fox'] = df['d_bin_fox'].replace('inf%', '0.00%')
    df['d_bin_chi'] = df['d_bin_chi'].replace('inf%', '0.00%')
    #--------------------------------------------------
    df['d_mb_bin'] = df['d_mb_bin'].replace('inf%', '0.00%')
    df['d_mb_gat'] = df['d_mb_gat'].replace('inf%', '0.00%')
    df['d_mb_bit'] = df['d_mb_bit'].replace('inf%', '0.00%')
    df['d_mb_mex'] = df['d_mb_mex'].replace('inf%', '0.00%')
    df['d_mb_kuc'] = df['d_mb_kuc'].replace('inf%', '0.00%')
    df['d_mb_fox'] = df['d_mb_fox'].replace('inf%', '0.00%')
    df['d_mb_chi'] = df['d_mb_chi'].replace('inf%', '0.00%')
    #--------------------------------------------------
    df['d_bit_bin'] = df['d_bit_bin'].replace('inf%', '0.00%')
    df['d_bit_mb'] = df['d_bit_mb'].replace('inf%', '0.00%')
    df['d_bit_gat'] = df['d_bit_gat'].replace('inf%', '0.00%')
    df['d_bit_mex'] = df['d_bit_mex'].replace('inf%', '0.00%')
    df['d_bit_kuc'] = df['d_bit_kuc'].replace('inf%', '0.00%')
    df['d_bit_fox'] = df['d_bit_fox'].replace('inf%', '0.00%')
    df['d_bit_chi'] = df['d_bit_chi'].replace('inf%', '0.00%')
    #--------------------------------------------------
    df['d_mex_bin'] = df['d_mex_bin'].replace('inf%', '0.00%')
    df['d_mex_mb'] = df['d_mex_mb'].replace('inf%', '0.00%')
    df['d_mex_gat'] = df['d_mex_gat'].replace('inf%', '0.00%')
    df['d_mex_bit'] = df['d_mex_bit'].replace('inf%', '0.00%')
    df['d_mex_kuc'] = df['d_mex_kuc'].replace('inf%', '0.00%')
    df['d_mex_fox'] = df['d_mex_fox'].replace('inf%', '0.00%')
    df['d_mex_chi'] = df['d_mex_chi'].replace('inf%', '0.00%')
    #--------------------------------------------------
    df['d_kuc_bin'] = df['d_kuc_bin'].replace('inf%', '0.00%')
    df['d_kuc_mb'] = df['d_kuc_mb'].replace('inf%', '0.00%')
    df['d_kuc_gat'] = df['d_kuc_gat'].replace('inf%', '0.00%')
    df['d_kuc_bit'] = df['d_kuc_bit'].replace('inf%', '0.00%')
    df['d_kuc_mex'] = df['d_kuc_mex'].replace('inf%', '0.00%')
    df['d_kuc_fox'] = df['d_kuc_fox'].replace('inf%', '0.00%')
    df['d_kuc_chi'] = df['d_kuc_chi'].replace('inf%', '0.00%')
    #--------------------------------------------------
    df['d_fox_bin'] = df['d_fox_bin'].replace('inf%', '0.00%')
    df['d_fox_mb'] = df['d_fox_mb'].replace('inf%', '0.00%')
    df['d_fox_gat'] = df['d_fox_gat'].replace('inf%', '0.00%')
    df['d_fox_bit'] = df['d_fox_bit'].replace('inf%', '0.00%')
    df['d_fox_mex'] = df['d_fox_mex'].replace('inf%', '0.00%')
    df['d_fox_kuc'] = df['d_fox_kuc'].replace('inf%', '0.00%')
    df['d_fox_chi'] = df['d_fox_chi'].replace('inf%', '0.00%')
    #--------------------------------------------------
    df['d_chi_bin'] = df['d_chi_bin'].replace('inf%', '0.00%')
    df['d_chi_mb'] = df['d_chi_mb'].replace('inf%', '0.00%')
    df['d_chi_gat'] = df['d_chi_gat'].replace('inf%', '0.00%')
    df['d_chi_bit'] = df['d_chi_bit'].replace('inf%', '0.00%')
    df['d_chi_mex'] = df['d_chi_mex'].replace('inf%', '0.00%')
    df['d_chi_kuc'] = df['d_chi_kuc'].replace('inf%', '0.00%')
    df['d_chi_fox'] = df['d_chi_fox'].replace('inf%', '0.00%')
    #--------------------------------------------------
    df.to_csv('merge.csv', index=False)
inf_fix()
#------------------------------------------------------
def zero_fix():
    df = pd.read_csv('merge.csv')
    colunas_d = [coluna for coluna in df.columns if coluna.startswith('d_')]
    for indice, linha in df.iterrows():
        for coluna in colunas_d:
            valor_celula = linha[coluna]
            if isinstance(valor_celula, str) and '%' in valor_celula:
                valor_numerico = float(valor_celula.replace('%', '').strip())
                if valor_numerico > 1000:
                    df.at[indice, coluna] = '0.00%'
    df.to_csv('merge.csv', index=False)
zero_fix()
#------------------------------------------------------
def filtragem_percentual():
    caminho_arquivo = 'merge.csv'
    df = pd.read_csv(caminho_arquivo)
    #print(f"Total de linhas no arquivo: {len(df)}")
    colunas_a_verificar = df.columns[df.columns.str.startswith('d_')]
    for indice, linha in df.iterrows():
        valores_maiores_que_0_005 = [float(valor.strip('%')) / 100 for valor in linha[colunas_a_verificar] if float(valor.strip('%')) / 100 > 0.005]
       # print(f"Linha {indice + 1}:")
        #print(f"  Valores nas colunas: {linha[colunas_a_verificar].tolist()}")
       # if valores_maiores_que_0_005:
            #print("  Pelo menos um valor maior que 0.005% na linha.")
            #print(f"  Valores maiores que 0.005%: {valores_maiores_que_0_005}")
        #else:
           #print("  Nenhum valor maior que 0.005% na linha.")
        #print("=" * 50)
    df_filtrado = df[df[colunas_a_verificar].apply(lambda linha: any(float(valor.strip('%')) / 100 > 0.005 for valor in linha), axis=1)]
  # print(f"Total de linhas após filtragem: {len(df_filtrado)}")
    df_filtrado.to_csv('merge.csv', index=False)
filtragem_percentual()
#######################################################
# timer
#######################################################
def fim_tempo():
    print(separador)
    fim_binance = datetime.now().time()
    fim_str = fim_binance.strftime(" %H:%M:%S")
    print(f" 📜 - FIM: {fim_str}")
    fim_datetime = datetime.strptime(fim_str, " %H:%M:%S")
    tempo_decorrido = fim_datetime - inicio_geral
    print(" 📜 - TOT: ", tempo_decorrido)
fim_tempo()
#######################################################
# cruzar dados 
#######################################################
def divisor():
    from itertools import product
    import os
    compra_options = ['bin', 'mb', 'gat', 'bit', 'mex', 'kuc', 'fox', 'chi']
    venda_options = ['bin', 'mb', 'gat', 'bit', 'mex', 'kuc', 'fox', 'chi']
    df = pd.read_csv('merge.csv')
    for compra in compra_options:
        for venda in venda_options:
            if compra != venda:
                cols_to_show = [f'symbol_{compra}', f'brl_{compra}', f'brl_{venda}', f'd_{compra}_{venda}']
                filtered_df = df[cols_to_show]
                filtered_df = filtered_df.sort_values(by=[f'd_{compra}_{venda}'], ascending=False)
                temp_folder = 'temp'
                if not os.path.exists(temp_folder):
                    os.makedirs(temp_folder)
                file_path = os.path.join(temp_folder, f'{compra}_vs_{venda}.csv')
                filtered_df.to_csv(file_path, index=False)
divisor()
#------------------------------------------------------
def concatenacao():
    import os
    import pandas as pd
    dfs = []
    pasta_temp = 'temp'
    for arquivo in os.listdir(pasta_temp):
        if arquivo.endswith('.csv'):
            caminho_arquivo = os.path.join(pasta_temp, arquivo)
            df_temp = pd.read_csv(caminho_arquivo, header=None, skiprows=1)
            df_temp['codigo'] = df_temp.iloc[:, 0]
            df_temp['brl_compra'] = df_temp.iloc[:, 1]
            df_temp['brl_venda'] = df_temp.iloc[:, 2]
            df_temp['dif'] = df_temp.iloc[:, 3]
            df_temp['ordem'] = arquivo  
            df_temp = df_temp[['codigo', 'brl_compra', 'brl_venda', 'dif', 'ordem']]
            dfs.append(df_temp)
    # Concatena todos os DataFrames em um único DataFrame
    df_final = pd.concat(dfs, ignore_index=True)
    df_final['dif'] = df_final['dif'].str.rstrip('%')
    df_final['dif'] = df_final['dif'].astype(float)
    df_final = df_final[df_final['dif'] > 0.005]
    df_final.to_csv('merge1.csv', index=False)
concatenacao()
#------------------------------------------------------
# correções 
#------------------------------------------------------
def remove_dotcsv():
    df = pd.read_csv('merge1.csv')
    df['ordem'] = df['ordem'].str.replace('.csv', '')
    df.to_csv('merge1.csv', index=False)
remove_dotcsv()
#------------------------------------------------------
def zero_fix():
    df = pd.read_csv('merge1.csv')
    df['brl_compra'] = df['brl_compra'].apply(lambda x: '{:,.2f}'.format(float(x)).replace('.', '#').replace(',', '.').replace('#', ','))
    df['brl_venda'] = df['brl_venda'].apply(lambda x: '{:,.2f}'.format(float(x)).replace('.', '#').replace(',', '.').replace('#', ','))
    df.to_csv('merge2.csv', index=False)
zero_fix()
#------------------------------------------------------
def frame_copy():
    df = pd.read_csv('merge2.csv')
    df['bkp_brl_compra'] = df['brl_compra'].copy()
    df['bkp_brl_venda'] = df['brl_venda'].copy()
    df.to_csv('merge2.csv', index=False)
frame_copy()
#------------------------------------------------------
def v1():
    df = pd.read_csv('merge2.csv') 
    df['bkp_brl_compra'] = df['bkp_brl_compra'].str.replace('.', '').str.replace(',', '.')
    df['bkp_brl_venda'] = df['bkp_brl_venda'].str.replace('.', '').str.replace(',', '.')
    df['percentual'] = ((df['bkp_brl_venda'].astype(float) - df['bkp_brl_compra'].astype(float)) / df['bkp_brl_compra'].astype(float)) * 100
    df.to_csv('merge3.csv', index=False)
v1()
#------------------------------------------------------
def nova_ordem():
    df = pd.read_csv('merge3.csv')
    df = df[(df['percentual'].astype(float) >= 0.5) & (df['percentual'].astype(float) >= 0.5)]
    df = df.drop('brl_compra', axis=1)
    df = df.drop('brl_venda', axis=1)
    df = df.drop('dif', axis=1)
    df = df.rename(columns={'bkp_brl_compra': 'brl_compra'})
    df = df.rename(columns={'bkp_brl_venda': 'brl_venda'})
    df.to_csv('merge4.csv', index=False)
nova_ordem()
#------------------------------------------------------
def stripe_ordem():
    df = pd.read_csv('merge4.csv')
    mapeamento_corretoras = {'bin': 'Binance', 'mb': 'Mercado Bitcoin', 'gat': 'Gateio',  'mex': 'Mexc', 'bit': 'Bitso', 'kuc': 'Kucoin', 'fox': 'Foxbit', 'chi': 'Chiliz'}
    df['c_compra'] = df['ordem'].str.split('_').str[0].map(mapeamento_corretoras)
    df['c_venda'] = df['ordem'].str.split('_').str[2].map(mapeamento_corretoras)
    df.to_csv('merge4.csv', index=False)
stripe_ordem()
#------------------------------------------------------
def percentual_2f():
    df = pd.read_csv('merge4.csv')
    df['percentual'] = df['percentual'].apply(lambda x: '{:.2f}'.format(x))
    df['_ID'] = range(1, len(df) + 1)
    df = df[['_ID', 'ordem', 'c_compra', 'c_venda', 'codigo', 'brl_compra', 'brl_venda', 'percentual']]
    df.to_csv('merge5.csv', index=False)
percentual_2f()
#------------------------------------------------------
#  wordpress 
#------------------------------------------------------
def jet():
    df = pd.read_csv('merge5.csv')
    #-------------------------------------------
    df['cct_status'] = ''
    mask = df.apply(lambda row: any(row), axis=1)
    df.loc[mask, 'cct_status'] = 'publish'
    #-------------------------------------------
    df['cct_author_id'] = ''
    mask = df.apply(lambda row: any(row), axis=1)
    df.loc[mask, 'cct_author_id'] = '1'
    #-------------------------------------------
    df['cct_created'] = ''
    mask = df.apply(lambda row: any(row), axis=1)
    from datetime import datetime
    df.loc[mask, 'cct_created'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #-------------------------------------------
    df['cct_modified'] = ''
    mask = df.apply(lambda row: any(row), axis=1)
    from datetime import datetime
    df.loc[mask, 'cct_modified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #-------------------------------------------
    df.rename(columns={'codigo': 'symbol'}, inplace=True)
    #-------------------------------------------   
    df.drop(columns=['ordem'], inplace=True)
    #------------------------------------------- 
    df = df[['_ID', 'cct_status', 'c_compra', 'c_venda', 'brl_compra', 'brl_venda',  'cct_author_id', 'cct_created', 'cct_modified', 'symbol', 'percentual']]
    #-------------------------------------------   
    df.to_csv('merge7.csv', index=False)
jet()
#------------------------------------------------------
def criar_colunas_orderbook():
    df = pd.read_csv('merge7.csv')
    for i in range(1, 11):
        df[f'compra_preco_{i}'] = None
        df[f'compra_volume_{i}'] = None
        df[f'compra_liquidez_{i}'] = None
        df[f'venda_preco_{i}'] = None
        df[f'venda_volume_{i}'] = None
        df[f'venda_liquidez_{i}'] = None
    df.to_csv('merge8.csv', index=False)
criar_colunas_orderbook()
#------------------------------------------------------
def preencher_orderbook_preset():
    df = pd.read_csv('merge8.csv')
    for i in range(1, 11):
        df[f'compra_preco_{i}'] = f'compra_preco_{i}'
        df[f'compra_volume_{i}'] = f'compra_volume_{i}'
        df[f'compra_liquidez_{i}'] = f'compra_liquidez_{i}'
        df[f'venda_preco_{i}'] = f'venda_preco_{i}'
        df[f'venda_volume_{i}'] = f'venda_volume_{i}'
        df[f'venda_liquidez_{i}'] = f'venda_liquidez_{i}'
    df.to_csv('final_table.csv', index=False)
preencher_orderbook_preset()



def gerar_codigo_ob():
    df = pd.read_csv('final_table.csv') 
    mapeamento = {
        "binance": "bin",
        "mercado bitcoin": "mb",
        "gateio": "gat",
        "mexc": "mex",
        "bitso": "bit",
        "kucoin": "kuc",
        "foxbit": "fox",
        "chiliz": "chi"
    }
    def obter_sigla(palavra):
        return mapeamento.get(palavra.lower(), "Sigla não encontrada")
    df['ob_compra'] = df['c_compra'].apply(obter_sigla)
    df['ob_venda'] = df['c_venda'].apply(obter_sigla)
    df.to_csv('final_table.csv', index=False)
gerar_codigo_ob()

def obter_nomes():
    from collections import OrderedDict
    df = pd.read_csv('final_table.csv')
    lista_symbol = df['symbol'].tolist()
    lista_symbol = list(OrderedDict.fromkeys(lista_symbol))
    #---------------------------------------------------
    caminho_arquivo = 'dom_gateio.json'
    with open(caminho_arquivo, 'r', encoding='utf-8') as arquivo:
        data = json.load(arquivo)
    main_info = []
    if "data" in data and isinstance(data["data"], list):
        for entry in data["data"]:
            if isinstance(entry, dict) and "symbol" in entry and "name" in entry:
                symbol_and_name = {"symbol": entry["symbol"], "name": entry["name"]}
                main_info.append(symbol_and_name)
            else:
                print("Invalid entry format:", entry)
    else:
        print("Invalid data format. Expected a dictionary with a 'data' key containing a list.")
    #---------------------------------------------------
    symbol_list = [item['symbol'] for item in main_info]
    set_1 = set(lista_symbol)
    set_2 = set(symbol_list)
    total_em_comum = len(set_1.intersection(set_2))
    total_faltante = len(set_1.symmetric_difference(set_2))
    relacao_faltantes = set_1.difference(set_2)
    #---------------------------------------------------
    #print(f"Moedas no mapa = {len(lista_symbol)}")
    lista_symbol = [valor for valor in lista_symbol if valor not in relacao_faltantes]
    #---------------------------------------------------
    set_1 = set(lista_symbol)
    relacao_faltantes = set_1.difference(set_2)
    total_faltante = len(set_1.symmetric_difference(set_2))
    #---------------------------------------------------
    df_main = pd.DataFrame(main_info)
    df_final = pd.read_csv('final_table.csv')
    if 'name' not in df_final.columns:
        df_final['name'] = ''
    df_final = pd.read_csv('final_table.csv')
    if 'name' not in df_final.columns:
        df_final['name'] = ''
    for symbol in lista_symbol:
        name = df_main.loc[df_main['symbol'] == symbol, 'name'].values[0]
        df_final.loc[df_final['symbol'] == symbol, 'name'] = name
    df_final.to_csv('final_table.csv', index=False)
    #---------------------------------------------------
    file_path = 'final_table.csv'
    df = pd.read_csv(file_path)
    for index, row in df.iterrows():
        if pd.isna(row['name']):
            df.drop(index, inplace=True)
    df.to_csv('final_table.csv', index=False)
obter_nomes()
#---------------------------------------------------
def obter_link_nome():
    dados = pd.read_csv('final_table.csv')
    dados['link_img'] = 'https://cryptologos.cc/logos/' + dados['name'] + '-' + dados['symbol'] + '-' +'logo.png'
    dados.to_csv('final_table.csv', index=False)
    df = pd.read_csv('final_table.csv')
    df['link_img'] = df['link_img'].str.lower()
    df.to_csv('final_table.csv', index=False)
obter_link_nome()

#------------------------------------------------------
def create_table():
    nome_arquivo_csv = 'final_table.csv'
    nome_arquivo_sql = 'create.sql'
    nome_tabela = 'wp_cripto_table'
    df = pd.read_csv(nome_arquivo_csv)
    nomes_colunas = df.columns.tolist()
    script_sql = f"CREATE TABLE {nome_tabela} (\n"
    for coluna in nomes_colunas:
        script_sql += f"    {coluna} TEXT,\n"
    script_sql = script_sql.rstrip(',\n') + "\n);\n"
    for index, linha in df.iterrows():
        valores = ', '.join([f"'{valor}'" for valor in linha])
        script_sql += f"INSERT INTO {nome_tabela} ({', '.join(nomes_colunas)}) VALUES ({valores});\n"
    with open(nome_arquivo_sql, 'w') as arquivo_sql:
        arquivo_sql.write(script_sql)
create_table()
#------------------------------------------------------
def update_table():
    nome_arquivo_csv = 'final_table.csv'
    nome_arquivo_sql = 'jet.sql'
    nome_tabela = 'wp_jet_cct_ordem_cripto'
    df = pd.read_csv(nome_arquivo_csv)
    nomes_colunas = df.columns.tolist()
    script_sql = ""
    script_sql += f"DELETE FROM {nome_tabela};\n"
    for index, linha in df.iterrows():
        valores = ', '.join([f"'{valor}'" for valor in linha])
        script_sql += f"INSERT INTO {nome_tabela} ({', '.join(nomes_colunas)}) VALUES ({valores}) ON DUPLICATE KEY UPDATE {', '.join([f'{col} = VALUES({col})' for col in nomes_colunas])};\n"
    with open(nome_arquivo_sql, 'w') as arquivo_sql:
        arquivo_sql.write(script_sql)
update_table()
#------------------------------------------------------