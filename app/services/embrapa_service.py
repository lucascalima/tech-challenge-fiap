from bs4 import BeautifulSoup


def extrair_dados_tabela(html):
    soup = BeautifulSoup(html, 'html.parser')
# Pegando dentro <table class='tb_base tb_dados'>, o <tbody> e todas as <tr> que ela possui.
    linhas = soup.select('table.tb_base.tb_dados tbody tr')
    
    dados = []
# Variável que mantém em memória o último nome de categoria principal encontrado.
    categoria_atual = None

    for linha in linhas:
        colunas = linha.find_all('td')
        if not colunas:
            continue
# Resgatando o nome do item ou categoria
        nome = colunas[0].get_text(strip=True)
# Resgatando a quantidade de produtos
        quantidade = colunas[1].get_text(strip=True)

# Se for uma linha de categoria principal, este esta com "class='tb_item'" no html principal
        if 'tb_item' in colunas[0]['class']:
            categoria_atual = nome
        else:
            dados.append({
                'categoria': categoria_atual,
                'tipo_produto': nome,
                'quantidade': quantidade
            })

    return dados

def extrair_exportacao_importacao(html):
    # Extrai os dados da tabela de exportação e importação
    soup = BeautifulSoup(html, 'html.parser')
    # Pegando dentro <table class='tb_base tb_dados'>, o <tbody> e todas as <tr> que ela possui.
    tabela = soup.find('table', class_='tb_base tb_dados')

    linhas = tabela.find_all('tr')[1:]  # Ignora o cabeçalho

    dados = []

    for linha in linhas:
        colunas = linha.find_all('td')
        if len(colunas) == 3:
            pais = colunas[0].get_text(strip=True)
            quantidade = colunas[1].get_text(strip=True)
            valor = colunas[2].get_text(strip=True)

            if quantidade != '-' and valor != '-':
                dados.append({
                    'pais': pais,
                    'quantidade_kg': quantidade + ' kg',
                    'valor_usd': '$' + valor
                })
    return dados