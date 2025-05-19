# Tech Challenge

Este é o projeto **Tech Challenge**, desenvolvido como parte do primeiro módulo da pós-graduação em Machine Learning. O objetivo do projeto é criar uma aplicação web que consome dados de tabelas HTML de um site da Embrapa e os disponibiliza por meio de uma API REST.

## Funcionalidades

A aplicação oferece as seguintes rotas para acessar os dados:

- **`/produtos`**: Retorna dados sobre produtos extraídos da tabela HTML.
- **`/processamento`**: Retorna dados relacionados ao processamento de produtos.
- **`/comercializacao`**: Retorna dados sobre a comercialização de produtos.
- **`/importacao`**: Retorna dados sobre importação, incluindo país, quantidade e valor.
- **`/exportacao`**: Retorna dados sobre exportação, incluindo país, quantidade e valor.

## Estrutura do Projeto
 ├── .gitignore 
 ├── README.md 
 ├── requirements.txt 
 ├── run.py 
    └── app/ 
    └── services/ 
        ├── __init__.py 
        └── embrapa_service.py
   └── routes.py
   └── __init__.py

- **`run.py`**: Arquivo principal que inicia o servidor Flask
- **`routes.py `**: definição das rotas da API.
- **`app/services/embrapa_service.py`**: Contém funções para extração e processamento de dados das tabelas HTML.
- **`requirements.txt`**: Lista de dependências do projeto.
- **`.gitignore`**: Arquivo que define os arquivos e diretórios a serem ignorados pelo Git.

## Tecnologias Utilizadas

- **Python**: Linguagem principal do projeto.
- **Flask**: Framework web para criação da API REST.
- **BeautifulSoup**: Biblioteca para extração de dados de HTML.
- **Requests**: Biblioteca para realizar requisições HTTP.

## Como Executar

1. Clone o repositório:
   ```bash
   git clone <URL_DO_REPOSITORIO>
   cd tech_challenge
2. No VS Code: F1 → Dev Containers: Rebuild and Reopen in Container
3. Execute a aplicação:
   python main.py
4. Acesse a API em http://127.0.0.1:8080/api/docs

Contribuição: