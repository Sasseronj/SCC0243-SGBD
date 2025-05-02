# SCC0243-SGBD

# 📘 Projeto de Integração com PostgreSQL via Docker

## 🚀 Primeiros Passos

Este projeto foi desenvolvido com suporte ao PostgreSQL utilizando Docker e scripts em Python para criação e inserção de dados no banco. Abaixo estão descritas as instruções necessárias para iniciar o ambiente localmente.

### 🐳 1. Subindo o Banco de Dados com Docker

Antes de qualquer coisa, é necessário iniciar o banco de dados PostgreSQL via Docker. Para isso, execute o seguintes comandos no terminal:

```bash
mkdir ./docker/postgres/pgdata
docker compose -f ./docker/docker_postgres.yml up -d --build
```

Esse comando irá construir e levantar um container com o PostgreSQL configurado, com os dados armazenados em:

```
./docker/postgres/pgdata
```

### 🔗 2. String de Conexão

Após subir o container, a aplicação poderá se conectar ao banco utilizando a seguinte string de conexão SQLAlchemy:

```
postgresql+psycopg2://postgresadmin:admin123@localhost:5000/postgresdb
```

> Certifique-se de que a porta `5000` está disponível no seu sistema.

### 🗂 3. Criação das Tabelas

No diretório `src/scripts/` você encontrará os scripts responsáveis por estruturar e popular o banco de dados:

- `create_table.py`: cria as tabelas de acordo com o modelo relacional.
- `insert_data.py`: insere os dados nas tabelas criadas.

O esquema das tabelas segue o modelo descrito neste diagrama e os dados obtidos no drive:  
* [Diagrama do Banco de Dados](https://dbdiagram.io/d/F1-67eb2f944f7afba184debe16)
* [Dados](https://drive.google.com/drive/folders/1tsCWEG-hNj0NPa4YSlPwqRQf4fsH4Xpm?usp=drive_link)


Para executar um dos scripts, utilize os seguintes comandos:

```bash
python3 src/scripts/create_table.py <schema>
python3 src/scripts/insert_data.py <schema>
```

Substitua `<schema>` pelo nome do schema que deseja utilizar no banco de dados.

### 📓 4. Notebooks

O diretório `notebooks/` contém notebooks úteis para extração de dados via API, como:

- `extract_api.ipynb`: exemplo de uso para obter dados externos que podem ser carregados no banco posteriormente.
