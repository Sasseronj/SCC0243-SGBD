# SCC0243-SGBD

# 📘 Projeto de Integração com PostgreSQL via Docker

## 🚀 Primeiros Passos

Este projeto foi desenvolvido com suporte ao PostgreSQL utilizando Docker e scripts em Python para criação e inserção de dados no banco. Abaixo estão descritas as instruções necessárias para iniciar o ambiente localmente.

### 🐳 1. Subindo o Banco de Dados com Docker

Antes de qualquer coisa, é necessário iniciar o banco de dados PostgreSQL via Docker. Para isso, execute o seguintes comandos no terminal:

```bash
docker compose -f ./docker/docker_postgres.yml up -d --build
```

### 🔗 2. Dados

Após subir o container, faça o dowload dos arquivos do link abaixo:

* [Diagrama do Banco de Dados](https://dbdiagram.io/d/F1-67eb2f944f7afba184debe16)
* [Dowload Dados](https://drive.google.com/drive/folders/1tsCWEG-hNj0NPa4YSlPwqRQf4fsH4Xpm?usp=drive_link)

> Certifique-se de que os dados estejam numa pasta `data` no diretório raíz do repositório.

### 🗂 3. Criação das Tabelas

No diretório `src/scripts/` você encontrará os scripts responsáveis por estruturar e popular o banco de dados:

- `create_table.py`: cria as tabelas de acordo com o modelo relacional.
- `insert_data.py`: insere os dados nas tabelas criadas.

Para executar um dos scripts, utilize os seguintes comandos:

```bash
python3 src/scripts/create_table.py <schema>
python3 src/scripts/insert_data.py <schema>
```

Substitua `<schema>` pelo nome do schema que deseja utilizar no banco de dados.

### 📓 4. App

Por fim execute a aplicação:

```
python3 app.py
```
