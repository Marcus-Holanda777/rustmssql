# SQL Server to Parquet Export Tool 🦀

Este projeto é uma ferramenta de linha de comando (CLI) desenvolvida em Rust, que permite exportar consultas do SQL Server para arquivos no formato Parquet.

> [!WARNING]
> No momento só existe suporte ao login integrada onde não é preciso informar a senha para se conectar no SQL SERVER.

## Recursos

- **Consultas dinâmicas:** Execute consultas diretamente da linha de comando ou de um arquivo SQL.
- **Conexão configurável:** Especifique as informações do servidor SQL Server na linha de comando.
- **Parâmetros personalizados:** Forneça um vetor de parâmetros para consultas parametrizadas.
- **Saída Parquet:** Salve os resultados das consultas no formato Parquet, altamente eficiente para armazenamento e análise de dados.

## Tecnologias Utilizadas

- **[Tiberius](https://github.com/prisma/tiberius):** Para conexão e execução de consultas no SQL Server.
- **[Parquet](https://docs.rs/parquet/latest/parquet/):** Para criação de arquivos Parquet a partir dos resultados das consultas.
- **Rust:** Linguagem de programação segura e de alta performance.

## Instalação

1. Certifique-se de que você possui o [Rust](https://www.rust-lang.org/tools/install) instalado em sua máquina.
2. Clone este repositório:
   ```bash
   git clone https://github.com/Marcus-Holanda777/rustmssql.git
   cd rustmssql
   ```
3. Compile o projeto:
   ```bash
   cargo build --release
   ```
4. O executável estará disponível em `target/release/rustmssql`.

## Uso

### Comandos Básicos

#### Executar uma consulta direta:
```bash
rustmssql --name-server "localhost" --query "SELECT * FROM tabela" --file-parquet "resultado.parquet"
```

#### Executar uma consulta de um arquivo:
```bash
rustmssql --name-server "localhost" --file-parquet "resultado.parquet" --path-file "consulta.sql"
```

#### Passar parâmetros para a consulta:
```bash
rustmssql --name-server "localhost" --file-parquet "resultado.parquet" --query "SELECT * FROM tabela WHERE coluna = @P1" 1290 
```

### Parâmetros Disponíveis

- `--help`: Ajuda sobre o uso do programa.
- `--name-server`: Endereço do servidor SQL Server (obrigatório).
- `--file-parquet`: Caminho do arquivo Parquet a ser gerado (opcional).
- `--query`: Consulta SQL a ser executada (alternativo ao `--query-file`).
- `--path-file`: Caminho para um arquivo contendo a consulta SQL (alternativo ao `--query`).
- `--params`: Vetor de parâmetros para consultas parametrizadas (opcional).

### Download bibário para windows

["Windows"](https://github.com/Marcus-Holanda777/rustmssql/releases/tag/v0.1.0)

## Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.