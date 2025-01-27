# SQL Server to Parquet Export Tool 🦀

Este projeto é uma ferramenta de linha de comando (CLI) desenvolvida em Rust, que permite exportar consultas do SQL Server para arquivos no formato Parquet.

## Formas de Autenticação com SQL Server 

> [!IMPORTANT]
> O projeto oferece suporte a duas formas de autenticação para se conectar ao SQL Server:

1. **Autenticação com Usuário e Senha:**
   - Esta é a autenticação padrão.
   - Certifique-se de fornecer as credenciais corretas (usuário e senha) ao configurar a conexão.

2. **Autenticação Integrada do Windows:**
   - Disponível apenas para sistemas Windows.
   - A autenticação integrada utiliza as credenciais do usuário atualmente logado no Windows.
   - Para utilizar esta opção, configure o método de autenticação para "Integrated" ao estabelecer a conexão.

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

#### Executar uma consulta direta com autenticação integrada:
```bash
rustmssql -n "localhost" -q "SELECT * FROM tabela" -f "resultado.parquet"
```

#### Executar uma consulta de um arquivo com autenticação integrada:
```bash
rustmssql -n "localhost" -p "resultado.parquet" -f "consulta.sql"
```

#### Executar uma consulta direta informando usuário e senha:
```bash
rustmssql --n "localhost" -u "sa" -s "abcd.1234" -q "SELECT * FROM tabela" -f "resultado.parquet"
```

#### Executar uma consulta de um arquivo informando usuário e senha:
```bash
rustmssql -n "localhost" -u "sa" -s "abcd.1234" -f "resultado.parquet" -p "consulta.sql"
```

#### Passar parâmetros para a consulta:
```bash
rustmssql -n "localhost" -f "resultado.parquet" -q "SELECT * FROM tabela WHERE coluna = @P1" 1290 
```

### Parâmetros Disponíveis

- `--help`: Ajuda sobre o uso do programa.
- `--name-server`: Endereço do servidor SQL Server (obrigatório).
- `--file-parquet`: Caminho do arquivo Parquet a ser gerado (opcional).
- `--query`: Consulta SQL a ser executada (alternativo ao `--query-file`).
- `--path-file`: Caminho para um arquivo contendo a consulta SQL (alternativo ao `--query`).
- `--user`: nome do usuário (opcional).
- `--secret`: senha de acesso (depende do `--user`).
- `--params`: Vetor de parâmetros para consultas parametrizadas (opcional).

## Download arquivo binário para windows

[Windows](https://github.com/Marcus-Holanda777/rustmssql/releases/tag/v0.1.1)

## Contribuição

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.