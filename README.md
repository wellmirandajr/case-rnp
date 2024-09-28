# Case RNP

## Visão Geral

Este repositório contém um pipeline de ingestão de dados para o banco de dados **Ecommerce**, utilizando Spark no Databricks. O pipeline conecta-se a um banco de dados PostgreSQL via JDBC e processa várias tabelas, salvando os resultados em formato Parquet.

## Visão do Processo

1. **Conexão ao PostgreSQL**: 
   - Estabelece uma conexão com o banco de dados PostgreSQL utilizando JDBC no Databricks.

2. **Ingestão de Dados**:
   - Cria um arquivo Parquet para cada uma das seguintes tabelas:
     - `customers`
     - `employees`
     - `orderdetails`
     - `orders`
     - `payments`
     - `product_lines`
     - `products`
     - `offices`
   - Salva os arquivos Parquet no diretório DBFS: 
     ```
     /dbfs/FileStore/tables/case_rnp
     ```

3. **Criação de DataFrames**:
   - Lê os arquivos Parquet salvos e cria um DataFrame para cada arquivo.

4. **Criação do Schema**:
   - Cria o schema **RNP** no catálogo do Databricks.

5. **Criação de Tabelas Delta**:
   - Cria tabelas Delta no schema **RNP** para cada DataFrame.
   - Adiciona duas colunas de timestamp de ingestão:
     - `ts_ingestao_brt` (timestamp da ingestão)
     - `dt_ingestao_brt` (data da ingestão)

6. **Consultas Focadas no Negócio**:
   - Três tabelas Delta são criadas para responder às seguintes perguntas de negócio:
     - **Qual país possui a maior quantidade de itens cancelados?**
     - **Qual o faturamento da linha de produto mais vendida, considerando os pedidos com status 'Shipped' realizados em 2005?**
     - **Traga o nome, sobrenome e e-mail dos vendedores do Japão.**

## Começando

Para executar este pipeline, assegure-se de ter acesso a um ambiente Databricks e as permissões necessárias para se conectar ao banco de dados PostgreSQL. Clone este repositório e siga as instruções nos notebooks para executar o pipeline.

## Licença

Este projeto é licenciado sob a Licença MIT - veja o arquivo [LICENSE](LICENSE) para mais detalhes.
