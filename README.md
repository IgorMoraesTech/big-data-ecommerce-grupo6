# Big Data — E-commerce em Tempo Real

Trabalho Prático — Arquitetura de Big Data em Tempo Real.

Pipeline de Big Data para monitoramento de vendas e logística de um e-commerce, combinando processamento em tempo real e processamento batch.

## Equipe — Grupo 6

- José Igor Moraes Da Silva
- Álvaro Da Silva De Moura
- Maria Sharon Silva Oliveira
- Jonas Miguel De Oliveira Costa
- Pedro Henrique Holanda De Oliveira Bezerra

## Arquitetura

```text
                         +--------> Flink --------> HBase
                         |
Gerador Python -> Flume -+
                         |
                         +--------> HDFS ---------> Spark ---------> Hive
```

## Fluxo em tempo real

```text
gerador.py
    |
    v
JSON contínuo
    |
    v
Apache Flume
    |
    +----> HDFS (dados brutos)
    |
    v
Apache Flink
    |
    | janelas deslizantes
    | event time
    | watermarks
    v
HBase
```

## Fluxo batch

```text
HDFS histórico
    |
    v
Spark RDD
    |
    | reduceByKey
    | shuffle / wide dependency
    v
Spark SQL
    |
    v
Parquet consolidado no HDFS
    |
    v
Hive
```

## Tecnologias

- Python 3
- Apache Flume
- Apache Hadoop / HDFS
- Apache Flink
- Apache HBase
- Apache Spark
- Spark SQL
- Apache Hive
- Docker Compose

## Estrutura do projeto

```text
generator/   Gerador contínuo de eventos JSON
flume/       Configuração e imagem do Apache Flume
flink/       Job de processamento em tempo real
spark/       ETL histórico com RDDs e Spark SQL
hive/        Configuração do catálogo Hive
scripts/     Scripts auxiliares
docs/        Documentação do contrato de eventos
```

## Eventos

O gerador produz continuamente eventos dos tipos:

- `click`
- `add_to_cart`
- `purchase`
- `shipment_update`

Parte dos eventos possui atraso proposital no `event_time`, permitindo demonstrar o uso de watermarks no Flink.

O contrato completo está em:

```text
docs/CONTRATO_EVENTOS.md
```

## Subindo a infraestrutura

Inicialmente, suba o HDFS:

```powershell
docker compose up -d namenode datanode
```

## Pipeline de streaming

Suba os serviços de tempo real:

```powershell
docker compose up -d promoter flume hbase hbase-rest jobmanager taskmanager generator
```

Submeta o job Flink:

```powershell
docker exec flink-jobmanager flink run -d /opt/project/flink/target/ecommerce-flink-1.0.0.jar
```

Verifique se o job está em execução:

```powershell
docker exec flink-jobmanager flink list
```

## Dados brutos no HDFS

O Flume replica os eventos para o HDFS.

Para verificar os dados armazenados:

```powershell
docker exec namenode hdfs dfs -ls -R /bigdata/ecommerce/raw
```

Os eventos são armazenados em uma estrutura particionada por data e hora:

```text
/bigdata/ecommerce/raw/YYYY/MM/DD/HH
```

## Flink

O job Flink realiza o processamento em tempo real utilizando:

- `event_time`
- watermarks
- janelas deslizantes
- agrupamento por produto
- detecção de tendências em tempo real

Os watermarks permitem tolerar eventos que chegam fora de ordem.

As janelas utilizam intervalos deslizantes para contabilizar eventos recentes por produto.

Quando uma tendência é detectada, o resultado é enviado para o HBase.

Os alertas armazenam:

```text
product_id
count
window_start
window_end
```

Exemplo de consulta no HBase:

```powershell
"scan 'trend_alerts', {LIMIT => 10}" | docker exec -i hbase hbase shell -n
```

## Spark

O processamento batch utiliza o histórico armazenado no HDFS.

Foi criado também um script auxiliar para gerar dados históricos de teste:

```powershell
docker run --rm -v "${PWD}:/workspace" -w /workspace python:3.11-slim python scripts/seed_historico.py --date 2026-09-24 --count 1000 --output data/history-2026-09-24.jsonl
```

O processamento utiliza RDDs e operações como:

```text
map
filter
reduceByKey
```

O `reduceByKey` gera um shuffle entre partições, caracterizando uma wide dependency.

Durante a execução, a lineage do RDD apresenta um `ShuffledRDD`, evidenciando a dependência ampla.

Após a etapa com RDDs, os dados são convertidos para DataFrame e processados também com Spark SQL.

## Executando o Spark

Suba o cluster Spark:

```powershell
docker compose up -d spark-master spark-worker
```

Execute o ETL histórico:

```powershell
docker exec -e PYSPARK_PYTHON=python3 -e PYSPARK_DRIVER_PYTHON=python3 spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 512m --executor-cores 1 --conf spark.pyspark.python=python3 --conf spark.pyspark.driver.python=python3 /opt/project/spark/etl_historico.py --date 2026-09-24
```

Durante a execução são exibidos:

- quantidade de eventos válidos
- quantidade de eventos de produto
- lineage do RDD
- evidência da wide dependency
- schema do consolidado
- métricas agregadas
- resultado da consulta Hive

## Métricas de negócio

O consolidado diário contém:

- quantidade de cliques
- adições ao carrinho
- compras
- unidades vendidas
- receita
- taxa de conversão por produto

O resultado possui as colunas:

```text
product_id
category
clicks
add_to_cart
purchases
units_sold
revenue
conversion_rate_pct
reference_date
```

## Parquet no HDFS

O resultado consolidado é gravado em formato Parquet no HDFS:

```text
/bigdata/ecommerce/curated/daily_product_metrics
```

Os dados são particionados por:

```text
reference_date
```

Exemplo:

```text
/bigdata/ecommerce/curated/daily_product_metrics/reference_date=2026-09-24
```

Para verificar os arquivos:

```powershell
docker exec namenode hdfs dfs -ls -R /bigdata/ecommerce/curated/daily_product_metrics
```

## Hive

O Spark utiliza suporte ao Hive e registra o consolidado como uma tabela externa:

```text
ecommerce.daily_product_metrics
```

O metastore utiliza Derby persistido em volume Docker.

Os dados permanecem fisicamente no HDFS em formato Parquet.

Para listar os bancos e tabelas:

```powershell
docker exec spark-master /spark/bin/spark-sql -e "SHOW DATABASES; USE ecommerce; SHOW TABLES;"
```

Para consultar o consolidado:

```powershell
docker exec spark-master /spark/bin/spark-sql -e "SELECT product_id, category, purchases, revenue, conversion_rate_pct, reference_date FROM ecommerce.daily_product_metrics WHERE reference_date='2026-09-24' ORDER BY revenue DESC;"
```

## Arquitetura de armazenamento

Cada tecnologia possui uma função específica:

```text
HDFS
    armazenamento dos eventos brutos
    armazenamento dos arquivos Parquet consolidados

HBase
    armazenamento dos alertas de tempo real

Hive
    catálogo analítico sobre o consolidado histórico
```

## Decisões de arquitetura

O pipeline separa processamento em tempo real e processamento batch.

O Flume realiza a ingestão e replica os eventos para dois destinos:

```text
Flume -> HDFS
Flume -> Flink
```

O HDFS mantém os dados brutos para processamento histórico.

O Flink foi escolhido para o fluxo em tempo real por oferecer suporte a:

- processamento orientado a event time
- watermarks
- janelas deslizantes

O HBase é utilizado para persistir alertas de tendências com baixa latência.

O Spark realiza o processamento batch do histórico utilizando:

- RDDs
- operações com shuffle
- wide dependencies
- Spark SQL

O Hive disponibiliza o consolidado para consultas analíticas.

## Interfaces

HDFS NameNode:

```text
http://localhost:9870
```

Flink:

```text
http://localhost:8081
```

Spark Master:

```text
http://localhost:8083
```

HBase Master:

```text
http://localhost:16010
```

## Observação sobre recursos

Em máquinas com pouca memória disponível, os blocos de streaming e batch podem ser executados separadamente.

Streaming:

```text
Flume
Flink
HBase
```

Batch:

```text
Spark
Hive
```

O HDFS permanece ativo em ambos os fluxos.

## Resumo do pipeline

```text
                         STREAMING
                            |
Gerador Python -> Flume ----+----> Flink ----> HBase
                 |
                 |
                 +----> HDFS
                          |
                          v
                        Spark
                          |
                          v
                      Spark SQL
                          |
                          v
                        Parquet
                          |
                          v
                         Hive
```

O projeto demonstra uma arquitetura de Big Data híbrida, combinando processamento de eventos em tempo real com processamento analítico histórico.