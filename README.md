# Big Data — E-commerce em Tempo Real

Trabalho Prático — Arquitetura de Big Data em Tempo Real.

Pipeline de Big Data para monitoramento de vendas e logística de um e-commerce, combinando processamento em tempo real (streaming) e processamento em lote (batch).

## Equipe — Grupo 6

- José Igor Moraes Da Silva
- Álvaro Da Silva De Moura
- Maria Sharon Silva Oliveira
- Jonas Miguel De Oliveira Costa
- Pedro Henrique Holanda De Oliveira Bezerra

## Arquitetura

```text
                                      +----------------------+
                                      |                      v
Gerador Python -> Flume -> FileRoll -> Promoter -> Flink -> HBase
                    |
                    |
                    +-------------> HDFS (raw)
                                      |
                                      v
                                    Spark
                                      |
                                      v
                                  Spark SQL
                                      |
                                      v
                              Parquet no HDFS
                                      |
                                      v
                                     Hive
```

O Apache Flume realiza o fan-out dos eventos para dois fluxos:

```text
1. Flume -> HDFS
2. Flume -> FileRoll -> Promoter -> Flink -> HBase
```

O componente `promoter` é um script auxiliar que move apenas arquivos já finalizados pelo FileRoll para `/stream/ready`, evitando que o Flink leia arquivos que ainda estão sendo escritos.

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
FileRoll
    |
    v
Promoter
    |
    v
Apache Flink
    |
    | event time
    | watermark de 5 segundos
    | janela deslizante de 20 segundos
    | slide de 5 segundos
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
    | map
    | filter
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
- Hive Metastore
- Docker Compose

## Estrutura do projeto

```text
generator/   Gerador contínuo de eventos JSON
flume/       Configuração e imagem do Apache Flume
flink/       Job de processamento em tempo real
spark/       ETL histórico com RDDs e Spark SQL
hive/        Configuração do Hive Metastore
hbase/       Documentação da camada HBase
scripts/     Scripts auxiliares
docs/        Documentação do contrato de eventos
data/        Arquivos locais utilizados durante testes
```

O contrato dos eventos está documentado em:

```text
docs/CONTRATO_EVENTOS.md
```

## Pré-requisitos

Para executar o projeto é necessário:

- Docker Desktop
- Docker Compose
- Git

> **Observação:** na primeira execução, o Docker precisa de acesso à internet para baixar as imagens utilizadas pelo projeto.

Não é necessário instalar Maven localmente. A compilação do job Flink pode ser realizada utilizando uma imagem Maven via Docker.

## Clonando o projeto

```powershell
git clone https://github.com/IgorMoraesTech/big-data-ecommerce-grupo6.git
cd big-data-ecommerce-grupo6
```

## Eventos

O gerador produz continuamente eventos dos tipos:

- `click`
- `add_to_cart`
- `purchase`
- `shipment_update`

Cada evento possui campos como:

```text
event_id
event_type
event_time
user_id
session_id
product_id
category
quantity
unit_price
order_id
shipping_status
```

Parte dos eventos é gerada propositalmente com atraso no `event_time`.

Isso permite demonstrar o tratamento de eventos fora de ordem pelo Apache Flink utilizando watermarks.

## Compilando o job Flink

O diretório `flink/target/` não é versionado no GitHub.

Por isso, após clonar o projeto, o JAR deve ser compilado antes da submissão ao Flink.

No PowerShell, execute:

```powershell
docker run --rm -v "${PWD}:/workspace" -w /workspace maven:3.9.6-eclipse-temurin-11 mvn -f flink/pom.xml clean package -DskipTests
```

Após a compilação, deve existir:

```text
flink/target/ecommerce-flink-1.0.0.jar
```

## Subindo o HDFS

Suba primeiro o NameNode e o DataNode:

```powershell
docker compose up -d namenode datanode
```

Verifique os containers:

```powershell
docker compose ps
```

A interface do NameNode fica disponível em:

```text
http://localhost:9870
```

## Pipeline de streaming

Suba os componentes do fluxo em tempo real:

```powershell
docker compose up -d --build promoter flume hbase hbase-rest jobmanager taskmanager generator
```

Aguarde alguns segundos para que HBase e Flink concluam a inicialização:

```powershell
Start-Sleep -Seconds 25
```

Verifique:

```powershell
docker compose ps
```

## Preparando o HBase

O job Flink grava os alertas na tabela:

```text
trend_alerts
```

utilizando a família de colunas:

```text
info
```

Verifique primeiro se a tabela já existe:

```powershell
"exists 'trend_alerts'" | docker exec -i hbase hbase shell -n
```

Caso a tabela ainda não exista, crie-a:

```powershell
"create 'trend_alerts', 'info'" | docker exec -i hbase hbase shell -n
```

Essa criação é necessária apenas na primeira execução enquanto o volume HBase não for removido.

## Submetendo o job Flink

Com o JAR compilado e os serviços ativos:

```powershell
docker exec flink-jobmanager flink run -d /opt/project/flink/target/ecommerce-flink-1.0.0.jar
```

Verifique se o job está executando:

```powershell
docker exec flink-jobmanager flink list
```

O resultado esperado contém:

```text
E-commerce - Tendencias em Tempo Real (RUNNING)
```

A interface web do Flink fica disponível em:

```text
http://localhost:8081
```

## Ingestão com Flume

O gerador escreve continuamente eventos JSON em:

```text
/data/events.log
```

O Flume utiliza uma source do tipo `exec` com:

```text
tail -n 0 -F /data/events.log
```

e replica cada evento para dois canais.

### Destino HDFS

Os eventos brutos são gravados em:

```text
/bigdata/ecommerce/raw/YYYY/MM/DD/HH
```

### Destino Flink

O segundo sink utiliza `file_roll` e grava inicialmente em:

```text
/stream/incoming
```

O script:

```text
scripts/promote_stream_files.py
```

move os arquivos finalizados para:

```text
/stream/ready
```

O Flink monitora continuamente esse diretório.

## Verificando os dados brutos no HDFS

Execute:

```powershell
docker exec namenode hdfs dfs -ls -R /bigdata/ecommerce/raw
```

Os arquivos JSONL são organizados por ano, mês, dia e hora.

Exemplo:

```text
/bigdata/ecommerce/raw/2026/09/25/18/events-....jsonl
```

## Processamento com Flink

O Flink considera os eventos:

```text
click
add_to_cart
purchase
```

que possuem um `product_id`.

Os eventos são agrupados por produto:

```text
keyBy(product_id)
```

O processamento utiliza `event_time`.

### Watermarks

Foi configurada tolerância de:

```text
5 segundos
```

para eventos fora de ordem.

### Janela deslizante

O job utiliza:

```text
tamanho da janela: 20 segundos
slide:             5 segundos
```

Portanto, a cada 5 segundos é calculada uma janela contendo os últimos 20 segundos de eventos.

### Detecção de tendência

É gerado um alerta quando a contagem da janela é:

```text
count >= 3
```

O alerta possui:

```text
product_id
count
window_start
window_end
```

## Persistência dos alertas no HBase

Os resultados do Flink são enviados ao HBase por meio da API REST.

A chave da linha utiliza:

```text
product_id + window_end
```

Exemplo de consulta:

```powershell
"scan 'trend_alerts', {LIMIT => 10}" | docker exec -i hbase hbase shell -n
```

Exemplo das colunas armazenadas:

```text
info:product_id
info:count
info:window_start
info:window_end
```

A interface do HBase Master fica disponível em:

```text
http://localhost:16010
```

## Processamento batch com Spark

O Spark processa o histórico armazenado no HDFS.

O job está localizado em:

```text
spark/etl_historico.py
```

O processamento utiliza RDDs e Spark SQL.

> **Observação:** a data `2026-09-24` é utilizada como exemplo reproduzível da validação do projeto. Para processar outra data, altere de forma consistente a data usada no gerador histórico, o caminho correspondente no HDFS e o argumento `--date` do job Spark.

## Gerando histórico de teste

Para permitir uma demonstração reprodutível do processamento do dia anterior, existe o script:

```text
scripts/seed_historico.py
```

Exemplo para gerar 1000 eventos referentes a `2026-09-24`:

```powershell
docker run --rm -v "${PWD}:/workspace" -w /workspace python:3.11-slim python scripts/seed_historico.py --date 2026-09-24 --count 1000 --output data/history-2026-09-24.jsonl
```

## Enviando o histórico ao HDFS

Crie o diretório correspondente:

```powershell
docker exec namenode hdfs dfs -mkdir -p /bigdata/ecommerce/raw/2026/09/24/00
```

Copie o arquivo para o container do NameNode:

```powershell
docker cp .\data\history-2026-09-24.jsonl namenode:/tmp/history-2026-09-24.jsonl
```

Envie o arquivo para o HDFS:

```powershell
docker exec namenode hdfs dfs -put -f /tmp/history-2026-09-24.jsonl /bigdata/ecommerce/raw/2026/09/24/00/history-2026-09-24.jsonl
```

Opcionalmente, remova a cópia temporária do container:

```powershell
docker exec namenode rm -f /tmp/history-2026-09-24.jsonl
```

Verifique:

```powershell
docker exec namenode hdfs dfs -ls /bigdata/ecommerce/raw/2026/09/24/00
```

## RDDs e wide dependency

O ETL realiza operações como:

```text
map
filter
reduceByKey
```

O `reduceByKey` exige redistribuição dos dados entre partições.

Isso produz um shuffle e caracteriza uma **wide dependency**.

Durante a execução, o job imprime a lineage do RDD.

Um trecho esperado contém:

```text
ShuffledRDD
PairwiseRDD
reduceByKey
```

Essa saída evidencia a wide dependency utilizada no processamento.

## Métricas de negócio

O Spark consolida as métricas por produto:

- cliques
- adições ao carrinho
- compras
- unidades vendidas
- receita
- taxa de conversão

O resultado possui:

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

## Executando o Spark

Em uma máquina com memória limitada, recomenda-se parar primeiro os componentes do streaming:

```powershell
docker compose stop generator flume promoter taskmanager jobmanager hbase-rest hbase
```

O HDFS pode permanecer ativo.

Suba o Spark:

```powershell
docker compose up -d spark-master spark-worker
```

Aguarde a inicialização do cluster Spark:

```powershell
Start-Sleep -Seconds 15
```

Execute o ETL:

```powershell
docker exec -e PYSPARK_PYTHON=python3 -e PYSPARK_DRIVER_PYTHON=python3 spark-master /spark/bin/spark-submit --master spark://spark-master:7077 --executor-memory 512m --executor-cores 1 --conf spark.pyspark.python=python3 --conf spark.pyspark.driver.python=python3 /opt/project/spark/etl_historico.py --date 2026-09-24
```

Durante a execução são exibidos:

- data de referência
- número de eventos válidos
- número de eventos de produto
- lineage do RDD
- `ShuffledRDD`
- schema do consolidado
- métricas por produto
- resultado da consulta Hive

Uma execução bem-sucedida termina com:

```text
[spark] ETL concluido com sucesso
```

A interface do Spark Master fica disponível em:

```text
http://localhost:8083
```

## Parquet no HDFS

O consolidado diário é gravado em formato Parquet em:

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

Para verificar:

```powershell
docker exec namenode hdfs dfs -ls -R /bigdata/ecommerce/curated/daily_product_metrics
```

Uma execução concluída produz também:

```text
_SUCCESS
```

## Hive

O Spark utiliza suporte nativo ao Hive por meio de:

```text
enableHiveSupport()
```

O consolidado é registrado como uma tabela externa Hive:

```text
ecommerce.daily_product_metrics
```

Os dados permanecem fisicamente em formato Parquet no HDFS.

O Hive Metastore utiliza Derby persistido em um volume Docker, permitindo que o catálogo permaneça disponível mesmo após reiniciar os containers Spark.

A localização dos dados é:

```text
hdfs://namenode:9000/bigdata/ecommerce/curated/daily_product_metrics
```

## Consultando o catálogo Hive

Para verificar os bancos e tabelas:

```powershell
docker exec spark-master /spark/bin/spark-sql -e "SHOW DATABASES; USE ecommerce; SHOW TABLES;"
```

O resultado deve conter:

```text
default
ecommerce
```

e:

```text
daily_product_metrics
```

## Consultando o consolidado

```powershell
docker exec spark-master /spark/bin/spark-sql -e "SELECT product_id, category, purchases, revenue, conversion_rate_pct, reference_date FROM ecommerce.daily_product_metrics WHERE reference_date='2026-09-24' ORDER BY revenue DESC;"
```

## Arquitetura de armazenamento

Cada tecnologia possui uma finalidade específica:

```text
HDFS
    |
    +-- eventos JSON brutos
    |
    +-- histórico
    |
    +-- consolidado Parquet

HBase
    |
    +-- alertas produzidos pelo Flink em tempo real

Hive
    |
    +-- catálogo analítico do consolidado produzido pelo Spark
```

## Decisões de arquitetura

### Por que Flink?

O processamento de tendências exige baixa latência e tratamento explícito de tempo de evento.

O Flink fornece diretamente:

- `event_time`
- watermarks
- janelas deslizantes
- processamento contínuo

Isso permite tratar eventos que chegam fora de ordem sem depender apenas do horário em que foram recebidos.

### Por que HBase?

Os alertas produzidos pelo streaming são registros pequenos, atualizados continuamente e identificados por chave.

O HBase é adequado para esse tipo de acesso de baixa latência.

O HDFS, por outro lado, é utilizado para armazenamento distribuído dos dados brutos e históricos.

### Por que Spark?

O Spark é utilizado para o processamento batch do histórico.

O ETL demonstra:

- RDDs
- transformações
- `reduceByKey`
- shuffle
- wide dependency
- DataFrames
- Spark SQL

### Por que Hive?

O Hive fornece a camada de catálogo analítico sobre o resultado consolidado pelo Spark.

A tabela externa permite consultar com SQL os arquivos Parquet armazenados no HDFS.

## Interfaces

| Componente | Endereço |
|---|---|
| HDFS NameNode | `http://localhost:9870` |
| Flink | `http://localhost:8081` |
| Spark Master | `http://localhost:8083` |
| HBase Master | `http://localhost:16010` |

## Execução em máquinas com pouca memória

Em ambientes com memória limitada, os dois blocos podem ser demonstrados separadamente.

### Streaming

```text
HDFS
Generator
Flume
Promoter
Flink
HBase
```

### Batch

```text
HDFS
Spark
Hive
```

Os dados persistidos nos volumes Docker e no HDFS permanecem disponíveis entre as duas etapas.

## Comandos de validação

### Flink em execução

```powershell
docker exec flink-jobmanager flink list
```

### Dados brutos no HDFS

```powershell
docker exec namenode hdfs dfs -ls -R /bigdata/ecommerce/raw
```

### Alertas no HBase

```powershell
"scan 'trend_alerts', {LIMIT => 10}" | docker exec -i hbase hbase shell -n
```

### Consolidado no HDFS

```powershell
docker exec namenode hdfs dfs -ls -R /bigdata/ecommerce/curated/daily_product_metrics
```

### Tabela Hive

```powershell
docker exec spark-master /spark/bin/spark-sql -e "SHOW DATABASES; USE ecommerce; SHOW TABLES;"
```

### Consulta analítica

```powershell
docker exec spark-master /spark/bin/spark-sql -e "SELECT product_id, category, purchases, revenue, conversion_rate_pct, reference_date FROM ecommerce.daily_product_metrics WHERE reference_date='2026-09-24' ORDER BY revenue DESC;"
```

## Resumo

```text
                                TEMPO REAL
                                    |
                                    v
Gerador -> Flume -> FileRoll -> Promoter -> Flink -> HBase
             |
             |
             +---------------------> HDFS
                                      |
                                      | HISTÓRICO
                                      v
                                    Spark
                                      |
                            RDD + wide dependency
                                      |
                                      v
                                  Spark SQL
                                      |
                                      v
                                Parquet / HDFS
                                      |
                                      v
                                    Hive
```

O projeto demonstra uma arquitetura de Big Data híbrida, unindo ingestão contínua, processamento orientado a eventos, armazenamento distribuído e processamento analítico histórico.
