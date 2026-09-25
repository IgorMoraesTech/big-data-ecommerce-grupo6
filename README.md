# Big Data — E-commerce em Tempo Real

Pipeline de monitoramento de vendas e logística de um e-commerce.

## Arquitetura

```text
Gerador Python -> Flume -> HDFS -> Spark -> Hive
                      \-> Flink -> HBase
```

## Objetivos
- JSON contínuo em Python.
- Ingestão com Flume.
- Logs brutos no HDFS.
- Streaming com Flink, janelas deslizantes e watermarks.
- Alertas/resultados em HBase.
- Batch/ETL com Spark.
- Consolidado no Hive.

## Estrutura
- `generator/`
- `flume/`
- `flink/`
- `spark/`
- `hive/`
- `hbase/`
- `scripts/`
- `docs/`
- `data/input/`
- `data/sample/`

O contrato de eventos oficial do grupo está em `docs/CONTRATO_EVENTOS.md`.
