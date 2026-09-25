# Arquitetura inicial

1. Python grava JSONL em `data/input/events.log`.
2. Flume acompanha o arquivo via TAILDIR e grava bruto no HDFS.
3. Flink processa streaming com `event_time`, janela deslizante e watermark; saída em HBase.
4. Spark lê o histórico do HDFS em lote, executa ETL e wide dependency; saída em Hive.

## MVP de streaming sugerido
Produtos em tendência por quantidade de eventos por `product_id` numa janela deslizante.

## MVP batch sugerido
Por produto/categoria: cliques, carrinhos, compras, receita e conversão.
Manter explicitamente `groupBy`/`reduceByKey` ou `join` para evidenciar wide dependency.
