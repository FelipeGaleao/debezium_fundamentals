# Debezium Fundamentals
O objetivo desse repositório é executar um laboratório com o Debezium + Kafka. <br>
O Debezium é uma plataforma open source que permite a captura de eventos (como transações) em banco de dados e a publicação desses eventos em um ou mais tópicos do Kafka. A plataforma é escalável, podendo ser deployada em uma ou mais máquinas através de Kubernetes (ou Docker Swarm).

## Componentes
- MySQL (DB para ser monitorado pelo Debezium)
- Kafka (Broker que receberá os eventos do Debezium)
- Kafka Connect (Conector que permite a integração entre o Debezium e o Kafka)
- Kafka UI (Interface web para gerenciar o Kafka)
- Python App (Aplicação que insere dados na tabela do MySQL)

O Debezium é construído sobre o Apache Kafka, e possui uma gama de conectores compatíveis com o [Kafka Connect](https://kafka.apache.org/connect) (Framework do Apache Kafka que facilita a integração de dados entre Kafka e outros sistemas, como bancos de dados, data warehouse, ferramentas de observabilidade, etc).

Cada conector funciona com um banco de dados específico, e possui uma configuração própria. Os conectores registram o histórico de alterações no banco de dados e publicam esses eventos em um ou mais tópicos do Kafka.

Por utilizar Kafka, o Debezium torna-se confiável, pois mesmo se a aplicação Debezium parar inesperadamente ou perder sua conexão, ele não deve perder eventos que aconteceram durante a falha. Após o aplicativo reiniciar, ele continuará a capturar eventos a partir do ponto em que parou.

## Como executar?

1. Iniciar os containers
```bash
docker-compose up -d
```

2. Iniciar a aplicação que insere dados na tabela do MySQL
```bash
python3 app/insert_flights.py
```

3. Publicar o conector do Debezium via Kafka Connect
```bash
./connectors/publish-connector.sh
```

4. Acessar a interface web do Kafka UI via browser
```bash
http://localhost:8080/
```

5. Acessar o tópico do Kafka
```bash
http://localhost:8080/ui/clusters/local/all-topics/flights-connector-v1.flights.flights
```