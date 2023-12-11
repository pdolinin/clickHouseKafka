CREATE TABLE IF NOT EXISTS test.payments_queue
(
	id UInt64,
	status String,
	cents Int64,
	created_at Datetime,
	payment_method String,
	version UInt64
)
ENGINE=Kafka('localhost:9292', 'payments_topic', 'payments_group1', 'JSONEachRow');

CREATE TABLE IF NOT EXISTS test.payments
(
	id UInt64,
	status String,
	cents Int64,
	created_at Datetime,
	payment_method String,
	version UInt64
)
ENGINE = ReplacingMergeTree()
ORDER BY (id, payment_method, status)
PARTITION BY (toStartOfDay(toDate(created_at)), status);

CREATE TABLE IF NOT EXISTS test.completed_payments_sum
(
	cents Int64,
	payment_method String,
	created_at Date
)
ENGINE = SummingMergeTree()
ORDER BY (payment_method, created_at)
PARTITION BY (toStartOfMonth(created_at));

CREATE MATERIALIZED VIEW IF NOT EXISTS test.payments_consumer
TO test.payments
AS SELECT *
FROM test.payments_queue;

CREATE MATERIALIZED VIEW IF NOT EXISTS test.completed_payments_consumer
TO test.completed_payments_sum
AS SELECT cents, payment_method, toDate(created_at)
FROM test.payments_queue
WHERE status = 'completed';

INSERT INTO test.completed_payments_sum VALUES
( 101,  'Paypal', '2023-07-13');

INSERT INTO test.payments VALUES
(1, 'completed', 100, '2023-07-12', 'Paypal', 1);