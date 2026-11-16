CREATE EXTERNAL STREAM default.random_events
(
  `event_id` string,
  `event_time` datetime64(3),
  `device_id` string,
  `metric_name` string,
  `metric_value` float64,
  `event_type` string
)
ENGINE = ExternalStream
SETTINGS type = 'kafka', brokers = 'broker1:9092', topic = 'proton_window_test_events', data_format = 'JSONEachRow';

CREATE STREAM default.hop_results
(
  `win_start` datetime64(3),
  `win_end` datetime64(3),
  `device_id` string,
  `event_count` uint64,
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
  `_tp_sn` int64 CODEC(Delta(8), ZSTD(1)),
  INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 32,
  INDEX _tp_sn_index _tp_sn TYPE minmax GRANULARITY 32
)
ENGINE = Stream(1, 1, rand())
PARTITION BY to_YYYYMM(_tp_time)
ORDER BY to_start_of_hour(_tp_time)
SETTINGS index_granularity = 8192;

 CREATE MATERIALIZED VIEW default.mv_24h_hop_results INTO default.hop_results
(
  `win_start` datetime64(3),
  `win_end` datetime64(3),
  `device_id` string,
  `event_count` uint64,
  `_tp_time` datetime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
  `_tp_sn` int64
) AS
SELECT
  window_start AS win_start, window_end AS win_end, device_id, count(*) AS event_count
FROM
  hop(default.random_events, event_time, 1s, 24h)
GROUP BY
  window_start, window_end, device_id;



