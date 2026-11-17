CREATE STREAM auth_events (
  user_id string,
  src_asn int32,
  user_agent string,
  outcome string,
  idp string,
  event_time datetime64(3)   -- explicit event time, so we control lateness
) SETTINGS event_time_column = 'event_time';

-- Load generator (tune eps_* to 1k / 10k / 100k for scaling runs)
CREATE RANDOM STREAM auth_gen (
  user_id string DEFAULT concat('u', to_string(rand() % 100000)),
  src_asn int32 DEFAULT rand() % 50,
  user_agent string DEFAULT concat('ua', to_string(rand() % 20)),
  outcome string DEFAULT if(rand() % 10 = 0, 'success', 'fail'),
  idp string DEFAULT 'entra',
  event_time datetime64(3) DEFAULT now64(3)
) SETTINGS eps = 10000;

CREATE MATERIALIZED VIEW feed INTO auth_events AS SELECT * FROM auth_gen;

SELECT window_start, window_end, src_asn,
       count() AS attempts,
       count_distinct(user_id) AS users
FROM hop(auth_events, event_time, 1s, 6h)
WHERE outcome = 'fail'
GROUP BY window_start, window_end, src_asn;
