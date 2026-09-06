>>> -- During a migration: watch table metadata locks and kill competing queries that block the ALTER
.RUN """
SELECT t.PROCESSLIST_ID AS id,
       t.PROCESSLIST_USER AS user,
       t.PROCESSLIST_HOST AS host,
       ml.OBJECT_SCHEMA,
       ml.OBJECT_NAME,
       ml.LOCK_TYPE, ml.LOCK_STATUS,
       t.PROCESSLIST_TIME AS sec,
       t.PROCESSLIST_STATE AS state,
       t.PROCESSLIST_INFO AS info
FROM performance_schema.metadata_locks ml
JOIN performance_schema.threads t ON t.THREAD_ID = ml.OWNER_THREAD_ID
WHERE ml.OBJECT_TYPE = 'TABLE'
  AND t.PROCESSLIST_ID IS NOT NULL
  AND t.PROCESSLIST_ID <> CONNECTION_ID()
ORDER BY t.PROCESSLIST_TIME DESC
""" |
.WATCH 1 |
.FOR_RUN "KILL {{_0}}"
<<<

>>> -- Kill query or session
.RUN "SHOW PROCESSLIST" |
.WATCH "1" |
.FOR_RUN "KILL {{_0}}"
<<<
