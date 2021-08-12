DROP VIEW IF EXISTS `default.red_packet_msg_plus`;
CREATE VIEW `red_packet_msg_plus` AS select 
 `defult.red_packet_msg`.`id` as `id`,
 `defult.red_packet_msg`.`receiver_uuid` as `receiver_uuid`,
 `defult.red_packet_msg`.`sender_uuid` as `sender_uuid`,
 `defult.red_packet_msg`.`msg_id` as `msg_id`,
 `defult.red_packet_msg`.`content` as `content`,
 `defult.red_packet_msg`.`create_time` as `create_time`,
 `defult.red_packet_msg`.`expire_time` as `expire_time`,
 `defult.red_packet_msg`.`original_ratio` as `original_ratio`,
 `defult.red_packet_msg`.`red_dot` as `red_dot`,
 `defult.red_packet_msg`.`receive_ratio` as `receive_ratio`,
 `defult.red_packet_msg`.`type` as `type`,
 `defult.red_packet_msg`.`alive_time` as `alive_time`,
 `defult.red_packet_msg`.`sub_type` as `sub_type`,
 `defult.red_packet_msg`.`session_id` as `session_id`,
 case when receive_ratio > 0 then concat(
        `defult.red_packet_msg`.`sender_uuid`,`defult.red_packet_msg`.`receiver_uuid`) else null end as `session_id_receive_ratio_zero`,
 `defult.red_packet_msg`.`dt` as `dt`,
 `defult.red_packet_msg`.`hour_begin` as `hour_begin`,
 `defult.red_packet_msg`.`pt` as `pt`
from
(
select 
      A.id AS id,
      A.receiver_uuid AS receiver_uuid,
      A.sender_uuid AS sender_uuid,
      A.msg_id AS msg_id,
      A.content AS content,
      A.create_time AS create_time,
      A.expire_time AS expire_time,
      A.original_ratio AS original_ratio,
      A.red_dot AS red_dot,
      --T1.receive_ratio AS receive_ratio,
      case when T1.receive_ratio > 0 then T1.receive_ratio else A.receive_ratio end AS receive_ratio,
      A.type AS type,
      A.alive_time AS alive_time,
      A.sub_type AS sub_type,
      A.session_id AS session_id,
      --case when receive_ratio > 0 then concat(A.receiver_uuid,A.sender_uuid) else null end as session_id_receive_ratio_zero,
      --T1.session_id_receive_ratio_zero AS session_id_receive_ratio_zero,
      A.dt AS dt,
      A.hour_begin as hour_begin,
      A.pt as pt 
from(
SELECT  
  `message_1_message_system_red_packet_msg`.`after`.id,
  `message_1_message_system_red_packet_msg`.`after`.receiver_uuid,
  `message_1_message_system_red_packet_msg`.`after`.sender_uuid,
  `message_1_message_system_red_packet_msg`.`after`.msg_id,
  `message_1_message_system_red_packet_msg`.`after`.content,
  `message_1_message_system_red_packet_msg`.`after`.create_time,
  `message_1_message_system_red_packet_msg`.`after`.expire_time,
  `message_1_message_system_red_packet_msg`.`after`.original_ratio,
  `message_1_message_system_red_packet_msg`.`after`.red_dot,
  `message_1_message_system_red_packet_msg`.`after`.receive_ratio,
  `message_1_message_system_red_packet_msg`.`after`.type,
  `message_1_message_system_red_packet_msg`.`after`.alive_time,
  `message_1_message_system_red_packet_msg`.`after`.sub_type,
  concat(`message_1_message_system_red_packet_msg`.`after`.`receiver_uuid`,`message_1_message_system_red_packet_msg`.`after`.`sender_uuid`) as session_id,
  case when `message_1_message_system_red_packet_msg`.`after`.`receive_ratio` > 0 then concat(`message_1_message_system_red_packet_msg`.`after`.`receiver_uuid`,`message_1_message_system_red_packet_msg`.`after`.`sender_uuid`) else null end as session_id_receive_ratio_zero,
  cast(from_unixtime(`message_1_message_system_red_packet_msg`.`after`.`create_time`, "yyyy-MM-dd") as date) as dt,
  cast(from_unixtime(`message_1_message_system_red_packet_msg`.`after`.`create_time`, "yyyy-MM-dd HH:00:00") as timestamp) as hour_begin,
  `message_1_message_system_red_packet_msg`.`year` || `message_1_message_system_red_packet_msg`.`month` || `message_1_message_system_red_packet_msg`.`day` || `message_1_message_system_red_packet_msg`.`hour` as pt
  -- row_number() over (partition by `message_1_message_system_red_packet_msg`.`after`.id order by `message_1_message_system_red_packet_msg`.ts_ms desc) as rn
from `message_1_message_system_red_packet_msg` 
where `message_1_message_system_red_packet_msg`.`op` in ('r','c') 
) A 
left join
(
  select 
      T.id AS id,
      T.receiver_uuid AS receiver_uuid,
      T.sender_uuid AS sender_uuid,
      T.msg_id AS msg_id,
      T.content AS content,
      T.create_time AS create_time,
      T.expire_time AS expire_time,
      T.original_ratio AS original_ratio,
      T.red_dot AS red_dot,
      T.receive_ratio AS receive_ratio,
      T.type AS type,
      T.alive_time AS alive_time,
      T.sub_type AS sub_type,
      T.session_id AS session_id,
      T.session_id_receive_ratio_zero AS session_id_receive_ratio_zero,
      T.dt AS dt,
      T.hour_begin as hour_begin,
      T.pt as pt      
from(
select  
  `message_1_message_system_red_packet_msg`.`after`.id as id,
  `message_1_message_system_red_packet_msg`.`after`.receiver_uuid AS receiver_uuid,
  `message_1_message_system_red_packet_msg`.`after`.sender_uuid AS sender_uuid,
  `message_1_message_system_red_packet_msg`.`after`.msg_id AS msg_id,
  `message_1_message_system_red_packet_msg`.`after`.content AS content,
  `message_1_message_system_red_packet_msg`.`after`.create_time AS create_time,
  `message_1_message_system_red_packet_msg`.`after`.expire_time AS expire_time,
  `message_1_message_system_red_packet_msg`.`after`.original_ratio AS original_ratio,
  `message_1_message_system_red_packet_msg`.`after`.red_dot AS red_dot,
  `message_1_message_system_red_packet_msg`.`after`.receive_ratio AS receive_ratio,
  `message_1_message_system_red_packet_msg`.`after`.type AS type,
  `message_1_message_system_red_packet_msg`.`after`.alive_time AS alive_time,
  `message_1_message_system_red_packet_msg`.`after`.sub_type AS sub_type,
  concat(`message_1_message_system_red_packet_msg`.`after`.`sender_uuid`,`message_1_message_system_red_packet_msg`.`after`.`receiver_uuid`) as session_id,
  case when `message_1_message_system_red_packet_msg`.`after`.`receive_ratio` > 0 then concat(`message_1_message_system_red_packet_msg`.`after`.`receiver_uuid`,`message_1_message_system_red_packet_msg`.`after`.`sender_uuid`) else null end as session_id_receive_ratio_zero,
  cast(from_unixtime(`message_1_message_system_red_packet_msg`.`after`.`create_time`, "yyyy-MM-dd") as date) as dt,
  cast(from_unixtime(`message_1_message_system_red_packet_msg`.`after`.`create_time`, "yyyy-MM-dd HH:00:00") as timestamp) as hour_begin,
  `message_1_message_system_red_packet_msg`.`year` || `message_1_message_system_red_packet_msg`.`month` || `message_1_message_system_red_packet_msg`.`day` || `message_1_message_system_red_packet_msg`.`hour` as pt,
   row_number() over (partition by `message_1_message_system_red_packet_msg`.`after`.`id` order by `message_1_message_system_red_packet_msg`.`ts_ms` desc) as rn
from `message_1_message_system_red_packet_msg` 
where `message_1_message_system_red_packet_msg`.`op` in ('u') 
) T WHERE rn =1
) T1 
on A.id = T1.id
) `defult.red_packet_msg`



bbs_2.bbs.call_log
{"before":null,"after":{"bbs_2.bbs.call_log.Value":{
"id":7282,
"uuid":"aamhqqogwb0v5e",
"dial_uuid":"nd0y8hxq5pe",
"answer_uuid":"wh1bsrh9ca4","talk_time":16,"cost":100,"status":4,"create_time":1516592735,"update_time":0,"receive_time":1516592737,"refuse_time":0,"dial_end_time":1516592754,"answer_end_time":1516592755,"error_end_time":0,"price":100,"match_price":0,"answer_show_time":1516592736,"dial_end_reason":"主动挂","answer_end_reason":"被动挂Offline_reason=0","record_url":{"string":"101060184/20180122/aamhqqogwb0v5e_034539/0_20180122034539196.aac"},"call_type":"call","source":1,"dial_appcode":1,"answer_appcode":1,"gift_income":0,"record_source":1,"match_duration":{"int":0},"is_dial_anonymity":0,"is_answer_anonymity":0,"check_status":1,"operation_user":"","recheck_user":"","op_time":0,"recheck_time":0,"model":{"string":""}}},"op":"r","ts_ms":{"long":1595415447078},"transaction":null}

NULL  {"id":7282,"uuid":"aamhqqogwb0v5e","dial_uuid":"nd0y8hxq5pe","answer_uuid":"wh1bsrh9ca4","talk_time":16,"cost":100,"status":4,"create_time":1516592735,"update_time":0,"receive_time":1516592737,"refuse_time":0,"dial_end_time":1516592754,"answer_end_time":1516592755,"error_end_time":0,"price":100,"match_price":0,"answer_show_time":1516592736,"dial_end_reason":"主动挂","answer_end_reason":"被动挂Offline_reason=0","record_url":"101060184/20180122/aamhqqogwb0v5e_034539/0_20180122034539196.aac","call_type":"call","source":1,"dial_appcode":1,"answer_appcode":1,"gift_income":0,"record_source":1,"match_duration":0,"is_dial_anonymity":0,"is_answer_anonymity":0,"check_status":1,"operation_user":"","recheck_user":"","op_time":0,"recheck_time":0,"model":"","call_id":null,"remote_tel":null,"audio_card_prop_num":null,"dialer_appcode":null,"dialer_cloned":null,"is_new":null,"match_free_num":null} r 1595415447078 NULL  2020  07  22  18

./kafka-topics --bootstrap-server 10.73.152.240:9092 --create --topic call_log_sync --replication-factor 2 --partitions 3 --config cleanup.policy=compact --config min.compaction.lag.ms=345600000 --config delete.retention.ms=345600000
./kafka-topics --bootstrap-server 10.73.152.240:9092 --create --topic red_packet_test --replication-factor 2 --partitions 6 --config cleanup.policy=delete  --config retention.ms=345600000

./kafka-topics  --bootstrap-server 10.73.152.240:9092  --reset-offsets --topic red_packet_test_topic --to-earliest --execute

./kafka-topics --describe  --bootstrap-server 10.73.152.240:9092 --topic  from_topic_bbs_2.bbs.call_log_to_hive_call_log

./kafka-topics.sh --list --bootstrap-server 10.73.152.240:9092   --property schema.registry.url=http://10.73.154.120:8081 


./kafka-avro-console-consumer --bootstrap-server 10.73.152.240:9092 --topic bbs_call_log_rel_event_test  --property schema.registry.url=http://10.73.154.120:8081 --from-beginning


./kafka-topics  --bootstrap-server 10.73.152.240:9092 --list
./kafka-avro-console-consumer --bootstrap-server 10.73.152.240:9092  --topic from_topic_bbs_2.bbs.call_log_to_hive_call_log --reset-offsets --to-earliest --execute
./kafka-avro-console-consumer --bootstrap-server 10.73.152.240:9092  --topic from_topic_bbs_2.bbs.call_log_to_hive_call_log --from-beginning


./kafka-avro-console-consumer --bootstrap-server 10.73.152.240:9092 --topic red_packet_test_topic  --property schema.registry.url=http://10.73.154.120:8081 --from-beginning |grep lbla

kafka-consumer-groups.sh  --bootstrap-server 10.202.13.27:9092 --group cjw --reset-offsets --topic cjw-test --to-earliest --execute



kafka-run-class kafka.tools.GetOffsetShell --broker-list [broker list] --topic [topic name] --time [-1:获取最新offset, -2:获取最旧offset]

./kafka-run-class kafka.tools.GetOffsetShell --broker-list 10.73.152.240:9092 --reset-offsets --topic from_topic_bbs_2.bbs.call_log_to_hive_call_log --time -1

./kafka-consumer-groups --bootstrap-server 10.73.152.240:9092 --list




{
  "name": "from_topic_bbs_2.bbs.call_log_to_hive_call_log_sink_1",
  "connector.class": "io.confluent.connect.hdfs.HdfsSinkConnector",
  "hadoop.conf.dir": "/usr/local/confluent-5.5.0/etc/hadoop",
  "flush.size": "100",
  "schema.compatibility": "BACKWARD",
  "tasks.max": "1",
  "topics": "from_topic_bbs_2.bbs.call_log_to_hive_call_log",
  "timezone": "Asia/Shanghai",
  "hdfs.url": "hdfs://10.73.153.26:9000",
  "transforms": "TombstoneHandler",
  "hive.metastore.uris": "thrift://10.73.153.26:9083",
  "locale": "zh_CN",
  "transforms.TombstoneHandler.type": "io.aiven.kafka.connect.transforms.TombstoneHandler",
  "format.class": "io.confluent.connect.hdfs.parquet.ParquetFormat",
  "hive.integration": "true",
  "transforms.TombstoneHandler.behavior": "drop_silent",
  "partitioner.class": "io.confluent.connect.storage.partitioner.HourlyPartitioner",
  "rotate.schedule.interval.ms": "300000",
  "timestamp.extractor": "Record"
}