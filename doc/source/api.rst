
API
====

.. toctree::
   :maxdepth: 2
   :caption: Contents:

Main Class
-----------
.. py:currentmodule:: aliyun.log
.. autosummary::
   LogClient
   LogException
   LogResponse


Logging Handler Class
--------------------------
.. py:currentmodule:: aliyun.log
.. autosummary::
   SimpleLogHandler
   QueuedLogHandler
   LogFields


Request and Config Class
--------------------------
.. py:currentmodule:: aliyun.log
.. autosummary::
   GetHistogramsRequest
   GetLogsRequest
   GetProjectLogsRequest
   ListTopicsRequest
   ListLogstoresRequest
   PutLogsRequest
   LogtailConfigGenerator
   PluginConfigDetail
   SeperatorFileConfigDetail
   SimpleFileConfigDetail
   FullRegFileConfigDetail
   JsonFileConfigDetail
   ApsaraFileConfigDetail
   SyslogConfigDetail
   MachineGroupDetail
   IndexConfig
   OssShipperConfig
   OdpsShipperConfig
   ShipperTask


Response Class
-----------------

.. py:currentmodule:: aliyun.log
.. autosummary::
   CreateProjectResponse
   DeleteProjectResponse
   GetProjectResponse
   ListProjectResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   GetLogsResponse
   ListLogstoresResponse
   ListTopicsResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   GetCursorResponse
   GetCursorTimeResponse
   ListShardResponse
   DeleteShardResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   GetHistogramsResponse
   Histogram
   GetLogsResponse
   QueriedLog
   PullLogResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   CreateIndexResponse
   UpdateIndexResponse
   DeleteIndexResponse
   GetIndexResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   CreateLogtailConfigResponse
   DeleteLogtailConfigResponse
   GetLogtailConfigResponse
   UpdateLogtailConfigResponse
   ListLogtailConfigResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   CreateMachineGroupResponse
   DeleteMachineGroupResponse
   GetMachineGroupResponse
   UpdateMachineGroupResponse
   ListMachineGroupResponse
   ListMachinesResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   ApplyConfigToMachineGroupResponse
   RemoveConfigToMachineGroupResponse
   GetMachineGroupAppliedConfigResponse
   GetConfigAppliedMachineGroupsResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   CreateShipperResponse
   UpdateShipperResponse
   DeleteShipperResponse
   GetShipperConfigResponse
   ListShipperResponse
   GetShipperTasksResponse
   RetryShipperTasksResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   ConsumerGroupEntity
   CreateConsumerGroupResponse
   ConsumerGroupCheckPointResponse
   ConsumerGroupHeartBeatResponse
   ConsumerGroupUpdateCheckPointResponse
   DeleteConsumerGroupResponse
   ListConsumerGroupResponse
   UpdateConsumerGroupResponse

.. py:currentmodule:: aliyun.log
.. autosummary::
   CreateEntityResponse
   UpdateEntityResponse
   DeleteEntityResponse
   GetEntityResponse
   ListEntityResponse

Project
--------
.. py:currentmodule:: aliyun.log.LogClient
.. autosummary::
   list_project
   create_project
   get_project
   delete_project
   copy_project


Logstore
----------
.. autosummary::
   copy_logstore
   list_logstore
   create_logstore
   get_logstore
   update_logstore
   delete_logstore
   list_topics


Index
-------
.. autosummary::
   create_index
   update_index
   delete_index
   get_index_config


Logtail Config
-----------------
.. autosummary::
   create_logtail_config
   update_logtail_config
   delete_logtail_config
   get_logtail_config
   list_logtail_config


Machine Group
---------------
.. autosummary::
   create_machine_group
   delete_machine_group
   update_machine_group
   get_machine_group
   list_machine_group
   list_machines


Apply Logtail Config
----------------------
.. autosummary::
   apply_config_to_machine_group
   remove_config_to_machine_group
   get_machine_group_applied_configs
   get_config_applied_machine_groups


Shard
-------
.. autosummary::
   list_shards
   split_shard
   merge_shard


Cursor
--------
.. autosummary::
   get_cursor
   get_cursor_time
   get_previous_cursor_time
   get_begin_cursor
   get_end_cursor


Logs
--------
.. autosummary::
   put_logs
   pull_logs
   pull_log
   pull_log_dump
   get_log
   get_logs
   get_log_all
   get_histograms
   get_project_logs

Consumer group
----------------
.. autosummary::
   create_consumer_group
   update_consumer_group
   delete_consumer_group
   list_consumer_group
   update_check_point
   get_check_point


Dashboard
----------
.. autosummary::
   list_dashboard
   create_dashboard
   get_dashboard
   update_dashboard
   delete_dashboard



Saved search
----------------
.. autosummary::
   list_savedsearch
   create_savedsearch
   get_savedsearch
   update_savedsearch
   delete_savedsearch



Alert
-----------------
.. autosummary::
   list_alert
   create_alert
   get_alert
   update_alert
   delete_alert


Shipper
----------
.. autosummary::
   create_shipper
   update_shipper
   delete_shipper
   get_shipper_config
   list_shipper
   get_shipper_tasks
   retry_shipper_tasks


Definitions
-------------

.. autoclass:: aliyun.log.LogClient
   :members:
.. py:currentmodule:: aliyun.log
.. autoclass:: LogException
.. autoclass:: GetHistogramsRequest
.. autoclass:: GetLogsRequest
.. autoclass:: GetProjectLogsRequest
.. autoclass:: IndexConfig
.. autoclass:: ListTopicsRequest
.. autoclass:: ListLogstoresRequest
.. autoclass:: LogtailConfigGenerator
   :members:
.. autoclass:: PluginConfigDetail
.. autoclass:: SeperatorFileConfigDetail
.. autoclass:: SimpleFileConfigDetail
.. autoclass:: FullRegFileConfigDetail
.. autoclass:: JsonFileConfigDetail
.. autoclass:: ApsaraFileConfigDetail
.. autoclass:: SyslogConfigDetail
.. autoclass:: MachineGroupDetail
.. autoclass:: PutLogsRequest
.. autoclass:: OssShipperConfig
.. autoclass:: OdpsShipperConfig
.. autoclass:: ShipperTask

.. autoclass:: LogResponse
   :members:
.. autoclass:: CreateProjectResponse
   :members:
.. autoclass:: DeleteProjectResponse
   :members:
.. autoclass:: GetProjectResponse
   :members:
.. autoclass:: ListProjectResponse
   :members:

.. autoclass:: GetLogsResponse
   :members:
.. autoclass:: ListLogstoresResponse
   :members:
.. autoclass:: ListTopicsResponse
   :members:

.. autoclass:: GetCursorResponse
   :members:
.. autoclass:: GetCursorTimeResponse
   :members:
.. autoclass:: ListShardResponse
   :members:
.. autoclass:: DeleteShardResponse
   :members:

.. autoclass:: GetHistogramsResponse
   :members:
.. autoclass:: Histogram
   :members:
.. autoclass:: GetLogsResponse
   :members:
.. autoclass:: QueriedLog
   :members:
.. autoclass:: PullLogResponse
   :members:

.. autoclass:: CreateIndexResponse
   :members:
.. autoclass:: UpdateIndexResponse
   :members:
.. autoclass:: DeleteIndexResponse
   :members:
.. autoclass:: GetIndexResponse
   :members:

.. autoclass:: CreateLogtailConfigResponse
   :members:
.. autoclass:: DeleteLogtailConfigResponse
   :members:
.. autoclass:: GetLogtailConfigResponse
   :members:
.. autoclass:: UpdateLogtailConfigResponse
   :members:
.. autoclass:: ListLogtailConfigResponse
   :members:

.. autoclass:: CreateMachineGroupResponse
   :members:
.. autoclass:: DeleteMachineGroupResponse
   :members:
.. autoclass:: GetMachineGroupResponse
   :members:
.. autoclass:: UpdateMachineGroupResponse
   :members:
.. autoclass:: ListMachineGroupResponse
   :members:
.. autoclass:: ListMachinesResponse
   :members:

.. autoclass:: ApplyConfigToMachineGroupResponse
   :members:
.. autoclass:: RemoveConfigToMachineGroupResponse
   :members:
.. autoclass:: GetMachineGroupAppliedConfigResponse
   :members:
.. autoclass:: GetConfigAppliedMachineGroupsResponse
   :members:

.. autoclass:: CreateShipperResponse
   :members:
.. autoclass:: UpdateShipperResponse
   :members:
.. autoclass:: DeleteShipperResponse
   :members:
.. autoclass:: GetShipperConfigResponse
   :members:
.. autoclass:: ListShipperResponse
   :members:
.. autoclass:: GetShipperTasksResponse
   :members:
.. autoclass:: RetryShipperTasksResponse
   :members:

.. autoclass:: ConsumerGroupEntity
   :members:
.. autoclass:: CreateConsumerGroupResponse
   :members:
.. autoclass:: ConsumerGroupCheckPointResponse
   :members:
.. autoclass:: ConsumerGroupHeartBeatResponse
   :members:
.. autoclass:: ConsumerGroupUpdateCheckPointResponse
   :members:
.. autoclass:: DeleteConsumerGroupResponse
   :members:
.. autoclass:: ListConsumerGroupResponse
   :members:
.. autoclass:: UpdateConsumerGroupResponse
   :members:

.. autoclass:: CreateEntityResponse
   :members:
.. autoclass:: UpdateEntityResponse
   :members:
.. autoclass:: DeleteEntityResponse
   :members:
.. autoclass:: GetEntityResponse
   :members:
.. autoclass:: ListEntityResponse
   :members:

.. autoclass:: SimpleLogHandler
.. autoclass:: QueuedLogHandler
.. autoclass:: LogFields


