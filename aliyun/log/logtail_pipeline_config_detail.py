#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.

"""
Logtail Pipeline Config Detail Classes

This module provides classes for managing Logtail pipeline configurations.
"""

from __future__ import print_function
import json

__all__ = ['LogtailPipelineConfigDetail']


class LogtailPipelineConfigDetail(object):
    """Logtail Pipeline Config Detail
    
    :type config_name: string
    :param config_name: config name, can only contain lowercase letters, numbers, - and _, 
                        must start and end with lowercase letter or number, length between 2-128 characters
    
    :type log_sample: string
    :param log_sample: log sample, supports multiple log entries
    
    :type global_config: dict
    :param global_config: global configuration
    
    :type inputs: list
    :param inputs: input plugin list, currently only allows 1 input plugin
    
    :type processors: list
    :param processors: processor plugin list
    
    :type aggregators: list
    :param aggregators: aggregator plugin list, maximum 1 aggregator plugin allowed
    
    :type flushers: list
    :param flushers: flusher plugin list, currently only allows 1 flusher_sls plugin
    """
    
    def __init__(self, config_name, inputs, flushers, log_sample=None, 
                 global_config=None, processors=None, aggregators=None):
        """Initialize a Logtail Pipeline Config Detail
        
        :type config_name: string
        :param config_name: config name
        
        :type inputs: list
        :param inputs: input plugin list
        
        :type flushers: list
        :param flushers: flusher plugin list
        
        :type log_sample: string
        :param log_sample: log sample (optional)
        
        :type global_config: dict
        :param global_config: global configuration (optional)
        
        :type processors: list
        :param processors: processor plugin list (optional)
        
        :type aggregators: list
        :param aggregators: aggregator plugin list (optional)
        """
        self.config_name = config_name
        self.log_sample = log_sample
        self.global_config = global_config if global_config is not None else {}
        self.inputs = inputs if inputs is not None else []
        self.processors = processors if processors is not None else []
        self.aggregators = aggregators if aggregators is not None else []
        self.flushers = flushers if flushers is not None else []
    
    def to_json(self):
        """Convert to JSON object
        
        :return: dict object ready for JSON serialization
        """
        json_value = {
            "configName": self.config_name
        }
        
        # Add optional fields
        if self.log_sample:
            json_value["logSample"] = self.log_sample
        
        if self.global_config:
            json_value["global"] = self.global_config
        
        # Required fields
        json_value["inputs"] = self.inputs
        json_value["flushers"] = self.flushers
        
        # Optional fields
        if self.processors:
            json_value["processors"] = self.processors
        
        if self.aggregators:
            json_value["aggregators"] = self.aggregators
        
        return json_value
    
    @staticmethod
    def from_json(json_value):
        """Create LogtailPipelineConfigDetail from JSON object
        
        :type json_value: dict
        :param json_value: JSON object
        
        :return: LogtailPipelineConfigDetail object
        """
        config_name = json_value.get("configName", "")
        log_sample = json_value.get("logSample", None)
        global_config = json_value.get("global", None)
        inputs = json_value.get("inputs", [])
        processors = json_value.get("processors", None)
        aggregators = json_value.get("aggregators", None)
        flushers = json_value.get("flushers", [])
        
        return LogtailPipelineConfigDetail(
            config_name=config_name,
            inputs=inputs,
            flushers=flushers,
            log_sample=log_sample,
            global_config=global_config,
            processors=processors,
            aggregators=aggregators
        )



