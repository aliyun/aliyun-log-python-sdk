#!/usr/bin/env python
# encoding: utf-8

# Copyright (C) Alibaba Cloud Computing
# All rights reserved.


def split_and_strip(s, sep=None):
    return [x.strip() for x in s.split(sep)]
