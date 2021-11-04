# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import unicode_literals

from awsairflowlib.hooks.aws_glue_crawler_hook import AwsGlueCrawlerHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class AWSGlueCrawlerOperator(BaseOperator):
    """
    Creates an AWS Glue Crawler. AWS Glue is a serverless Spark
    ETL service for running Spark Jobs on the AWS cloud.
    Language support: Python and Scala
    :param crawler_name: unique crawler name per AWS Account
    :type str
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    :param iam_role_name: AWS IAM Role for Glue Crawler Execution
    :type str
    """
    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    @apply_defaults
    def __init__(self,
                 crawler_name='aws_glue_default_crawler',
                 aws_conn_id='aws_default',
                 region_name=None,
                 iam_role_name=None,
                 *args, **kwargs
                 ):
        super(AWSGlueCrawlerOperator, self).__init__(*args, **kwargs)
        self.crawler_name = crawler_name
        self.aws_conn_id = aws_conn_id,
        self.region_name = region_name
        self.iam_role_name = iam_role_name

    def execute(self, context):
        """
        Executes AWS Glue Crawler from Airflow
        :return:
        """
        glue_crawler = AwsGlueCrawlerHook(crawler_name=self.crawler_name,
                                  aws_conn_id=self.aws_conn_id,
                                  region_name=self.region_name,
                                  iam_role_name=self.iam_role_name)

        self.log.info("Initializing AWS Glue Crawler: {}".format(self.crawler_name))
        glue_crawler_run = glue_crawler.initialize_crawler()
        self.log.info('AWS Glue Crawler: {crawler_name} status: COMPLETED.'
                      .format(crawler_name=self.crawler_name)
                      )

        self.log.info('Done.')