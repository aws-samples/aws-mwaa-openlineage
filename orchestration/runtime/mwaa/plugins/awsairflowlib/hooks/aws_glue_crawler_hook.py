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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import time


class AwsGlueCrawlerHook(AwsBaseHook):
    """
    Interact with AWS Glue - crawler
    :param crawler_name: unique crawler name per AWS account
    :type str
    :param region_name: aws region name (example: us-east-1)
    :type region_name: str
    :param iam_role_name: AWS IAM Role for Glue Crawler
    :type str
    """

    def __init__(self,
                 crawler_name=None,
                 aws_conn_id='aws_default',
                 region_name=None,
                 iam_role_name=None,
                 *args, **kwargs):
        kwargs['client_type'] = 'glue'
        self.crawler_name = crawler_name
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.role_name = iam_role_name
        super(AwsGlueCrawlerHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        conn = self.get_client_type('glue', self.region_name)
        return conn

    def get_iam_execution_role(self):
        """
        :return: iam role for crawler execution
        """
        iam_client = self.get_client_type('iam', self.region_name)

        try:
            glue_execution_role = iam_client.get_role(RoleName=self.role_name)
            self.log.info("Iam Role Name: {}".format(self.role_name))
            return glue_execution_role
        except Exception as general_error:
            raise AirflowException(
                'Failed to create aws glue crawler, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def initialize_crawler(self):
        """
        Initializes connection with AWS Glue
        to run crawler
        :return:
        """
        glue_client = self.get_conn()

        try:
            crawler_run = glue_client.start_crawler(
                Name=self.crawler_name
            )
            return self.crawler_completion(self.crawler_name)
        except Exception as general_error:
            raise AirflowException(
                'Failed to run aws glue crawler, error: {error}'.format(
                    error=str(general_error)
                )
            )

    def crawler_completion(self, crawler_name=None):
        """
        :param crawler_name:
        :return:
        """
        glue_client = self.get_conn()

        crawler_run_state = 'RUNNING'
        ready = False
        stopping = False

        while True:
            if ready or stopping:
                self.log.info("Exiting Crawler {} Run State: {}"
                              .format(crawler_name, crawler_run_state))
                return {'CrawlerRunState': crawler_run_state}

            else:
                self.log.info("Polling for AWS Glue Crawler {} current run state"
                              .format(crawler_name))
                crawler_status = glue_client.get_crawler(
                    Name=crawler_name
                )
                crawler_run_state = (crawler_status['Crawler']['State']).upper()
                self.log.info("Crawler Run state is {}".format(crawler_run_state))
                ready = crawler_run_state == 'READY'
                stopping = crawler_run_state == 'STOPPING'
                time.sleep(6)

