# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""The EMR Serverless step definitions for workflow."""
from __future__ import absolute_import

from typing import Any, Dict, List, Union, Optional

from sagemaker.workflow.entities import (
    RequestType,
)
from sagemaker.workflow.properties import (
    Properties,
)
from sagemaker.workflow.step_collections import StepCollection
from sagemaker.workflow.steps import Step, StepTypeEnum, CacheConfig


# Error message constants
ERR_STR_WITHOUT_APP_ID_AND_APP_CONFIG = (
    "EMRServerlessStep {step_name} must have either application_id or application_config"
)

ERR_STR_WITH_BOTH_APP_ID_AND_APP_CONFIG = (
    "EMRServerlessStep {step_name} cannot have both application_id and application_config"
)

ERR_STR_WITHOUT_JOB_CONFIG = (
    "EMRServerlessStep {step_name} must have job_config"
)

ERR_STR_WITHOUT_EXECUTION_ROLE_ARN = (
    "EMRServerlessStep {step_name} must have execution_role_arn"
)


class EMRServerlessStep(Step):
    """EMR Serverless step for workflow."""

    def __init__(
        self,
        name: str,
        application_id: Optional[str] = None,
        application_config: Optional[Dict[str, Any]] = None,
        job_config: Dict[str, Any] = None,
        execution_role_arn: Optional[str] = None,
        depends_on: Optional[List[Union[str, Step, StepCollection]]] = None,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        cache_config: Optional[CacheConfig] = None,
    ):
        """Constructs an EMRServerlessStep.

        Args:
            name(str): The name of the EMR Serverless step.
            application_id(str): Optional. The ID of an existing EMR Serverless application.
                Either application_id or application_config must be provided, but not both.
            application_config(Dict[str, Any]): Optional. Configuration for creating a new
                EMR Serverless application. Either application_id or application_config must
                be provided, but not both.
            job_config(Dict[str, Any]): Required. Configuration for the job to run on the
                EMR Serverless application.
            execution_role_arn(str): Required. The ARN of the role used for job execution.
            depends_on(List[Union[str, Step, StepCollection]]): Optional. A list of
                Step/StepCollection names or Step/StepCollection instances that this
                EMRServerlessStep depends on.
            display_name(str): Optional. Display name of the step.
            description(str): Optional. Description of the step.
            cache_config(CacheConfig): Optional. Cache configuration for the step.
        """
        super(EMRServerlessStep, self).__init__(
            name,
            display_name or name,
            description or name,
            StepTypeEnum.EMR_SERVERLESS,
            depends_on,
        )

        # Validate inputs
        self._validate_inputs(name, application_id, application_config, job_config, execution_role_arn)

        # Build arguments dictionary
        emr_serverless_args = {
            "JobConfig": job_config,
            "ExecutionRoleArn": execution_role_arn,
        }

        if application_id is not None:
            emr_serverless_args["ApplicationId"] = application_id
        elif application_config is not None:
            emr_serverless_args["ApplicationConfig"] = application_config

        # Store arguments and set up properties
        self.args = emr_serverless_args
        self.cache_config = cache_config

        # Set up properties
        root_property = Properties(
            step_name=name, 
            step=self, 
            shape_name="Step", 
            service_name="emr-serverless"
        )
        # Store service name for testing
        self._service_name = "emr-serverless"
        self._properties = root_property

    def _validate_inputs(
        self, 
        step_name: str, 
        application_id: Optional[str], 
        application_config: Optional[Dict[str, Any]],
        job_config: Dict[str, Any],
        execution_role_arn: Optional[str],
    ):
        """Validates the inputs for the EMR Serverless step.

        Args:
            step_name(str): The name of the step.
            application_id(str): The ID of an existing EMR Serverless application.
            application_config(Dict[str, Any]): Configuration for creating a new application.
            job_config(Dict[str, Any]): Configuration for the job to run.
            execution_role_arn(str): The ARN of the role used for job execution.

        Raises:
            ValueError: If the inputs are invalid.
        """
        # Either application_id or application_config must be provided
        if application_id is None and application_config is None:
            raise ValueError(ERR_STR_WITHOUT_APP_ID_AND_APP_CONFIG.format(step_name=step_name))

        # Both application_id and application_config cannot be provided
        if application_id is not None and application_config is not None:
            raise ValueError(ERR_STR_WITH_BOTH_APP_ID_AND_APP_CONFIG.format(step_name=step_name))

        # job_config is required
        if job_config is None:
            raise ValueError(ERR_STR_WITHOUT_JOB_CONFIG.format(step_name=step_name))

        # execution_role_arn is required
        if execution_role_arn is None:
            raise ValueError(ERR_STR_WITHOUT_EXECUTION_ROLE_ARN.format(step_name=step_name))

    @property
    def arguments(self) -> RequestType:
        """The arguments dict that is used to call EMR Serverless APIs.

        Returns:
            RequestType: The arguments dict.
        """
        return self.args

    @property
    def properties(self) -> Properties:
        """A Properties object representing the EMR Serverless job response model.

        Returns:
            Properties: The properties object.
        """
        return self._properties

    def to_request(self) -> RequestType:
        """Updates the dictionary with cache configuration.

        Returns:
            RequestType: The request dictionary.
        """
        request_dict = super().to_request()
        if self.cache_config:
            request_dict.update(self.cache_config.config)
        return request_dict
