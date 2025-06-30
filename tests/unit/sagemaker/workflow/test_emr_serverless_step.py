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
from __future__ import absolute_import

import json
import pytest

from sagemaker.workflow.emr_serverless_step import (
    EMRServerlessStep,
    ERR_STR_WITHOUT_APP_ID_AND_APP_CONFIG,
    ERR_STR_WITH_BOTH_APP_ID_AND_APP_CONFIG,
    ERR_STR_WITHOUT_JOB_CONFIG,
    ERR_STR_WITHOUT_EXECUTION_ROLE_ARN,
)
from sagemaker.workflow.steps import CacheConfig
from sagemaker.workflow.pipeline import Pipeline, PipelineGraph
from sagemaker.workflow.parameters import ParameterString
from tests.unit.sagemaker.workflow.helpers import CustomStep, ordered


def test_emr_serverless_step_with_application_id():
    """Test creating an EMR Serverless step with an existing application ID."""
    # Given
    step_name = "MyEMRServerlessStep"
    application_id = "app-12345"
    job_config = {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script.py"}}}
    execution_role_arn = "arn:aws:iam::123456789012:role/EMRServerlessJobRole"

    # When
    step = EMRServerlessStep(
        name=step_name,
        application_id=application_id,
        job_config=job_config,
        execution_role_arn=execution_role_arn,
        depends_on=["TestStep"],
        display_name="MyEMRServerlessStep",
        description="MyEMRServerlessStepDescription",
        cache_config=CacheConfig(enable_caching=True, expire_after="PT1H"),
    )
    step.add_depends_on(["SecondTestStep"])

    # Then
    expected_request = {
        "Name": "MyEMRServerlessStep",
        "Type": "EMRServerless",
        "Arguments": {
            "ApplicationId": "app-12345",
            "JobConfig": {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script.py"}}},
            "ExecutionRoleArn": "arn:aws:iam::123456789012:role/EMRServerlessJobRole",
        },
        "DependsOn": ["TestStep", "SecondTestStep"],
        "DisplayName": "MyEMRServerlessStep",
        "Description": "MyEMRServerlessStepDescription",
        "CacheConfig": {"Enabled": True, "ExpireAfter": "PT1H"},
    }

    assert step.to_request() == expected_request
    assert step._service_name == "emr-serverless"
    assert step.properties.shape_name == "Step"


def test_emr_serverless_step_with_application_config():
    """Test creating an EMR Serverless step with application configuration."""
    # Given
    step_name = "MyEMRServerlessStep"
    application_config = {
        "Name": "MySparkApp",
        "Type": "SPARK",
        "ReleaseLabel": "emr-6.9.0",
    }
    job_config = {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script.py"}}}
    execution_role_arn = "arn:aws:iam::123456789012:role/EMRServerlessJobRole"

    # When
    step = EMRServerlessStep(
        name=step_name,
        application_config=application_config,
        job_config=job_config,
        execution_role_arn=execution_role_arn,
    )

    # Then
    expected_request = {
        "Name": "MyEMRServerlessStep",
        "Type": "EMRServerless",
        "Arguments": {
            "ApplicationConfig": {
                "Name": "MySparkApp",
                "Type": "SPARK",
                "ReleaseLabel": "emr-6.9.0",
            },
            "JobConfig": {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script.py"}}},
            "ExecutionRoleArn": "arn:aws:iam::123456789012:role/EMRServerlessJobRole",
        },
        "DisplayName": "MyEMRServerlessStep",
        "Description": "MyEMRServerlessStep",
    }

    assert step.to_request() == expected_request
    assert "ApplicationId" not in step.args
    assert "ApplicationConfig" in step.args


def test_pipeline_interpolates_emr_serverless_outputs(sagemaker_session):
    """Test that the pipeline correctly interpolates EMR Serverless step outputs."""
    # Given
    custom_step = CustomStep("TestStep")
    parameter = ParameterString("MyStr")

    # When
    step_emr_serverless_1 = EMRServerlessStep(
        name="emr_serverless_step_1",
        application_id="app-12345",
        job_config={"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script1.py"}}},
        execution_role_arn="arn:aws:iam::123456789012:role/EMRServerlessJobRole",
        display_name="emr_serverless_step_1",
        description="MyEMRServerlessStepDescription",
        depends_on=[custom_step],
    )

    step_emr_serverless_2 = EMRServerlessStep(
        name="emr_serverless_step_2",
        application_config={
            "Name": "MySparkApp",
            "Type": "SPARK",
            "ReleaseLabel": "emr-6.9.0",
        },
        job_config={"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script2.py"}}},
        execution_role_arn="arn:aws:iam::123456789012:role/EMRServerlessJobRole",
        display_name="emr_serverless_step_2",
        description="MyEMRServerlessStepDescription",
        depends_on=[custom_step],
    )

    pipeline = Pipeline(
        name="MyPipeline",
        parameters=[parameter],
        steps=[step_emr_serverless_1, step_emr_serverless_2, custom_step],
        sagemaker_session=sagemaker_session,
    )

    # Then
    pipeline_def = json.loads(pipeline.definition())
    assert ordered(pipeline_def) == ordered(
        {
            "Version": "2020-12-01",
            "Metadata": {},
            "Parameters": [{"Name": "MyStr", "Type": "String"}],
            "PipelineExperimentConfig": {
                "ExperimentName": {"Get": "Execution.PipelineName"},
                "TrialName": {"Get": "Execution.PipelineExecutionId"},
            },
            "Steps": [
                {
                    "Name": "emr_serverless_step_1",
                    "Type": "EMRServerless",
                    "Arguments": {
                        "ApplicationId": "app-12345",
                        "JobConfig": {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script1.py"}}},
                        "ExecutionRoleArn": "arn:aws:iam::123456789012:role/EMRServerlessJobRole",
                    },
                    "DependsOn": ["TestStep"],
                    "Description": "MyEMRServerlessStepDescription",
                    "DisplayName": "emr_serverless_step_1",
                },
                {
                    "Name": "emr_serverless_step_2",
                    "Type": "EMRServerless",
                    "Arguments": {
                        "ApplicationConfig": {
                            "Name": "MySparkApp",
                            "Type": "SPARK",
                            "ReleaseLabel": "emr-6.9.0",
                        },
                        "JobConfig": {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script2.py"}}},
                        "ExecutionRoleArn": "arn:aws:iam::123456789012:role/EMRServerlessJobRole",
                    },
                    "Description": "MyEMRServerlessStepDescription",
                    "DisplayName": "emr_serverless_step_2",
                    "DependsOn": ["TestStep"],
                },
                {
                    "Name": "TestStep",
                    "Type": "Training",
                    "Arguments": {},
                },
            ],
        }
    )
    adjacency_list = PipelineGraph.from_pipeline(pipeline).adjacency_list
    assert ordered(adjacency_list) == ordered(
        {"emr_serverless_step_1": [], "emr_serverless_step_2": [], "TestStep": ["emr_serverless_step_1", "emr_serverless_step_2"]}
    )


def test_emr_serverless_step_validation_error_no_app_id_or_config():
    """Test validation error when neither application_id nor application_config is provided."""
    # Given
    step_name = "MyEMRServerlessStep"
    job_config = {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script.py"}}}
    execution_role_arn = "arn:aws:iam::123456789012:role/EMRServerlessJobRole"

    # When/Then
    with pytest.raises(ValueError) as error:
        EMRServerlessStep(
            name=step_name,
            job_config=job_config,
            execution_role_arn=execution_role_arn,
        )
    assert f"EMRServerlessStep {step_name} must have either application_id or application_config" in str(error.value)


def test_emr_serverless_step_validation_error_both_app_id_and_config():
    """Test validation error when both application_id and application_config are provided."""
    # Given
    step_name = "MyEMRServerlessStep"
    application_id = "app-12345"
    application_config = {
        "Name": "MySparkApp",
        "Type": "SPARK",
        "ReleaseLabel": "emr-6.9.0",
    }
    job_config = {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script.py"}}}
    execution_role_arn = "arn:aws:iam::123456789012:role/EMRServerlessJobRole"

    # When/Then
    with pytest.raises(ValueError) as error:
        EMRServerlessStep(
            name=step_name,
            application_id=application_id,
            application_config=application_config,
            job_config=job_config,
            execution_role_arn=execution_role_arn,
        )
    assert f"EMRServerlessStep {step_name} cannot have both application_id and application_config" in str(error.value)


def test_emr_serverless_step_validation_error_no_job_config():
    """Test validation error when job_config is not provided."""
    # Given
    step_name = "MyEMRServerlessStep"
    application_id = "app-12345"
    execution_role_arn = "arn:aws:iam::123456789012:role/EMRServerlessJobRole"

    # When/Then
    with pytest.raises(ValueError) as error:
        EMRServerlessStep(
            name=step_name,
            application_id=application_id,
            execution_role_arn=execution_role_arn,
        )
    assert f"EMRServerlessStep {step_name} must have job_config" in str(error.value)


def test_emr_serverless_step_validation_error_no_execution_role():
    """Test validation error when execution_role_arn is not provided."""
    # Given
    step_name = "MyEMRServerlessStep"
    application_id = "app-12345"
    job_config = {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script.py"}}}

    # When/Then
    with pytest.raises(ValueError) as error:
        EMRServerlessStep(
            name=step_name,
            application_id=application_id,
            job_config=job_config,
        )
    assert f"EMRServerlessStep {step_name} must have execution_role_arn" in str(error.value)


def test_emr_serverless_step_to_request():
    """Test serialization to request format."""
    # Given
    step_name = "MyEMRServerlessStep"
    application_id = "app-12345"
    job_config = {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script.py"}}}
    execution_role_arn = "arn:aws:iam::123456789012:role/EMRServerlessJobRole"

    # When
    step = EMRServerlessStep(
        name=step_name,
        application_id=application_id,
        job_config=job_config,
        execution_role_arn=execution_role_arn,
    )
    request = step.to_request()

    # Then
    assert request["Type"] == "EMRServerless"
    assert request["Name"] == step_name
    assert request["Arguments"]["ApplicationId"] == application_id
    assert request["Arguments"]["JobConfig"] == job_config
    assert request["Arguments"]["ExecutionRoleArn"] == execution_role_arn


def test_emr_serverless_step_properties():
    """Test properties access."""
    # Given
    step_name = "MyEMRServerlessStep"
    application_id = "app-12345"
    job_config = {"JobDriver": {"SparkSubmit": {"EntryPoint": "s3://bucket/script.py"}}}
    execution_role_arn = "arn:aws:iam::123456789012:role/EMRServerlessJobRole"

    # When
    step = EMRServerlessStep(
        name=step_name,
        application_id=application_id,
        job_config=job_config,
        execution_role_arn=execution_role_arn,
    )

    # Then
    assert step._service_name == "emr-serverless"
    assert step.properties.shape_name == "Step"
