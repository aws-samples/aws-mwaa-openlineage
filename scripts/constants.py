constants = {
    # project
    "PROJECT_TAG": "aws-cdk-dl",
    # emr defaults
    "RELEASE_LABEL": "emr-6.0.0",
    "EMR_JOB_FLOW_PROFILE_NAME": "emrJobFlowProfile",
    "EMR_SERVICE_PROFILE_NAME": "emrServiceProfile",
    "CORE_INSTANCE_COUNT": 1,
    "CORE_INSTANCE_TYPE": "m4.large",
    "MASTER_INSTANCE_COUNT": 1,
    "MASTER_INSTANCE_TYPE": "m4.large",
    # job specific configs
    "csv_to_parquet.py": { 
        # 1 m4.large = 32 mins
        # 2 r5.xlarge = 8 mins
        # 3 r5.xlarge = 7 mins
        # 4 r5.xlarge = 4 mins ** this is the sweet spot
        # 5 r5.xlarge = 4 mins
        "CORE_INSTANCE_COUNT": 4,
        "CORE_INSTANCE_TYPE": "r5.xlarge",
    },
}
