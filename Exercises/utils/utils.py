from pathlib import Path

from awsume.awsumepy import awsume


def get_assumed_role_boto3_session(profile_name):
    return awsume(profile_name)


def create_directory(directory):
    Path(directory).mkdir(parents=True, exist_ok=True)
