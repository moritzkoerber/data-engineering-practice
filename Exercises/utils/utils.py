from awsume.awsumepy import awsume


def get_assumed_role_boto3_session(profile_name):
    return awsume(profile_name)
