import gzip
import io

from utils.utils import get_assumed_role_boto3_session

boto3_session = get_assumed_role_boto3_session("GenericUser")
file_name = "wet.paths.gz"
s3_bucket = "commoncrawl"
s3_client = boto3_session.client("s3")


def main():
    f_obj, f_obj2 = io.BytesIO(), io.BytesIO()
    s3_client.download_fileobj(
        s3_bucket, f"crawl-data/CC-MAIN-2022-05/{file_name}", f_obj
    )
    f_obj.seek(0)
    with gzip.GzipFile(fileobj=f_obj) as f:
        uri = f.readline().decode("utf-8").strip()
        s3_client.download_fileobj(s3_bucket, uri, f_obj2)
    f_obj2.seek(0)
    with gzip.GzipFile(fileobj=f_obj2) as f:
        content = f.read().decode("utf-8").splitlines()
        for x in content:
            print(x)


if __name__ == "__main__":
    main()
