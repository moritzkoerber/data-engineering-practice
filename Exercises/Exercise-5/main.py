import psycopg2


def main():
    host = "postgres"
    database = "postgres"
    user = "postgres"
    pas = "postgres"
    # fmt: off
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)  # noqa
    # fmt: on
    # your code here


if __name__ == "__main__":
    main()
