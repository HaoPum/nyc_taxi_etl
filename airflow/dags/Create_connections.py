import subprocess

def setup_connections():
    connections = [
        {
            'conn_id': 'postgres_default',
            'conn_type': 'postgres',
            'host': 'postgres',
            'port': '5432',
            'login': 'postgres',
            'password': 'postgres',
            'schema': 'taxi_db'
        },

        {
            'conn_id': 'spark_default',
            'conn_type': 'spark',
            'host': 'spark://spark-master:7077',
            'extra': '{"queue": "default", "deploy-mode": "client"}'
        },
    ]

    for conn in connections:
        cmd_parts = ["connections add", 
                     conn['conn_id'],
                     f"--conn-type {conn['conn_type']}",
                     f"--conn-host {conn['host']}",
                     ]
        if 'port' in conn:
            cmd_parts.append(f"--conn-port {conn['port']}")
        if 'login' in conn:
            cmd_parts.append(f"--conn-login {conn['login']}")
        if 'password' in conn:
            cmd_parts.append(f"--conn-password {conn['password']}")
        if 'schema' in conn:
            cmd_parts.append(f"--conn-schema {conn['schema']}")
        if 'extra' in conn:
            cmd_parts.append(f"--conn-extra '{conn['extra']}'")

        cmd = " ".join(cmd_parts)
        create_connections(cmd)

def create_connections(cmd):
    cmd_run = f"docker compose exec airflow-webserver airflow {cmd}"
    print(cmd_run)
    result = subprocess.run(cmd_run, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print("Error", result.stderr)
        return False
    print("Success", result.stdout)

if __name__ == "__main__":
    setup_connections()
