FROM prefecthq/prefect:2.7.7-python3.9
COPY dockerRequirements.txt .
RUN pip install -r dockerRequirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows
COPY data /opt/prefect/data