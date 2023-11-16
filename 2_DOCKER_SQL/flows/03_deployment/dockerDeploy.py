from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterFlow import etlParentFlow

docker_container_block = DockerContainer.load("mydcontainer")
dockerDep = Deployment.build_from_flow(flow=etlParentFlow,name='dockerFlow',infrastructure=docker_container_block)

if __name__ == "__main__":
    dockerDep.apply()