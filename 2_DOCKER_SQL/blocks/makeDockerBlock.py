from prefect.infrastructure.docker import DockerContainer

docker_container_block = DockerContainer.load("mydcontainer")