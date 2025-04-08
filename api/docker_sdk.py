import docker
from docker.errors import APIError, NotFound
import os

def get_docker_client():
    """Initializes and returns a Docker client instance."""
    global docker_client
    if docker_client is None:
        try:
            docker_client = docker.from_env()
            # Test connection
            docker_client.ping()
            print("Docker client initialized successfully.")
        except Exception as e:
            print(f"Error initializing Docker client: {e}")
            print("Ensure Docker socket is mounted and Docker is running.")
            docker_client = None
    return docker_client

def get_container_sdk(service_name: str):
    """Finds a running container for a given Docker Compose service name."""
    client = get_docker_client()
    if not client:
        return None

    project_name = os.getenv("COMPOSE_PROJECT_NAME")
    if not project_name:
        print("Error: COMPOSE_PROJECT_NAME environment variable not set in API container.")
        return None

    try:
        # Primary method: Find using Docker Compose labels
        containers = client.containers.list(filters={
            "label": f"com.docker.compose.project={project_name}",
            "label": f"com.docker.compose.service={service_name}",
            "status": "running"
        })
        if containers:
            print(f"Found running container for service '{service_name}' via labels: {containers[0].name}")
            return containers[0]
        else:
            print(f"Warning: No running container found for service '{service_name}' using labels.")
            # Fallback: Try common naming convention
            expected_name = f"{project_name}-{service_name}-1"
            try:
                container = client.containers.get(expected_name)
                if container.status == 'running':
                     print(f"Found running container by name fallback: {container.name}")
                     return container
                else:
                    print(f"Container '{expected_name}' found but not running (status: {container.status}).")
                    return None
            except NotFound:
                print(f"Container '{expected_name}' not found by name either.")
                return None
            except APIError as e:
                 print(f"API error during fallback container search for {service_name}: {e}")
                 return None

    except APIError as e:
        print(f"Docker API error finding container for {service_name}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error finding container {service_name}: {e}")
        return None