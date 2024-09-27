from dags.utils import get_task_log_config


def test_get_task_log_config_gets_config_from_aws(ecs_client):
    # Register a task definition
    response = ecs_client.register_task_definition(
        family="test-task",
        containerDefinitions=[
            {
                "name": "test-container",
                "image": "test",
                "memory": 512,
                "cpu": 256,
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": "/ecs/my-task",
                        "awslogs-region": "us-east-1",
                        "awslogs-stream-prefix": "ecs"
                    }
                },
            }
        ],
    )

    # get the config
    log_config = get_task_log_config(ecs_client,'test-task')

    assert log_config
