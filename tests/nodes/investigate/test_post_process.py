import pytest
from app.nodes.investigate.execution.execute_actions import ActionExecutionResult
from app.nodes.investigate.processing.post_process import merge_evidence

@pytest.mark.parametrize(
    "action_name, data, expected_keys",
    [
        (
            "list_eks_pods",
            {
                "pods": [{"name": "fake-pod-1"}],
                "failing_pods": [{"name": "fake-pod-2"}],
                "high_restart_pods": [],
                "total_pods": 2
            },
            ["eks_pods", "eks_failing_pods", "eks_high_restart_pods", "eks_total_pods"]
        ),
        (
            "get_eks_events",
            {
                "warning_events": [{"message": "Back-off restarting failed container"}],
                "total_warning_count": 1
            },
            ["eks_events", "eks_total_warning_count"]
        ),
        (
            "list_eks_deployments",
            {
                "deployments": [{"name": "api"}],
                "degraded_deployments": [],
                "total_deployments": 1
            },
            ["eks_deployments", "eks_degraded_deployments", "eks_total_deployments"]
        ),
        (
            "get_eks_node_health",
            {
                "nodes": [{"name": "ip-10-0-0-1.ec2.internal"}],
                "not_ready_count": 0,
                "total_nodes": 1
            },
            ["eks_node_health", "eks_not_ready_count", "eks_total_nodes"]
        ),
        (
            "get_eks_pod_logs",
            {
                "logs": "Error: Connection refused...",
                "pod_name": "fake-pod-1",
                "namespace": "default"
            },
            ["eks_pod_logs", "eks_pod_logs_pod_name", "eks_pod_logs_namespace"]
        ),
        (
            "get_eks_deployment_status",
            {
                "deployment_name": "api",
                "desired_replicas": 3,
                "ready_replicas": 3,
                "unavailable_replicas": 0,
                "conditions": []
            },
            ["eks_deployment_status"]
        )
    ]
)
def test_merge_evidence_eks_tools(action_name, data, expected_keys):
    result = ActionExecutionResult(action_name=action_name, success=True, data=data)
    evidence = merge_evidence({}, {action_name: result})

    for key in expected_keys:
        assert key in evidence
        
    # Validate the data content itself
    if action_name == "list_eks_pods":
        assert evidence["eks_pods"][0]["name"] == "fake-pod-1"
        assert evidence["eks_failing_pods"][0]["name"] == "fake-pod-2"
    elif action_name == "get_eks_events":
        assert evidence["eks_events"][0]["message"] == "Back-off restarting failed container"
    elif action_name == "list_eks_deployments":
        assert evidence["eks_deployments"][0]["name"] == "api"
    elif action_name == "get_eks_node_health":
        assert evidence["eks_node_health"][0]["name"] == "ip-10-0-0-1.ec2.internal"
        assert evidence["eks_not_ready_count"] == 0
    elif action_name == "get_eks_pod_logs":
        assert evidence["eks_pod_logs"] == "Error: Connection refused..."
        assert evidence["eks_pod_logs_pod_name"] == "fake-pod-1"
    elif action_name == "get_eks_deployment_status":
        assert evidence["eks_deployment_status"]["deployment_name"] == "api"
        assert evidence["eks_deployment_status"]["ready_replicas"] == 3
