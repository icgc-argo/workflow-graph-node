@Library(value='jenkins-pipeline-library@master', changelog=false) _
pipelineRDPCWorkflowGraphNode(
    buildImage: "ghcr.io/icgc-argo/graalvm-docker-image:java11-21.2.0-extras-1.0.0",
    dockerRegistry: "ghcr.io",
    dockerRepo: "icgc-argo/workflow-graph-node",
    gitRepo: "icgc-argo/workflow-graph-node",
    testCommand: "./mvnw test -ntp"
)
