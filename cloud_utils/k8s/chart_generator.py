def add_deployment(release_name, chart_name):
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": {
            "name": release_name
        },
        "labels": {
    # {{- if.Values.expiry}}
    # expiry: {{.Values.expiry}}
    # {{- end}}
            "app.kubernetes.io/name": chart_name,
            "helm.sh/chart": chart_name+"version",
            "app.kubernetes.io/instance": release_name,
            "app.kubernetes.io/managed-by": "Helm",
    # version: "{{ .Values.version.commitSHA }}"
    # base-release: {{.Values.baseRelease}}
        }
    }
