{{- define "ska-sdp-global-sky-model.labels" }}
helm.sh/chart: "{{ .Chart.Name }}-{{ .Chart.Version }}"
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
component: global-sky-model
domain: science-data-processing
{{- end }}


{{- define "ska-sdp-global-sky-model.ingress_path_prepend" }}
    {{- if $.Values.ingress.namespaced }}
        {{- printf "/%s%s" .Release.Namespace $.Values.ingress.pathStart }}
    {{- else }}
        {{- printf "%s" $.Values.ingress.pathStart }}
    {{- end }}
{{- end }}
