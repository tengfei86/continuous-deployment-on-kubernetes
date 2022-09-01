{{- define "phoenix.authorization" }}

apiVersion: "rbac.istio.io/v1alpha1"
kind: ServiceRole
metadata:
  name: {{ include "phoenixmsp.fullname" . }}
spec:
  rules:
  - services:
    - "{{ include "phoenixmsp.fullname" . }}.{{ .Release.Namespace }}.svc.cluster.local"
    paths: ["*"]
    methods: ["*"]

---
apiVersion: "rbac.istio.io/v1alpha1"
kind: ServiceRoleBinding
metadata:
  name: {{ include "phoenixmsp.fullname" . }}
  namespace: {{ .Release.Namespace }}
spec:
  subjects:
  - user: "*"
    properties:
      request.auth.claims[service-roles]: "service-user"
  roleRef:
    kind: ServiceRole
    name: "{{ include "phoenixmsp.fullname" . }}"
{{- end }}      
    
