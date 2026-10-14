# External Dependencies

This file lists the dependencies used in this repository.

{{/* compress has actually a BSD 3-Clause license, but the License file in the repo confuses go-license tooling, hence the manual exception. Its reported name depends on which of its packages are imported. */ -}}
| Dependency | License |
|------------|---------|
| Go | BSD-3-Clause |
{{ range . }}| {{ .Name }} | {{ if or (eq .Name "github.com/klauspost/compress") (eq .Name "github.com/klauspost/compress/flate") }}BSD-3-Clause{{ else }}{{ .LicenseName }}{{ end }} |
{{ end -}}
