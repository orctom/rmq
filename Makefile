


# Apply go fmt to the codebase
fmt:
	go list -f '{{.Dir}}' $(MODULE)/pkg/... $(MODULE)/gen/...| xargs gofmt -s -l -w


