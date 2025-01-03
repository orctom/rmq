
help:
	@grep -B1 -E "^[a-zA-Z0-9_-]+\:([^\=]|$$)" Makefile \
	 | grep -v -- -- \
	 | sed 'N;s/\n/###/' \
	 | sed -n 's/^#: \(.*\)###\(.*\):.*/\2###\1/p' \
	 | column -t  -s '###'

#: Apply go fmt to the codebase
fmt:
	go list -f '{{.Dir}}' $(MODULE)/pkg/... $(MODULE)/gen/...| xargs gofmt -s -l -w

#: build the project
build:
	go mod tidy
	go build

#: create git tag with the version number
release:
	git tag `pi version -s`
	git push origin `pi version -s`

#: re-create git tag with the version number
release-again:
	git tag -d `pi version -s`
	git push -d origin `pi version -s`
	git tag `pi version -s`
	git push origin `pi version -s`
