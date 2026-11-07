.PHONY: build-server package-server test lint

build-server:
	mkdir -p dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/scf-server ./cmd/scf-server

package-server: build-server
	cp deploy/scf_bootstrap dist/scf_bootstrap
	chmod +x dist/scf_bootstrap
	rm -f dist/scf-server.zip
	cd dist && zip scf-server.zip scf-server scf_bootstrap

test:
	go test -race -count=1 ./...

lint:
	go vet ./...
	@test -z "$$(gofmt -l .)" || (gofmt -l .; exit 1)
