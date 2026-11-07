.PHONY: build-server package-server build-cli deploy test lint

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

# deploy 仅提示手动命令，避免 make 在未确认时创建真实云资源。
build-cli:
	mkdir -p dist
	go build -trimpath -o dist/scfproxy ./cmd/scfproxy

deploy:
	@echo "请先运行 make package-server 和 make build-cli，再手动执行 dist/scfproxy deploy -r <地域>；使用 -h 查看参数。"
