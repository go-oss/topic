.PHONY: test mockgen vendor

test: dep mockgen
	go test -race -cover ./...

mockgen:
	go get github.com/golang/mock/mockgen
	mockgen -source ./interface.go -destination ./mock.go -package topic

dep:
	dep ensure -v -vendor-only
