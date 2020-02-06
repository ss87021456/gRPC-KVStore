protoc:
	protoc -I proto/ proto/*.proto --go_out=plugins=grpc:proto

build:
	cd server/ && go build
	cd client/ && go build

clean:
	rm server/server client/client data.json history.log