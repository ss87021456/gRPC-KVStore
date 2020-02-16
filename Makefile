protoc:
	protoc -I proto/ proto/*.proto --go_out=plugins=grpc:proto

build:
	cd server/ && go build -o kvserver
	cd client/ && go build -o kvclient

clean:
	rm server/kvserver client/kvclient data.json history.log new.log