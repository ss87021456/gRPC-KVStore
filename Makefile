protoc:
	protoc -I proto/ proto/*.proto --go_out=plugins=grpc:proto

clean:
	rm server/server