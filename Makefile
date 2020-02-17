protoc:
	protoc -I proto/ proto/*.proto --go_out=plugins=grpc:proto

build:
	cd server/ && go build -o kvserver
	cd client/ && go build -o kvclient

clean:
	if [ -a server/kvserver ]; then rm server/kvserver; fi;
	if [ -a client/kvclient ]; then rm client/kvclient; fi;
	if [ -a data.json ]; then rm data.json; fi;
	if [ -a history.log ]; then rm history.log; fi;
	if [ -a new.log ]; then rm new.log; fi;