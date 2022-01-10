# GO Executor

Listens on WS for message in format:
```
{"jobId":"1234","command":"driver:workspace:create","parameters":"{\"host\":\"123\"}"}
```
and returns all messages from stdout stderr from cmd back on ws chanel to all clients

## RUN
example with php
```
$ CMD_PATH="php,/path/go-executor/cmd.php" MSG_PATH="/path/go-executor/tmp" go run ./cmd/executor/main.go --debug
```

### Create some jobs

this will trigger job every milisecond

```
go run ./client.go -addr=localhost:1323
```