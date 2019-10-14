package main

import (
	"os"
	"strconv"
	//"fmt"
)

func main() {

	args := os.Args[1:4]

	serverId := args[0]
	n, _ := strconv.Atoi(args[1])
	masterPort := args[2]
	id_num, _ := strconv.Atoi(serverId)

	peerPort := strconv.Itoa(20000 + id_num)

	var peers []string
	var server Server

	for i := 0; i < n; i++ {
		peerStr := strconv.Itoa(20000 + i)
		peers = append(peers, peerStr)
	}

	if masterPort == "10002" { // this is only on first startup
		server = Server{pid: serverId, peers: peers, masterPort: masterPort,
			peerPort: peerPort, broadcastMode: false, is_coord: true}
	} else {
		server = Server{pid: serverId, peers: peers, masterPort: masterPort,
			peerPort: peerPort, broadcastMode: false, is_coord: false}
	}

	server.run()

	os.Exit(0)

}
