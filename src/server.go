package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	pid           string
	peers         []string
	masterPort    string
	peerPort      string
	broadcastMode bool
	alive         []string
	messages      []string
	playlist      map[string]string //dictionary of <song_name, song_URL>
	is_coord      bool
}

const (
	CONNECT_HOST = "localhost"
	CONNECT_TYPE = "tcp"
)

func (self *Server) run() {

	curr_log := self.read_DTLog()
	fmt.Println(curr_log) // TODO: temp fix to use curr_log, remember to remove
	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterPort)
	lPeer, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.peerPort)

	if error != nil {
		fmt.Println("Error listening!")
	}

	go self.sendPeers(false, "ping")
	go self.receivePeers(lPeer)
	if self.is_coord {
		self.coordHandleMaster(lMaster)
	} else {
		self.participantHandleMaster(lMaster)
	}

}

func (self *Server) coordHandleMaster(lMaster net.Listener) {
	// coordinator gets messeages from master, and then sends messages accordingly to participants
	// runs 3pc coordinator algorithm
}

func (self *Server) participantHandleCoord() {
	// Participants get messages from coordinator, runs 3pc participant algorithm
}

func (self *Server) participantHandleMaster(lMaster net.Listener) {
	defer lMaster.Close()

	connMaster, error := lMaster.Accept()
	reader := bufio.NewReader(connMaster)
	for {

		if error != nil {
			fmt.Println("Error while accepting connection")
			continue
		}

		message, _ := reader.ReadString('\n')

		message = strings.TrimSuffix(message, "\n")
		message_slice := strings.Split(message, " ")

		retMessage := ""
		removeComma := 0
		if message_slice[0] == "alive" {
			retMessage += "alive "
			for _, port := range self.alive {
				retMessage += port + ","
				removeComma = 1
			}

			retMessage = retMessage[0 : len(retMessage)-removeComma]
			lenStr := strconv.Itoa(len(retMessage))

			retMessage = lenStr + "-" + retMessage

		} else if message_slice[0] == "get" {
			song_name := message_slice[1]
			song_url := self.playlist[song_name]
			if song_url == "" {
				retMessage = "NONE"
			} else {
				retMessage = song_url
			}

			lenStr := strconv.Itoa(len(retMessage))
			retMessage = lenStr + "-" + retMessage

		} else {

			broadcastMessage := after(message, "broadcast ")
			if broadcastMessage != "" {
				self.messages = append(self.messages, broadcastMessage)
				self.sendPeers(true, broadcastMessage)
			} else {
				retMessage += "Invalid command. Use 'get', 'alive', or 'broadcast <message>'"
			}
		}

		connMaster.Write([]byte(retMessage))

	}

	connMaster.Close()

}

func (self *Server) receivePeers(lPeer net.Listener) {
	defer lPeer.Close()

	for {
		connPeer, error := lPeer.Accept()

		if error != nil {
			fmt.Println("Error while accepting connection")
			continue
		}

		message, _ := bufio.NewReader(connPeer).ReadString('\n')
		message = strings.TrimSuffix(message, "\n")
		if message == "ping" {
			connPeer.Write([]byte(self.pid))
		} else {
			self.messages = append(self.messages, message)
		}
		connPeer.Close()

	}

}

func (self *Server) sendPeers(broadcastMode bool, message string) {

	for {

		var tempAlive []string

		for _, otherPort := range self.peers {

			if otherPort != self.peerPort {
				peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
				if err != nil {
					continue
				}

				fmt.Fprintf(peerConn, message+"\n")
				response, _ := bufio.NewReader(peerConn).ReadString('\n')
				tempAlive = append(tempAlive, response)
			}

		}

		if broadcastMode {
			break
		}

		tempAlive = append(tempAlive, self.pid)
		sort.Strings(tempAlive)
		self.alive = tempAlive
		time.Sleep(1000 * time.Millisecond)
	}

}

func after(input string, target string) string {
	pos := strings.LastIndex(input, target)
	if pos == -1 {
		return ""
	}
	adjustedPos := pos + len(target)
	if adjustedPos >= len(input) {
		return ""
	}
	return input[adjustedPos:len(input)]
}

func (self *Server) write_DTLog(line string) {
	/*
		All lines in log will be lower case. The first line is always "start"
	*/
	file_name := self.pid + "_DTLog.txt"
	f, _ := os.OpenFile(file_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	f.Write([]byte(line))
	f.Close()
}

func (self *Server) read_DTLog() string {
	file_name := self.pid + "_DTLog.txt"
	file, err := os.Open(file_name)
	if err != nil {
		// file doesnt exist yet, create one
		self.write_DTLog("start\n")
		fmt.Println("New log created for " + self.pid + ".")
	}
	defer file.Close()
	log_content, err := ioutil.ReadAll(file)
	return string(log_content)
}
