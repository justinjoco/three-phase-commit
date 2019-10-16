package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	pid              string
	peers            []string
	masterFacingPort string
	peerFacingPort   string
	broadcastMode    bool
	up_set           []string
	messages         []string
	playlist         map[string]string //dictionary of <song_name, song_URL>
	is_coord         bool
}

const (
	CONNECT_HOST = "localhost"
	CONNECT_TYPE = "tcp"
)

func (self *Server) run() {

	curr_log := self.read_DTLog()
	fmt.Println(curr_log) // TODO: temp fix to use curr_log, remember to remove
	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	lPeer, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.peerFacingPort)

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
		command := message_slice[0]
		args := message_slice[1:]

		retMessage := ""
		if command == "add" || command == "delete" {
			retMessage += "ack "
			commit_abort := self.three_pc(command, args)
			if commit_abort {
				retMessage = "commit"
			} else {
				retMessage = "abort"
			}
			lenStr := strconv.Itoa(len(retMessage))
			retMessage = lenStr + "-" + retMessage

		} else if command == "get" {
			song_name := args[0]
			song_url := self.playlist[song_name]
			if song_url == "" {
				retMessage = "NONE"
			} else {
				retMessage = song_url
			}

			lenStr := strconv.Itoa(len(retMessage))
			retMessage = lenStr + "-" + retMessage

		} else {
			retMessage += "Invalid command. This is the coordinator use 'add <song> <url>', 'get', or 'delete <song>'"
		}

		connMaster.Write([]byte(retMessage))

	}

	connMaster.Close()
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

		if message_slice[0] == "get" {
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
			retMessage += "Invalid command. This is a participant. Use 'get'"
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

			if otherPort != self.peerFacingPort {
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
		self.up_set = tempAlive
		time.Sleep(1000 * time.Millisecond)
	}

}

func (self *Server) three_pc(command string, args []string) bool {
	return true
}

func (self *Server) write_DTLog(line string) {
	/*
		All lines in log will be lower case. The first line is always "start"
	*/
	path := "./logs"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700) //creates the directory if not exist
	}
	file_name := self.pid + "_DTLog.txt"
	file_path := filepath.Join(path, file_name)
	f, _ := os.OpenFile(file_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	f.Write([]byte(line))
	f.Close()
}

func (self *Server) read_DTLog() string {
	file_name := self.pid + "_DTLog.txt"
	file_path := filepath.Join("./logs", file_name)
	file, err := os.Open(file_path)
	if err != nil {
		// file doesnt exist yet, create one
		self.write_DTLog("start\n")
		fmt.Println("New log created for " + self.pid + ".")
	}
	defer file.Close()
	log_content, err := ioutil.ReadAll(file)
	return string(log_content)
}
