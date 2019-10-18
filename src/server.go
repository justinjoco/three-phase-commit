package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	pid              string
	peers            []string
	masterFacingPort string
	peerFacingPort   string
	up_set           map[string]string //maps a process's pid to its portfacing number
	playlist         map[string]string //dictionary of <song_name, song_URL>
	is_coord         bool
	state			 string 			//Saves the current state of process: TODO
	songQuery        map[string]string //map containing the song's name and URL for deletion or adding
	request			 string            //Saved add or delete command 
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

	go self.heartbeat()
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
	coordMessage := "coordinator " + self.pid
	coordLenStr := strconv.Itoa(len(coordMessage))
	connMaster.Write([]byte(coordLenStr + "-" +coordMessage))
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
			commit_abort := self.coordHandleParticipants(command, args)
			if commit_abort {
				retMessage += "commit"
			} else {
				retMessage += "abort"
			}
			lenStr := strconv.Itoa(len(retMessage))
			retMessage = lenStr + "-" + retMessage

		} else if command == "get" {
			retMessage += "resp "
			song_name := args[0]
			song_url := self.playlist[song_name]
			if song_url == "" {
				retMessage += "NONE"
			} else {
				retMessage += song_url
			}

			lenStr := strconv.Itoa(len(retMessage))
			retMessage = lenStr + "-" + retMessage


		}else{
			retMessage += "Invalid command. This is the coordinator use 'add <songName> <songURL>', 'get <songName>', or 'delete <songName>'"
		}

		connMaster.Write([]byte(retMessage))

	}

	connMaster.Close()
}

func (self *Server) coordHandleParticipants(command string, args []string) bool{
	//ADD or DELETE request: sending + receiving
	songName := ""
	songURL := ""
	numUp := len(self.up_set)
	retBool := false
	participantChannel := make(chan string)
	self.write_DTLog(command + " start-3PC")
	fmt.Println("Sending VOTE-REQ")
	if command == "add" {
		songName = args[0]
		songURL = args[1]
		message := command + " " + songName + " " + songURL 
		
		for _, otherPort := range self.up_set {
			go self.msgParticipant(otherPort, message, participantChannel)
		}

	}else{
		songName = args[0]
		message := command + " " + songName
		for _, otherPort := range self.up_set {
			go self.msgParticipant(otherPort, message, participantChannel)
		}
	}


	//VOTE-REQ
	yes_votes := 0
	num_voted := 0
	vote_success := false
	for start := time.Now(); time.Since(start) < time.Second; {
		if num_voted == numUp {
			fmt.Println("All votes say yes! Send precommit!")
			if yes_votes == num_voted {
				vote_success = true
			}
			break
		}
		select{
			case response := <- participantChannel:
				if response == "yes" {
					yes_votes+=1
				}
				num_voted += 1
		}
	}



	//Precommit Send + Receiving
	if vote_success {
		self.write_DTLog("precommit")
		for _, otherPort := range self.up_set {
			go self.msgParticipant(otherPort, "precommit\n", participantChannel)
		}
		ack_success := false
		ack_votes := 0
		for start := time.Now(); time.Since(start) < time.Second; {
			if ack_votes == numUp {
				fmt.Println("All precommits acknowledged!")
				ack_success = true
				break
			}
			select{
				case response := <- participantChannel:
					if response == "ack\n" {
						break
					}else{
						ack_votes+=1
					}
			}
		}

		//COMMIT SENDING
		if ack_success {
			retBool = true
			self.write_DTLog("commit")
			for _, otherPort := range self.up_set {
				go self.msgParticipant(otherPort, "commit\n", participantChannel)
			}

			fmt.Println("Commit sent!")
		}
	} else{
		//ABORT SENDING
		self.write_DTLog("abort")
		for _, otherPort := range self.up_set {
			go self.msgParticipant(otherPort, "abort\n", participantChannel)
		}
	}

	return retBool
}


func (self *Server) participantHandleCoord(message string, connCoord net.Conn) {
	//Receiving add/delete + sending YES/NO
	message_slice := strings.Split(message, " ")
	command := message_slice[0]
	fmt.Println(command)
	if command == "add"{

		songName := message_slice[1]
		songURL := message_slice[2]
		if !self.is_coord{
			self.write_DTLog(message)
		}
		songQuery := map[string]string{
			"songName": songName,
			"songURL": songURL,
		}		
		self.request = "add"
		self.songQuery = songQuery
		urlSize := len(songURL)
		pid, _ := strconv.Atoi(self.pid)
		if urlSize > pid + 5 {
			connCoord.Write([]byte("no"))
		}else{
			connCoord.Write([]byte("yes"))
			if !self.is_coord{
				self.write_DTLog("yes")
			}
		}

	
	}else if command == "delete" {
		if !self.is_coord{
			self.write_DTLog(message)
		}
		songName := message_slice[1]
		self.request = "delete"
		songQuery := map[string]string{
			"songName": songName,
			"songURL": "",
		}		
		self.request = "delete"
		self.songQuery = songQuery

		connCoord.Write([]byte("yes"))

		if !self.is_coord{
			self.write_DTLog("yes")
		}
		

	}else if command == "precommit"{
		connCoord.Write([]byte("ack"))
	} else if command == "commit" {
		fmt.Println("commiting add/delete request")

		if self.request == "add" {
			self.playlist[self.songQuery["songName"]] = self.songQuery["songURL"]
		} else{
			delete(self.playlist, self.songQuery["songName"])
		}

		if !self.is_coord{
			self.write_DTLog("commit")
		}
	} else if command == "abort" {
		fmt.Println("abort request")

		if !self.is_coord{
			self.write_DTLog("abort")
		}
		
	} else {
		connCoord.Write([]byte("Invalid message"))
	}



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
		command := message_slice[0]

		retMessage := ""

		if command == "get" {
			retMessage += "resp "
			song_name := message_slice[1]
			if song_url, ok := self.playlist[song_name]; ok {
				retMessage += song_url
			} else{
				retMessage += "NONE"
			}
		
			fmt.Println(self.playlist)


			lenStr := strconv.Itoa(len(retMessage))
			retMessage = lenStr + "-" + retMessage
		} else {		
			retMessage += "Invalid command. This is a participant. Use 'get <songName>'"
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
			self.participantHandleCoord(message, connPeer)
		}
		connPeer.Close()

	}

}

func (self *Server) heartbeat() {


	for {

		tempAlive := make(map[string]string)

		for _, otherPort := range self.peers {

			peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
			if err != nil {
				continue
			}

			fmt.Fprintf(peerConn, "ping"+"\n")
			response, _ := bufio.NewReader(peerConn).ReadString('\n')
			tempAlive[response] = otherPort

		}


		self.up_set = tempAlive
		time.Sleep(1000 * time.Millisecond)

	}

}

func (self *Server) msgParticipant(otherPort string, message string, channel chan string) {
	
	peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Fprintf(peerConn, message+"\n")
	response, _ := bufio.NewReader(peerConn).ReadString('\n')

	fmt.Println(response)
	channel <- response
	
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
	f.Write([]byte(line + "\n"))
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
