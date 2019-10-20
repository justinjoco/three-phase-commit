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
	state            string            //Saves the current state of process: TODO
	songQuery        map[string]string //map containing the song's name and URL for deletion or adding
	request          string            //Saved add or delete command
	crashStage       string            //Initialized to "", could be "after_vote"||"before_vote"||"after_ack"||
	// "vote_req"||"partial_precommit"||"partial_commit"
	sentTo []string //A list of processes that the coordinator has sent command to before crash
	//If it's a participant, this will be an empty list all the time
	waitingFor string // "precommit"||"abort"||"commit"
	coordID    string // Each process will keep track of the id of the current coord
}

const (
	CONNECT_HOST = "localhost"
	CONNECT_TYPE = "tcp"
)

//Start a server
func (self *Server) run() {

	curr_log := self.read_DTLog()
	fmt.Println(curr_log) // TODO: temp fix to use curr_log, remember to remove
	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	lPeer, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.peerFacingPort)

	if error != nil {
		fmt.Println("Error listening!")
	}

	defer lMaster.Close()
	connMaster, err := lMaster.Accept()
	//Update UP set on each heartbeat iteration
	go self.heartbeat(connMaster, err)

	//Listen on peer facing port
	go self.receivePeers(lPeer)


	self.handleMaster(connMaster, err) //Adding peerFacing port to close if process crashed

}

//Coordinator handles master's commands (add, delete, get, crash operations)
func (self *Server) handleMaster(connMaster net.Conn, err error) {

	reader := bufio.NewReader(connMaster)
	fmt.Println("IM HANDLING MASTER")
	for {

		message, _ := reader.ReadString('\n')

		message = strings.TrimSuffix(message, "\n")
		message_slice := strings.Split(message, " ")
		command := message_slice[0]
		args := message_slice[1:]

		retMessage := ""
		// fmt.Println(message)

		switch command {
		//Start 3PC instance
		case "add", "delete":
			retMessage += "ack "
			commit_abort := self.coordHandleParticipants(command, args)
			if commit_abort {
				retMessage += "commit"
			} else {
				retMessage += "abort"
			}
			lenStr := strconv.Itoa(len(retMessage))
			retMessage = lenStr + "-" + retMessage
		//Returns songURL if songName is in playlist
		case "get":
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

		case "crash":
			fmt.Println("Crashing immediately")
			os.Exit(1)
		//TODO
		case "crashVoteREQ":
			fmt.Println("Crashing after sending vote req to ... ")
			self.crashStage = "vote_req"
			self.sentTo = args
			fmt.Println(args)
		//TODO
		case "crashPartialPreCommit":
			fmt.Println("Crashing after sending precommit to ... ")
			self.crashStage = "partial_precommit"
			self.sentTo = args
		//TODO
		case "crashPartialCommit":
			fmt.Println("Crashing after sending commit to ...")
			self.crashStage = "partial_commit"
			self.sentTo = args
			//TODO
		case "crashAfterVote":
			fmt.Println("Will crash after voting in next 3PC instance")
			self.crashStage = "after_vote"
		//TODO
		case "crashBeforeVote":
			fmt.Println("Will crash before voting in next 3PC instance")
			self.crashStage = "before_vote"
		//TODO
		case "crashAfterAck":
			fmt.Println("Will crash after sending ACK in next 3PC instance")
			self.crashStage = "after_ack"
		default:
			retMessage += "Invalid command. This is the coordinator use 'add <songName> <songURL>', 'get <songName>', or 'delete <songName>'"

		}
		connMaster.Write([]byte(retMessage))

	}

	connMaster.Close()
}


//Coordinator sends and receives messages to and fro the participants (which includes itself)
func (self *Server) coordHandleParticipants(command string, args []string) bool {
	//ADD or DELETE request: sending + receiving
	songName := ""
	songURL := ""
	numUpForVote := len(self.up_set)
	retBool := false
	participantChannel := make(chan string)
	self.write_DTLog(command + " start-3PC")
	// broadcast "start" to all participants, after p receives start, it knows to wait for vote_req
	fmt.Println("Sending VOTE-REQ")
	//Using switch to avoid having a lot of if-else statements
	switch command {
	case "add":
		songName = args[0]
		songURL = args[1]
		message := command + " " + songName + " " + songURL
		if self.crashStage != "vote_req" {
			for _, otherPort := range self.up_set {
				go self.msgParticipant(otherPort, message, participantChannel) // vote-req
			}
		} else {
			if len(self.sentTo) == 0 {
				fmt.Println("vote req sent, crashing now!")
				os.Exit(1)
			}
			for _, otherID := range self.sentTo {
				fmt.Println("stsarting iteration going through the sentTo list")
				if otherPort, ok := self.up_set[otherID]; ok {
					peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
					if err != nil {
						fmt.Println(err)
					}
					fmt.Fprintf(peerConn, message+"\n")
				}
				fmt.Println("going through the sentTo list")
			}
			fmt.Println("vote req sent, crashing now!")
			os.Exit(1)
		}

	case "delete":
		songName = args[0]
		message := command + " " + songName
		if self.crashStage != "vote_req" {
			for _, otherPort := range self.up_set {
				go self.msgParticipant(otherPort, message, participantChannel) // vote-req
			}
		} else {
			if len(self.sentTo) == 0 {
				os.Exit(1)
			}
			for _, otherID := range self.sentTo {
				if otherPort, ok := self.up_set[otherID]; ok {
					peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
					if err != nil {
						fmt.Println(err)
					}
					fmt.Fprintf(peerConn, message+"\n")
				}
			}
			os.Exit(1)
		}

	default:
		fmt.Println("Invalid command")
	}
	//VOTE-REQ
	yes_votes := 0
	num_voted := 0
	vote_success := false

	//Timeout on 1 second passing
	for start := time.Now(); time.Since(start) < time.Second; {
		if num_voted == numUpForVote {
			fmt.Println("All votes gathered!")
			if yes_votes == num_voted {
				vote_success = true
			}
			break
		}
		select {
		case response := <-participantChannel:
			if response == "yes" {
				yes_votes += 1
			}
			num_voted += 1
		}
	}

	//Time has past or got votes from everyone
	// if timeout, send ABORT to everyone

	//Precommit Send + Receiving
	if vote_success {

		numUpForAck := len(self.up_set)

		if self.crashStage != "partial_precommit" {
			for _, otherPort := range self.up_set {
				go self.msgParticipant(otherPort, "precommit\n", participantChannel) // vote-req
			}
		} else {
			if len(self.sentTo) == 0 {
				os.Exit(1)
			}
			for _, otherID := range self.sentTo {
				if otherPort, ok := self.up_set[otherID]; ok {
					peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
					if err != nil {
						fmt.Println(err)
					}
					fmt.Fprintf(peerConn, "precommit\n")
				}
			}
			os.Exit(1)
		}
	
		ack_votes := 0

		//Timeout on 1 second passing
		for start := time.Now(); time.Since(start) < time.Second; {
			if ack_votes == numUpForAck {
				fmt.Println("All precommits acknowledged!")
				break
			}
			select {
			//Read from participant Channel
			case response := <-participantChannel:
				if response == "ack\n" {
					break
				} else {
					ack_votes += 1
				}
			}
		}

		//Send commit to participants	
		retBool = true
		self.write_DTLog("commit")
		if self.crashStage != "partial_commit" {
			for _, otherPort := range self.up_set {
				go self.msgParticipant(otherPort, "commit\n", participantChannel) // vote-req
			}
		} else {
			if len(self.sentTo) == 0 {
				os.Exit(1)
			}
			for _, otherID := range self.sentTo {
				if otherPort, ok := self.up_set[otherID]; ok {
					peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
					if err != nil {
						fmt.Println(err)
					}
					fmt.Fprintf(peerConn, "commit\n")
				}
			}
			os.Exit(1)
		}

		fmt.Println("Commit sent!")
		
	} else {
		//Send abort to participants
		self.write_DTLog("abort")
		for _, otherPort := range self.up_set {
			go self.msgParticipant(otherPort, "abort\n", participantChannel)
		}
	}

	return retBool
}

//Participant handles coordinator's message depending on message content
func (self *Server) participantHandleCoord(message string, connCoord net.Conn) {
	//Receiving add/delete + sending YES/NO
	message_slice := strings.Split(message, " ")
	command := message_slice[0]

	command = strings.Trim(command, "\n")

	//On add or delete, this server records the input song's info and its add/delete operation for future 3PC stages
	switch command {
	//Sends no to coord if songUrl is bigger than self.pid + 5; yes otherwise -> records vote in DT log
	case "add":
		self.state = "aborted"
		songName := message_slice[1]
		songURL := message_slice[2]
		if !self.is_coord {
			self.write_DTLog(message)
		}
		if self.crashStage == "before_vote" {
			os.Exit(1)
		}
		songQuery := map[string]string{
			"songName": songName,
			"songURL":  songURL,
		}
		self.request = "add"
		self.songQuery = songQuery
		urlSize := len(songURL)
		pid, _ := strconv.Atoi(self.pid)
		if urlSize > pid+5 {
			connCoord.Write([]byte("no"))
		} else {
			connCoord.Write([]byte("yes")) // sending back to the coordinator, now need to wait for precommit
			self.waitingFor = "precommit"
			self.state = "uncertain"
			if !self.is_coord {
				self.write_DTLog("yes")
			}
		}
		if self.crashStage == "after_vote" {
			os.Exit(1)
		}
	//Always send yes to coord and records vote in DT log
	case "delete":
		self.state = "aborted"
		if !self.is_coord {
			self.write_DTLog(message)
		} // record message before crashing
		if self.crashStage == "before_vote" {
			os.Exit(1)
		}
		songName := message_slice[1]
		self.request = "delete"
		songQuery := map[string]string{
			"songName": songName,
			"songURL":  "",
		}
		self.request = "delete"
		self.songQuery = songQuery

		connCoord.Write([]byte("yes"))
		self.waitingFor = "precommit"
		self.state = "uncertain"
		if !self.is_coord {
			self.write_DTLog("yes")
		}
		if self.crashStage == "after_vote" {
			os.Exit(1)
		}

	//Send back ack on precommit receipt
	case "precommit":
		if self.state == "uncertain"{
			connCoord.Write([]byte("ack"))
			self.waitingFor = "commit"
			self.state = "committable"
			if self.crashStage == "after_ack" {
				os.Exit(1)
			}
		}
	//Adds song to playlist or deletes song in playlist on commit receipt
	case "commit":
		fmt.Println("COMMITTING")
		if self.state == "committable" {
			if self.request == "add" {
				self.playlist[self.songQuery["songName"]] = self.songQuery["songURL"]
			} else {
				delete(self.playlist, self.songQuery["songName"])
			}
			self.waitingFor = ""
			self.state = "committed"
			if !self.is_coord {
				self.write_DTLog("commit")
			}
		}
	//Aborts 3PC on abort receipt
	case "abort":
		fmt.Println("ABORTING")
		if self.state != "aborted"{
			if !self.is_coord {
				self.write_DTLog("abort")
			}
			self.state = "aborted"
			self.waitingFor = ""
		}
	case "state_req":
		connCoord.Write([]byte(self.state)) // TODO: update state for each process
	case "ur_elected":
		self.is_coord = true
		self.coordID = self.pid
	
	//No valid message given
	default:
		connCoord.Write([]byte("Invalid message"))

	}
}

//Listens on peer facing port; handles heartbeat-oriented messages and those that aren't
func (self *Server) receivePeers(lPeer net.Listener) {
	defer lPeer.Close()

	for {
		connPeer, error := lPeer.Accept()
		if error != nil {
			fmt.Println("Error while accepting connection")
			continue
		}
	
		recvBuf := make([]byte, 1024)
		n, _ := connPeer.Read(recvBuf[:])


		// No timeout, get the message and continue as normal
		message := string(recvBuf[:n])
		message = strings.TrimSuffix(message, "\n")
		if message != "ping" {
			self.participantHandleCoord(message, connPeer)
		} else {
			if self.is_coord {
				connPeer.Write([]byte(self.pid + " +"))
			} else {
				connPeer.Write([]byte(self.pid + " -"))
			}
		}
		connPeer.Close()

	}

}

func (self *Server) electNewCoord(connMaster net.Conn, err error) {
	fmt.Println("RUNNING ELECTION")
	min := 1000000
	for id, _ := range self.up_set {
		int_id, _ := strconv.Atoi(id)
		if int_id < min {
			min = int_id
		}
	}

	fmt.Println("ELECTING NEW COORD")
	if strconv.Itoa(min) == self.pid {
		fmt.Println("I am the coordinator")
		self.is_coord = true
		self.coordID = self.pid
		// just send my pid to master
		// self.coordHandleMaster(connMaster, err)
		coordMessage := "coordinator " + self.pid
		coordLenStr := strconv.Itoa(len(coordMessage))
		connMaster.Write([]byte(coordLenStr + "-" + coordMessage))
		self.terminationProtocol(connMaster)

	} else {
		fmt.Println("Someone else is the coordinator")

		
	}
	
	fmt.Println("NEW COORD ID:" + strconv.Itoa(min))
}


func (self *Server) terminationProtocol(connMaster net.Conn) {
	fmt.Println("RUNNING TERMINATION PROTOCOL")


	participantChannel := make(chan string)

	for _, otherPort := range self.up_set {
		go self.msgParticipant(otherPort, "state_req", participantChannel) // vote-req
	}

	num_uncertain := 0
	num_responses := 0
	num_participants := len(self.up_set)

	message := ""
	//Timeout on 1 second passing


	for start := time.Now(); time.Since(start) < time.Second; {
		if num_responses == num_participants {
			fmt.Println("All votes gathered!")
			if num_uncertain == num_responses {
				message = "abort"
			}
			break
		}
		select {
			case response := <-participantChannel:
				if response == "aborted" {
					message = "abort"				
				}else if response == "committed"{
					message = "commit"
				}else if response == "uncertain"{
					num_uncertain += 1
				}
				num_responses += 1
			}
	}

	fmt.Println(num_responses)
	if message != ""{
		for _, otherPort := range self.up_set {
			go self.msgParticipant(otherPort, message, participantChannel)
		}

	}else {
	
		numUpForAck := len(self.up_set)
		
		for _, otherPort := range self.up_set {
			go self.msgParticipant(otherPort, "precommit\n", participantChannel) // vote-req
		}
		 
		ack_votes := 0
		//Timeout on 1 second passing
		for start := time.Now(); time.Since(start) < time.Second; {
			if ack_votes == numUpForAck {
				fmt.Println("All precommits acknowledged!")
				break
			}
			select {
			//Read from participant Channel
			case response := <-participantChannel:
				if response == "ack\n" {
					break
				} else {
					ack_votes += 1
				}
			}
		}

		//Send commit to participants	
		
		self.write_DTLog("commit")

		for _, otherPort := range self.up_set {
			go self.msgParticipant(otherPort, "commit\n", participantChannel) // vote-req
		}
		message = "commit"
		fmt.Println("Commit sent!")


	}

	retMessage := "ack " + message
	lenStr := strconv.Itoa(len(retMessage))
	retMessage = lenStr + "-" + retMessage
	connMaster.Write([]byte(retMessage))

}

func (self *Server) recovery(file *os.File) {
	fmt.Println("RECOVERING")
}

//Updates UP set on heartbeat replies
func (self *Server) heartbeat(connMaster net.Conn, err error) {

	for {

		tempAlive := make(map[string]string)
		tempCoordID := self.coordID
		for _, otherPort := range self.peers {

			peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
			if err != nil {
				continue
			}

			fmt.Fprintf(peerConn, "ping"+"\n")
			response, _ := bufio.NewReader(peerConn).ReadString('\n')
			response_slice := strings.Split(response, " ")
			pid := response_slice[0]
			coord_bool := response_slice[1]
			tempAlive[pid] = otherPort
			if coord_bool == "+" {
				tempCoordID = pid
			}
		}
		self.up_set = tempAlive
		if tempCoordID != "" {
			if _, ok := self.up_set[tempCoordID]; !ok {
				
				self.electNewCoord(connMaster, err)
			}
		}else{
			fmt.Println("No one in up set except me")
			if len(self.up_set) == 1 {
				self.coordID = self.pid
				self.is_coord = true
			}
		}

		self.coordID = tempCoordID
		time.Sleep(1000 * time.Millisecond)

	}

}

//Message a particpant with given otherPort; records participant's response in Go channel
func (self *Server) msgParticipant(otherPort string, message string, channel chan string) {

	peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Fprintf(peerConn, message+"\n")
	response, _ := bufio.NewReader(peerConn).ReadString('\n')

	fmt.Println(response)
	channel <- response

	peerConn.Close()
}

//Writes new DT log if it doesn't exist; appends line to DT log, otherwise
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

//Reads DT log; writes a new one if it doesn't exist
func (self *Server) read_DTLog() string {
	file_name := self.pid + "_DTLog.txt"
	file_path := filepath.Join("./logs", file_name)
	file, err := os.Open(file_path)
	if err != nil {
		// file doesnt exist yet, create one
		self.write_DTLog("start\n")
		fmt.Println("New log created for " + self.pid + ".")
	}
	self.recovery(file)
	defer file.Close()
	log_content, err := ioutil.ReadAll(file)
	return string(log_content)
}
