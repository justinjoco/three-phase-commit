package main

import (
	"bufio"
	"container/list"
//	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Server struct {
	pid              string
	peers            []string
	masterFacingPort string
	peerFacingPort   string
	upSet            map[string]string //maps a process's pid to its portfacing number
	playlist         map[string]string //dictionary of <song_name, song_URL>
	isCoord          bool
	state            string            //Saves the current state of process: TODO
	songQuery        map[string]string //map containing the song's name and URL for deletion or adding
	commandQ         *list.List        //Saved add or delete command
	crashStage       string            //Initialized to "", could be "after_vote"||"before_vote"||"after_ack"||
	// "vote_req"||"partial_precommit"||"partial_commit"
	sentTo []string //A list of processes that the coordinator has sent command to before crash
	coordID          string // Each process will keep track of the id of the current coord
	crashUpSet       []string
	waitingFor		 string
	recoveryMode     bool // True if the process just recovered and DT log exists
	savedCommand	 string
	requestTs		 int
	foundLPF		 bool

}

const (
	CONNECT_HOST = "localhost"
	CONNECT_TYPE = "tcp"
)

//Start a server
func (self *Server) Run() {

	self.ReadDTLog()
	lMaster, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.masterFacingPort)
	lPeer, error := net.Listen(CONNECT_TYPE, CONNECT_HOST+":"+self.peerFacingPort)

	if error != nil {
		fmt.Println("Error listening!")
	}

	defer lMaster.Close()
	connMaster, err := lMaster.Accept()
	//Update UP set on each heartbeat iteration
	go self.Heartbeat(connMaster, err)

	//Listen on peer facing port
	go self.ReceivePeers(lPeer)

	self.HandleMaster(connMaster, err) //Adding peerFacing port to close if process crashed

}

//Coordinator handles master's commands (add, delete, get, crash operations)
func (self *Server) HandleMaster(connMaster net.Conn, err error) {

	reader := bufio.NewReader(connMaster)
	fmt.Println("IM HANDLING MASTER")
	for {

		message, _ := reader.ReadString('\n')

		message = strings.TrimSuffix(message, "\n")
		messageSlice := strings.Split(message, " ")
		command := messageSlice[0]
		args := messageSlice[1:]

		retMessage := ""
		// fmt.Println(message)
		if self.recoveryMode {
			if len(self.crashUpSet) >= 2{
				self.WaitForLPF()
			}
			self.recoveryMode = false
		}

		//Start 3PC instance
		switch command {
			case "add", "delete":
				self.commandQ.PushBack(message)
				retMessage += "ack "
				self.WriteDTLog(message)
				self.requestTs+=1
				commitAbort := self.CoordHandleParticipants(command, args)
				if commitAbort {
					retMessage += "commit"
				} else {
					retMessage += "abort"
				}
				lenStr := strconv.Itoa(len(retMessage))
				retMessage = lenStr + "-" + retMessage
			//Returns songURL if songName is in playlist
			case "get":
				retMessage += "resp "
				songName := args[0]
				songURL := self.playlist[songName]
				if songURL == "" {
					retMessage += "NONE"
				} else {
					retMessage += songURL
				}

				lenStr := strconv.Itoa(len(retMessage))
				retMessage = lenStr + "-" + retMessage

			case "crash":
				fmt.Println("Crashing immediately")
				os.Exit(1)
			//TODO
			case "crashVoteREQ":
				fmt.Println("Crashing after sending vote req to ... ")
				self.crashStage = "vote-req"
				self.sentTo = args
				fmt.Println(args)
			//TODO
			case "crashPartialPreCommit":
				fmt.Println("Crashing after sending precommit to ... ")
				self.crashStage = "partial-precommit"
				self.sentTo = args
			//TODO
			case "crashPartialCommit":
				fmt.Println("Crashing after sending commit to ...")
				self.crashStage = "partial-commit"
				self.sentTo = args
				//TODO
			case "crashAfterVote":
				fmt.Println("Will crash after voting in next 3PC instance")
				self.crashStage = "after-vote"
			//TODO
			case "crashBeforeVote":
				fmt.Println("Will crash before voting in next 3PC instance")
				self.crashStage = "before-vote"
			//TODO
			case "crashAfterAck":
				fmt.Println("Will crash after sending ACK in next 3PC instance")
				self.crashStage = "after-ack"
			default:
				retMessage += "Invalid command. This is the coordinator use 'add <songName> <songURL>', 'get <songName>', or 'delete <songName>'"

		}
		connMaster.Write([]byte(retMessage))

	}

	connMaster.Close()
}




//Coordinator sends and receives messages to and fro the participants (which includes itself)
func (self *Server) CoordHandleParticipants(command string, args []string) bool {
	//ADD or DELETE request: sending + receiving
	songName := ""
	songURL := ""
	numUpForVote := len(self.upSet)
	retBool := false
	participantChannel := make(chan string)

	// broadcast "start" to all participants, after p receives start, it knows to wait for vote_req
	fmt.Println("Sending VOTE-REQ")
	//Using switch to avoid having a lot of if-else statements
	switch command {
		case "add":
			songName = args[0]
			songURL = args[1]
			message := command + " " + songName + " " + songURL
			if self.crashStage != "vote-req" {
				for _, otherPort := range self.upSet {
					go self.MsgParticipant(otherPort, message, participantChannel) // vote-req
				}
			} else {
				if len(self.sentTo) == 0 {
					fmt.Println("vote req sent, crashing now!")
					os.Exit(1)
				}
				for _, otherID := range self.sentTo {
					fmt.Println("stsarting iteration going through the sentTo list")
					if otherPort, ok := self.upSet[otherID]; ok {
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
			if self.crashStage != "vote-req" {
				for _, otherPort := range self.upSet {
					go self.MsgParticipant(otherPort, message, participantChannel) // vote-req
				}
			} else {
				if len(self.sentTo) == 0 {
					os.Exit(1)
				}
				for _, otherID := range self.sentTo {
					if otherPort, ok := self.upSet[otherID]; ok {
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
	yesVotes := 0
	numVoted := 0
	voteSuccess := false

	//Timeout on 1 second passing
	for start := time.Now(); time.Since(start) < time.Second; {
		if numVoted == numUpForVote {
			fmt.Println("All votes gathered!")
			if yesVotes == numVoted {
				voteSuccess = true
			}
			break
		}
		select {
		case response := <-participantChannel:
			if response == "yes" {
				yesVotes += 1
			}
			numVoted += 1
		}
	}

	//Time has past or got votes from everyone
	// if timeout, send ABORT to everyone

	//Precommit Send + Receiving
	if voteSuccess {

		numUpForAck := len(self.upSet)

		if self.crashStage != "partial-precommit" {
			for _, otherPort := range self.upSet {
				go self.MsgParticipant(otherPort, "precommit\n", participantChannel) // vote-req
			}
		} else {
			if len(self.sentTo) == 0 {
				os.Exit(1)
			}
			for _, otherID := range self.sentTo {
				if otherPort, ok := self.upSet[otherID]; ok {
					peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
					if err != nil {
						fmt.Println(err)
					}
					fmt.Fprintf(peerConn, "precommit\n")
				}
			}
			os.Exit(1)
		}

		ackVotes := 0

		//Timeout on 1 second passing
		for start := time.Now(); time.Since(start) < time.Second; {
			if ackVotes == numUpForAck {
				fmt.Println("All precommits acknowledged!")
				break
			}
			select {
			//Read from participant Channel
			case response := <-participantChannel:
				if response == "ack\n" {
					break
				} else {
					ackVotes += 1
				}
			}
		}

		//Send commit to participants
		retBool = true
		self.WriteDTLog("commit")
		if self.crashStage != "partial-commit" {
			for _, otherPort := range self.upSet {
				go self.MsgParticipant(otherPort, "commit\n", participantChannel) // vote-req
			}
		} else {
			if len(self.sentTo) == 0 {
				os.Exit(1)
			}
			for _, otherID := range self.sentTo {
				if otherPort, ok := self.upSet[otherID]; ok {
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
		self.WriteDTLog("abort")
		for _, otherPort := range self.upSet {
			go self.MsgParticipant(otherPort, "abort\n", participantChannel)
		}
	}

	return retBool
}

//Participant handles coordinator's message depending on message content
func (self *Server) ParticipantHandleCoord(message string, connCoord net.Conn) {
	//Receiving add/delete + sending YES/NO
	messageSlice := strings.Split(message, " ")
	command := messageSlice[0]

	command = strings.Trim(command, "\n")

	//On add or delete, this server records the input song's info and its add/delete operation for future 3PC stages
	//Sends no to coord if songUrl is bigger than self.pid + 5; yes otherwise -> records vote in DT log
	switch command {
		case "add":
			self.state = "aborted"
			songName := messageSlice[1]
			songURL := messageSlice[2]
			if !self.isCoord {
				self.requestTs+=1
				self.WriteDTLog(message)
			}
			self.savedCommand = "add"
			if self.crashStage == "before-vote" {
				os.Exit(1)
			}
			songQuery := map[string]string{
				"songName": songName,
				"songURL":  songURL,
			}
			
			self.songQuery = songQuery
			urlSize := len(songURL)
			pid, _ := strconv.Atoi(self.pid)
			if urlSize > pid+5 {
				connCoord.Write([]byte("no"))
			} else {
				connCoord.Write([]byte("yes")) // sending back to the coordinator, now need to wait for precommit
				self.waitingFor = "precommit"
				self.state = "uncertain"
				if !self.isCoord {
					self.WriteDTLog("yes")
				}
			}
			if self.crashStage == "after-vote" {
				os.Exit(1)
			}
		//Always send yes to coord and records vote in DT log
		case "delete":
			self.state = "aborted"
			if !self.isCoord {
				self.requestTs+=1
				self.WriteDTLog(message)
			} // record message before crashing
			self.savedCommand = "delete"
			if self.crashStage == "before-vote" {
				os.Exit(1)
			}
			songName := messageSlice[1]		
			songQuery := map[string]string{
				"songName": songName,
				"songURL":  "",
			}
			self.songQuery = songQuery

			connCoord.Write([]byte("yes"))
			self.waitingFor = "precommit"
			self.state = "uncertain"
			if !self.isCoord {
				self.WriteDTLog("yes")
			}
			if self.crashStage == "after-vote" {
				os.Exit(1)
			}

		//Send back ack on precommit receipt
		case "precommit":
			if self.state == "uncertain" {
				connCoord.Write([]byte("ack"))
				self.waitingFor = "commit"
				self.state = "committable"
				if self.crashStage == "after-ack" {
					os.Exit(1)
				}
			}
		//Adds song to playlist or deletes song in playlist on commit receipt
		case "commit":
			fmt.Println("COMMITTING")
			if self.state == "committable" {
				if self.savedCommand == "add" {
					self.playlist[self.songQuery["songName"]] = self.songQuery["songURL"]
				} else {
					delete(self.playlist, self.songQuery["songName"])
				}
				self.waitingFor = ""
				self.state = "committed"
				if !self.isCoord {
					self.WriteDTLog("commit")
				}
			}
		//Aborts 3PC on abort receipt
		case "abort":
			fmt.Println("ABORTING")
			if self.state != "aborted" {
				if !self.isCoord {
					self.WriteDTLog("abort")
				}
				self.state = "aborted"
				self.waitingFor = ""
			}
		case "state-req":
			requestTs, _ := strconv.Atoi(messageSlice[1])
			if requestTs > self.requestTs {
				self.state = "aborted"
				self.requestTs = requestTs
			}
			connCoord.Write([]byte(self.state)) // TODO: update state for each process
		case "ur-elected":
			self.isCoord = true
			self.coordID = self.pid

		//No valid message given
		default:
			connCoord.Write([]byte("Invalid message"))

	}
}

//Listens on peer facing port; handles heartbeat-oriented messages and those that aren't
func (self *Server) ReceivePeers(lPeer net.Listener) {
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
		if message == "playlist" {
			playlistMsg := ""
			if self.requestTs == 0 {
				playlistMsg = "0"
			}else{
				playlistMsg = strconv.Itoa(self.requestTs) + " "
				songList := make([]string, 0)
				for songName, songURL := range self.playlist{
					songStr := songName + ":" + songURL
					songList = append(songList, songStr)
				}
				playlistMsg += strings.Join(songList, " ")
			}
			connPeer.Write([]byte(playlistMsg))

		} else if message == "crashed-up-set" {
		//	fmt.Println(self.crashUpSet)
			crashUpSetMsg := strings.Join(self.crashUpSet, " ")
		//	fmt.Println(crashUpSetMsg)
			connPeer.Write([]byte(crashUpSetMsg))

		}else if message == "ping"{
			if self.isCoord {
				connPeer.Write([]byte(self.pid + " +"))
			} else {
				connPeer.Write([]byte(self.pid + " -"))
			}
		}else{
			self.ParticipantHandleCoord(message, connPeer)
		}
		connPeer.Close()

	}

}

func (self *Server) ElectNewCoord(connMaster net.Conn, err error) {
	fmt.Println("RUNNING ELECTION")
	min := 1000000
	for id, _ := range self.upSet {
		intId, _ := strconv.Atoi(id)
		if intId < min {
			min = intId
		}
	}

	fmt.Println("ELECTING NEW COORD")
	if strconv.Itoa(min) == self.pid {
		fmt.Println("I am the coordinator")
		self.isCoord = true
		self.coordID = self.pid
		// just send my pid to master
		// self.coordHandleMaster(connMaster, err)
		coordMessage := "coordinator " + self.pid
		coordLenStr := strconv.Itoa(len(coordMessage))
		connMaster.Write([]byte(coordLenStr + "-" + coordMessage))
		self.TerminationProtocol(connMaster)

	} else {
		fmt.Println("Someone else is the coordinator")

	}

	fmt.Println("NEW COORD ID:" + strconv.Itoa(min))
}

func (self *Server) TerminationProtocol(connMaster net.Conn) {
	fmt.Println("RUNNING TERMINATION PROTOCOL")

	participantChannel := make(chan string)

	for _, otherPort := range self.upSet {
		go self.MsgParticipant(otherPort, "state-req " +  strconv.Itoa(self.requestTs), participantChannel) // vote-req
	}

	numUncertain := 0
	numResponses := 0
	numParticipants := len(self.upSet)

	message := ""
	//Timeout on 1 second passing

	for start := time.Now(); time.Since(start) < time.Second; {
		if numResponses == numParticipants {
			fmt.Println("All votes gathered!")
			if numUncertain == numResponses {
				message = "abort"
			}
			break
		}
		select {
		case response := <-participantChannel:
			fmt.Println("response")
			if response == "aborted" {
				message = "abort"
			} else if response == "committed" {
				message = "commit"
			} else if response == "uncertain" {
				numUncertain += 1
			}
			numResponses += 1
		}
	}

	fmt.Println(numResponses)
	if message != "" {
		for _, otherPort := range self.upSet {
			go self.MsgParticipant(otherPort, message, participantChannel)
		}

	} else {

		numUpForAck := len(self.upSet)

		for _, otherPort := range self.upSet {
			go self.MsgParticipant(otherPort, "precommit\n", participantChannel) // vote-req
		}

		ackVotes := 0
		//Timeout on 1 second passing
		for start := time.Now(); time.Since(start) < time.Second; {
			if ackVotes == numUpForAck {
				fmt.Println("All precommits acknowledged!")
				break
			}
			select {
			//Read from participant Channel
			case response := <-participantChannel:
				if response == "ack\n" {
					break
				} else {
					ackVotes += 1
				}
			}
		}

		//Send commit to participants

		self.WriteDTLog("commit")

		for _, otherPort := range self.upSet {
			go self.MsgParticipant(otherPort, "commit\n", participantChannel) // vote-req
		}
		message = "commit"
		fmt.Println("Commit sent!")

	}

	retMessage := "ack " + message
	lenStr := strconv.Itoa(len(retMessage))
	retMessage = lenStr + "-" + retMessage
	connMaster.Write([]byte(retMessage))

}

func (self *Server) Recovery(file *os.File) {
	fmt.Println(self.pid + " RECOVERING")

	scanner := bufio.NewScanner(file)
	latestCommand := ""
	songQuery := make(map[string]string)
	for scanner.Scan() {
		line := scanner.Text()
		lineSlice := strings.Split(line, " ")
		if lineSlice[0] == "up" {
			self.crashUpSet = lineSlice[1:]
		} else if lineSlice[0] == "add" {
			self.requestTs+=1
			latestCommand = "add"
			songQuery["songName"] = lineSlice[1]
			songQuery["songURL"] = lineSlice[2]
		} else if lineSlice[0] == "delete" {
			self.requestTs+=1
			latestCommand = "delete"
			songQuery["songName"] = lineSlice[1]
		} else if lineSlice[0] == "commit" {
			if latestCommand == "add" {
				self.playlist[songQuery["songName"]] = songQuery["songURL"]
			} else {
				delete(self.playlist, songQuery["songName"])
			}
		} else if lineSlice[0] == "abort" {
			songQuery["songName"] = ""
			songQuery["songURL"] = ""
		}
	}

	for _, otherPort := range self.peers {
			peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
			if err != nil {
				continue
			}

			fmt.Fprintf(peerConn, "playlist"+"\n")
			response, _ := bufio.NewReader(peerConn).ReadString('\n')
			responseSlice := strings.Split(response, " ")
			fmt.Println(responseSlice[1:])
			requestTs, _ := strconv.Atoi(responseSlice[0])
			if requestTs >= self.requestTs && len(responseSlice) > 1{
				tempPlaylist := make(map[string]string)
				for _, song := range responseSlice[1:] {
					songSlice := strings.Split(song, ":")
					tempPlaylist[songSlice[0]] = songSlice[1]
				}
				self.playlist = tempPlaylist
			}		
	}

}


func (self *Server) WaitForLPF(){

	for {
		if self.foundLPF{
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}

}

//Updates UP set on heartbeat replies
func (self *Server) Heartbeat(connMaster net.Conn, err error) {

	for {

		tempAlive := make(map[string]string)
		tempCoordID := self.coordID
		for _, otherPort := range self.peers {

			peerConn, err := net.Dial("tcp", "127.0.0.1:"+otherPort)
			if err != nil {
				continue
			}

			fmt.Fprintf(peerConn, "ping"+"\n")

			reader := bufio.NewReader(peerConn)
			response, _ := reader.ReadString('\n')
			responseSlice := strings.Split(response, " ")
			pid := responseSlice[0]
			coordBool := responseSlice[1]
			tempAlive[pid] = otherPort
			if coordBool == "+"{
				tempCoordID = pid
			}
			peerConn.Close()
			//fmt.Println(pid)
			

			if self.recoveryMode && len(self.crashUpSet) > 1 {
				
				peerConn, err = net.Dial("tcp", "127.0.0.1:"+otherPort)
				if err != nil {
					continue
				}

				fmt.Fprintf(peerConn, "crashed-up-set"+"\n")

				reader = bufio.NewReader(peerConn)
				response, _ = reader.ReadString('\n')
				responseSlice = strings.Split(response, " ")
				if len(responseSlice) == 1{
					self.foundLPF = true
				}
				peerConn.Close() 

			}


		}
		if !reflect.DeepEqual(tempAlive, self.upSet) {
			pids := make([]string, 0)
			for pid, _ := range tempAlive {
				pids = append(pids, pid)
			}
			self.WriteDTLog("up " + strings.Join(pids, " "))
			self.upSet = tempAlive
		}

		if tempCoordID != "" {
			if _, ok := self.upSet[tempCoordID]; !ok {

				self.ElectNewCoord(connMaster, err)
			}
		} else {
			fmt.Println("No one in up set except me")

			self.coordID = self.pid
			self.isCoord = true
			coordMessage := "coordinator " + self.pid
			coordLenStr := strconv.Itoa(len(coordMessage))
			connMaster.Write([]byte(coordLenStr + "-" + coordMessage))

		}
		if self.coordID != tempCoordID {
			self.coordID = tempCoordID
		}

		
		time.Sleep(1000 * time.Millisecond)

	}

}

//Message a particpant with given otherPort; records participant's response in Go channel
func (self *Server) MsgParticipant(otherPort string, message string, channel chan string) {

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
func (self *Server) WriteDTLog(line string) {
	/*
		All lines in log will be lower case. The first line is always "start"
	*/
	path := "./logs"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700) //creates the directory if not exist
	}
	fileName := "DTLog" + self.pid + ".txt"
	filePath := filepath.Join(path, fileName)
	f, _ := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	f.Write([]byte(line + "\n"))
	f.Close()
}

//Reads DT log; writes a new one if it doesn't exist
func (self *Server) ReadDTLog() {
	fileName := "DTLog" + self.pid + ".txt"
	filePath := filepath.Join("./logs", fileName)
	file, err := os.Open(filePath)
	if err != nil {
		// file doesnt exist yet, create one
		self.WriteDTLog("start\n")
		fmt.Println("New log created for " + self.pid + ".")
	} else {
		self.foundLPF = false
		self.recoveryMode = true
		self.Recovery(file)
	}

	defer file.Close()

}
