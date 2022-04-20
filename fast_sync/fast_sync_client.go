package fast_sync

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/ethereum/go-ethereum/log"
	"net"
)

//7700000 na 8192
//const BUFFER_SIZE = 8192
const BUFFER_SIZE = 1048576
const serverSocketPort = "7002"

//RLP

// size of bytes in CommunicationHeader
const COMMUNICATION_HEADER_SIZE = 5

type CommunicationHeader struct {
	//0 is trimmed so 1 is ok, 2 is for continue from previous message, 3+ is error
	status     byte
	dataLength [4]byte
}

func InitClient(hostAdress string, gdb *gossip.Store) {
	log.Warn("fastsync-client")
	mainDB := gdb.GetMainDb()
	attemptConnect(hostAdress, &mainDB)
}

func attemptConnect(adress string, mainDB *kvdb.Store) {
	//get port and ip address to dial
	connection, err := net.Dial("tcp", adress+":"+serverSocketPort)
	if err != nil {
		log.Error(err.Error())
	}

	getDataFromServer(connection, mainDB)
}

func getDataFromServer(connection net.Conn, mainDB *kvdb.Store) {
	//var currentByte int64 = 0

	readBuffer := make([]byte, BUFFER_SIZE)

	helloMessage := []byte("get")
	fmt.Println("sending message: ", string(helloMessage))
	_, err := connection.Write(helloMessage)
	if err != nil {
		return
	}

	//TODO create db

	//file, err := os.Create(strings.TrimSpace("../client_received.txt"))
	//if err != nil {
	//	return
	//}

	var currentBufferReadByte = uint32(0)

	for {
		//TODO timeout
		_, err := connection.Read(readBuffer)
		if err != nil {
			fmt.Println("from read")
			fmt.Println(err.Error())
			break
		}

		cleanedFileBuffer := bytes.Trim(readBuffer, "\x00")

		if len(cleanedFileBuffer) == 0 {
			cleanedFileBuffer = readBuffer
		}

		totalLength, err := readHeader(&cleanedFileBuffer)
		if err != nil {
			fmt.Println(err.Error())
			break
		} else {
			currentBufferReadByte = uint32(COMMUNICATION_HEADER_SIZE)
		}

		if totalLength == 0 {
			//finished?
			fmt.Println("This is finish")
			break
		}

		wasError := false
		for (currentBufferReadByte + 1) < totalLength {
			//fmt.Println("receiving totalLen: ", totalLength)
			//fmt.Println("currentBufferReadByte: ", currentBufferReadByte)
			key, value, err := readNextKeyVal(&cleanedFileBuffer, &currentBufferReadByte)
			if currentBufferReadByte > totalLength {
				fmt.Println("Error overpassed total length")
				wasError = true
				break
			}
			if err != nil {
				fmt.Println(err.Error())
				wasError = true
				break
			}

			err = (*mainDB).Put(key, value)
			if err != nil {
				fmt.Println("db: ", err)
				break
			}
			//displayValues(key, value, "client")

			//_, err = file.WriteAt(key, currentByte)
			//if err == io.EOF {
			//	wasError = true
			//	break
			//}
			//currentByte += int64(len(key))
			//_, err = file.WriteAt(value, currentByte)
			//if err == io.EOF {
			//	wasError = true
			//	break
			//}
			//currentByte += int64(len(value))
		}
		if wasError {
			fmt.Println("Error occured: in was error")
			break
		}

	}
}

func displayValues(key []byte, value []byte, s string) {
	//fmt.Print(s, ": ")
	//for i := range key {
	//	fmt.Print(key[i], ", ")
	//}
	//fmt.Println("")
	//
	//fmt.Print(s, ": ")
	//for i := range value {
	//	fmt.Print(value[i], ", ")
	//}
	//fmt.Println("")

	//hash kontrola
	//h1 := sha1.New()
	//h1.Write(key)
	//bs1 := h1.Sum(nil)
	//
	//h2 := sha1.New()
	//h2.Write(value)
	//bs2 := h2.Sum(nil)
	//
	//fmt.Printf("%s : %x %x\n", s, bs1, bs2)
}

func readHeader(received *[]byte) (uint32, error) {
	srvErr := (*received)[0]
	if srvErr == 1 || srvErr == 2 {
		var arr = make([]byte, 4)
		for i := 1; i < len(*received); i++ {
			arr[i-1] = (*received)[i]
		}

		totalLength := convertByteArrToInt(&arr, 0)

		return totalLength, nil
	} else if srvErr == 3 {
		return 0, errors.New("header error at server: " + string(int(srvErr)))
	} else {
		return 0, errors.New("header error started else: " + string(int(srvErr)))
	}

}

func readNextKeyVal(received *[]byte, currentReadByte *uint32) ([]byte, []byte, error) {
	keyToBeRead := convertByteArrToInt(received, *currentReadByte)
	*currentReadByte = *currentReadByte + 4
	valueToBeRead := convertByteArrToInt(received, *currentReadByte)
	*currentReadByte = *currentReadByte + 4
	//fmt.Println("key: ", keyToBeRead, " value: ", valueToBeRead)
	if keyToBeRead == 0 || valueToBeRead == 0 {
		return []byte{}, []byte{}, errors.New("unable to read 0 bytes from server")
	}

	keyRes := make([]byte, keyToBeRead)
	valRes := make([]byte, valueToBeRead)

	copy(keyRes, (*received)[*currentReadByte:(*currentReadByte+keyToBeRead)])
	*currentReadByte = *currentReadByte + keyToBeRead
	copy(valRes, (*received)[*currentReadByte:(*currentReadByte+valueToBeRead)])
	*currentReadByte = *currentReadByte + valueToBeRead

	//fmt.Println("key: ", string(len(keyRes)), " value: ", string(len(valRes)))
	return keyRes, valRes, nil
}
