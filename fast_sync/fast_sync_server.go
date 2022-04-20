package fast_sync

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/ethereum/go-ethereum/log"
	"net"
	"strings"
	"time"
)

//func InitServer(engine *abft.Lachesis, dagIndex *vecmt.Index, gdb *gossip.Store, cdb *abft.Store, genesisStore *genesisstore.Store, blockProc gossip.BlockProc) {
func InitServer(gdb *gossip.Store) {
	log.Warn("fastsync-server")

	//go snapshotService(engine)

	//go testIterateTroughDb(gdb)

	//go testFunction()
	//go testNetxxxVal()
	//go testEncodeIntToByteAndBack()

	server, error := net.Listen("tcp", "localhost:"+serverSocketPort)
	if error != nil {
		log.Error("There was an error starting the server" + error.Error())
		return
	}

	go serverMessageHandling(server, gdb)
}

//func testEncodeIntToByteAndBack() {
//	test := uint32(5)
//	ofset := 0
//	arr := convertIntToByteArr(test)
//
//	fmt.Println("arr len is: ", len(arr))
//
//	res := convertByteArrToInt(&arr, ofset)
//
//	fmt.Println("result is: ", res)
//}

//func testNetxxxVal() {
//	buffer := []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
//	current := 2
//	key, value, err := readNextKeyVal(&buffer, &current)
//	if err != nil {
//		fmt.Println(err.Error())
//	} else {
//		fmt.Println("key: ", key, " value: ", value)
//
//	}
//}

//func testFunction() {
//	buffer := make([]byte, BUFFER_SIZE)
//
//	key := []byte{1, 2, 3, 4}
//	value := []byte{5, 6, 7, 8}
//	currentBufferByte := 2
//	insertNextVal(&buffer, &key, &value, &currentBufferByte)
//
//	for i := 0; i < 14; i++ {
//		fmt.Println("Buffer at [", i, "] : ", buffer[i])
//	}
//}

func testIterateTroughDb(gdb *gossip.Store) {
	snap, err := gdb.GetMainDb().GetSnapshot()
	if err == nil {
		fmt.Println("Error unable to get snapshot")
	}

	numberOfItems := 0

	totalBytes := 0

	t1 := time.Now()

	iterator := snap.NewIterator(nil, nil)
	defer iterator.Release()
	for iterator.Next() {
		value := iterator.Value()
		if value != nil {
			key := string(iterator.Key()) + string(value)
			numberOfItems += 1
			totalBytes += len([]byte(key))

			if len(key) > 300000 || len(value) > 300000 {
				fmt.Println("len key: ", len(key), " len value: ", len(value))
			}

			if (numberOfItems % 1000000) == 0 {
				fmt.Printf("numb_Items: %d\n", numberOfItems)
			}
			if key == "____!!!!" {
				//fmt.Println("key: " + key)
			}
		}
	}
	fmt.Printf("numb_Items_total: %d\n", numberOfItems)
	t2 := time.Now()
	fmt.Printf("totalBytes: %d\n", totalBytes)

	fmt.Printf("It took : %f seconds", t2.Sub(t1).Seconds())
}

func MarkEndOfEpoch(newEpoch idx.Epoch, store *gossip.Store) {
	fetchData(newEpoch, store)
}

func fetchData(newEpoch idx.Epoch, store *gossip.Store) {

}

func snapshotService(engine *abft.Lachesis) {
	//cfg := makeAllConfigs(ctx)
	//
	//rawProducer := integration.DBProducer(path.Join(cfg.Node.DataDir, "chaindata"), cfg.Cachescale)
	//gdb, err := makeRawGossipStore(rawProducer, cfg)

	//g.getSnapshot()
}

func serverMessageHandling(server net.Listener, gdb *gossip.Store) {

	for {
		connection, error := server.Accept()
		if error != nil {
			fmt.Println("There was am error with the connection" + error.Error())
			return
		}
		fmt.Println("connected")

		go connectionHandler(connection, gdb)
	}
}

func connectionHandler(connection net.Conn, gdb *gossip.Store) {
	var buffer = make([]byte, BUFFER_SIZE)
	_, err := connection.Read(buffer)
	if err != nil {
		fmt.Println("There is an err reading from connection", err.Error())
		return
	}
	fmt.Println("command recieved: " + string(buffer))

	//loop until disconntect

	cleanedBuffer := bytes.Trim(buffer, "\x00")
	cleanedInputCommandString := strings.TrimSpace(string(cleanedBuffer))
	//arrayOfCommands := strings.Split(cleanedInputCommandString, " ")

	if cleanedInputCommandString == "get" {
		sendFileToClient(connection, gdb)
	} else {
		_, err = connection.Write([]byte("bad command"))
	}

	err = connection.Close()
	if err != nil {
		fmt.Println("err while closing connection")
	}
}

func sendFileToClient(connection net.Conn, gdb *gossip.Store) {
	//var currentByte int64 = 0
	fmt.Println("sending to client")
	var buffer []byte

	snap, err := gdb.GetMainDb().GetSnapshot()
	if err != nil {
		fmt.Println("Error unable to get snapshot")
		sendError(connection)
		return
	}

	//file, err := os.Create(strings.TrimSpace("server_sent.txt"))

	//ppth, err := filepath.Abs(file.Name())

	//fmt.Println("Created file at: " + ppth)

	iterator := snap.NewIterator(nil, nil)
	defer iterator.Release()

	var i = 0

	for iterator.Next() {
		//if i > 100000 {
		//	break
		//}
		i += 1
		if i%100000 == 0 {
			fmt.Println("Process: ", i)
		}

		value := iterator.Value()
		if value != nil {
			key := iterator.Key()

			buffer = make([]byte, BUFFER_SIZE)
			currentBufferByte := COMMUNICATION_HEADER_SIZE

			//fmt.Println("len key: ", len(key), " len value: ", len(value))
			if len(key) > 4000 || len(value) > 4000 {
				fmt.Println("len key: ", len(key), " len value: ", len(value))
			}
			err := insertNextVal(&buffer, &key, &value, &currentBufferByte)
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			//fmt.Println("totalLengthWas: ", currentBufferByte)
			err = sendMessage(&connection, &buffer, &currentBufferByte)
			if err != nil {
				fmt.Println("sending pipe broken")
				break
			}
			//_, err = file.WriteAt(key, currentByte)
			//if err == io.EOF {
			//	break
			//}
			//currentByte += int64(len(key))
			//_, err = file.WriteAt(value, currentByte)
			//if err == io.EOF {
			//	break
			//}
			//currentByte += int64(len(value))

			displayValues(key, value, "serrver")

		} else {
			log.Error("fastsync-server_value_not_found")
		}
	}
	//	TODO ukoncujici zprava s hashem pro kontrolu
}

func insertNextVal(buffer *[]byte, key *[]byte, value *[]byte, currentBufferByte *int) error {
	insertLengthOfItemAndUpdateCurrentBufferByte(buffer, key, currentBufferByte)

	insertLengthOfItemAndUpdateCurrentBufferByte(buffer, value, currentBufferByte)

	for i := 0; i < len(*key); i++ {
		(*buffer)[(*currentBufferByte)+i] = (*key)[i]
	}
	*currentBufferByte = (*currentBufferByte) + len(*key)

	//insertLengthOfItemAndUpdateCurrentBufferByte(buffer, value, currentBufferByte)

	for k := 0; k < len(*value); k++ {
		(*buffer)[(*currentBufferByte)+k] = (*value)[k]
	}
	*currentBufferByte = *currentBufferByte + len(*value)

	return nil
}

func insertLengthOfItemAndUpdateCurrentBufferByte(buffer *[]byte, arr *[]byte, currentBufferByte *int) {
	keyLength := uint32(len(*arr))
	itemLengthArr := convertIntToByteArr(keyLength)
	(*buffer)[(*currentBufferByte)] = itemLengthArr[0]
	*currentBufferByte = (*currentBufferByte) + 1
	(*buffer)[(*currentBufferByte)] = itemLengthArr[1]
	*currentBufferByte = (*currentBufferByte) + 1
	(*buffer)[(*currentBufferByte)] = itemLengthArr[2]
	*currentBufferByte = (*currentBufferByte) + 1
	(*buffer)[(*currentBufferByte)] = itemLengthArr[3]
	*currentBufferByte = (*currentBufferByte) + 1
}

func sendError(connection net.Conn) {
	ch := CommunicationHeader{3, [4]byte{}}
	resp := make([]byte, 1)
	resp[0] = ch.status
	connection.Write(resp)
}

func sendMessage(connection *net.Conn, buffer *[]byte, dtLen *int) error {
	ch := CommunicationHeader{1, convertIntToByteArr(uint32(*dtLen))}
	(*buffer)[0] = ch.status
	(*buffer)[1] = ch.dataLength[0]
	(*buffer)[2] = ch.dataLength[1]
	(*buffer)[3] = ch.dataLength[2]
	(*buffer)[4] = ch.dataLength[3]
	_, err := (*connection).Write(*buffer)
	if err != nil {
		return err
	}
	return nil
}

func convertIntToByteArr(toConv uint32) (out [4]byte) {
	binary.LittleEndian.PutUint32(out[:], toConv)
	return out
}

func convertByteArrToInt(buffer *[]byte, startingByte uint32) uint32 {
	arr := make([]byte, 4)
	for i := 0; i < 4; i++ {
		arr[i] = (*buffer)[startingByte+uint32(i)]
	}
	return binary.LittleEndian.Uint32(arr)
}
