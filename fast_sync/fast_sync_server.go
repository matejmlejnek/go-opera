package fast_sync

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
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

	//go testRLP()

	server, error := net.Listen("tcp", "localhost:"+serverSocketPort)
	if error != nil {
		log.Error("There was an error starting the server" + error.Error())
		return
	}

	go serverMessageHandling(server, gdb)
}

func testRLP() {
	buf := new(bytes.Buffer)
	//buf := bytes.Buffer{make([]byte, 100)}

	key := []byte("test_val")
	value := []byte("t_val")
	itm := Item{key, value}

	//buf.Write([]byte("test"))
	//bb, err := rlp.EncodeToBytes(itm)
	//if err != nil {
	//	fmt.Println("Err: ", err.Error())
	//}
	//rlp.Encode(buf, bb)
	err := rlp.Encode(buf, itm)
	if err != nil {
		fmt.Println("err:", err.Error())
		return
	}

	//(81c0)
	fmt.Printf("Test: \n\n(%x)\n\n\n", buf)

	//(c0)

	//reader := bufio.NewReader(buf)
	//readBuffer :=
	//reader.Read()
	//fmt.Printf("(%x)\n", readBuffer[0:n])
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

	writer := bufio.NewWriter(connection)

	snap, err := gdb.GetMainDb().GetSnapshot()
	if err != nil {
		fmt.Println("Error unable to get snapshot")
		err = sendError(writer)
		if err != nil {
			writer.Write([]byte("Error"))
		}
		return
	}

	iterator := snap.NewIterator(nil, nil)
	defer iterator.Release()

	var i = 0

	var currentLength = 0
	var itemsToSend []Item
	for iterator.Next() {
		if i > 1000 {
			break
		}

		i += 1
		if i%100000 == 0 {
			fmt.Println("Process: ", i)
		}
		//connection.Write([]byte("testaaaaa"))
		value := iterator.Value()
		if value != nil {
			key := iterator.Key()

			var newItem = Item{key, value}
			itemsToSend = append(itemsToSend, newItem)
			currentLength += len(value)
			currentLength += len(key)

			if currentLength > RECOMMENDED_MIN_BUNDLE_SIZE {
				fmt.Println("Sending: ", len(itemsToSend), " items.")
				err := sendBundle(writer, &itemsToSend, &currentLength)
				if err != nil {
					fmt.Println("sending pipe broken: ", err.Error())
					break
				}
				writer.Flush()
			}
		} else {
			fmt.Println("fastsync-server_value_not_found")
		}
	}

	if len(itemsToSend) > 0 {
		err := sendBundle(writer, &itemsToSend, &currentLength)
		if err != nil {
			fmt.Println("sending pipe broken end: ", err.Error())
		}
		writer.Flush()
	}

	fmt.Println("sending finished")
	//	TODO ukoncujici zprava s hashem pro kontrolu
}

func sendBundle(writer *bufio.Writer,
	itemsToSend *[]Item, currentLength *int) error {
	hash := getHashOfKeyValuesInBundle(itemsToSend)

	fmt.Printf("h_new: %x\n", hash)
	fmt.Printf("items len: %d\n", len(*itemsToSend))

	var bundle = BundleOfItems{false, hash, getSignature(&hash), *itemsToSend}

	defer func() {
		*itemsToSend = []Item{}
		*currentLength = 0
	}()
	return rlp.Encode(writer, bundle)
}

func sendError(writer *bufio.Writer) error {
	fmt.Println("sending error")
	var bundle = BundleOfItems{true, []byte{}, []byte{}, []Item{}}
	return rlp.Encode(writer, bundle)
}
