package direct_sync

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/go-opera/integration"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const serverSocketPort = "7002"
const RECOMMENDED_MIN_BUNDLE_SIZE = 10000000
const LOGGING_INTERVAL = 10 * time.Second
const PEER_LIMIT = 1

var performanceSocketWrite time.Duration

var performanceFlushIdCompare time.Duration

var PeerCounter = SafePeerCounter{v: 0}

var EstimateGossipSize func() uint64 = nil

var p2pPrivateKey *ecdsa.PrivateKey

type SafePeerCounter struct {
	mu sync.Mutex
	v  int
}

func (c *SafePeerCounter) AllowNewConnection() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.v < PEER_LIMIT {
		c.v++
		log.Info(fmt.Sprintf("Added new connection - total: %d", c.v))
		return true
	} else {
		log.Info(fmt.Sprintf("Denied connection - total: %d", c.v))
		return false
	}
}

func (c *SafePeerCounter) ReleasedConnection() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.v--
	log.Info(fmt.Sprintf("Released connection - total: %d", c.v))
}

func InitServer(gdb *gossip.Store, gossipPath string, key *ecdsa.PrivateKey) {
	EstimateGossipSize = func() uint64 {
		var size uint64
		err := filepath.Walk(gossipPath, func(_ string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				size += uint64(info.Size())
			}
			return err
		})
		if err != nil {
			return 0
		}
		return size
	}

	p2pPrivateKey = key

	//go snapshotService()

	//TestIterateTroughDb(gdb)

	//go testFunction()
	//go testNetxxxVal()
	//go testEncodeIntToByteAndBack()

	//go testRLP()

	server, error := net.Listen("tcp", "0.0.0.0:"+serverSocketPort)
	if error != nil {
		log.Error("There was an error starting the server" + error.Error())
		return
	}

	go serverMessageHandling(server, gdb)
}

func TestIterateTroughDb(gdb *gossip.Store) {
	snap, err := gdb.GetMainDb().GetSnapshot()
	if err != nil {
		log.Crit("Error unable to get snapshot", "error", err)
	}

	numberOfItems := 0

	totalBytesKey := 0
	totalBytesValue := 0

	t1 := time.Now()

	mp := make(map[string]int)

	iterator := snap.NewIterator(nil, nil)
	defer iterator.Release()
	for iterator.Next() {
		key := iterator.Key()
		value := iterator.Value()
		numberOfItems += 1
		totalBytesKey += len(key)
		totalBytesValue += len(value)

		keyStr := hex.EncodeToString(key)
		if len(keyStr) > 4 {
			keyStr = keyStr[0:3]
		}
		mp[keyStr] = mp[keyStr] + 1

		if (numberOfItems % 10000000) == 0 {
			fmt.Printf("numb_Items: %d\n", numberOfItems)
		}
	}

	log.Info("Total number of prefix occurances:", "total count", len(mp))

	for prefix, count := range mp {
		fmt.Printf("prefix: %s count: %d\n", prefix, count)
	}

	fmt.Printf("numb_Items_total: %d\n", numberOfItems)
	t2 := time.Now()
	fmt.Printf("totalBytesKey: %d\n", totalBytesKey)
	fmt.Printf("totalBytesValue: %d\n", totalBytesValue)

	log.Info("IterateTroughDbFinished", "duration", t2.Sub(t1))
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
	defer func() {
		err := connection.Close()
		if err != nil {
			log.Warn("Sync server closing connection: ", err)
		}
	}()

	writer := bufio.NewWriter(connection)
	reader := bufio.NewReader(connection)
	stream := rlp.NewStream(reader, 0)

	challenge, err := readChallenge(stream)
	if err != nil {
		sendOverheadError(writer, err.Error())
		log.Warn("Error while receiving challenge: ", "error", err)
		return
	}
	err = sendChallengeAck(writer, challenge)
	if err != nil {
		log.Warn("Error while sending ChallengeAck: ", "error", err)
		return
	}

	//reading command
	var receivedCommand []byte
	receivedCommand, err = readCommand(stream)
	if err != nil {
		sendOverheadError(writer, err.Error())
		log.Warn("Error while receiving command: ", err)
		return
	}

	if string(receivedCommand) == "get" {
		if PeerCounter.AllowNewConnection() {
			defer PeerCounter.ReleasedConnection()
			sendFileToClient(writer, gdb)
		} else {
			sendOverheadError(writer, "Server is at maximum peer capacity")
		}
	} else {
		sendOverheadError(writer, "Bad command: "+string(receivedCommand))
	}
}

func sendChallengeAck(writer *bufio.Writer, challenge []byte) error {
	signature, err := signHash(&challenge)
	if err != nil {
		return err
	}

	return sendOverheadMessage(writer, signature)
}

func readChallenge(stream *rlp.Stream) ([]byte, error) {
	return readOverheadMessage(stream)
}

func readCommand(stream *rlp.Stream) ([]byte, error) {
	return readOverheadMessage(stream)
}

func sendFileToClient(writer *bufio.Writer, gdb *gossip.Store) {
	fmt.Println("sending to client")

	snap := gossip.SnapshotOfLastEpoch
	if snap == nil {
		err := "Server doesn't have snapshot for epoch initialized"
		log.Warn(err)
		sendOverheadError(writer, err)
		return
	}
	err := sendEstimatedSizeMessage(writer)
	if err != nil {
		log.Warn(err.Error())
		return
	}

	startTime = time.Now()

	ticker := time.NewTicker(LOGGING_INTERVAL)

	iterator := snap.NewIterator(nil, nil)
	defer iterator.Release()

	var i = 0

	var sentItems = 0

	var currentLength = 0
	var itemsToSend []Item
	for iterator.Next() {
		var timeSt = time.Now()
		if bytes.Compare(iterator.Key(), integration.FlushIDKey) == 0 {
			log.Info("Skipping flush key")
			continue
		}
		performanceFlushIdCompare += time.Now().Sub(timeSt)

		i += 1
		select {
		case <-ticker.C:
			{
				printServerPerformance()
				fmt.Println("Process: ", i)
			}
		default:
		}

		key := iterator.Key()
		value := iterator.Value()
		if value == nil {
			log.Warn("Key without value")
			return
		}

		var newItem = Item{key, value}
		itemsToSend = append(itemsToSend, newItem)
		currentLength += len(value)
		currentLength += len(key)

		if currentLength > RECOMMENDED_MIN_BUNDLE_SIZE {
			sentItems = sentItems + len(itemsToSend)
			err := sendBundle(writer, &itemsToSend, &currentLength)
			if err != nil {
				fmt.Println(fmt.Sprintf("sending pipe broken: %v", err.Error()))
				break
			}
		}
	}

	if len(itemsToSend) > 0 {
		sentItems = sentItems + len(itemsToSend)
		err = sendBundle(writer, &itemsToSend, &currentLength)
		if err != nil {
			log.Info(fmt.Sprintf("sending pipe broken end: %v", err.Error()))
		}
	}

	err = sendBundleFinished(writer)
	if err != nil {
		log.Info(fmt.Sprintf("sending pipe broken end: %v", err.Error()))
	}

	log.Info(fmt.Sprintf("Sent: %d items.", sentItems))
}

func sendEstimatedSizeMessage(writer *bufio.Writer) error {
	var estimatedSize uint64 = 0
	if EstimateGossipSize != nil {
		estimatedSize = EstimateGossipSize()
	}
	log.Info("estimated size: " + strconv.FormatUint(estimatedSize, 10))
	return sendOverheadMessage(writer, []byte(strconv.FormatUint(estimatedSize, 10)))
}

func sendBundle(writer *bufio.Writer,
	itemsToSend *[]Item, currentLength *int) error {

	var timeSt = time.Now()
	hash := getHashOfKeyValuesInBundle(itemsToSend)
	var timeSt2 = time.Now()
	performanceHash += timeSt2.Sub(timeSt)
	signature, err := signHash(&hash)
	if err != nil {
		return err
	}
	var timeSt3 = time.Now()
	performanceSignatures += timeSt3.Sub(timeSt2)

	var bundle = BundleOfItems{false, hash, signature, *itemsToSend}

	err = rlp.Encode(writer, bundle)

	_ = writer.Flush()
	*itemsToSend = []Item{}
	*currentLength = 0

	performanceSocketWrite += time.Now().Sub(timeSt3)
	return err
}

func signHash(message *[]byte) ([]byte, error) {
	signature, err := crypto.Sign(*message, p2pPrivateKey)
	if err != nil {
		return nil, err
	}
	return signature, nil
}

func sendBundleFinished(writer *bufio.Writer) error {
	var bundle = BundleOfItems{true, []byte{}, []byte{}, []Item{}}

	defer writer.Flush()
	return rlp.Encode(writer, bundle)
}

func sendOverheadError(writer *bufio.Writer, error string) {
	challenge := OverheadMessage{ErrorOccured: true, Payload: []byte(error)}
	rlp.Encode(writer, challenge)
	writer.Flush()
}

func printServerPerformance() {
	var totalTime = time.Now().Sub(startTime)
	var rest = totalTime - performanceHash - performanceSignatures - performanceSocketWrite - performanceFlushIdCompare
	log.Info("performance: ", "totalTime", totalTime, "performanceHash", performanceHash, "performanceSignatures", performanceSignatures, "performanceSocketWrite", performanceSocketWrite, "performanceFlushIdCompare", performanceFlushIdCompare, "restTime", rest)
}
