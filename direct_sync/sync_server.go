package direct_sync

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/go-opera/integration"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/status-im/keycard-go/hexutils"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const serverSocketPort = "7002"
const RECOMMENDED_MIN_BUNDLE_SIZE = 10000000
const PROGRESS_LOGGING_FREQUENCY = 30 * time.Second
const PEER_LIMIT = 1

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
	log.Info("public key is", "key", hexutils.BytesToHex(elliptic.Marshal(p2pPrivateKey.PublicKey.Curve, p2pPrivateKey.PublicKey.X, p2pPrivateKey.PublicKey.Y)))

	server, error := net.Listen("tcp", "0.0.0.0:"+serverSocketPort)
	if error != nil {
		log.Error("There was an error starting the server" + error.Error())
		return
	}

	go serverMessageHandling(server, gdb)
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
		log.Warn("Error while receiving challenge: ", err)
		return
	}
	err = sendChallengeAck(writer, challenge)
	if err != nil {
		log.Warn("Error while sending ChallengeAck: ", err)
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
			sendFileToClient(writer)
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

func sendFileToClient(writer *bufio.Writer) {
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

	iterator := snap.NewIterator(nil, nil)
	defer iterator.Release()

	var i = 0

	var sentItems = 0

	ticker := time.NewTicker(PROGRESS_LOGGING_FREQUENCY)

	var bundlesInSendingQueueCounter uint32 = 0

	sendingQueue := make(chan *BundleOfItems)
	stopSignal := make(chan bool, 1)
	errorOccuredSignal := make(chan error, 1)
	defer func() {
		close(sendingQueue)

		stopSignal <- true
		close(stopSignal)
	}()

	go sendingService(writer, sendingQueue, &bundlesInSendingQueueCounter, stopSignal, errorOccuredSignal)

	var currentLength = 0
	var itemsToSend []Item

	var errorOccured error = nil

downloadingLoop:
	for iterator.Next() {
		if bytes.Compare(iterator.Key(), integration.FlushIDKey) == 0 {
			log.Info("Skipping flush key")
			continue
		}

		i += 1
		select {
		case errorOccured = <-errorOccuredSignal:
			{
				log.Warn("errorOccuredSignal")
				break downloadingLoop
			}
		case <-ticker.C:
			{
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
			err := sendBundle(sendingQueue, &itemsToSend, &currentLength, &bundlesInSendingQueueCounter)
			if err != nil {
				fmt.Println(fmt.Sprintf("sending pipe broken: %v", err.Error()))
				break
			}
		}
	}
	ticker.Stop()

	if errorOccured != nil {
		log.Warn("RLP encoding", "error", errorOccured)
		log.Warn("Send to client did not finish successfully...")
		return
	}

	if len(itemsToSend) > 0 {
		sentItems = sentItems + len(itemsToSend)
		err = sendBundle(sendingQueue, &itemsToSend, &currentLength, &bundlesInSendingQueueCounter)
		if err != nil {
			log.Info(fmt.Sprintf("sending pipe broken end: %v", err.Error()))
		}
	}

	for {
		stillBeingProcessed := atomic.LoadUint32(&bundlesInSendingQueueCounter)
		if stillBeingProcessed == 0 {
			log.Info("Sending finished.")
			break
		}
		log.Info("Waiting another 5 sec for last items in queue to be sent.")
		time.Sleep(5 * time.Second)
	}

	err = sendBundleFinished(writer)
	if err != nil {
		log.Info(fmt.Sprintf("sending pipe broken end: %v", err.Error()))
	}

	log.Info(fmt.Sprintf("Sent: %d items.", sentItems))
}

func sendingService(writer *bufio.Writer, sendingQueue chan *BundleOfItems, bundlesInSendingQueueCounter *uint32, stopSignal chan bool, errorOccuredSignal chan error) {
	defer close(errorOccuredSignal)
sendingServiceLoop:
	for {
		select {
		case bundle := <-sendingQueue:
			{
				err := rlp.Encode(writer, bundle)
				if err != nil {
					errorOccuredSignal <- err
					break
				}
				_ = writer.Flush()
				atomic.AddUint32(bundlesInSendingQueueCounter, ^uint32(0))
			}
		case <-stopSignal:
			{
				log.Info("Sending service stopped")
				break sendingServiceLoop
			}
		}
	}

}

func sendEstimatedSizeMessage(writer *bufio.Writer) error {
	var estimatedSize uint64 = 0
	if EstimateGossipSize != nil {
		estimatedSize = EstimateGossipSize()
	}
	log.Info("estimated size: " + strconv.FormatUint(estimatedSize, 10))
	return sendOverheadMessage(writer, []byte(strconv.FormatUint(estimatedSize, 10)))
}

func sendBundle(sendingQueue chan *BundleOfItems, itemsToSend *[]Item, currentLength *int, bundlesInSendingQueueCounter *uint32) error {
	hash := getHashOfKeyValuesInBundle(itemsToSend)

	signature, err := signHash(&hash)
	if err != nil {
		return err
	}

	var bundle = BundleOfItems{false, hash, signature, *itemsToSend}

	atomic.AddUint32(bundlesInSendingQueueCounter, 1)
	sendingQueue <- &bundle

	*itemsToSend = []Item{}
	*currentLength = 0

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
