package direct_sync

import (
	"bufio"
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/go-opera/integration"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/pierrec/lz4/v4"
	"github.com/status-im/keycard-go/hexutils"
	"io"
	"io/ioutil"
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

var (
	performanceSocketWrite       int64
	performanceDbRead            int64
	performanceCompressionEncode int64

	metricSocketWriteTime       = metrics.GetOrRegisterCounter("directsync/socket/write", nil)
	metricDbReadTime            = metrics.GetOrRegisterCounter("directsync/db/read", nil)
	metricCompressionEncodeTime = metrics.GetOrRegisterCounter("directsync/compression/encode", nil)

	PeerCounter = SafePeerCounter{v: 0}

	EstimateGossipSize func() uint64 = nil

	p2pPrivateKey *ecdsa.PrivateKey
)

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

func InitServer(gossipPath string, key *ecdsa.PrivateKey, gdb *gossip.Store) {
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
		log.Crit("There was an error starting the server" + error.Error())
		return
	}

	go TestIterateTroughDb(gdb)

	go serverMessageHandling(server)
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

func serverMessageHandling(server net.Listener) {

	for {
		connection, error := server.Accept()
		if error != nil {
			fmt.Println("There was am error with the connection" + error.Error())
			return
		}
		fmt.Println("connected")

		go connectionHandler(connection)
	}
}

func connectionHandler(connection net.Conn) {
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
		log.Warn("Error while receiving command: ", "error", err)
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

	serverStartTime := time.Now()

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

	sendingQueue := make(chan *BundleOfItems, 100)
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
	timeSt := time.Now()

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
				printServerPerformance(&serverStartTime)
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

		timeSince := int64(time.Since(timeSt))
		atomic.AddInt64(&performanceDbRead, timeSince)
		metricDbReadTime.Inc(timeSince)

		if currentLength > RECOMMENDED_MIN_BUNDLE_SIZE {
			sentItems = sentItems + len(itemsToSend)
			err := sendBundle(sendingQueue, &itemsToSend, &currentLength, &bundlesInSendingQueueCounter)
			if err != nil {
				fmt.Println(fmt.Sprintf("sending pipe broken: %v", err.Error()))
				break
			}
		}
		timeSt = time.Now()
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
			log.Info("Sending finish message.")
			break
		}
		log.Info("Waiting another 5 sec for last items in queue to be sent.")
		time.Sleep(5 * time.Second)
	}

	err = sendBundleFinished(sendingQueue, &bundlesInSendingQueueCounter)
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
		case <-stopSignal:
			{
				log.Info("Sending service stopped")
				break sendingServiceLoop
			}
		case bundle := <-sendingQueue:
			{
				if bundle == nil {
					continue
				}
				pr, pw := io.Pipe()
				zw := lz4.NewWriter(pw)

				var timeSt = time.Now()
				go func() {
					err := rlp.Encode(zw, bundle)
					if err != nil {
						log.Warn("Error while RLP", "error", err)
						errorOccuredSignal <- err
					}
					_ = zw.Close()
					_ = pw.Close()
				}()
				buf, err := ioutil.ReadAll(pr)
				if err != nil {
					log.Warn("Read compressed buffer", "error", err)
					atomic.StoreUint32(bundlesInSendingQueueCounter, 0)
					errorOccuredSignal <- err
					break
				}
				timeSince := int64(time.Since(timeSt))
				atomic.AddInt64(&performanceCompressionEncode, timeSince)
				metricCompressionEncodeTime.Inc(timeSince)

				timeSt1 := time.Now()
				err = rlp.Encode(writer, buf)
				if err != nil {
					log.Warn("Error while RLP byte slice", "error", err)
					errorOccuredSignal <- err
				}

				_ = writer.Flush()
				timeSince2 := int64(time.Since(timeSt1))
				atomic.AddInt64(&performanceSocketWrite, timeSince2)
				metricSocketWriteTime.Inc(timeSince2)

				// bundlesInSendingQueueCounter decrement
				atomic.AddUint32(bundlesInSendingQueueCounter, ^uint32(0))
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
	var timeSt = time.Now()
	hash := getHashOfKeyValuesInBundle(itemsToSend)
	timeSince := int64(time.Since(timeSt))
	atomic.AddInt64(&performanceHash, timeSince)
	metricHashingTime.Inc(timeSince)

	var timeSt2 = time.Now()
	signature, err := signHash(&hash)
	if err != nil {
		return err
	}
	timeSince2 := int64(time.Since(timeSt2))
	atomic.AddInt64(&performanceSignatures, timeSince2)
	metricSignatureTime.Inc(timeSince2)

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

func sendBundleFinished(sendingQueue chan *BundleOfItems, bundlesInSendingQueueCounter *uint32) error {
	var bundle = BundleOfItems{true, []byte{}, []byte{}, []Item{}}

	if atomic.LoadUint32(bundlesInSendingQueueCounter) > 0 {
		return errors.New("queue should have been empty")
	}

	atomic.AddUint32(bundlesInSendingQueueCounter, 1)
	sendingQueue <- &bundle

	time.Sleep(500 * time.Millisecond)
	for {
		stillBeingProcessed := atomic.LoadUint32(bundlesInSendingQueueCounter)
		if stillBeingProcessed == 0 {
			log.Info("Finishing message sent.")
			break
		}
		log.Info("Waiting for finishing message to be sent.")
		time.Sleep(1 * time.Second)
	}

	return nil
}

func sendOverheadError(writer *bufio.Writer, error string) {
	challenge := OverheadMessage{ErrorOccured: true, Payload: []byte(error)}
	rlp.Encode(writer, challenge)
	writer.Flush()
}

func printServerPerformance(serverStartTime *time.Time) {
	var totalTime = int64(time.Since(*serverStartTime))
	log.Info("performance: ", "totalTime", time.Duration(totalTime), "performanceHash", time.Duration(atomic.LoadInt64(&performanceHash)), "performanceSignatures", time.Duration(atomic.LoadInt64(&performanceSignatures)),
		"performanceSocketWrite", time.Duration(atomic.LoadInt64(&performanceSocketWrite)), "performanceDbRead", time.Duration(atomic.LoadInt64(&performanceDbRead)), "performanceCompressionEncode", time.Duration(atomic.LoadInt64(&performanceCompressionEncode)))
}
