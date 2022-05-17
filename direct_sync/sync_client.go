package direct_sync

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/status-im/keycard-go/hexutils"
	"io"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"sync/atomic"
	"time"
)

var startTime time.Time
var performanceHash int64
var performanceSignatures int64
var performanceSocketRead int64
var performanceDbWrite int64
var performanceDbCompact int64

var bundlesInWrittingQueueCounter uint32 = 0
var receivedItems uint32 = 0

const CompactThreshold uint32 = 5000000

var nextCompact = CompactThreshold

var publicKeyFromChallenge []byte

type Item struct {
	Key   []byte
	Value []byte
}

type OverheadMessage struct {
	ErrorOccured bool
	Payload      []byte
	//	progress? total size then estimate
}

type BundleOfItems struct {
	Finished  bool
	Hash      []byte
	Signature []byte
	Data      []Item
}

func DownloadDataFromServer(address string, gdb *gossip.Store) {
	//get port and ip address to dial
	connection, err := net.Dial("tcp", address+":"+serverSocketPort)
	if err != nil {
		log.Error(err.Error())
	}
	getDataFromServer(connection, gdb)
}

func getDataFromServer(connection net.Conn, gdb *gossip.Store) {
	mainDB := gdb.GetMainDb()

	writer := bufio.NewWriter(connection)
	reader := bufio.NewReader(connection)
	stream := rlp.NewStream(reader, 0)

	challenge, err := sendChallenge(writer)
	if err != nil {
		log.Crit(fmt.Sprintf("\"Sending challenge:: %v", err))
	}

	err = readChallengeAck(stream, challenge)
	if err != nil {
		log.Crit(fmt.Sprintf("Read challenge ack: %v", err))
	}

	err = sendGetCommand(writer)
	if err != nil {
		log.Crit(fmt.Sprintf("Sending get command: %v", err))
	}

	bytesSizeEstimate, err := readEstimatedSizeMessage(stream)
	if err != nil {
		log.Crit(fmt.Sprintf("Server response on command: %v", err))
	}
	log.Info("Estimated size: " + strconv.FormatUint(bytesSizeEstimate, 10))

	startTime = time.Now()

	ticker := time.NewTicker(PROGRESS_LOGGING_FREQUENCY)

	hashingQueue := make(chan *BundleOfItems)
	dbWriterQueue := make(chan *[]Item)
	stopWriterSignal := make(chan bool)
	stopHashingSignal := make(chan bool)
	go dbWriter(dbWriterQueue, mainDB, gdb, stopWriterSignal)
	go hashingService(hashingQueue, dbWriterQueue, stopHashingSignal)

	for {
		bundle := BundleOfItems{}
		var timeSt = time.Now()
		err := readBundle(stream, &bundle)
		atomic.AddInt64(&performanceSocketRead, int64(time.Now().Sub(timeSt)))
		if err != nil {
			if err == io.EOF {
				log.Crit("EOF - end of stream")
			} else {
				log.Crit(fmt.Sprintf("Reading bundle: %v", err))
			}
		}

		if bundle.Finished {
			break
		}

		err = verifySignatures(&bundle)
		if err != nil {
			fmt.Println(err.Error())
		}

		atomic.AddUint32(&bundlesInWrittingQueueCounter, 1)
		hashingQueue <- &bundle

		select {
		case <-ticker.C:
			{
				log.Info(fmt.Sprintf("Received %d", atomic.LoadUint32(&receivedItems)))
				printClientPerformance()
			}
		default:
		}
	}
	ticker.Stop()

	for {
		stillBeingProcessed := atomic.LoadUint32(&bundlesInWrittingQueueCounter)
		if stillBeingProcessed == 0 {
			log.Info("Flush finished.")
			break
		}
		log.Info("Flush not complete waiting 5 sec.")
		time.Sleep(5 * time.Second)
	}

	stopWriterSignal <- true
	stopHashingSignal <- true
	close(dbWriterQueue)

	log.Info("Progress: 100%")
	fmt.Println("Saved: ", atomic.LoadUint32(&receivedItems), " items.")
}

func hashingService(hashingQueue chan *BundleOfItems, dbWriterQueue chan *[]Item, stopSignal chan bool) {
	for {
		select {
		case data := <-hashingQueue:
			{
				err := verifySignatures(data)
				if err != nil {
					log.Crit("Error signatures", "error", err)
				}
				dbWriterQueue <- &data.Data
			}
		case <-stopSignal:
			{
				break
			}
		}
	}
}

func dbWriter(dbWriterQueue chan *[]Item, mainDB kvdb.Store, gdb *gossip.Store, stopSignal chan bool) {
	for {
		select {
		case data := <-dbWriterQueue:
			{
				if len(*data) == 0 {
					continue
				}

				var timeSt = time.Now()
				for i := range *data {
					err := mainDB.Put((*data)[i].Key, (*data)[i].Value)

					if err != nil {
						log.Crit(fmt.Sprintf("Insert into db: %v", err))
					}
				}

				err := gdb.FlushDBs()
				if err != nil {
					log.Crit("Gossip flush: ", "err", err)
				}
				atomic.AddInt64(&performanceDbWrite, int64(time.Now().Sub(timeSt)))

				var currentItems = atomic.AddUint32(&receivedItems, uint32(len(*data)))

				if currentItems > atomic.LoadUint32(&nextCompact) {
					atomic.AddUint32(&nextCompact, CompactThreshold)
					var timeSt = time.Now()
					err = mainDB.Compact([]byte{0x00}, []byte{0xFF})
					atomic.AddInt64(&performanceDbCompact, int64(time.Now().Sub(timeSt)))
					if err != nil {
						log.Crit("Compact pebble database.", "error", err)
					}
				}
				atomic.AddUint32(&bundlesInWrittingQueueCounter, ^uint32(0))
			}
		case <-stopSignal:
			{
				break
			}
		}
	}
}

func readEstimatedSizeMessage(stream *rlp.Stream) (uint64, error) {
	estimatedSize, err := readOverheadMessage(stream)
	if err != nil {
		return 0, err
	}
	parseUint, err := strconv.ParseUint(string(estimatedSize), 10, 64)
	if err != nil {
		return 0, err
	}
	return parseUint, nil
}

func getPublicKey(hash *[]byte, signature *[]byte) []byte {
	sigPublicKey, err := crypto.Ecrecover(*hash, *signature)
	if err != nil {
		log.Crit("PublicKeyRecovery", "error", err)
	}
	if len(sigPublicKey) == 0 {
		log.Crit("PublicKeyRecoveryEmpty")
	}
	return sigPublicKey
}

func getHashOfKeyValuesInBundle(bundle *[]Item) []byte {
	var b bytes.Buffer
	_ = rlp.Encode(io.Writer(&b), *bundle)
	return crypto.Keccak256Hash(b.Bytes()).Bytes()
}

func readBundle(stream *rlp.Stream, b *BundleOfItems) error {
	err := stream.Decode(b)
	if err != nil {
		return err
	}
	return nil
}

func sendGetCommand(writer *bufio.Writer) error {
	return sendOverheadMessage(writer, []byte("get"))
}

func readChallengeAck(stream *rlp.Stream, challenge []byte) error {
	payload, err := readOverheadMessage(stream)
	if err != nil {
		return err
	}
	publicKeyFromChallenge = getPublicKey(&challenge, &payload)
	log.Info("Challenge accepted", "public key", hexutils.BytesToHex(publicKeyFromChallenge))
	return nil
}

func sendChallenge(writer *bufio.Writer) ([]byte, error) {
	randomNum := []byte(strconv.FormatInt(rand.Int63(), 10))
	challenge := crypto.Keccak256Hash(randomNum).Bytes()

	return challenge, sendOverheadMessage(writer, challenge)
}

func verifySignatures(bundle *BundleOfItems) error {
	var timeSt = time.Now()
	hash := getHashOfKeyValuesInBundle(&(bundle.Data))

	if !reflect.DeepEqual(hash, bundle.Hash) {
		return errors.New("Hash not matching original")
	}

	var timeSt2 = time.Now()
	atomic.AddInt64(&performanceHash, int64(timeSt2.Sub(timeSt)))

	publicKey := getPublicKey(&hash, &bundle.Signature)

	if !reflect.DeepEqual(publicKey, publicKeyFromChallenge) {
		return errors.New("Signature not matching original")
	}
	atomic.AddInt64(&performanceSignatures, int64(time.Now().Sub(timeSt2)))
	return nil
}

func readOverheadMessage(stream *rlp.Stream) ([]byte, error) {
	e := OverheadMessage{}
	err := stream.Decode(&e)
	if err != nil {
		return nil, err
	}

	if e.ErrorOccured {
		return nil, errors.New("Error: " + string(e.Payload))
	}

	return e.Payload, nil
}

func sendOverheadMessage(writer *bufio.Writer, message []byte) error {
	challenge := OverheadMessage{ErrorOccured: false, Payload: message}
	err := rlp.Encode(writer, challenge)
	if err != nil {
		return err
	}
	err = writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

func printClientPerformance() {
	var totalTime = int64(time.Now().Sub(startTime))
	log.Info("performance: ", "totalTime", time.Duration(totalTime), "performanceHash", time.Duration(performanceHash), "performanceSignatures", time.Duration(performanceSignatures), "performanceSocketRead", time.Duration(performanceSocketRead), "performanceDbWrite", time.Duration(performanceDbWrite), "performanceDbCompact", time.Duration(performanceDbCompact))
}
