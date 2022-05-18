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
var performanceChannelInsertHash int64
var performanceChannelInsertDb int64

var receivedItems uint64 = 0

const CompactThreshold uint64 = 5000000

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

	var bundlesInWrittingQueueCounter uint32 = 0
	dbWriterQueue := make(chan *[]Item)
	stopWriterSignal := make(chan bool, 1)

	hashingQueue := make(chan *BundleOfItems)
	stopHashingSignal := make(chan bool, 1)

	defer func() {
		close(hashingQueue)

		stopHashingSignal <- true
		close(stopHashingSignal)

		stopWriterSignal <- true
		close(stopWriterSignal)
	}()
	go hashingService(hashingQueue, dbWriterQueue, stopHashingSignal)
	go dbWriter(dbWriterQueue, mainDB, gdb, stopWriterSignal, &bundlesInWrittingQueueCounter)

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
			log.Info("Download finished.")
			break
		}

		atomic.AddUint32(&bundlesInWrittingQueueCounter, 1)
		var timeSt2 = time.Now()
		hashingQueue <- &bundle
		atomic.AddInt64(&performanceChannelInsertHash, int64(time.Now().Sub(timeSt2)))
		select {
		case <-ticker.C:
			{
				log.Info(fmt.Sprintf("Received %d", atomic.LoadUint64(&receivedItems)))
				printClientPerformance()
			}
		default:
		}
	}
	ticker.Stop()

	for {
		stillBeingProcessed := atomic.LoadUint32(&bundlesInWrittingQueueCounter)
		if stillBeingProcessed == 0 {
			log.Info("Database flush finished.")
			break
		}
		log.Info("Flush not complete waiting 5 sec.")
		time.Sleep(5 * time.Second)
	}

	log.Info("Progress: 100%")
	fmt.Println("Saved: ", atomic.LoadUint64(&receivedItems), " items.")
}

func hashingService(hashingQueue chan *BundleOfItems, dbWriterQueue chan *[]Item, stopSignal chan bool) {
	defer close(dbWriterQueue)
hashingServiceLoop:
	for {
		select {
		case data := <-hashingQueue:
			{
				err := verifySignatures(data)
				if err != nil {
					log.Crit("Error signatures", "error", err)
				}
				var timeSt = time.Now()
				dbWriterQueue <- &data.Data
				atomic.AddInt64(&performanceChannelInsertDb, int64(time.Now().Sub(timeSt)))
			}
		case <-stopSignal:
			{
				break hashingServiceLoop
			}
		}
	}
}

func dbWriter(dbWriterQueue chan *[]Item, mainDB kvdb.Store, gdb *gossip.Store, stopSignal chan bool, bundlesInWrittingQueueCounter *uint32) {
dbWriterLoop:
	for {
		select {
		case data := <-dbWriterQueue:
			{
				log.Info("dbWriter", "count", atomic.LoadUint32(bundlesInWrittingQueueCounter))
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

				atomic.AddUint64(&receivedItems, uint64(len(*data)))

				atomic.AddUint32(bundlesInWrittingQueueCounter, ^uint32(0))
			}
		case <-stopSignal:
			{
				break dbWriterLoop
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
	log.Info("performance: ", "totalTime", time.Duration(totalTime), "performanceHash", time.Duration(atomic.LoadInt64(&performanceHash)), "performanceSignatures", time.Duration(atomic.LoadInt64(&performanceSignatures)),
		"performanceSocketRead", time.Duration(atomic.LoadInt64(&performanceSocketRead)), "performanceDbWrite", time.Duration(atomic.LoadInt64(&performanceDbWrite)), "performanceDbCompact",
		time.Duration(atomic.LoadInt64(&performanceDbCompact)), "performanceChannelInsertHash", time.Duration(atomic.LoadInt64(&performanceChannelInsertHash)), "performanceChannelInsertDb", time.Duration(atomic.LoadInt64(&performanceChannelInsertDb)))
}
