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
	"github.com/ethereum/go-ethereum/metrics"
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

var (
	startTime             time.Time
	performanceHash       int64
	performanceSignatures int64
	performanceSocketRead int64
	performanceDbWrite    int64

	metricHashingTime    = metrics.GetOrRegisterCounter("directsync/hash", nil)
	metricSignatureTime  = metrics.GetOrRegisterCounter("directsync/signature", nil)
	metricSocketReadTime = metrics.GetOrRegisterCounter("directsync/socket/read", nil)
	metricDbWriteTime    = metrics.GetOrRegisterCounter("directsync/db/write", nil)

	receivedItems uint64 = 0

	publicKeyFromChallenge []byte
)

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
		log.Crit(err.Error())
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
	dbWriterQueue := make(chan *[]Item, 1)
	stopWriterSignal := make(chan bool, 1)

	hashingQueue := make(chan *BundleOfItems, 1)
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
		timeSince := int64(time.Since(timeSt))
		atomic.AddInt64(&performanceSocketRead, timeSince)
		metricSocketReadTime.Inc(timeSince)

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
		hashingQueue <- &bundle

		select {
		case <-ticker.C:
			{
				log.Info(fmt.Sprintf("Received %d", atomic.LoadUint64(&receivedItems)))
				printClientPerformance(mainDB)
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
		case <-stopSignal:
			{
				break hashingServiceLoop
			}
		case data := <-hashingQueue:
			{
				if data == nil {
					continue
				}
				err := verifySignatures(data)
				if err != nil {
					log.Crit("Error signatures", "error", err)
				}
				dbWriterQueue <- &data.Data
			}
		}
	}
}

func dbWriter(dbWriterQueue chan *[]Item, mainDB kvdb.Store, gdb *gossip.Store, stopSignal chan bool, bundlesInWrittingQueueCounter *uint32) {
dbWriterLoop:
	for {
		select {
		case <-stopSignal:
			{
				break dbWriterLoop
			}
		case data := <-dbWriterQueue:
			{
				if data == nil {
					continue
				}
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
				timeSince := int64(time.Since(timeSt))
				atomic.AddInt64(&performanceDbWrite, timeSince)
				metricDbWriteTime.Inc(timeSince)

				atomic.AddUint64(&receivedItems, uint64(len(*data)))

				atomic.AddUint32(bundlesInWrittingQueueCounter, ^uint32(0))
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

	timeSince := int64(time.Since(timeSt))
	atomic.AddInt64(&performanceHash, timeSince)
	metricHashingTime.Inc(timeSince)
	var timeSt2 = time.Now()

	publicKey := getPublicKey(&hash, &bundle.Signature)

	if !reflect.DeepEqual(publicKey, publicKeyFromChallenge) {
		return errors.New("Signature not matching original")
	}

	timeSince2 := int64(time.Since(timeSt2))
	atomic.AddInt64(&performanceSignatures, timeSince2)
	metricSignatureTime.Inc(timeSince2)
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

func printClientPerformance(mainDB kvdb.Store) {
	var totalTime = int64(time.Since(startTime))
	log.Info("performance: ", "totalTime", time.Duration(totalTime), "performanceHash", time.Duration(atomic.LoadInt64(&performanceHash)), "performanceSignatures", time.Duration(atomic.LoadInt64(&performanceSignatures)),
		"performanceSocketRead", time.Duration(atomic.LoadInt64(&performanceSocketRead)), "performanceDbWrite", time.Duration(atomic.LoadInt64(&performanceDbWrite)))
}
