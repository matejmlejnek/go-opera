package direct_sync

import (
	"bufio"
	"crypto/sha512"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"io"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"sync"
	"time"
)

var bundlesInWrittingQueueCounter = SafeCounter{v: 0}

type SafeCounter struct {
	mu sync.Mutex
	v  int
}

func (c *SafeCounter) GetValue() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.v
}

func (c *SafeCounter) Increment() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.v++
}

func (c *SafeCounter) Decrement() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.v--
}

type Item struct {
	Key   []byte
	Value []byte
}

type OverheadMessage struct {
	ErrorOccured bool
	Payload      string
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

	err := sendChallenge(writer)
	if err != nil {
		log.Crit(fmt.Sprintf("\"Sending challenge:: %v", err))
	}

	//pubKey, err := readChallengeAck(stream)
	_, err = readChallengeAck(stream)
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

	ticker := time.NewTicker(10 * time.Second)

	var receivedItems = 0

	dbWriterQueue := make(chan []Item)
	stopSignal := make(chan bool)
	go dbWriter(dbWriterQueue, mainDB, gdb, stopSignal)

	for {
		bundle := BundleOfItems{}
		err := readBundle(stream, &bundle)
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

		bundlesInWrittingQueueCounter.Increment()
		dbWriterQueue <- bundle.Data
		receivedItems += len(bundle.Data)

		select {
		case <-ticker.C:
			{
				log.Info(fmt.Sprintf("Received %d", receivedItems))
			}
		default:
		}
	}
	ticker.Stop()

	for {
		if bundlesInWrittingQueueCounter.GetValue() == 0 {
			break
		}
		log.Info("Flush not complete waiting 5 sec.")
		time.Sleep(5 * time.Second)
	}

	close(dbWriterQueue)

	log.Info("Progress: 100%")
	fmt.Println("Saved: ", receivedItems, " items.")
}

func dbWriter(dbWriterQueue chan []Item, mainDB kvdb.Store, gdb *gossip.Store, stopSignal chan bool) {
	for {
		select {
		case data := <-dbWriterQueue:
			{
				for i := range data {
					err := mainDB.Put(data[i].Key, data[i].Value)

					if err != nil {
						log.Crit(fmt.Sprintf("Insert into db: %v", err))
					}
				}

				err := gdb.FlushDBs()
				if err != nil {
					log.Crit("Gossip flush: ", err)
				}
				bundlesInWrittingQueueCounter.Decrement()

				log.Info("Left in queue: ", "counter", bundlesInWrittingQueueCounter.GetValue())
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
	parseUint, err := strconv.ParseUint(estimatedSize, 10, 64)
	if err != nil {
		return 0, err
	}
	return parseUint, nil
}

func getSignature(hash *[]byte) []byte {
	//TODO signatur
	return []byte{1, 2, 3}
}

func getHashOfKeyValuesInBundle(bundle *[]Item) []byte {
	hasher := sha512.New()
	_ = rlp.Encode(hasher, *bundle)
	return hasher.Sum(nil)
}

func readBundle(stream *rlp.Stream, b *BundleOfItems) error {
	err := stream.Decode(b)
	if err != nil {
		return err
	}
	return nil
}

func sendGetCommand(writer *bufio.Writer) error {
	return sendOverheadMessage(writer, "get")
}

func readChallengeAck(stream *rlp.Stream) (string, error) {
	payload, err := readOverheadMessage(stream)
	if err != nil {
		return "", err
	}
	// todo pub key from original challenge and payload

	return payload, nil
}

func sendChallenge(writer *bufio.Writer) error {
	randomNum := strconv.FormatInt(rand.Int63(), 10)
	return sendOverheadMessage(writer, randomNum)
}

func verifySignatures(bundle *BundleOfItems) error {
	hash := getHashOfKeyValuesInBundle(&(bundle.Data))

	//fmt.Printf("h1: %x\n", hash)
	//fmt.Printf("h2: %x\n", bundle.Hash)

	if !reflect.DeepEqual(hash, bundle.Hash) {
		return errors.New("Hash not matching original")
	}

	signature := getSignature(&hash)

	if !reflect.DeepEqual(signature, bundle.Signature) {
		return errors.New("Signature not matching original")
	}

	return nil
}

func readOverheadMessage(stream *rlp.Stream) (string, error) {
	e := OverheadMessage{}
	err := stream.Decode(&e)
	if err != nil {
		return "", err
	}

	if e.ErrorOccured {
		return "", errors.New("Error: " + e.Payload)
	}

	return e.Payload, nil
}

func sendOverheadMessage(writer *bufio.Writer, message string) error {
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
