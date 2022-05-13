package direct_sync

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/status-im/keycard-go/hexutils"
	"io"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"time"
)

var startTime time.Time
var performanceHash time.Duration
var performanceSignatures time.Duration
var performanceSocketRead time.Duration
var performanceDbWrite time.Duration

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

	ticker := time.NewTicker(LOGGING_INTERVAL)

	//var progress uint64 = 0

	//var currentWrittenBytes uint64 = 0
	receivedItems := 0
	for {
		bundle := BundleOfItems{}
		var timeSt = time.Now()
		err := readBundle(stream, &bundle)
		performanceSocketRead += time.Now().Sub(timeSt)
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

		for i := range bundle.Data {
			var timeSt = time.Now()
			err = mainDB.Put(bundle.Data[i].Key, bundle.Data[i].Value)
			performanceDbWrite += time.Now().Sub(timeSt)
			receivedItems = receivedItems + 1

			if err != nil {
				log.Crit(fmt.Sprintf("Insert into db: %v", err))
			}
		}

		select {
		case <-ticker.C:
			{
				log.Info(fmt.Sprintf("Received %d", receivedItems))
				printClientPerformance()
				go func() {
					err = gdb.FlushDBs()
					if err != nil {
						log.Crit("Gossip flush: ", err)
					}
				}()
			}
		default:
		}
	}
	ticker.Stop()

	err = gdb.FlushDBs()
	if err != nil {
		log.Crit("Flush db: ", err)
	}

	log.Info("Progress: 100%")
	fmt.Println("Saved: ", receivedItems, " items.")
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
		log.Crit("PublicKeyRecoveryEmpty", "error", err)
	}
	return sigPublicKey
}

func getHashOfKeyValuesInBundle(bundle *[]Item) []byte {
	var b bytes.Buffer
	_ = rlp.Encode(io.Writer(&b), *bundle)
	return crypto.Keccak256Hash(b.Bytes()).Bytes()
	//hasher := sha512.New()
	//_ = rlp.Encode(hasher, *bundle)
	//return hasher.Sum(nil)
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

	//fmt.Printf("h1: %x\n", hash)
	//fmt.Printf("h2: %x\n", bundle.Hash)

	if !reflect.DeepEqual(hash, bundle.Hash) {
		return errors.New("Hash not matching original")
	}

	var timeSt2 = time.Now()
	performanceHash += timeSt2.Sub(timeSt)

	publicKey := getPublicKey(&hash, &bundle.Signature)

	if !reflect.DeepEqual(publicKey, publicKeyFromChallenge) {
		return errors.New("Signature not matching original")
	}

	//log.Info("Keys were equal", "signature", bundle.Signature, "publicKey", hexutils.BytesToHex(publicKey))

	performanceSignatures += time.Now().Sub(timeSt2)

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
	var totalTime = time.Now().Sub(startTime)
	var rest = totalTime - performanceHash - performanceSignatures - performanceSocketRead - performanceDbWrite
	log.Info("performance: ", "totalTime", totalTime, "performanceHash", performanceHash, "performanceSignatures", performanceSignatures, "performanceSocketRead", performanceSocketRead, "performanceDbWrite", performanceDbWrite, "restTime", rest)
}
