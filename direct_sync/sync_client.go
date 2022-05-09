package direct_sync

import (
	"bufio"
	"crypto/sha512"
	"errors"
	"fmt"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"io"
	"math/rand"
	"net"
	"reflect"
	"strconv"
	"time"
)

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

	//var progress uint64 = 0

	//var currentWrittenBytes uint64 = 0
	receivedItems := 0
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

		for i := range bundle.Data {
			//kk, err1 := rlp.EncodeToBytes(bundle.Data[i].Key)
			//vv, err2 := rlp.EncodeToBytes(bundle.Data[i].Value)
			//if err1 != nil || err2 != nil {
			//currentWrittenBytes = currentWrittenBytes + uint64(len(bundle.Data[i].Key)) + uint64(len(bundle.Data[i].Value))
			//} else {
			//currentWrittenBytes = currentWrittenBytes + uint64(len(kk)) + uint64(len(vv))
			//}

			//out1 := fmt.Sprintf("%d-%d", len(bundle.Data[i].Key), uint64(len(bundle.Data[i].Key)))
			//out1arr := strings.Split(out1, "-")
			//if strings.Compare(out1arr[0], out1arr[1]) != 0 {
			//	log.Warn("err key comp")
			//}
			//
			//out2 := fmt.Sprintf("%d-%d", len(bundle.Data[i].Value), uint64(len(bundle.Data[i].Value)))
			//out2arr := strings.Split(out2, "-")
			//if strings.Compare(out2arr[0], out2arr[1]) != 0 {
			//	log.Warn("err value comp")
			//}

			//currentWrittenBytes = currentWrittenBytes + uint64(len(bundle.Data[i].Key)) + uint64(len(bundle.Data[i].Value))

			err = mainDB.Put(bundle.Data[i].Key, bundle.Data[i].Value)
			receivedItems = receivedItems + 1

			if err != nil {
				log.Crit(fmt.Sprintf("Insert into db: %v", err))
			}
		}

		select {
		case <-ticker.C:
			{
				log.Info(fmt.Sprintf("Received %d", receivedItems))
				go func() {
					err = gdb.FlushDBs()
					if err != nil {
						log.Crit("Gossip flush: ", err)
					}
				}()
				//if progress < 99 {
				//
				//	progress = (currentWrittenBytes * 100) / bytesSizeEstimate
				//	if progress > 99 {
				//		progress = 99
				//	}
				//}
				//str := fmt.Sprintf("Progress: ~ %d%% (%d)", progress, currentWrittenBytes)
				//log.Info(str)
			}
		default:
		}
	}
	ticker.Stop()

	finishedFlush := make(chan bool)

	go func() {
		err = gdb.FlushDBs()
		if err != nil {
			log.Crit("Flush db: ", err)
		}
		finishedFlush <- true
	}()
	<-finishedFlush

	log.Info("Progress: 100%")
	fmt.Println("Saved: ", receivedItems, " items.")
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
