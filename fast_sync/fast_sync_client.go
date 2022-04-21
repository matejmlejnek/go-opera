package fast_sync

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
	"net"
	"reflect"
)

//7700000 na 8192
//const BUFFER_SIZE = 8192
const BUFFER_SIZE = 1024
const serverSocketPort = "7002"
const RECOMMENDED_MIN_BUNDLE_SIZE = 10000

type Item struct {
	Key   []byte
	Value []byte
}

type BundleOfItems struct {
	Error     bool
	Hash      []byte
	Signature []byte
	Data      []Item
}

func InitClient(hostAdress string, gdb *gossip.Store) {
	log.Warn("fastsync-client")
	mainDB := gdb.GetMainDb()
	attemptConnect(hostAdress, &mainDB)
}

func attemptConnect(adress string, mainDB *kvdb.Store) {
	//get port and ip address to dial
	connection, err := net.Dial("tcp", adress+":"+serverSocketPort)
	if err != nil {
		log.Error(err.Error())
	}

	getDataFromServer(connection, mainDB)
}

func getDataFromServer(connection net.Conn, mainDB *kvdb.Store) {
	//var currentByte int64 = 0

	//readBuffer := make([]byte, BUFFER_SIZE)

	helloMessage := []byte("get")
	fmt.Println("sending message: ", string(helloMessage))
	_, err := connection.Write(helloMessage)
	if err != nil {
		return
	}

	reader := bufio.NewReader(connection)

	stream := rlp.NewStream(reader, 0)

	for {
		e := BundleOfItems{}
		err = stream.Decode(&e)
		if err == io.EOF {
			fmt.Println("EOF end of stream")
			break
		} else if err != nil {
			fmt.Println("Stream Error: ", err.Error())
			break
		}

		fmt.Println("Decoded Bundle happyyy")

		if e.Error {
			fmt.Println("Error from parsed object")
			break
		}

		fmt.Println("ReceivedBundle: ", string(e.Hash))
		fmt.Println("ReceivedBundle: ", string(e.Signature))
		fmt.Println("Received: ", len(e.Data), " items")
		//fmt.Println("Before signatures")
		//
		//err := verifySignatures(e)
		//if err != nil {
		//	fmt.Println(err.Error())
		//	break
		//}
		//
		//fmt.Println("About to put data")
		//
		//for i := range e.data {
		//	err = (*mainDB).Put(e.data[i].key, e.data[i].value)
		//	if err != nil {
		//		fmt.Println("db: ", err)
		//		break
		//	}
		//}
		//
		//fmt.Println("Saved: ", len(e.data))
	}
}

func verifySignatures(bundle *BundleOfItems) error {
	hash := getHashOfKeyValuesInBundle(&(bundle.Data))

	fmt.Printf("h1: %x\n", hash)
	fmt.Printf("h2: %x\n", bundle.Hash)

	if !reflect.DeepEqual(hash, bundle.Hash) {
		return errors.New("Hash not matching original")
	}

	signature := getSignature(&hash)

	if !reflect.DeepEqual(signature, bundle.Signature) {
		return errors.New("Signature not matching original")
	}

	return nil
}

func getSignature(hash *[]byte) []byte {
	//TODO signatur
	return []byte{1, 2, 3}
}

func getHashOfKeyValuesInBundle(bundle *[]Item) []byte {
	hasher := sha512.New()
	_ = rlp.Encode(hasher, *bundle)
	return hasher.Sum(nil)

	//h2 := sha512.New()
	//for i := range bundle.data {
	//	h2.Write(bundle.data[i].key)
	//	h2.Write(bundle.data[i].value)
	//}
	//return h2.Sum(nil)
}

func displayValues(key []byte, value []byte, s string) {
	//fmt.Print(s, ": ")
	//for i := range key {
	//	fmt.Print(key[i], ", ")
	//}
	//fmt.Println("")
	//
	//fmt.Print(s, ": ")
	//for i := range value {
	//	fmt.Print(value[i], ", ")
	//}
	//fmt.Println("")

	//hash kontrola
	//h1 := sha1.New()
	//h1.Write(key)
	//bs1 := h1.Sum(nil)
	//
	//h2 := sha1.New()
	//h2.Write(value)
	//bs2 := h2.Sum(nil)
	//
	//fmt.Printf("%s : %x %x\n", s, bs1, bs2)
}
