package main

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"
)

const (
	AddressLength     = 20
	NumBuckets        = 160
	NumNodesPerBucket = 3
	NumPeers          = 100
	NumKeys           = 200
	NumGetValue       = 100
)

type Peer struct {
	ID   []byte
	DHT  *DHT
	Data map[string][]byte
}

type DHT struct {
	Buckets [NumBuckets][]*Peer
}

type KeyValue struct {
	Key   []byte
	Value []byte
}

func NewPeer(id []byte, dht *DHT) *Peer {
	return &Peer{
		ID:   id,
		DHT:  dht,
		Data: make(map[string][]byte),
	}
}

func NewDHT() *DHT {
	dht := &DHT{}
	for i := range dht.Buckets {
		dht.Buckets[i] = []*Peer{}
	}
	return dht
}

func (p *Peer) SetValue(key []byte, value []byte) bool {
	if !p.isKeyValid(key) {
		return false
	}

	if p.hasKeyValue(key) {
		return true
	}

	p.Data[string(key)] = value

	bucketIndex := p.getBucketIndex(key)
	bucket := p.DHT.Buckets[bucketIndex]

	peers := p.findClosestPeers(key, bucket, NumNodesPerBucket)

	for _, peer := range peers {
		if peer != nil {
			peer.SetValue(key, value)
		}
	}

	return true
}

func (p *Peer) GetValue(key []byte) []byte {
	if value, ok := p.Data[string(key)]; ok {
		fmt.Printf("Peer %s found value: %s\n", hex.EncodeToString(p.ID), hex.EncodeToString(value))
		return value
	}

	fmt.Printf("Peer %s does not have value for key: %s\n", hex.EncodeToString(p.ID), hex.EncodeToString(key))

	bucketIndex := p.getBucketIndex(key)
	bucket := p.DHT.Buckets[bucketIndex]

	peers := p.findClosestPeers(key, bucket, NumNodesPerBucket)

	for _, peer := range peers {
		if peer != nil {
			value := peer.GetValue(key)
			if value != nil {
				fmt.Printf("Peer %s received value: %s\n", hex.EncodeToString(p.ID), hex.EncodeToString(value))
				return value
			}
		}
	}

	return nil
}

func (p *Peer) isKeyValid(key []byte) bool {
	return len(key) == AddressLength
}

func (p *Peer) hasKeyValue(key []byte) bool {
	_, ok := p.Data[string(key)]
	return ok
}

func (p *Peer) getBucketIndex(key []byte) int {
	hash := sha1.Sum(key)
	hashString := hex.EncodeToString(hash[:])
	bucketIndex := 0
	for i := 0; i < len(hashString); i++ {
		if hashString[i] != '0' {
			bucketIndex = i
			break
		}
	}
	return bucketIndex
}

func (p *Peer) findClosestPeers(key []byte, bucket []*Peer, numPeers int) []*Peer {
	peers := make([]*Peer, numPeers)
	copy(peers, bucket)
	return peers
}

func main() {
	rand.Seed(time.Now().UnixNano())

	peers := make([]*Peer, NumPeers)
	dht := NewDHT()

	for i := 0; i < NumPeers; i++ {
		id := make([]byte, AddressLength)
		rand.Read(id)
		peer := NewPeer(id, dht)
		peers[i] = peer

		bucketIndex := peer.getBucketIndex(id)
		bucket := dht.Buckets[bucketIndex]
		if len(bucket) < NumNodesPerBucket {
			bucket = append(bucket, peer)
		}
		dht.Buckets[bucketIndex] = bucket
	}

	keyValuePairs := make([]KeyValue, NumKeys)
	for i := 0; i < NumKeys; i++ {
		key := make([]byte, AddressLength)
		value := make([]byte, 10)
		rand.Read(key)
		rand.Read(value)
		keyValuePairs[i] = KeyValue{
			Key:   key,
			Value: value,
		}
	}

	for _, kv := range keyValuePairs {
		randomPeerIndex := rand.Intn(NumPeers)
		peer := peers[randomPeerIndex]
		peer.SetValue(kv.Key, kv.Value)
	}

	for i := 0; i < NumGetValue; i++ {
		randomKeyIndex := rand.Intn(NumKeys)
		key := keyValuePairs[randomKeyIndex].Key
		randomPeerIndex := rand.Intn(NumPeers)
		peer := peers[randomPeerIndex]
		peer.GetValue(key)
	}
}
