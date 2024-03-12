package merkledag

import (
	"encoding/json"
	"hash"
)

type Link struct {
	Name string
	Hash []byte
	Size int
}

type Object struct {
	Links []Link
	Data  []byte
}

const BlockSize = 512 * 1024 // 512 KB

func Add(store KVStore, node Node, h hash.Hash) []byte {
	// TODO 将分片写入到KVStore中，并返回Merkle Root
	switch node.Type() {
	case FILE:
		return StoreFile(store, node, h)
	case DIR:
		return StoreDir(store, node, h)

	}
	return nil
}

func StoreFile(store KVStore, node Node, h hash.Hash) []byte {
	file, ok := node.(File)
	if !ok {
		panic("Node is not a File")
	}
	var hashes [][]byte
	data := file.Bytes()
	if len(data) > BlockSize {
		blocks := splitIntoBlocks(data, BlockSize)
		var objData []byte
		for _, block := range blocks {
			// 计算块的哈希
			blob := Object{
				Links: nil,
				Data:  block,
			}
			h.Reset()

			objData = append(objData, []byte("blob")...)
			jsonMarshal, _ := json.Marshal(blob)
			h.Write(jsonMarshal)
			hash := h.Sum(nil)
			hashes = append(hashes, hash)
			// 将块的哈希和块的数据写入到 KVStore 中
			err := store.Put(hash, block)
			if err != nil {
				panic(err)
			}
		}
		// 计算Merkle Root
		links := make([]Link, 0, len(hashes))
		for i, hash := range hashes {
			links = append(links, Link{
				Name: "",
				Hash: hash,
				Size: len(blocks[i]),
			})
		}
		fileObject := Object{
			Links: links,
			Data:  objData,
		}
		h.Reset()
		jsonMarshal, _ := json.Marshal(fileObject)
		h.Write(jsonMarshal)
		hash := h.Sum(nil)
		err := store.Put(hash, jsonMarshal)
		if err != nil {
			panic(err)
		}
		return hash
	} else {
		blob := Object{
			Links: nil,
			Data:  data,
		}
		jsonMarshal, _ := json.Marshal(blob)
		h.Reset()
		h.Write(jsonMarshal)
		hash := h.Sum(nil)
		err := store.Put(hash, jsonMarshal)
		if err != nil {
			panic(err)
		}
		return hash
	}

}

func StoreDir(store KVStore, node Node, h hash.Hash) []byte {
	dir, _ := node.(Dir)
	iter := dir.It() //调用了dir目录的It方法，获取目录迭代器
	//迭代器遍历每个子节点
	treeObject := Object{}
	for iter.Next() {

		child := iter.Node()
		//递归调用Add方法
		hash := Add(store, child, h)
		treeObject.Links = append(treeObject.Links, Link{
			Name: child.Name(),
			Hash: hash,
		})
		treeObject.Data = append(treeObject.Data, []byte("tree")...)
		//将子节点的哈希和子节点的名称写入到KVStore中
		jsonMarshal, _ := json.Marshal(treeObject)
		h.Reset()
		h.Write(jsonMarshal)
		hash = h.Sum(nil)
		err := store.Put(hash, jsonMarshal)
		if err != nil {
			panic(err)
		}

	}

	//对整个目录节点进行序列化，计算哈希值
	jsonMarshal, _ := json.Marshal(treeObject)
	h.Reset()
	h.Write(jsonMarshal)
	hash := h.Sum(nil)
	err := store.Put(hash, jsonMarshal)
	if err != nil {
		panic(err)
	}
	return hash
}

func splitIntoBlocks(data []byte, blockSize int) [][]byte {
	var blocks [][]byte

	for i := 0; i < len(data); i += blockSize {
		end := i + blockSize
		if end > len(data) {
			end = len(data)
		}

		blocks = append(blocks, data[i:end])
	}

	return blocks
}
