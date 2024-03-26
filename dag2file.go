package merkledag

import (
	"encoding/json"
	"strings"
)

// Hash to file
func Hash2File(store KVStore, hash []byte, path string, hp HashPool) []byte {
	// 根据hash和path， 返回对应的文件, hash对应的类型是tree
	flag, _ := store.Has(hash)
	if flag {
		data, _ := store.Get(hash)
		var obj Object
		json.Unmarshal(data, &obj)
		pathArr := strings.Split(path, "\\")
		for _, link := range obj.Links {

			objType := string(data[len(data)-4:])
			if link.Name == pathArr[0] {
				if objType == "tree" {
					return Hash2File(store, link.Hash, strings.Join(pathArr[1:], "\\"), hp)
				} else {
					continue
				}
			}
			linkdata, err := store.Get(link.Hash)
			if err != nil {
				panic(err)
			}
			return linkdata
		}

	}
	return nil
}
