package distributor_test

import (
	"fmt"

	"github.com/couchbase/eventing/application"
	"github.com/couchbase/eventing/supervisor2/distributor"
)

type distributionHelperTest struct {
	numnodes int
	garbaged []*application.KeyspaceInfo
}

func (x *distributionHelperTest) GetGarbagedFunction(namespaces map[application.KeyspaceInfo]struct{}) []*application.KeyspaceInfo {
	return x.garbaged
}

func (x *distributionHelperTest) GetNamespaceDistribution(namespace *application.KeyspaceInfo) int {
	return x.numnodes
}

func (_ *distributionHelperTest) Score(*application.KeyspaceInfo) int {
	return 1
}

func ExampleDistributor() {
	k1 := &application.KeyspaceInfo{BucketID: "bucket1", ScopeID: "s1"}
	helper := &distributionHelperTest{numnodes: 3}
	fmt.Println("Running main")
	fs1 := distributor.NewFunctionScopeDistributor("n0", helper)
	fs1.ReDistribute("abc", []string{"n3", "n2", "n1", "n0"})
	fs1.Distribute(k1, 3)
	k10 := &application.KeyspaceInfo{BucketID: "bucket3", ScopeID: "s1"}
	helper.numnodes = 2
	fs1.Distribute(k10, 2)
	fmt.Println(fs1.GetVbMap(k1, 128))
	fmt.Println(fs1.GetVbMap(k10, 128))

	fs2 := distributor.NewFunctionScopeDistributor("n2", helper)
	fmt.Println(fs2.GetVbMap(k1, 128))

	redisBytes, _ := fs1.ReDistribute("abc2", []string{"n2", "n1", "n3"})
	for _, rewrittenBytes := range redisBytes {
		fs2.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	fmt.Println("Done redistributing by adding n3")
	fmt.Println(fs2.GetVbMap(k1, 128))
	fmt.Println(fs1.GetVbMap(k1, 128))

	fs3 := distributor.NewFunctionScopeDistributor("n3", helper)
	for _, rewrittenBytes := range redisBytes {
		fs3.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	fmt.Println(fs3.GetVbMap(k1, 128))

	fmt.Println("Done redistributing by removing n2")
	redisBytes, _ = fs1.ReDistribute("abc3", []string{"n1", "n3"})
	for _, rewrittenBytes := range redisBytes {
		fs3.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	for _, rewrittenBytes := range redisBytes {
		fs2.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	fmt.Println(fs1.GetVbMap(k1, 128))
	fmt.Println(fs2.GetVbMap(k1, 128))
	fmt.Println(fs3.GetVbMap(k1, 128))

	fmt.Println("Done redistributing by adding n3")
	fmt.Println(fs2.GetVbMap(k1, 128))
	fmt.Println(fs1.GetVbMap(k1, 128))

	fmt.Println("Done redistributing by removing n3, n4")
	redisBytes, _ = fs1.ReDistribute("abc4", []string{"n2", "n1", "n3", "n4"})
	for _, rewrittenBytes := range redisBytes {
		fs2.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	fs4 := distributor.NewFunctionScopeDistributor("n4", helper)
	for _, rewrittenBytes := range redisBytes {
		fs4.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	for _, rewrittenBytes := range redisBytes {
		fs3.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	for _, rewrittenBytes := range redisBytes {
		fs1.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	fmt.Println(fs1.GetVbMap(k1, 128))
	fmt.Println(fs2.GetVbMap(k1, 128))
	fmt.Println(fs3.GetVbMap(k1, 128))
	fmt.Println(fs4.GetVbMap(k1, 128))

	// swap rebalance
	fmt.Println("Done redistributing by removing removing n3, n4 adding n5")
	fs5 := distributor.NewFunctionScopeDistributor("n5", helper)
	redisBytes, _ = fs1.ReDistribute("abc4", []string{"n2", "n1", "n5"})
	for _, rewrittenBytes := range redisBytes {
		fs2.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs4.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	for _, rewrittenBytes := range redisBytes {
		fs3.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs1.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs5.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	fmt.Println(fs1.GetVbMap(k1, 128))
	fmt.Println(fs2.GetVbMap(k1, 128))
	fmt.Println(fs3.GetVbMap(k1, 128))
	fmt.Println(fs4.GetVbMap(k1, 128))
	fmt.Println(fs5.GetVbMap(k1, 128))

	// swap rebalance
	fmt.Println("Swapping n5 with n3")
	redisBytes, _ = fs1.ReDistribute("abc4", []string{"n2", "n1", "n3"})
	for _, rewrittenBytes := range redisBytes {
		fs2.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs4.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	for _, rewrittenBytes := range redisBytes {
		fs3.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs1.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs5.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	fmt.Println(fs1.GetVbMap(k1, 128))
	fmt.Println(fs2.GetVbMap(k1, 128))
	fmt.Println(fs3.GetVbMap(k1, 128))
	fmt.Println(fs4.GetVbMap(k1, 128))
	fmt.Println(fs5.GetVbMap(k1, 128))

	fmt.Println("Swapping n3 with n4 and n5")
	redisBytes, _ = fs1.ReDistribute("abc4", []string{"n2", "n1", "n4", "n5"})
	for _, rewrittenBytes := range redisBytes {
		fs2.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs4.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	for _, rewrittenBytes := range redisBytes {
		fs3.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs1.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs5.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	fmt.Println(fs1.GetVbMap(k1, 128))
	fmt.Println(fs2.GetVbMap(k1, 128))
	fmt.Println(fs3.GetVbMap(k1, 128))
	fmt.Println(fs4.GetVbMap(k1, 128))
	fmt.Println(fs5.GetVbMap(k1, 128))

	// testing garbage collection
	fmt.Printf("Deleting k1")
	helper.garbaged = []*application.KeyspaceInfo{
		k1,
	}
	k2 := &application.KeyspaceInfo{
		BucketID: "bucket2",
		ScopeID:  "s2",
	}

	encodedBytes, removedBytes := fs1.Distribute(k2, 4)
	fs1.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs1.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs2.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs2.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs3.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs3.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs4.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs4.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs5.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs5.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fmt.Println("Garbaged collected")
	fmt.Println(fs1.GetVbMap(k1, 128))
	fmt.Println(fs2.GetVbMap(k1, 128))
	fmt.Println(fs3.GetVbMap(k1, 128))
	fmt.Println(fs4.GetVbMap(k1, 128))
	fmt.Println(fs5.GetVbMap(k1, 128))

	fmt.Println(fs1.GetVbMap(k2, 128))
	fmt.Println(fs2.GetVbMap(k2, 128))
	fmt.Println(fs3.GetVbMap(k2, 128))
	fmt.Println(fs4.GetVbMap(k2, 128))
	fmt.Println(fs5.GetVbMap(k2, 128))

	fmt.Println("Garbage collecting k2 and adding it back")
	helper.garbaged = []*application.KeyspaceInfo{
		k2,
	}
	k3 := &application.KeyspaceInfo{
		BucketID: "bucket2",
		ScopeID:  "s2",
	}
	encodedBytes, removedBytes = fs1.Distribute(k3, 4)
	if encodedBytes != nil {
		panic("Expected nil encodedBytes")
	}

	encodedBytes, removedBytes = fs1.Distribute(k3, 3)
	fs1.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs1.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs2.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs2.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs3.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs3.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs4.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs4.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs5.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs5.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fmt.Println("Garbaged collected after adding k3")
	fmt.Println(fs1.GetVbMap(k2, 128))
	fmt.Println(fs2.GetVbMap(k2, 128))
	fmt.Println(fs3.GetVbMap(k2, 128))
	fmt.Println(fs4.GetVbMap(k2, 128))
	fmt.Println(fs5.GetVbMap(k2, 128))

	fmt.Println(fs1.GetVbMap(k3, 128))
	fmt.Println(fs2.GetVbMap(k3, 128))
	fmt.Println(fs3.GetVbMap(k3, 128))
	fmt.Println(fs4.GetVbMap(k3, 128))
	fmt.Println(fs5.GetVbMap(k3, 128))

	fmt.Println("Garbage collecting k3 and adding k4")
	k4 := &application.KeyspaceInfo{
		BucketID: "bucket2",
		ScopeID:  "s3",
	}
	encodedBytes, removedBytes = fs1.Distribute(k4, 3)
	fs1.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs1.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs2.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs2.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs3.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs3.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs4.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs4.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs5.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs5.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fmt.Println("Garbaged collected after adding k4")
	fmt.Println(fs1.GetVbMap(k3, 128))
	fmt.Println(fs2.GetVbMap(k3, 128))
	fmt.Println(fs3.GetVbMap(k3, 128))
	fmt.Println(fs4.GetVbMap(k3, 128))
	fmt.Println(fs5.GetVbMap(k3, 128))

	fmt.Println(fs1.GetVbMap(k4, 128))
	fmt.Println(fs2.GetVbMap(k4, 128))
	fmt.Println(fs3.GetVbMap(k4, 128))
	fmt.Println(fs4.GetVbMap(k4, 128))
	fmt.Println(fs5.GetVbMap(k4, 128))

	helper.garbaged = []*application.KeyspaceInfo{
		k4,
	}

	k5 := &application.KeyspaceInfo{
		BucketID: "bucket3",
		ScopeID:  "s1",
	}
	encodedBytes, removedBytes = fs1.Distribute(k5, 3)
	fs1.AddDistribution("bucket3", encodedBytes)
	for _, removed := range removedBytes {
		fs1.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs2.AddDistribution("bucket3", encodedBytes)
	for _, removed := range removedBytes {
		fs2.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs3.AddDistribution("bucket3", encodedBytes)
	for _, removed := range removedBytes {
		fs3.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs4.AddDistribution("bucket3", encodedBytes)
	for _, removed := range removedBytes {
		fs4.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs5.AddDistribution("bucket3", encodedBytes)
	for _, removed := range removedBytes {
		fs5.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fmt.Println("Garbaged collected after adding k5 collecting k4")
	fmt.Println(fs1.GetVbMap(k4, 128))
	fmt.Println(fs2.GetVbMap(k4, 128))
	fmt.Println(fs3.GetVbMap(k4, 128))
	fmt.Println(fs4.GetVbMap(k4, 128))
	fmt.Println(fs5.GetVbMap(k4, 128))

	fmt.Println(fs1.GetVbMap(k5, 128))
	fmt.Println(fs2.GetVbMap(k5, 128))
	fmt.Println(fs3.GetVbMap(k5, 128))
	fmt.Println(fs4.GetVbMap(k5, 128))
	fmt.Println(fs5.GetVbMap(k5, 128))

	encodedBytes, removedBytes = fs1.Distribute(k4, 3)
	fs1.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs1.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs2.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs2.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs3.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs3.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs4.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs4.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs5.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs5.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	helper.garbaged = []*application.KeyspaceInfo{}
	fmt.Println("Adding 2 keyspace")
	fmt.Println(fs1.GetVbMap(k4, 128))
	fmt.Println(fs2.GetVbMap(k4, 128))
	fmt.Println(fs3.GetVbMap(k4, 128))
	fmt.Println(fs4.GetVbMap(k4, 128))
	fmt.Println(fs5.GetVbMap(k4, 128))

	fmt.Println(fs1.GetVbMap(k5, 128))
	fmt.Println(fs2.GetVbMap(k5, 128))
	fmt.Println(fs3.GetVbMap(k5, 128))
	fmt.Println(fs4.GetVbMap(k5, 128))
	fmt.Println(fs5.GetVbMap(k5, 128))

	encodedBytes, removedBytes = fs1.Distribute(k3, 4)
	fs1.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs1.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs2.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs2.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs3.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs3.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs4.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs4.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fs5.AddDistribution("bucket2", encodedBytes)
	for _, removed := range removedBytes {
		fs5.AddDistribution(removed.Namespace.BucketID, removed.Value)
	}

	fmt.Println("Adding k3 and check now")
	fmt.Println(fs1.GetVbMap(k3, 128))
	fmt.Println(fs2.GetVbMap(k3, 128))
	fmt.Println(fs3.GetVbMap(k3, 128))
	fmt.Println(fs4.GetVbMap(k3, 128))
	fmt.Println(fs5.GetVbMap(k3, 128))

	redisBytes, _ = fs1.ReDistribute("abc4", []string{"n2", "n1", "n3", "n4", "n5"})
	for _, rewrittenBytes := range redisBytes {
		fs2.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs4.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}
	for _, rewrittenBytes := range redisBytes {
		fs3.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs1.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	for _, rewrittenBytes := range redisBytes {
		fs5.AddDistribution(rewrittenBytes.Namespace.BucketID, rewrittenBytes.Value)
	}

	fmt.Println("Adding 3 keyspace after redistributing")
	fmt.Println(fs1.GetVbMap(k3, 128))
	fmt.Println(fs2.GetVbMap(k3, 128))
	fmt.Println(fs3.GetVbMap(k3, 128))
	fmt.Println(fs4.GetVbMap(k3, 128))
	fmt.Println(fs5.GetVbMap(k3, 128))

	fmt.Println(fs1.GetVbMap(k4, 128))
	fmt.Println(fs2.GetVbMap(k4, 128))
	fmt.Println(fs3.GetVbMap(k4, 128))
	fmt.Println(fs4.GetVbMap(k4, 128))
	fmt.Println(fs5.GetVbMap(k4, 128))

	fmt.Println(fs1.GetVbMap(k5, 128))
	fmt.Println(fs2.GetVbMap(k5, 128))
	fmt.Println(fs3.GetVbMap(k5, 128))
	fmt.Println(fs4.GetVbMap(k5, 128))
	fmt.Println(fs5.GetVbMap(k5, 128))
}
