package dcpManager

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/eventing/common"
	dcpConn "github.com/couchbase/eventing/dcp_connection"
	"github.com/couchbase/eventing/logging"
	"github.com/couchbase/eventing/notifier"
)

var (
	ErrTryLater      = errors.New("dcp consumer not spawned try later")
	errManagerClosed = errors.New("manager already closed")
)

const (
	eventingPrefix       = "eventing_:"
	eventingStreamPrefix = "eventing_stream:"
	eventingInfoPrefix   = "eventing_info:"
	eventReceiveChanSize = 2048
)

// lazily open the connection
type manager struct {
	id            string
	mode          dcpConn.Mode
	notif         notifier.Observer
	bucketName    string
	dcpConnConfig map[dcpConn.ConfigKey]interface{}

	// kv address to dcp consumer
	consumersLock *sync.Mutex
	consumers     *atomic.Value //map[string]dcpConn.DcpConsumer

	// Map of vb to kv address
	vbRunning map[uint16]string

	dcpEventPool     *sync.Pool
	eventReceiveChan chan *dcpConn.DcpEvent

	colCache *cidToKeyspaceNameCache

	idToChannelLock sync.RWMutex
	idToChannel     map[uint16]chan<- *dcpConn.DcpEvent

	vbMap     *atomic.Value // *notifier.VBmap
	tlsConfig *atomic.Value // *notifier.TlsConfig

	stat   *stats
	closed bool
	close  func()
}

func NewDcpManager(mode dcpConn.Mode, id string, bucketName string, notif notifier.Observer, dcpConnConfig map[dcpConn.ConfigKey]interface{}) DcpManager {
	return NewDcpManagerWithContext(context.Background(), mode, id, bucketName, notif, dcpConnConfig)
}

func NewDcpManagerWithContext(ctx context.Context, mode dcpConn.Mode, id string, bucketName string, notif notifier.Observer, dcpConnConfig map[dcpConn.ConfigKey]interface{}) DcpManager {
	logPrefix := "DcpManager::NewDcpManager"

	m := &manager{
		id:            id,
		notif:         notif,
		bucketName:    bucketName,
		mode:          mode,
		dcpConnConfig: dcpConnConfig,

		consumersLock:    &sync.Mutex{},
		consumers:        &atomic.Value{},
		vbRunning:        make(map[uint16]string),
		eventReceiveChan: make(chan *dcpConn.DcpEvent, eventReceiveChanSize),
		idToChannel:      make(map[uint16]chan<- *dcpConn.DcpEvent),
		vbMap:            &atomic.Value{},
		tlsConfig:        &atomic.Value{},

		stat:   &stats{},
		closed: false,
		close:  func() {},
	}

	m.consumers.Store(make(map[string]dcpConn.DcpConsumer))
	m.vbMap.Store(&notifier.VBmap{})
	m.tlsConfig.Store(&notifier.TlsConfig{})

	m.dcpEventPool = &sync.Pool{
		New: func() interface{} {
			return &dcpConn.DcpEvent{}
		},
	}

	event := notifier.InterestedEvent{
		Event:  notifier.EventVbmapChanges,
		Filter: m.bucketName,
	}

	currentState, err := m.notif.GetCurrentState(event)
	if err != nil {
		logging.Errorf("%s id: %s Error fetching vbmap for bucket: %s err: %v", logPrefix, id, m.bucketName, err)
		// TODO: Lets see what to do here
		// If bucket gets deleted then bail out
		return m
	}
	m.vbMap.Store(currentState)

	event.Event = notifier.EventTLSChanges
	event.Filter = ""
	currentState, err = m.notif.GetCurrentState(event)
	if err != nil {
		logging.Errorf("%s id: %s Error fetching tls changes for bucket: %s err: %v", logPrefix, id, m.bucketName, err)
		return m
	}
	m.tlsConfig.Store(currentState)

	m.colCache = initCidToCol(id, bucketName)
	m.colCache.refreshManifest(notif)

	ctx, close := context.WithCancel(ctx)
	m.close = close

	logging.Infof("%s DcpManager started id: %s for bucketname: %s mode: %v", logPrefix, m.id, m.bucketName, m.mode)
	go m.waitForConfigChanges(ctx)
	go m.receiveChan(ctx)
	return m
}

type stats struct {
	NumFiltered      uint64                  `json:"num_filtered"`
	DcpConsumerStats []common.StatsInterface `json:"dcp_consumer_stats"`
	VbMapping        string                  `json:"vb_mapping"`
	NumRegistered    uint64                  `json:"num_registered"`
}

func (m *manager) GetRuntimeStats() common.StatsInterface {
	stats := &stats{}

	consumers := m.consumers.Load().(map[string]dcpConn.DcpConsumer)
	stats.DcpConsumerStats = make([]common.StatsInterface, 0, len(consumers))
	for _, consumer := range consumers {
		stats.DcpConsumerStats = append(stats.DcpConsumerStats, consumer.GetRuntimeStats())
	}

	vbMap := m.vbMap.Load().(*notifier.VBmap)
	stats.VbMapping = vbMap.String()

	m.idToChannelLock.RLock()
	stats.NumRegistered = uint64(len(m.idToChannel))
	m.idToChannelLock.RUnlock()

	return common.NewMarshalledData(stats)
}

func (m *manager) RegisterID(id uint16, sendChannel chan<- *dcpConn.DcpEvent) {
	m.idToChannelLock.Lock()
	defer m.idToChannelLock.Unlock()

	m.idToChannel[id] = sendChannel
}

func (m *manager) DeregisterID(id uint16) {
	m.idToChannelLock.Lock()
	defer m.idToChannelLock.Unlock()

	delete(m.idToChannel, id)
}

func (m *manager) DoneDcpEvent(event *dcpConn.DcpEvent) {
	event.Reset()
	m.dcpEventPool.Put(event)
}

func (m *manager) StartStreamReq(sr *dcpConn.StreamReq) error {
	return m.startRequestInternal(sr)
}

func (m *manager) PauseStreamReq(sr *dcpConn.StreamReq) {
	m.pauseRequestInternal(sr)
}

func (m *manager) CloseRequest(sr *dcpConn.StreamReq) (*dcpConn.StreamReq, error) {
	return m.closeRequestInternal(sr)
}

func (m *manager) GetFailoverLog(vbs []uint16) (map[uint16]dcpConn.FailoverLog, error) {
	returnMap := make(map[uint16]dcpConn.FailoverLog)
	nodeLists := make(map[notifier.NodeAddress][]uint16)
	consumerMap := make(map[notifier.NodeAddress]dcpConn.DcpConsumer)

	for _, vb := range vbs {
		kvAddress, consumer, err := m.getDcpConsumerForVb(vb)
		if err != nil {
			return nil, err
		}

		consumerMap[kvAddress] = consumer
		nodeLists[kvAddress] = append(nodeLists[kvAddress], vb)
	}

	for kvAddress, vbs := range nodeLists {
		dcpConsumer := consumerMap[kvAddress]
		failoverMap, err := dcpConsumer.GetFailoverLog(vbs)
		if err != nil {
			return returnMap, err
		}

		for vb, fLog := range failoverMap {
			returnMap[vb] = fLog
		}
	}

	return returnMap, nil
}

func (m *manager) GetSeqNumber(vbs []uint16, collectionID string) (map[uint16]uint64, error) {
	returnMap := make(map[uint16]uint64)
	nodeLists := make(map[notifier.NodeAddress]dcpConn.DcpConsumer)
	for _, vb := range vbs {
		returnMap[vb] = 0
	}

	for _, vb := range vbs {
		kvAddress, consumer, err := m.getDcpConsumerForVb(vb)
		if err != nil {
			return nil, err
		}
		nodeLists[kvAddress] = consumer
	}

	for _, dcpConsumer := range nodeLists {
		seqMap := dcpConsumer.GetSeqNumber(collectionID)
		for vb, seq := range seqMap {
			if _, ok := returnMap[vb]; ok {
				returnMap[vb] = seq
			}
		}
	}

	return returnMap, nil
}

func (m *manager) CloseConditional() bool {
	m.idToChannelLock.RLock()
	defer m.idToChannelLock.RUnlock()

	registeredIDLen := len(m.idToChannel)
	if registeredIDLen != 0 {
		return false
	}

	m.closeManager()
	return true
}

func (m *manager) CloseManager() {
	m.closeManager()
}

// No more requests gonna come for this
func (m *manager) closeManager() {
	logPrefix := fmt.Sprintf("dcpManager::closeManager[%s:%s]", m.id, m.bucketName)
	m.close()

	m.consumersLock.Lock()
	m.closed = true
	logging.Infof("%s closing dcp manager...", logPrefix)
	consumers := m.consumers.Swap(make(map[string]dcpConn.DcpConsumer)).(map[string]dcpConn.DcpConsumer)
	m.consumersLock.Unlock()
	for _, consumer := range consumers {
		// No need to handle any pending requests
		consumer.CloseDcpConsumer()
	}
}

func (m *manager) startRequestInternal(sr *dcpConn.StreamReq) error {
	_, dcpConsumer, err := m.getDcpConsumerForVb(sr.Vbno)
	if err != nil {
		return err
	}
	dcpConsumer.StartStreamReq(sr)
	return nil
}

func (m *manager) pauseRequestInternal(sr *dcpConn.StreamReq) {
	_, dcpConsumer, err := m.getDcpConsumerForVb(sr.Vbno)
	if err != nil {
		return
	}

	dcpConsumer.PauseStreamReq(sr)
}

func (m *manager) closeRequestInternal(sr *dcpConn.StreamReq) (*dcpConn.StreamReq, error) {
	_, dcpConsumer, err := m.getDcpConsumerForVb(sr.Vbno)
	if err != nil {
		return nil, err
	}

	return dcpConsumer.StopStreamReq(sr), nil
}

func (m *manager) findKvNodeAddress(vb uint16) (notifier.NodeAddress, error) {
	vbMap := m.vbMap.Load().(*notifier.VBmap)
	kvIndex, ok := vbMap.VbToKv[vb]
	if !ok {
		return notifier.NodeAddress{}, fmt.Errorf("vb in vb map not found")
	}

	if len(vbMap.ServerList) < kvIndex {
		return notifier.NodeAddress{}, fmt.Errorf("kv node not found")
	}

	kvAddress := vbMap.ServerList[kvIndex]
	return kvAddress, nil
}

func (m *manager) getDcpConsumerForVb(vb uint16) (notifier.NodeAddress, dcpConn.DcpConsumer, error) {
	kvAddressStruct, err := m.findKvNodeAddress(vb)
	if err != nil {
		return kvAddressStruct, nil, err
	}

	consumers := m.consumers.Load().(map[string]dcpConn.DcpConsumer)
	kvAddress := kvAddressStruct.NonSSLAddress

	dcpConsumer, ok := consumers[kvAddress]
	if !ok {
		m.consumersLock.Lock()
		if m.closed {
			m.consumersLock.Unlock()
			return kvAddressStruct, nil, errManagerClosed
		}
		kvAddressStruct, err = m.findKvNodeAddress(vb)
		if err != nil {
			m.consumersLock.Unlock()
			return kvAddressStruct, nil, err
		}

		kvAddress = kvAddressStruct.NonSSLAddress
		consumers2 := m.consumers.Load().(map[string]dcpConn.DcpConsumer)
		dcpConsumer, ok = consumers2[kvAddress]
		if !ok {
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			val := r.Intn(100000)

			prefix := eventingPrefix
			switch m.mode {
			case dcpConn.StreamRequestMode:
				prefix = eventingStreamPrefix

			case dcpConn.InfoMode:
				prefix = eventingInfoPrefix
			}

			hostKvAddress := kvAddressStruct.NonSSLAddress
			tlsConfig := m.tlsConfig.Load().(*notifier.TlsConfig)
			if tlsConfig.EncryptData {
				hostKvAddress = kvAddressStruct.SSLAddress
			}
			clientName := fmt.Sprintf("%s:%s_%s%d", prefix, m.id, hostKvAddress, val)

			config := dcpConn.Config{
				Mode:       m.mode,
				ClientName: clientName,
				BucketName: m.bucketName,
				KvAddress:  hostKvAddress,
				DcpConfig:  m.dcpConnConfig,
			}

			dcpConsumer = dcpConn.GetDcpConsumer(config, tlsConfig, m.eventReceiveChan, m.dcpEventPool)
			consumers3 := make(map[string]dcpConn.DcpConsumer)
			for kvAddress, consumer := range consumers2 {
				consumers3[kvAddress] = consumer
			}
			consumers3[kvAddress] = dcpConsumer
			if m.mode != dcpConn.StreamRequestMode {
				dcpConsumer.Wait()
			}
			m.consumers.Store(consumers3)
		}
		m.consumersLock.Unlock()
	}

	return kvAddressStruct, dcpConsumer, nil
}

func (m *manager) waitForConfigChanges(ctx context.Context) {
	logPrefix := fmt.Sprintf("dcpManager::waitForConfigChanges[%s:%s]", m.id, m.bucketName)

	sub := m.notif.GetSubscriberObject()

	vbMapEvent := notifier.InterestedEvent{
		Event:  notifier.EventVbmapChanges,
		Filter: m.bucketName,
	}
	tlsEvent := notifier.InterestedEvent{
		Event: notifier.EventTLSChanges,
	}

	defer func() {
		m.notif.DeregisterEvent(sub, vbMapEvent)
		m.notif.DeregisterEvent(sub, tlsEvent)

		select {
		case <-ctx.Done():
			return
		default:
		}

		go m.waitForConfigChanges(ctx)
	}()

	currentState, err := m.notif.RegisterForEvents(sub, vbMapEvent)
	if err != nil {
		logging.Errorf("%s Error fetching vbmap for bucket err: %v", logPrefix, err)
		// TODO: Lets see what to do here
		// If bucket gets deleted then bail out
		return
	}
	m.handleVbChangeReceived(currentState)

	currentState, err = m.notif.RegisterForEvents(sub, tlsEvent)
	if err != nil {
		logging.Errorf("%s Error fetching tls changes err: %v", logPrefix, err)
		return
	}
	m.handleTlsChange(currentState)

	for {
		select {
		case trans := <-sub.WaitForEvent():
			if trans == nil {
				logging.Errorf("%s observer event got closed. Restarting...", logPrefix)
				return
			}

			if trans.Deleted {
				m.closeManager()
				return
			}

			switch trans.Event.Event {
			case notifier.EventVbmapChanges:
				m.handleVbChangeReceived(trans.CurrentState)
				logging.Infof("%s detected changes in vb map. Changes applied", logPrefix)

			case notifier.EventTLSChanges:
				m.handleTlsChange(trans.CurrentState)
				logging.Infof("%s detected changes in tls config. Changes applied", logPrefix)
			}

		case <-ctx.Done():
			return
		}
	}
}

// Dcp consumer will send the request back to caller when state changed
func (m *manager) handleVbChangeReceived(newState interface{}) {
	newVbMap := newState.(*notifier.VBmap)
	newConsumerList := make(map[string]dcpConn.DcpConsumer)

	consumersToBeClosed := make([]dcpConn.DcpConsumer, 0)
	m.consumersLock.Lock()
	m.vbMap.Store(newState)
	newServerList := newVbMap.ServerList
	consumers := m.consumers.Swap(make(map[string]dcpConn.DcpConsumer)).(map[string]dcpConn.DcpConsumer)

	for address, consumer := range consumers {
		found := false
		for _, nodeAddress := range newServerList {
			if address == nodeAddress.NonSSLAddress {
				newConsumerList[address] = consumer
				found = true
				break
			}
		}

		if !found {
			consumersToBeClosed = append(consumersToBeClosed, consumer)
		}
	}
	m.consumers.Store(newConsumerList)
	m.consumersLock.Unlock()

	for _, consumer := range consumersToBeClosed {
		pendingEvents := consumer.CloseDcpConsumer()
		for _, dcpEvent := range pendingEvents {
			m.eventReceiveChan <- dcpEvent
		}
	}
}

func (m *manager) handleTlsChange(config interface{}) {
	m.consumersLock.Lock()
	defer m.consumersLock.Unlock()

	tlsConfig := config.(*notifier.TlsConfig)
	m.tlsConfig.Store(config)
	consumers := m.consumers.Load().(map[string]dcpConn.DcpConsumer)
	for _, consumer := range consumers {
		consumer.TlsSettingsChange(tlsConfig)
	}
}

func (m *manager) receiveChan(ctx context.Context) {
	for {
		select {
		case msg := <-m.eventReceiveChan:
			// Analyse the message and route it accordingly
			m.analyseDcpEvent(msg)

		case <-ctx.Done():
			return
		}
	}
}

func (m *manager) analyseDcpEvent(msg *dcpConn.DcpEvent) {
	send := true
	switch msg.Opcode {
	case dcpConn.DCP_MUTATION, dcpConn.DCP_DELETION, dcpConn.DCP_EXPIRATION:
		send = m.handleChangeEvent(msg)

	case dcpConn.DCP_STREAMREQ:
		send = m.handleStreamReq(msg)

	case dcpConn.DCP_STREAM_END:
		send = m.handleStreamEnd(msg)

	case dcpConn.DCP_SYSTEM_EVENT:
		send = m.handleSystemEvent(msg)
	}

	if send {
		m.idToChannelLock.RLock()
		sendChannel, ok := m.idToChannel[msg.ID]
		m.idToChannelLock.RUnlock()

		if !ok {
			m.DoneDcpEvent(msg)
			m.stat.NumFiltered++
			return
		}

		sendChannel <- msg
	}
}

func (m *manager) handleStreamReq(_ *dcpConn.DcpEvent) (send bool) {
	return true
}

func (m *manager) handleStreamEnd(_ *dcpConn.DcpEvent) (send bool) {
	return true
}

func (m *manager) handleChangeEvent(msg *dcpConn.DcpEvent) (send bool) {
	msg.Keyspace, send = m.colCache.getKeyspaceName(msg)
	return
}

func (m *manager) handleSystemEvent(msg *dcpConn.DcpEvent) (send bool) {
	send = m.colCache.updateManifest(msg)
	return
}
