package iot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/google/uuid"
	"github.com/julienschmidt/httprouter"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime/debug"
	"sync"
)

func Mock(ctx context.Context, kafkaUrl string) (repoUrl string, err error) {
	log.Println("start iot mock")

	sconf := sarama.NewConfig()
	sconf.Producer.Retry.Max = 5
	sconf.Producer.RequiredAcks = sarama.WaitForAll
	sconf.Producer.Return.Successes = true

	// sync producer
	prd, err := sarama.NewSyncProducer([]string{kafkaUrl}, sconf)

	router, err := getRouter(&Controller{
		mux:              sync.Mutex{},
		devices:          map[string]models.Device{},
		characteristics:  map[string]models.Characteristic{},
		devicesByLocalId: map[string]string{},
		deviceTypes:      map[string]models.DeviceType{},
		hubs:             map[string]models.Hub{},
		protocols:        map[string]models.Protocol{},
		kafkaProducer:    prd,
	})
	if err != nil {
		return repoUrl, err
	}

	server := &httptest.Server{
		Config: &http.Server{Handler: NewLogger(router, "DEBUG")},
	}
	server.Listener, err = net.Listen("tcp", ":")
	if err != nil {
		return repoUrl, err
	}
	server.Start()
	repoUrl = server.URL
	go func() {
		<-ctx.Done()
		server.Close()
	}()
	return repoUrl, nil
}

func getRouter(controller *Controller) (router *httprouter.Router, err error) {
	defer func() {
		if r := recover(); r != nil && err == nil {
			log.Printf("%s: %s", r, debug.Stack())
			err = errors.New(fmt.Sprint("Recovered Error: ", r))
		}
	}()
	router = httprouter.New()
	DevicesEndpoints(controller, router)
	DeviceTypesEndpoints(controller, router)
	ServicesEndpoint(controller, router)
	return
}

type Controller struct {
	mux              sync.Mutex
	devices          map[string]models.Device
	characteristics  map[string]models.Characteristic
	devicesByLocalId map[string]string
	deviceTypes      map[string]models.DeviceType
	hubs             map[string]models.Hub
	protocols        map[string]models.Protocol
	kafkaProducer    sarama.SyncProducer
}

type command string

const (
	putCommand    command = "PUT"
	deleteCommand command = "DELETE"
)

type deviceCommand struct {
	Command command       `json:"command"`
	Id      string        `json:"id"`
	Owner   string        `json:"owner"`
	Device  models.Device `json:"device"`
}

type deviceTypeCommand struct {
	Command    command           `json:"command"`
	Id         string            `json:"id"`
	Owner      string            `json:"owner"`
	DeviceType models.DeviceType `json:"device_type"`
}

func (this *Controller) ReadService(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	for _, dt := range this.deviceTypes {
		for _, service := range dt.Services {
			if service.Id == id {
				return service, nil, 200
			}
		}
	}
	return nil, errors.New("404"), 404
}

func (this *Controller) ReadDevice(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.devices[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishDeviceCreate(device models.Device) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	device.Id = uuid.NewString()
	this.devices[device.Id] = device
	this.devicesByLocalId[device.LocalId] = device.Id
	msg, err := json.Marshal(&deviceCommand{
		Command: putCommand,
		Id:      device.Id,
		Owner:   "someone",
		Device:  device,
	})
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}
	_, _, err = this.kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "devices",
		Value: sarama.ByteEncoder(msg),
		Key:   sarama.StringEncoder(device.Id),
	})
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}
	return device, nil, 200
}

func (this *Controller) PublishDeviceUpdate(id string, device models.Device) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.devices[id] = device
	this.devicesByLocalId[id] = device.LocalId
	msg, err := json.Marshal(&deviceCommand{
		Command: putCommand,
		Id:      device.Id,
		Owner:   "someone",
		Device:  device,
	})
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}
	_, _, err = this.kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "devices",
		Value: sarama.ByteEncoder(msg),
		Key:   sarama.StringEncoder(device.Id),
	})
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}
	return device, nil, 200
}

func (this *Controller) PublishDeviceDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.devices, id)
	delete(this.devicesByLocalId, id)
	msg, err := json.Marshal(&deviceCommand{
		Command: deleteCommand,
		Id:      id,
		Owner:   "someone",
	})
	if err != nil {
		return err, http.StatusInternalServerError
	}
	_, _, err = this.kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "devices",
		Value: sarama.ByteEncoder(msg),
		Key:   sarama.StringEncoder(id),
	})
	if err != nil {
		return err, http.StatusInternalServerError
	}
	return nil, 200
}

func (this *Controller) ReadHub(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.hubs[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishHubCreate(hub models.Hub) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	hub.Id = uuid.NewString()
	this.hubs[hub.Id] = hub
	return hub, nil, 200
}

func (this *Controller) PublishHubUpdate(id string, hub models.Hub) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.hubs[id] = hub
	return hub, nil, 200
}

func (this *Controller) PublishHubDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.hubs, id)
	return nil, 200
}

func (this *Controller) ReadDeviceType(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.deviceTypes[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishDeviceTypeCreate(devicetype models.DeviceType) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	devicetype.Id = uuid.NewString()
	for i, service := range devicetype.Services {
		service.Id = uuid.NewString()
		devicetype.Services[i] = service
	}
	this.deviceTypes[devicetype.Id] = devicetype

	msg, err := json.Marshal(&deviceTypeCommand{
		Command:    putCommand,
		Id:         devicetype.Id,
		Owner:      "someone",
		DeviceType: devicetype,
	})
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}
	_, _, err = this.kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "device-types",
		Value: sarama.ByteEncoder(msg),
		Key:   sarama.StringEncoder(devicetype.Id),
	})
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}

	return devicetype, nil, 200
}

func (this *Controller) PublishDeviceTypeUpdate(id string, devicetype models.DeviceType) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.deviceTypes[id] = devicetype

	msg, err := json.Marshal(&deviceTypeCommand{
		Command:    putCommand,
		Id:         id,
		Owner:      "someone",
		DeviceType: devicetype,
	})
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}
	_, _, err = this.kafkaProducer.SendMessage(&sarama.ProducerMessage{
		Topic: "device-types",
		Value: sarama.ByteEncoder(msg),
		Key:   sarama.StringEncoder(id),
	})
	if err != nil {
		return nil, err, http.StatusInternalServerError
	}

	return devicetype, nil, 200
}

func (this *Controller) PublishDeviceTypeDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.deviceTypes, id)
	return nil, 200
}

func (this *Controller) DeviceLocalIdToId(id string) (result string, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.devicesByLocalId[id]
	if exists {
		return result, nil, 200
	} else {
		return "", errors.New("404"), 404
	}
}

func (this *Controller) ReadProtocol(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.protocols[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishProtocolCreate(protocol models.Protocol) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	protocol.Id = uuid.NewString()
	this.protocols[protocol.Id] = protocol
	return protocol, nil, 200
}

func (this *Controller) PublishProtocolUpdate(id string, protocol models.Protocol) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.protocols[id] = protocol
	return protocol, nil, 200
}

func (this *Controller) PublishProtocolDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.protocols, id)
	return nil, 200
}

func (this *Controller) ReadCharacteristic(id string) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	var exists bool
	result, exists = this.characteristics[id]
	if exists {
		return result, nil, 200
	} else {
		return nil, errors.New("404"), 404
	}
}

func (this *Controller) PublishCharacteristicCreate(characteristic models.Characteristic) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	characteristic.Id = uuid.NewString()
	this.characteristics[characteristic.Id] = characteristic
	return characteristic, nil, 200
}

func (this *Controller) PublishCharacteristicUpdate(id string, characteristic models.Characteristic) (result interface{}, err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.characteristics[id] = characteristic
	return characteristic, nil, 200
}

func (this *Controller) PublishCharacteristicDelete(id string) (err error, code int) {
	this.mux.Lock()
	defer this.mux.Unlock()
	delete(this.characteristics, id)
	return nil, 200
}
