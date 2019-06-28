package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

type Exception struct {
	Date        string `json:"date"`
	Description string `json:"description"`
}

type Label struct {
	Number        string      `json:"number"`
	SenderName    string      `json:"sender_name"`
	SenderCo      string      `json:"sender_company"`
	SenderAddr    Address     `json:"sender_addr"`
	DeclaredValue int         `json:"declared_value"`
	Description   string      `json:"description"`
	ReceiverName  string      `json:"receiver_name"`
	ReceiverCo    string      `json:"receiver_company"`
	ReceiverAddr  Address     `json:"receiver_addr"`
	Weight        float64     `json:"weight_lb"`
	Dimensions    string      `json:"dimensions_in"`
	Created       string      `json:"created"`
	PickedUp      string      `json:"picked_up"`
	DeliverBy     string      `json:"deliver_by"`
	Delivered     string      `json:"delivered"`
	Exceptions    []Exception `json:"exceptions,omitempty"`
}

func MakeNumber() string {
	num := "1Z"
	for i := 0; i < 3; i++ {
		digit := rand.Intn(10)
		num += fmt.Sprintf("%v", digit)
	}
	readable := "01234567890ABCEFGHJKLMNPQRSTUVWXY"
	for i := 0; i < 3; i++ {
		digit := rand.Intn(len(readable))
		num += fmt.Sprintf("%c", readable[digit])
	}
	for i := 0; i < 10; i++ {
		digit := rand.Intn(10)
		num += fmt.Sprintf("%v", digit)
	}
	return fmt.Sprintf("%s %s %s %s %s %s", num[0:2], num[2:5], num[5:8], num[8:10], num[10:14], num[14:18])

}

func MakeLabel() Label {
	var l Label
	l.Number = MakeNumber()
	l.SenderName = MakeName()
	l.SenderCo = MakeCompany()
	l.SenderAddr = MakeAddress()
	l.ReceiverName = MakeName()
	l.ReceiverCo = MakeCompany()
	l.ReceiverAddr = MakeAddress()
	l.Description = MakeProduct()

	val := rand.Intn(12000)
	weight := float64(rand.Intn(100))
	dimx, dimy, dimz := 12.5, 9.5, 0.1
	switch rand.Int() % 5 {
	case 0:
		val = 100
		weight = 1
	case 1:
		val = 0
		weight = 0.1
		dimx = 15.0
	case 2:
		val /= 5
		weight = 3
		dimx, dimy, dimz = 16.0, 11.0, 3.0
	case 3:
		dimx, dimy, dimz = 16.5, 13.25, 10.75
	case 4:
		dimx, dimy, dimz = 13.0, 11.0, 2.0

	}
	val = (val / 100) * 100

	d2s := 24 * 60 * 60
	created_ofs := (rand.Intn(30) + 30) * d2s
	picked_ofs := rand.Intn(7) * d2s
	promised_ofs := rand.Intn(5) * d2s
	deliver_ofs := rand.Intn(3) * d2s

	created := int(time.Now().Unix()) - created_ofs
	picked := created + picked_ofs
	promised := picked + promised_ofs
	delivered := picked + deliver_ofs

	created_dt := time.Unix(int64(created), 0).Round(time.Hour * 24)
	picked_dt := time.Unix(int64(picked), 0).Round(time.Hour * 24)
	promised_dt := time.Unix(int64(promised), 0).Round(time.Hour * 24)
	delivered_dt := time.Unix(int64(delivered), 0).Round(time.Hour * 24)

	l.DeclaredValue = val
	l.Weight = weight
	l.Created = Realistic(created_dt, false).Format("01/02/2006 03:04 PM")
	l.PickedUp = Realistic(picked_dt, false).Format("01/02/2006 03:04 PM")
	l.DeliverBy = Realistic(promised_dt, true).Format("01/02/2006 03:04 PM")
	l.Delivered = Realistic(delivered_dt, false).Format("01/02/2006 03:04 PM")
	l.Dimensions = fmt.Sprintf("%s x %s x %s",
		strconv.FormatFloat(dimx, 'f', -1, 64),
		strconv.FormatFloat(dimy, 'f', -1, 64),
		strconv.FormatFloat(dimz, 'f', -1, 64))

	l.Exceptions = make([]Exception, 0)
	delay := delivered - promised
	for i := 0; i < delay; i += d2s / 2 {
		event := time.Unix(int64(promised+i), 0)
		var e Exception
		e.Date = Realistic(event, false).Format("01/02/2006 03:04 PM")
		e.Description = MakeReason()
		l.Exceptions = append(l.Exceptions, e)
	}

	return l
}

func Realistic(d time.Time, round bool) time.Time {
	min := rand.Intn(60)
	if round {
		min = (min / 30) * 30
	}
	return time.Date(d.Year(), d.Month(), d.Day(), rand.Intn(12)+8, min, 0, 0, time.UTC)
}
