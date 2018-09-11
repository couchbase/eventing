package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Txn struct {
	Type     string    `json:"type"`
	TxnID    string    `json:"txnid"`
	Amount   int       `json:"amount"`
	Product  string    `json:"product"`
	Card     string    `json:"card"`
	Merchant string    `json:"merchant"`
	City     City      `json:"city"`
	Date     time.Time `json:"date"`
}

type GeoLookup struct {
	State            string                `json:"state"`
	CityByCounty     map[string][]City     `json:"citybycounty"`
	MerchantByCounty map[string][]Merchant `json:"merchantbycounty"`
	MerchantByCity   map[string][]Merchant `json:"merchantbycity"`
}

func (r Txn) String() string {
	return fmt.Sprintf("Amount:%v,Merchant:%v,Product:%v,Date:%v,Card:%v",
		r.Amount, r.Merchant, r.Product, r.Date, r.Card)
}

var t_counter = 0

func MakeTxn(card Card, merchant Merchant, product string, amount int) Txn {
	ts := time.Date(2018, time.September, 14, 21, 24, 32, 0, time.Local)
	ts = ts.Add(time.Duration(-1*rand.Intn(3600)) * time.Second)
	ts = ts.Add(time.Duration(-1*rand.Intn(12)*24) * time.Hour)
	ts = ts.Add(time.Duration(-1*rand.Intn(365)*24) * time.Hour)
	t_counter++
	id := fmt.Sprintf("tx-%v-%v", ts.Unix(), t_counter)
	tx := Txn{"transaction", id, amount, product, card.CardNumber, merchant.Name, merchant.City, ts}
	return tx
}

func main() {
	cc_count := 5
	tx_count := 100

	l_amt := make([]Txn, 0)
	l_prod := make([]Txn, 0)
	l_loc := make([]Txn, 0)
	l_all := make([]Txn, 0)

	seed := 0
	if len(os.Args) > 1 {
		seed, _ = strconv.Atoi(os.Args[1])
	}
	rand.Seed(int64(seed))

	cardfile, err := os.Create("cards.json")
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(cardfile, "[")
	defer cardfile.Close()
	defer fmt.Fprintln(cardfile, "]")

	txnfile, err := os.Create("txns.json")
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(txnfile, "[")
	defer txnfile.Close()
	defer fmt.Fprintln(txnfile, "]")

	mrchfile, err := os.Create("merchants.json")
	if err != nil {
		panic(err)
	}
	fmt.Fprintln(mrchfile, "[")
	defer mrchfile.Close()
	defer fmt.Fprintln(mrchfile, "]")

	geolook := make(map[string]*GeoLookup)
	for _, city := range Cities {
		geolook[city.State] = &GeoLookup{
			State:            city.State,
			CityByCounty:     make(map[string][]City),
			MerchantByCounty: make(map[string][]Merchant),
			MerchantByCity:   make(map[string][]Merchant),
		}
		geolook[city.State].CityByCounty[city.County] = append(geolook[city.State].CityByCounty[city.County], city)
	}

	favlook := make(map[string][]string)

	merchants := make([]Merchant, 5000)
	for i := 0; i < len(merchants); i++ {
		merchant := MakeMerchant()
		merchants[i] = merchant
		lk := geolook[merchant.City.State]
		lk.MerchantByCounty[merchant.City.County] = append(lk.MerchantByCounty[merchant.City.County], merchant)
		lk.MerchantByCity[merchant.City.Name] = append(lk.MerchantByCity[merchant.City.Name], merchant)
		out(mrchfile, merchant)
	}

	for j := 0; j < cc_count; j++ {
		card := MakeCard()
		out(cardfile, card)
		for i := 0; i < tx_count; i++ {
			locals := geolook[card.City.State].MerchantByCity[card.City.Name]
			if len(locals) > 0 {
				amount := int(float64(card.Threshold/10) * rand.ExpFloat64())
				merchant := locals[rand.Intn(len(locals))]
				favs := favlook[card.CardNumber]
				var product string
				if len(favs) > 4 {
					product = favs[rand.Intn(len(favs))]
				} else {
					product = Products[rand.Intn(len(Products))]
					favs = append(favs, product)
					favlook[card.CardNumber] = favs
				}

				x_amt := rand.Intn(3) == 0
				x_loc := rand.Intn(3) == 0
				x_prod := rand.Intn(3) == 0

				if x_amt {
					amount = rand.Intn(5000) + 8000
				}
				if x_loc {
					merchant = merchants[rand.Intn(len(merchants))]
				}
				if x_prod {
					product = Products[rand.Intn(len(Products))]
				}

				tx := MakeTxn(card, merchant, product, amount)
				out(txnfile, tx)

				if x_amt {
					l_amt = append(l_amt, tx)
				}
				if x_loc {
					l_loc = append(l_loc, tx)
				}
				if x_prod {
					l_prod = append(l_prod, tx)
				}
				if x_amt && x_loc && x_prod {
					l_all = append(l_all, tx)
				}

			}
		}
	}
}

var needComma = make(map[io.Writer]bool)

func out(w io.Writer, obj interface{}) {
	comma := needComma[w]
	needComma[w] = true
	if comma {
		fmt.Fprintln(w, ",")
	}
	js, _ := json.MarshalIndent(obj, "", " ")
	fmt.Fprintln(w, string(js))
}
