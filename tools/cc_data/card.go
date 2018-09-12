package main

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
)

var Cards []Card = MakeCards()

func MakeCards() []Card {
	cards := make([]Card, 5000)
	for i := 0; i < len(cards); i++ {
		cards[i] = MakeCard()
	}
	return cards
}

type Card struct {
	Type       string `json:"type"`
	CardNumber string `json:"cardnumber"`
	FirstName  string `json:"firstname"`
	LastName   string `json:"lastname"`
	Street     string `json:"street"`
	City       City   `json:"city"`
	Issued     string `json:"issued"`
	Expiry     string `json:"expiry"`
	CCV        int    `json:"ccv"`
	Issuer     string `json:"issuer"`
	Threshold  int    `json:"threshold"`
}

func MakeCard() Card {
	card := Card{Type: "card"}

	cksum := 0
	for i := 0; i < 15; i++ {
		digit := rand.Intn(10)
		if i == 0 {
			digit = rand.Intn(2) + 4
		}
		ck := digit
		if i%2 == 0 {
			ck += digit
		}
		if ck > 9 {
			ck -= 9
		}
		cksum += digit
		card.CardNumber += fmt.Sprintf("%v", digit)
		if i%4 == 3 {
			card.CardNumber += "-"
		}
	}
	card.CardNumber += fmt.Sprintf("%v", cksum%10)

	card.FirstName = FirstNames[rand.Intn(len(FirstNames))]
	card.LastName = LastNames[rand.Intn(len(LastNames))]

	issue := 14 + rand.Intn(4)
	card.Issued = fmt.Sprintf("%v/%v", rand.Intn(12)+1, issue)
	card.Expiry = fmt.Sprintf("%v/%v", rand.Intn(12)+1, issue+4)

	card.CCV = rand.Intn(900) + 100
	card.Threshold = rand.Intn(27000) + 3000
	card.Issuer = Banks[rand.Intn(len(Banks))]
	card.Street = Streets[rand.Intn(len(Streets))]
	card.Street = strings.Title(strings.ToLower(card.Street))
	card.Street = strconv.Itoa(rand.Intn(6000)+1) + " " + card.Street
	city := Cities[rand.Intn(len(Cities))]
	card.City = city
	return card
}
