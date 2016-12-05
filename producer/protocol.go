package eventing

type Header struct {
	Command    string `json:"command"`
	Subcommand string `json:"subcommand"`
	Metadata   string `json:"metadata"`
}

type Payload struct {
	Message string `json:"message"`
}

type Message struct {
	Header  Header
	Payload Payload
	ResChan chan *Response
}

type Response struct {
	response string
	err      error
}
