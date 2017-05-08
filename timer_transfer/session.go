package timer

import (
	"os"
)

// Add increments session counter and stores os.File handle
// for requested file
func (s *Session) Add(file *os.File) SessionID {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.sCounter++
	s.sessionFileMap[s.sCounter] = file

	return s.sCounter
}

// Get returns os.File handle associated with specific session
// against RPC server
func (s *Session) Get(id SessionID) *os.File {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessionFileMap[id]
}

// Delete cleans up file handle references for SessionID
// where file transfer finished successfully otherwise
// session id could resume from last offset
func (s *Session) Delete(id SessionID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if file, ok := s.sessionFileMap[id]; ok {
		file.Close()
		delete(s.sessionFileMap, id)
	}
}

// Len returns length of all sessions that RPC server is currently
// keeping track of
func (s *Session) Len() int {
	return len(s.sessionFileMap)
}
