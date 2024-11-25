export GO111MODULE=auto
export GOPATH=~/go
rm -rf servercertfiles
mkdir servercertfiles
cd servercertfiles
sh ../generateCertificates.sh
echo "Done generating certificates"
cd ..
echo "Started test now"
go test -coverprofile=coverage.out -v -race