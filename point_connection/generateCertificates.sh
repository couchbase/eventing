mkdir -p {public,private,requests}
openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 3650 -sha256 -key ca.key -out ca.pem -subj "/CN=Test CA"
openssl x509 -in ./ca.pem -noout -pubkey
openssl x509 -text -noout -in ./ca.pem
openssl genrsa -out private/test.default.svc.key 2048
openssl req -new -key private/test.default.svc.key -out requests/test.default.svc.csr -subj "/CN=Test"
openssl req -text -noout -verify -in ./requests/test.default.svc.csr
echo "basicConstraints=CA:FALSE\n
subjectKeyIdentifier = hash\n
authorityKeyIdentifier = keyid,issuer:always\n
extendedKeyUsage=serverAuth\n
keyUsage = digitalSignature,keyEncipherment" >> server.ext
cp ./server.ext ./server.ext.tmp
echo "subjectAltName = IP:127.0.0.1" >> ./server.ext.tmp
openssl x509 -CA ca.pem -CAkey ca.key -CAcreateserial -days 365 -req \
-in requests/test.default.svc.csr \
-out public/test.default.svc.pem \
-extfile server.ext.tmp
cd ./public
mv test.default.svc.pem ../chain.pem
cd ../private
mv test.default.svc.key ../pkey.key
cd ..

mkdir clientcertfiles
cd clientcertfiles
echo "basicConstraints=CA:FALSE\n
subjectKeyIdentifier = hash\n
authorityKeyIdentifier = keyid,issuer:always\n
extendedKeyUsage=clientAuth\n
keyUsage = digitalSignature" >> client.ext
openssl genrsa -out ./client.key 2048
openssl req -new -key ./client.key -out ./client.csr -subj "/CN=clientuser"
cp ./client.ext ./client.ext.tmp
echo "subjectAltName = email:testUser" >> ./client.ext.tmp
openssl x509 -CA ../ca.pem -CAkey ../ca.key \
-CAcreateserial -days 365 -req -in ./client.csr \
-out ./client.pem -extfile ./client.ext.tmp

cd ..
mkdir diffCa
cd diffCa
openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 3650 -sha256 -key ca.key -out ca.pem -subj "/CN=Diff CA"
