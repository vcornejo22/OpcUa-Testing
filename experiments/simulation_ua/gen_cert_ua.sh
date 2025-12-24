#!/bin/bash
#
# Request the name of the CA 
read -p "Name Server: " NAME_SERVER

# Request broker IP
read -p "File conf (ssl): " CONFIG_FILE

# Request days of validity
while true; do 
  read -p "Days: " days 
  if [[ "$days" =~ ^[0-9]+$ ]]; then
    break 
  else
    echo "Incorrect"
  fi 
done 

DAYS=$((days))

# Create the directory to store the certificates
mkdir -p certs

# Generate a private key for the CA 
echo "[+] Private key for the CA"
openssl genrsa -des3 -out certs/ca-"$NAME_SERVER".key 2048

# Generates a self-signed certificate for CA 
echo "[+] Self-signed certificate for CA"
openssl req -x509 -new -nodes -key certs/ca-"$NAME_SERVER".key -sha256 -days "$DAYS" -out -out certs/ca-"$NAME_SERVER".pem

# Generate a certificate for OPC-UA
echo "[+] Certificate to OPC UA"
openssl genrsa -out certs/key-"$NAME_SERVER".pem 2048

# Generates a certificate Request (CSR) for the OPC-UA
echo "[+] Certificate (CSR) to OPC-UA"
openssl req -x509 -days "$DAYS" -new -out certs/cert-"$NAME_SERVER".pem -key certs/key-"$NAME_SERVER".pem -config $CONFIG_FILE.conf

# Sign the server's CSR with the CA to obtain the server's certificate 
echo "[+] Sign the server's CSR with CA"
openssl x509 -req -in certs/"cert-$NAME_SERVER".pem -CA certs/"ca-$NAME_SERVER".pem -CAkey certs/"ca-$NAME_SERVER".key -CAcreateserial -out certs/"cert-signed-$NAME_SERVER".pem -days $DAYS -sha256

echo "[+]"
openssl x509 -outform der -in certs/cert-$NAME_SERVER.pem -out certs/cert-$NAME_SERVER.der



