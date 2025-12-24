#!/bin/bash
#
# Request the name of the CA 
read -p "Name Server: " NAME_SERVER

# Request broker IP
read -p "Hostname Server: " BROKER_HOSTNAME
read -p "IP Server: " BROKER_IP

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
mkdir -p conf/certs

# Generate a private key for the CA 
echo "[+] Private key for the CA"
openssl genrsa -des3 -out conf/certs/ca-"$NAME_SERVER".key 2048

# Generates a self-signed certificate for CA 
echo "[+] Self-signed certificate for CA"
openssl req -x509 -new -nodes -key conf/certs/ca-"$NAME_SERVER".key -sha256 -days 3650 -out conf/certs/ca-"$NAME_SERVER".crt -subj "/C=PE/ST=Arequipa/L=Arequipa/O=Sacha/OU=CA/CN=${BROKER_HOSTNAME}" -addext "subjectAltName=IP:${BROKER_IP}"
#openssl req -new -x509 -days "$DAYS" -extensions v3_ca -key conf/certs/ca-"$NAME_SERVER".key -out conf/certs/ca-"$NAME_SERVER".crt -subj "/C=PE/ST=Arequipa/L=Arequipa/O=Sacha/OU=CA/CN=${BROKER_HOSTNAME}" #-addext "subjectAltName=IP:${BROKER_IP}"

# Generate private key for the BROKER_IP
echo "[+] Private key for he broker"
openssl genrsa -out conf/certs/server-"$NAME_SERVER".key 2048

# Generates a certificate request (CSR) for the broker 
echo "[+] Certificate (CSR) for the broker"
openssl req -new -out conf/certs/server-"$NAME_SERVER".csr -key conf/certs/server-"$NAME_SERVER".key -subj "/C=PE/ST=Arequipa/L=Hunter/O=Sacha/OU=Server/CN=${BROKER_HOSTNAME}" -addext "subjectAltName=IP:${BROKER_IP}"

# Sign the server's CSR with the CA to obtain the server's certificate 
echo "[+] Sign the server's CSR with CA"
openssl x509 -req -in conf/certs/server-"$NAME_SERVER".csr -CA conf/certs/ca-"$NAME_SERVER".crt -CAkey conf/certs/ca-"$NAME_SERVER".key -CAcreateserial -out conf/certs/server-"$NAME_SERVER".crt -days $DAYS

