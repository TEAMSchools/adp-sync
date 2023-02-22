#!/bin/bash

curl -ks \
	-cert "${CERT_LOC}" \
	-key "${KEY_LOC}" \
	-o tokenResponse \
	-d "client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&grant_type=client_credentials" \
	"${TOKEN_ENDPOINT}"

ACCESS_TOKEN=$(grep '"access_token":"' tokenResponse | sed 's/.*access_token":"\(.*\)",*/\1/')

echo "${ACCESS_TOKEN}"
