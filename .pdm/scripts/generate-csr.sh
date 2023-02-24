#!/bin/bash

openssl genrsa -out env/kipptaf_auth.key 2048
openssl req -new \
	-key env/kipptaf_auth.key \
	-out env/kipptaf_auth.csr \
	-subj "/C=US/ST=NJ/L=Newark/O=KIPP TAF/OU=Data/CN=KIPP TAF ADP API Access"
