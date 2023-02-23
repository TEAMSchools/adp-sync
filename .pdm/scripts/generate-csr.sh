#!/bin/bash

openssl genrsa -out env/kipptaf_auth.key 2048
openssl req -new -key env/kipptaf_auth.key -out env/kipptaf_auth.csr
