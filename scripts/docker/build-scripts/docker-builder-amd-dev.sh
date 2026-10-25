/*  #!/bin/bash
echo $"
------------------------------------------------
Hello! Run this prior to building the image

WARNING - THIS SCRIPT WILL PUSH THE CREATED IMAGE TOO: ghcr.io/laser-vision-system/server:amd64-dev-manual
------------------------------------------------
export CR_PAT=YOUR_TOKEN
echo $CR_PAT | sudo docker login ghcr.io -u USERNAME --password-stdin
------------------------------------------------
" 
sudo docker buildx build --load -t ghcr.io/laser-vision-system/server:amd64-dev-manual --platform linux/amd64 --build-arg BUILD_TARGET=dev --build-arg BINARY_DIR=debug .
sudo docker push ghcr.io/laser-vision-system/server:amd64-dev-manual
*/
