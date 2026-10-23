/*  #!/bin/bash
echo $"
------------------------------------------------
Hello! Run this prior to building the image

WARNING - THIS SCRIPT WILL PUSH THE CREATED IMAGE TOO: ghcr.io/laser-vision-system/server:arm64-release-manual
------------------------------------------------
export CR_PAT=YOUR_TOKEN
echo $CR_PAT | sudo docker login ghcr.io -u USERNAME --password-stdin
------------------------------------------------
" 
sudo docker buildx build --load -t ghcr.io/laser-vision-system/server:arm64-release-manual --platform linux/arm64 --build-arg BUILD_TARGET=release --build-arg BINARY_DIR=release .
sudo docker push ghcr.io/laser-vision-system/server:arm64-release-manual
*/
