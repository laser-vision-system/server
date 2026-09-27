# server
Central Control Tier ports and adaptors server.

## Docker image

In order to run our docker image you will need to pull the image from github. to start off with make sure you are logged in to login to the github docker registery by doing the following:
```
# Create a new personal access token (classic) with the appropriate scopes for the tasks you want to accomplish. If your organization requires SSO, you must enable SSO for your new token.
# Select the read:packages scope to download container images and read their metadata. 
export CR_PAT=YOUR_TOKEN 
echo $CR_PAT | docker login ghcr.io -u USERNAME --password-stdin 
```
After you are logged in run the following command to pull the image:
```
# For raspbery pi's or MAC machines
docker pull ghcr.io/cos40005-p12/server-arm64:BRANCH
# For other hardware 
docker pull ghcr.io/cos40005-p12/server-amd64:BRANCH
```
If you want to make and run a container for testing run the following:
```
sudo docker run -p 8080:8080 -it --rm --name server ghcr.io/cos40005-p12/server-CPUARCHITECTURE:BRANCH
```
For deployment on the server instead run the following:
```
sudo docker run -p 8080:8080 --name server ghcr.io/cos40005-p12/server-CPUARCHITECTURE:BRANCH
```
