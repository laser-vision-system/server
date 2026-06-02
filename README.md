# server
Central Control Tier ports and adaptors server.
# docker 

to build from github 
sudo docker build -t oatmeal0505/laser-api:tagname .
sudo docker compose up

to build from remote image source

standard - WILL KEEP RUNNING AFTER CLOSING TERMINAL
sudo docker run --name api -p 8080:8080 oatmeal0505/laser-api:tagname

temp - WILL BE REMOVED AFTER CLOSING TERMINAL
sudo docker run --name api -p 8080:8080 -rm oatmeal0505/laser-api:tagname

if the container is still arround 
sudo docker container stop api 
sudo docker container rm api

if you want to ditch the image 
sudo docker image rm oatmeal0505/laser-api:tagname
