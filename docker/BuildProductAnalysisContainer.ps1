sudo docker stop ProductAnalysis
sudo docker rm ProductAnalysis
docker build -t productanalysis:latest . -f docker/Dockerfile

docker run -d --name ProductAnalysis -p 5004:5000 productanalysis:latest