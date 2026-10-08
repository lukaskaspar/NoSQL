1. Téma: MongoDB sharded cluster nad Yelp Open Dataset
2. Autor: Lukáš Kašpar
3. Spuštění:
   cd Funkcni_reseni
   cp .env.example .env
   chmod +x scripts/*.sh
   docker compose up -d --build
4. Připojení:
   docker exec -it mongos mongosh -u clusterAdmin -p 'ChangeMeRoot123!' --authenticationDatabase admin
5. Dokumentace je ve složce Dokumentace.
6. Dotazy jsou ve složce Dotazy.