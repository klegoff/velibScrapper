Velib data scrapper.
Request opendata API, process the data, and store in PostgreSQL.

API source : 
https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/table/?disjunctive.name&disjunctive.is_installed&disjunctive.is_renting&disjunctive.is_returning&disjunctive.nom_arrondissement_communes

Run with :
sudo docker compose up --build

Retrieve data in using function retrieveData() in app.py. (host="localhost" when accessed by docker host)
