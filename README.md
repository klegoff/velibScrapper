Velib data scrapper.

Request opendata API, process the data, store them in PostgreSQL, and display them in Dash app.

API source : 
https://opendata.paris.fr/explore/dataset/velib-disponibilite-en-temps-reel/table/?disjunctive.name&disjunctive.is_installed&disjunctive.is_renting&disjunctive.is_returning&disjunctive.nom_arrondissement_communes

Run with :
sudo docker compose up --build

Connect to the front app at your_server:8050

Retrieve data in using functions in connection_utils.py. (host="localhost" when accessed by docker host)
