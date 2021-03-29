import requests
import pymongo
import time
import logging

logging.basicConfig(filename='creatingdb.log', level=logging.INFO)


class CreatingDb:

    def __init__(self):
        self.base_url = 'https://swapi.dev/api/'
        self.ship_data = []
        self.pilot_urls = []
        self.pilot_data = []

    def all_starship_data(self):
        # Saves all starship data for each page to list, hence list of dict
        url = self.base_url + 'starships/?page={}'
        for i in range(1, 5):
            data = requests.get(url.format(i))
            if data.status_code == 200:
                data = data.json()['results']
                logging.info("Successfulget request for {}".format(url.format(i)))
                for ship in data:
                    self.ship_data.append(ship)

            else:
                logging.warning("Unsuccessful get request for {}".format(url.format(i)))


    def char_name(self, url):
        data = requests.get(url)
        if data.status_code == 200:
            name = data.json()['name']
            logging.info("Successful get request for character {} using the link {}".format(name, url))
        else:
            logging.warning("Unsuccessful get request attempt on {}".format(url))
            name = []
        return name

    def matching_ship_to_pilots(self):
        client = pymongo.MongoClient()
        db = client.starwars
        for ship in self.ship_data:
            new_pilot_obj = []
            for pilot in ship['pilots']:
                name = self.char_name(pilot)
                if name is not []:
                    new_pilot_obj.append(next(db.characters.find({"name": name}, {"_id": 1}))['_id'])

            ship['pilots'] = new_pilot_obj

    def inserting_ship_data(self):
        client = pymongo.MongoClient()
        db = client.starwars
        db.starships_mine.insert_many(self.ship_data)

    def main(self):
        self.all_starship_data()
        self.matching_ship_to_pilots()
        self.inserting_ship_data()


t0 = time.time()
example = CreatingDb()
example.main()
t1 = time.time()
total = t1-t0
print(total)

